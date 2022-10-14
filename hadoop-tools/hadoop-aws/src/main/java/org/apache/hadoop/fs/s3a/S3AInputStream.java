/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.impl.CombinedFileRange;
import org.apache.hadoop.fs.VectoredReadUtils;
import org.apache.hadoop.fs.s3a.impl.ChangeTracker;
import org.apache.hadoop.fs.s3a.impl.InternalConstants;
import org.apache.hadoop.fs.s3a.impl.SDKStreamDrainer;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsAggregator;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.functional.CallableRaisingIOE;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.fs.VectoredReadUtils.isOrderedDisjoint;
import static org.apache.hadoop.fs.VectoredReadUtils.mergeSortedRanges;
import static org.apache.hadoop.fs.VectoredReadUtils.validateNonOverlappingAndReturnSortedRanges;
import static org.apache.hadoop.fs.s3a.Invoker.onceTrackingDuration;
import static org.apache.hadoop.util.StringUtils.toLowerCase;
import static org.apache.hadoop.util.functional.FutureIO.awaitFuture;

/**
 * The input stream for an S3A object.
 *
 * As this stream seeks withing an object, it may close then re-open the stream.
 * When this happens, any updated stream data may be retrieved, and, given
 * the consistency model of Amazon S3, outdated data may in fact be picked up.
 *
 * As a result, the outcome of reading from a stream of an object which is
 * actively manipulated during the read process is "undefined".
 *
 * The class is marked as private as code should not be creating instances
 * themselves. Any extra feature (e.g instrumentation) should be considered
 * unstable.
 *
 * Because it prints some of the state of the instrumentation,
 * the output of {@link #toString()} must also be considered unstable.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class S3AInputStream extends FSInputStream implements  CanSetReadahead,
        CanUnbuffer, StreamCapabilities, IOStatisticsSource {

  public static final String E_NEGATIVE_READAHEAD_VALUE
      = "Negative readahead value";

  public static final String OPERATION_OPEN = "open";
  public static final String OPERATION_REOPEN = "re-open";

  /**
   * This is the maximum temporary buffer size we use while
   * populating the data in direct byte buffers during a vectored IO
   * operation. This is to ensure that when a big range of data is
   * requested in direct byte buffer doesn't leads to OOM errors.
   */
  private static final int TMP_BUFFER_MAX_SIZE = 64 * 1024;

  /**
   * Atomic boolean variable to stop all ongoing vectored read operation
   * for this input stream. This will be set to true when the stream is
   * closed or unbuffer is called.
   */
  private final AtomicBoolean stopVectoredIOOperations = new AtomicBoolean(false);

  /**
   * This is the public position; the one set in {@link #seek(long)}
   * and returned in {@link #getPos()}.
   */
  private long pos;
  /**
   * Closed bit. Volatile so reads are non-blocking.
   * Updates must be in a synchronized block to guarantee an atomic check and
   * set
   */
  private volatile boolean closed;
  /**
   * wrappedStream is associated with an object (instance of S3Object). When
   * the object is garbage collected, the associated wrappedStream will be
   * closed. Keep a reference to this object to prevent the wrapperStream
   * still in use from being closed unexpectedly due to garbage collection.
   * See HADOOP-17338 for details.
   */
  private S3Object object;
  private S3ObjectInputStream wrappedStream;
  private final S3AReadOpContext context;
  private final InputStreamCallbacks client;

  /**
   * Thread pool used for vectored IO operation.
   */
  private final ExecutorService boundedThreadPool;
  private final String bucket;
  private final String key;
  private final String pathStr;
  private final long contentLength;
  private final String uri;
  private static final Logger LOG =
      LoggerFactory.getLogger(S3AInputStream.class);
  private final S3AInputStreamStatistics streamStatistics;
  private S3AInputPolicy inputPolicy;
  private long readahead = Constants.DEFAULT_READAHEAD_RANGE;

  /** Vectored IO context. */
  private final VectoredIOContext vectoredIOContext;

  /**
   * This is the actual position within the object, used by
   * lazy seek to decide whether to seek on the next read or not.
   */
  private long nextReadPos;

  /**
   * The end of the content range of the last request.
   * This is an absolute value of the range, not a length field.
   */
  private long contentRangeFinish;

  /**
   * The start of the content range of the last request.
   */
  private long contentRangeStart;

  /** change tracker. */
  private final ChangeTracker changeTracker;

  /**
   * IOStatistics report.
   */
  private final IOStatistics ioStatistics;

  /**
   * Threshold for stream reads to switch to
   * asynchronous draining.
   */
  private long asyncDrainThreshold;

  /** Aggregator used to aggregate per thread IOStatistics. */
  private final IOStatisticsAggregator threadIOStatistics;

  /**
   * Create the stream.
   * This does not attempt to open it; that is only done on the first
   * actual read() operation.
   * @param ctx operation context
   * @param s3Attributes object attributes
   * @param client S3 client to use
   * @param streamStatistics stream io stats.
   * @param boundedThreadPool thread pool to use.
   */
  public S3AInputStream(S3AReadOpContext ctx,
                        S3ObjectAttributes s3Attributes,
                        InputStreamCallbacks client,
                        S3AInputStreamStatistics streamStatistics,
                        ExecutorService boundedThreadPool) {
    Preconditions.checkArgument(isNotEmpty(s3Attributes.getBucket()),
        "No Bucket");
    Preconditions.checkArgument(isNotEmpty(s3Attributes.getKey()), "No Key");
    long l = s3Attributes.getLen();
    Preconditions.checkArgument(l >= 0, "Negative content length");
    this.context = ctx;
    this.bucket = s3Attributes.getBucket();
    this.key = s3Attributes.getKey();
    this.pathStr = s3Attributes.getPath().toString();
    this.contentLength = l;
    this.client = client;
    this.uri = "s3a://" + this.bucket + "/" + this.key;
    this.streamStatistics = streamStatistics;
    this.ioStatistics = streamStatistics.getIOStatistics();
    this.changeTracker = new ChangeTracker(uri,
        ctx.getChangeDetectionPolicy(),
        streamStatistics.getChangeTrackerStatistics(),
        s3Attributes);
    setInputPolicy(ctx.getInputPolicy());
    setReadahead(ctx.getReadahead());
    this.asyncDrainThreshold = ctx.getAsyncDrainThreshold();
    this.boundedThreadPool = boundedThreadPool;
    this.vectoredIOContext = context.getVectoredIOContext();
    this.threadIOStatistics = requireNonNull(ctx.getIOStatisticsAggregator());
  }

  /**
   * Set/update the input policy of the stream.
   * This updates the stream statistics.
   * @param inputPolicy new input policy.
   */
  private void setInputPolicy(S3AInputPolicy inputPolicy) {
    this.inputPolicy = inputPolicy;
    streamStatistics.inputPolicySet(inputPolicy.ordinal());
  }

  /**
   * Get the current input policy.
   * @return input policy.
   */
  @VisibleForTesting
  public S3AInputPolicy getInputPolicy() {
    return inputPolicy;
  }

  /**
   * Opens up the stream at specified target position and for given length.
   *
   * @param reason reason for reopen
   * @param targetPos target position
   * @param length length requested
   * @throws IOException on any failure to open the object
   */
  @Retries.OnceTranslated
  private synchronized void reopen(String reason, long targetPos, long length,
          boolean forceAbort) throws IOException {

    if (isObjectStreamOpen()) {
      closeStream("reopen(" + reason + ")", forceAbort, false);
    }

    contentRangeFinish = calculateRequestLimit(inputPolicy, targetPos,
        length, contentLength, readahead);
    LOG.debug("reopen({}) for {} range[{}-{}], length={}," +
        " streamPosition={}, nextReadPosition={}, policy={}",
        uri, reason, targetPos, contentRangeFinish, length,  pos, nextReadPos,
        inputPolicy);

    long opencount = streamStatistics.streamOpened();
    GetObjectRequest request = client.newGetRequest(key)
        .withRange(targetPos, contentRangeFinish - 1);
    String operation = opencount == 0 ? OPERATION_OPEN : OPERATION_REOPEN;
    String text = String.format("%s %s at %d",
        operation, uri, targetPos);
    changeTracker.maybeApplyConstraint(request);

    object = onceTrackingDuration(text, uri,
        streamStatistics.initiateGetRequest(), () ->
            client.getObject(request));


    changeTracker.processResponse(object, operation,
        targetPos);
    wrappedStream = object.getObjectContent();
    contentRangeStart = targetPos;
    if (wrappedStream == null) {
      throw new PathIOException(uri,
          "Null IO stream from " + operation + " of (" + reason +  ") ");
    }

    this.pos = targetPos;
  }

  @Override
  public synchronized long getPos() throws IOException {
    return (nextReadPos < 0) ? 0 : nextReadPos;
  }

  @Override
  public synchronized void seek(long targetPos) throws IOException {
    checkNotClosed();

    // Do not allow negative seek
    if (targetPos < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK
          + " " + targetPos);
    }

    if (this.contentLength <= 0) {
      return;
    }

    // Lazy seek
    nextReadPos = targetPos;
  }

  /**
   * Seek without raising any exception. This is for use in
   * {@code finally} clauses
   * @param positiveTargetPos a target position which must be positive.
   */
  private void seekQuietly(long positiveTargetPos) {
    try {
      seek(positiveTargetPos);
    } catch (IOException ioe) {
      LOG.debug("Ignoring IOE on seek of {} to {}",
          uri, positiveTargetPos, ioe);
    }
  }

  /**
   * Adjust the stream to a specific position.
   *
   * @param targetPos target seek position
   * @param length length of content that needs to be read from targetPos
   * @throws IOException
   */
  @Retries.OnceTranslated
  private void seekInStream(long targetPos, long length) throws IOException {
    checkNotClosed();
    if (wrappedStream == null) {
      return;
    }
    // compute how much more to skip
    long diff = targetPos - pos;
    if (diff > 0) {
      // forward seek -this is where data can be skipped

      int available = wrappedStream.available();
      // always seek at least as far as what is available
      long forwardSeekRange = Math.max(readahead, available);
      // work out how much is actually left in the stream
      // then choose whichever comes first: the range or the EOF
      long remainingInCurrentRequest = remainingInCurrentRequest();

      long forwardSeekLimit = Math.min(remainingInCurrentRequest,
          forwardSeekRange);
      boolean skipForward = remainingInCurrentRequest > 0
          && diff < forwardSeekLimit;
      if (skipForward) {
        // the forward seek range is within the limits
        LOG.debug("Forward seek on {}, of {} bytes", uri, diff);
        long skipped = wrappedStream.skip(diff);
        if (skipped > 0) {
          pos += skipped;
        }
        streamStatistics.seekForwards(diff, skipped);

        if (pos == targetPos) {
          // all is well
          LOG.debug("Now at {}: bytes remaining in current request: {}",
              pos, remainingInCurrentRequest());
          return;
        } else {
          // log a warning; continue to attempt to re-open
          LOG.warn("Failed to seek on {} to {}. Current position {}",
              uri, targetPos,  pos);
        }
      } else {
        // not attempting to read any bytes from the stream
        streamStatistics.seekForwards(diff, 0);
      }
    } else if (diff < 0) {
      // backwards seek
      streamStatistics.seekBackwards(diff);
      // if the stream is in "Normal" mode, switch to random IO at this
      // point, as it is indicative of columnar format IO
      if (inputPolicy.isAdaptive()) {
        LOG.info("Switching to Random IO seek policy");
        setInputPolicy(S3AInputPolicy.Random);
      }
    } else {
      // targetPos == pos
      if (remainingInCurrentRequest() > 0) {
        // if there is data left in the stream, keep going
        return;
      }

    }

    // if the code reaches here, the stream needs to be reopened.
    // close the stream; if read the object will be opened at the new pos
    closeStream("seekInStream()", false, false);
    pos = targetPos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  /**
   * Perform lazy seek and adjust stream to correct position for reading.
   *
   * @param targetPos position from where data should be read
   * @param len length of the content that needs to be read
   */
  @Retries.RetryTranslated
  private void lazySeek(long targetPos, long len) throws IOException {

    Invoker invoker = context.getReadInvoker();
    invoker.maybeRetry(streamStatistics.getOpenOperations() == 0,
        "lazySeek", pathStr, true,
        () -> {
          //For lazy seek
          seekInStream(targetPos, len);

          //re-open at specific location if needed
          if (wrappedStream == null) {
            reopen("read from new offset", targetPos, len, false);
          }
        });
  }

  /**
   * Increment the bytes read counter if there is a stats instance
   * and the number of bytes read is more than zero.
   * @param bytesRead number of bytes read
   */
  private void incrementBytesRead(long bytesRead) {
    streamStatistics.bytesRead(bytesRead);
    if (context.stats != null && bytesRead > 0) {
      context.stats.incrementBytesRead(bytesRead);
    }
  }

  @Override
  @Retries.RetryTranslated
  public synchronized int read() throws IOException {
    checkNotClosed();
    if (this.contentLength == 0 || (nextReadPos >= contentLength)) {
      return -1;
    }

    try {
      lazySeek(nextReadPos, 1);
    } catch (EOFException e) {
      return -1;
    }

    Invoker invoker = context.getReadInvoker();
    int byteRead = invoker.retry("read", pathStr, true,
        () -> {
          int b;
          // When exception happens before re-setting wrappedStream in "reopen" called
          // by onReadFailure, then wrappedStream will be null. But the **retry** may
          // re-execute this block and cause NPE if we don't check wrappedStream
          if (wrappedStream == null) {
            reopen("failure recovery", getPos(), 1, false);
          }
          try {
            b = wrappedStream.read();
          } catch (EOFException e) {
            return -1;
          } catch (SocketTimeoutException e) {
            onReadFailure(e, true);
            throw e;
          } catch (IOException e) {
            onReadFailure(e, false);
            throw e;
          }
          return b;
        });

    if (byteRead >= 0) {
      pos++;
      nextReadPos++;
    }

    if (byteRead >= 0) {
      incrementBytesRead(1);
    }
    return byteRead;
  }

  /**
   * Close the stream on read failure.
   * The filesystem's readException count will be incremented.
   * @param ioe exception caught.
   */
  @Retries.OnceTranslated
  private void onReadFailure(IOException ioe, boolean forceAbort) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Got exception while trying to read from stream {}, " +
          "client: {} object: {}, trying to recover: ",
          uri, client, object, ioe);
    } else {
      LOG.info("Got exception while trying to read from stream {}, " +
          "client: {} object: {}, trying to recover: " + ioe,
          uri, client, object);
    }
    streamStatistics.readException();
    closeStream("failure recovery", forceAbort, false);
  }

  /**
   * {@inheritDoc}
   *
   * This updates the statistics on read operations started and whether
   * or not the read operation "completed", that is: returned the exact
   * number of bytes requested.
   * @throws IOException if there are other problems
   */
  @Override
  @Retries.RetryTranslated
  public synchronized int read(byte[] buf, int off, int len)
      throws IOException {
    checkNotClosed();

    validatePositionedReadArgs(nextReadPos, buf, off, len);
    if (len == 0) {
      return 0;
    }

    if (this.contentLength == 0 || (nextReadPos >= contentLength)) {
      return -1;
    }

    try {
      lazySeek(nextReadPos, len);
    } catch (EOFException e) {
      // the end of the file has moved
      return -1;
    }

    Invoker invoker = context.getReadInvoker();

    streamStatistics.readOperationStarted(nextReadPos, len);
    int bytesRead = invoker.retry("read", pathStr, true,
        () -> {
          int bytes;
          // When exception happens before re-setting wrappedStream in "reopen" called
          // by onReadFailure, then wrappedStream will be null. But the **retry** may
          // re-execute this block and cause NPE if we don't check wrappedStream
          if (wrappedStream == null) {
            reopen("failure recovery", getPos(), 1, false);
          }
          try {
            bytes = wrappedStream.read(buf, off, len);
          } catch (EOFException e) {
            // the base implementation swallows EOFs.
            return -1;
          } catch (SocketTimeoutException e) {
            onReadFailure(e, true);
            throw e;
          } catch (IOException e) {
            onReadFailure(e, false);
            throw e;
          }
          return bytes;
        });

    if (bytesRead > 0) {
      pos += bytesRead;
      nextReadPos += bytesRead;
    }
    incrementBytesRead(bytesRead);
    streamStatistics.readOperationCompleted(len, bytesRead);
    return bytesRead;
  }

  /**
   * Verify that the input stream is open. Non blocking; this gives
   * the last state of the volatile {@link #closed} field.
   * @throws IOException if the connection is closed.
   */
  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException(uri + ": " + FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }

  /**
   * Close the stream.
   * This triggers publishing of the stream statistics back to the filesystem
   * statistics.
   * This operation is synchronized, so that only one thread can attempt to
   * close the connection; all later/blocked calls are no-ops.
   * @throws IOException on any problem
   */
  @Override
  public synchronized void close() throws IOException {
    if (!closed) {
      closed = true;
      try {
        stopVectoredIOOperations.set(true);
        // close or abort the stream; blocking
        closeStream("close() operation", false, true);
        // end the client+audit span.
        client.close();
        // this is actually a no-op
        super.close();
      } finally {
        // merge the statistics back into the FS statistics.
        streamStatistics.close();
        // Collect ThreadLevel IOStats
        mergeThreadIOStatistics(streamStatistics.getIOStatistics());
      }
    }
  }

  /**
   * Merging the current thread's IOStatistics with the current IOStatistics
   * context.
   *
   * @param streamIOStats Stream statistics to be merged into thread
   *                      statistics aggregator.
   */
  private void mergeThreadIOStatistics(IOStatistics streamIOStats) {
    threadIOStatistics.aggregate(streamIOStats);
  }

  /**
   * Close a stream: decide whether to abort or close, based on
   * the length of the stream and the current position.
   * If a close() is attempted and fails, the operation escalates to
   * an abort.
   *
   * The close is potentially; a future is returned.
   * It's the draining of a stream which is time consuming so
   * worth scheduling on a separate thread.
   * In stream close, when an abort is issued or when there's no
   * data to drain, block.
   * This does not set the {@link #closed} flag.
   * @param reason reason for stream being closed; used in messages
   * @param forceAbort force an abort; used if explicitly requested.
   * @param blocking should the call block for completion, or is async IO allowed
   * @return a future for the async operation
   */
  @Retries.OnceRaw
  private CompletableFuture<Boolean> closeStream(
      final String reason,
      final boolean forceAbort,
      final boolean blocking) {

    if (!isObjectStreamOpen()) {
      // steam is already closed
      return CompletableFuture.completedFuture(false);
    }

    // if the amount of data remaining in the current request is greater
    // than the readahead value: abort.
    long remaining = remainingInCurrentRequest();
    LOG.debug("Closing stream {}: {}", reason,
        forceAbort ? "abort" : "soft");
    boolean shouldAbort = forceAbort || remaining > readahead;
    CompletableFuture<Boolean> operation;
    SDKStreamDrainer drainer = new SDKStreamDrainer(
        uri,
        object,
        wrappedStream,
        shouldAbort,
        (int) remaining,
        streamStatistics,
        reason);

    if (blocking || shouldAbort || remaining <= asyncDrainThreshold) {
      // don't bother with async IO if the caller plans to wait for
      // the result, there's an abort (which is fast), or
      // there is not much data to read.
      operation = CompletableFuture.completedFuture(drainer.apply());

    } else {
      LOG.debug("initiating asynchronous drain of {} bytes", remaining);
      // schedule an async drain/abort
      operation = client.submit(drainer);
    }

    // either the stream is closed in the blocking call or the async call is
    // submitted with its own copy of the references
    wrappedStream = null;
    object = null;
    return operation;
  }

  /**
   * Forcibly reset the stream, by aborting the connection. The next
   * {@code read()} operation will trigger the opening of a new HTTPS
   * connection.
   *
   * This is potentially very inefficient, and should only be invoked
   * in extreme circumstances. It logs at info for this reason.
   *
   * Blocks until the abort is completed.
   *
   * @return true if the connection was actually reset.
   * @throws IOException if invoked on a closed stream.
   */
  @InterfaceStability.Unstable
  public synchronized boolean resetConnection() throws IOException {
    checkNotClosed();
    LOG.info("Forcing reset of connection to {}", uri);
    return awaitFuture(closeStream("reset()", true, true));
  }

  @Override
  public synchronized int available() throws IOException {
    checkNotClosed();

    long remaining = remainingInFile();
    if (remaining > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int)remaining;
  }

  /**
   * Bytes left in stream.
   * @return how many bytes are left to read
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public synchronized long remainingInFile() {
    return this.contentLength - this.pos;
  }

  /**
   * Bytes left in the current request.
   * Only valid if there is an active request.
   * @return how many bytes are left to read in the current GET.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public synchronized long remainingInCurrentRequest() {
    return this.contentRangeFinish - this.pos;
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public synchronized long getContentRangeFinish() {
    return contentRangeFinish;
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public synchronized long getContentRangeStart() {
    return contentRangeStart;
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  /**
   * String value includes statistics as well as stream state.
   * <b>Important: there are no guarantees as to the stability
   * of this value.</b>
   * @return a string value for printing in logs/diagnostics
   */
  @Override
  @InterfaceStability.Unstable
  public String toString() {
    String s = streamStatistics.toString();
    synchronized (this) {
      final StringBuilder sb = new StringBuilder(
          "S3AInputStream{");
      sb.append(uri);
      sb.append(" wrappedStream=")
          .append(isObjectStreamOpen() ? "open" : "closed");
      sb.append(" read policy=").append(inputPolicy);
      sb.append(" pos=").append(pos);
      sb.append(" nextReadPos=").append(nextReadPos);
      sb.append(" contentLength=").append(contentLength);
      sb.append(" contentRangeStart=").append(contentRangeStart);
      sb.append(" contentRangeFinish=").append(contentRangeFinish);
      sb.append(" remainingInCurrentRequest=")
          .append(remainingInCurrentRequest());
      sb.append(" ").append(changeTracker);
      sb.append(" ").append(vectoredIOContext);
      sb.append('\n').append(s);
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Subclass {@code readFully()} operation which only seeks at the start
   * of the series of operations; seeking back at the end.
   *
   * This is significantly higher performance if multiple read attempts are
   * needed to fetch the data, as it does not break the HTTP connection.
   *
   * To maintain thread safety requirements, this operation is synchronized
   * for the duration of the sequence.
   * {@inheritDoc}
   *
   */
  @Override
  @Retries.RetryTranslated
  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    checkNotClosed();
    validatePositionedReadArgs(position, buffer, offset, length);
    streamStatistics.readFullyOperationStarted(position, length);
    if (length == 0) {
      return;
    }
    int nread = 0;
    synchronized (this) {
      long oldPos = getPos();
      try {
        seek(position);
        while (nread < length) {
          int nbytes = read(buffer, offset + nread, length - nread);
          if (nbytes < 0) {
            throw new EOFException(FSExceptionMessages.EOF_IN_READ_FULLY);
          }
          nread += nbytes;
        }
      } finally {
        seekQuietly(oldPos);
      }
    }
  }

  /**
   * {@inheritDoc}.
   */
  @Override
  public int minSeekForVectorReads() {
    return vectoredIOContext.getMinSeekForVectorReads();
  }

  /**
   * {@inheritDoc}.
   */
  @Override
  public int maxReadSizeForVectorReads() {
    return vectoredIOContext.getMaxReadSizeForVectorReads();
  }

  /**
   * {@inheritDoc}
   * Vectored read implementation for S3AInputStream.
   * @param ranges the byte ranges to read.
   * @param allocate the function to allocate ByteBuffer.
   * @throws IOException IOE if any.
   */
  @Override
  public void readVectored(List<? extends FileRange> ranges,
                           IntFunction<ByteBuffer> allocate) throws IOException {
    LOG.debug("Starting vectored read on path {} for ranges {} ", pathStr, ranges);
    checkNotClosed();
    if (stopVectoredIOOperations.getAndSet(false)) {
      LOG.debug("Reinstating vectored read operation for path {} ", pathStr);
    }
    List<? extends FileRange> sortedRanges = validateNonOverlappingAndReturnSortedRanges(ranges);
    for (FileRange range : ranges) {
      validateRangeRequest(range);
      CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
      range.setData(result);
    }

    if (isOrderedDisjoint(sortedRanges, 1, minSeekForVectorReads())) {
      LOG.debug("Not merging the ranges as they are disjoint");
      streamStatistics.readVectoredOperationStarted(sortedRanges.size(), sortedRanges.size());
      for (FileRange range: sortedRanges) {
        ByteBuffer buffer = allocate.apply(range.getLength());
        boundedThreadPool.submit(() -> readSingleRange(range, buffer));
      }
    } else {
      LOG.debug("Trying to merge the ranges as they are not disjoint");
      List<CombinedFileRange> combinedFileRanges = mergeSortedRanges(sortedRanges,
              1, minSeekForVectorReads(),
              maxReadSizeForVectorReads());
      streamStatistics.readVectoredOperationStarted(sortedRanges.size(), combinedFileRanges.size());
      LOG.debug("Number of original ranges size {} , Number of combined ranges {} ",
              ranges.size(), combinedFileRanges.size());
      for (CombinedFileRange combinedFileRange: combinedFileRanges) {
        boundedThreadPool.submit(
            () -> readCombinedRangeAndUpdateChildren(combinedFileRange, allocate));
      }
    }
    LOG.debug("Finished submitting vectored read to threadpool" +
            " on path {} for ranges {} ", pathStr, ranges);
  }

  /**
   * Read the data from S3 for the bigger combined file range and update all the
   * underlying ranges.
   * @param combinedFileRange big combined file range.
   * @param allocate method to create byte buffers to hold result data.
   */
  private void readCombinedRangeAndUpdateChildren(CombinedFileRange combinedFileRange,
                                                  IntFunction<ByteBuffer> allocate) {
    LOG.debug("Start reading combined range {} from path {} ", combinedFileRange, pathStr);
    // This reference must be kept till all buffers are populated as this is a
    // finalizable object which closes the internal stream when gc triggers.
    S3Object objectRange = null;
    S3ObjectInputStream objectContent = null;
    try {
      objectRange = getS3ObjectAndValidateNotNull("readCombinedFileRange",
              combinedFileRange.getOffset(),
              combinedFileRange.getLength());
      objectContent = objectRange.getObjectContent();
      populateChildBuffers(combinedFileRange, objectContent, allocate);
    } catch (Exception ex) {
      LOG.debug("Exception while reading a range {} from path {} ", combinedFileRange, pathStr, ex);
      for(FileRange child : combinedFileRange.getUnderlying()) {
        child.getData().completeExceptionally(ex);
      }
    } finally {
      IOUtils.cleanupWithLogger(LOG, objectRange, objectContent);
    }
    LOG.debug("Finished reading range {} from path {} ", combinedFileRange, pathStr);
  }

  /**
   * Populate underlying buffers of the child ranges.
   * @param combinedFileRange big combined file range.
   * @param objectContent data from s3.
   * @param allocate method to allocate child byte buffers.
   * @throws IOException any IOE.
   */
  private void populateChildBuffers(CombinedFileRange combinedFileRange,
                                    S3ObjectInputStream objectContent,
                                    IntFunction<ByteBuffer> allocate) throws IOException {
    // If the combined file range just contains a single child
    // range, we only have to fill that one child buffer else
    // we drain the intermediate data between consecutive ranges
    // and fill the buffers one by one.
    if (combinedFileRange.getUnderlying().size() == 1) {
      FileRange child = combinedFileRange.getUnderlying().get(0);
      ByteBuffer buffer = allocate.apply(child.getLength());
      populateBuffer(child.getLength(), buffer, objectContent);
      child.getData().complete(buffer);
    } else {
      FileRange prev = null;
      for (FileRange child : combinedFileRange.getUnderlying()) {
        if (prev != null && prev.getOffset() + prev.getLength() < child.getOffset()) {
          long drainQuantity = child.getOffset() - prev.getOffset() - prev.getLength();
          drainUnnecessaryData(objectContent, drainQuantity);
        }
        ByteBuffer buffer = allocate.apply(child.getLength());
        populateBuffer(child.getLength(), buffer, objectContent);
        child.getData().complete(buffer);
        prev = child;
      }
    }
  }

  /**
   * Drain unnecessary data in between ranges.
   * @param objectContent s3 data stream.
   * @param drainQuantity how many bytes to drain.
   * @throws IOException any IOE.
   */
  private void drainUnnecessaryData(S3ObjectInputStream objectContent, long drainQuantity)
          throws IOException {
    int drainBytes = 0;
    int readCount;
    while (drainBytes < drainQuantity) {
      if (drainBytes + InternalConstants.DRAIN_BUFFER_SIZE <= drainQuantity) {
        byte[] drainBuffer = new byte[InternalConstants.DRAIN_BUFFER_SIZE];
        readCount = objectContent.read(drainBuffer);
      } else {
        byte[] drainBuffer = new byte[(int) (drainQuantity - drainBytes)];
        readCount = objectContent.read(drainBuffer);
      }
      drainBytes += readCount;
    }
    streamStatistics.readVectoredBytesDiscarded(drainBytes);
    LOG.debug("{} bytes drained from stream ", drainBytes);
  }

  /**
   * Validates range parameters.
   * In case of S3 we already have contentLength from the first GET request
   * during an open file operation so failing fast here.
   * @param range requested range.
   * @throws EOFException end of file exception.
   */
  private void validateRangeRequest(FileRange range) throws EOFException {
    VectoredReadUtils.validateRangeRequest(range);
    if(range.getOffset() + range.getLength() > contentLength) {
      final String errMsg = String.format("Requested range [%d, %d) is beyond EOF for path %s",
              range.getOffset(), range.getLength(), pathStr);
      LOG.warn(errMsg);
      throw new EOFException(errMsg);
    }
  }

  /**
   * Read data from S3 for this range and populate the buffer.
   * @param range range of data to read.
   * @param buffer buffer to fill.
   */
  private void readSingleRange(FileRange range, ByteBuffer buffer) {
    LOG.debug("Start reading range {} from path {} ", range, pathStr);
    // This reference must be kept till all buffers are populated as this is a
    // finalizable object which closes the internal stream when gc triggers.
    S3Object objectRange = null;
    S3ObjectInputStream objectContent = null;
    try {
      long position = range.getOffset();
      int length = range.getLength();
      objectRange = getS3ObjectAndValidateNotNull("readSingleRange", position, length);
      objectContent = objectRange.getObjectContent();
      populateBuffer(length, buffer, objectContent);
      range.getData().complete(buffer);
    } catch (Exception ex) {
      LOG.warn("Exception while reading a range {} from path {} ", range, pathStr, ex);
      range.getData().completeExceptionally(ex);
    } finally {
      IOUtils.cleanupWithLogger(LOG, objectRange, objectContent);
    }
    LOG.debug("Finished reading range {} from path {} ", range, pathStr);
  }

  /**
   * Get the s3 object for S3 server for a specified range.
   * Also checks if the vectored io operation has been stopped before and after
   * the http get request such that we don't waste time populating the buffers.
   * @param operationName name of the operation for which get object on S3 is called.
   * @param position position of the object to be read from S3.
   * @param length length from position of the object to be read from S3.
   * @return result s3 object.
   * @throws IOException exception if any.
   */
  private S3Object getS3ObjectAndValidateNotNull(final String operationName,
                                                 final long position,
                                                 final int length) throws IOException {
    checkIfVectoredIOStopped();
    S3Object objectRange = getS3Object(operationName, position, length);
    if (objectRange.getObjectContent() == null) {
      throw new PathIOException(uri,
              "Null IO stream received during " + operationName);
    }
    checkIfVectoredIOStopped();
    return objectRange;
  }

  /**
   * Populates the buffer with data from objectContent
   * till length. Handles both direct and heap byte buffers.
   * @param length length of data to populate.
   * @param buffer buffer to fill.
   * @param objectContent result retrieved from S3 store.
   * @throws IOException any IOE.
   */
  private void populateBuffer(int length,
                              ByteBuffer buffer,
                              S3ObjectInputStream objectContent) throws IOException {

    if (buffer.isDirect()) {
      VectoredReadUtils.readInDirectBuffer(length, buffer,
          (position, tmp, offset, currentLength) -> {
            readByteArray(objectContent, tmp, offset, currentLength);
            return null;
          });
      buffer.flip();
    } else {
      readByteArray(objectContent, buffer.array(), 0, length);
    }
    // update io stats.
    incrementBytesRead(length);
  }


  /**
   * Read data into destination buffer from s3 object content.
   * @param objectContent result from S3.
   * @param dest destination buffer.
   * @param offset start offset of dest buffer.
   * @param length number of bytes to fill in dest.
   * @throws IOException any IOE.
   */
  private void readByteArray(S3ObjectInputStream objectContent,
                            byte[] dest,
                            int offset,
                            int length) throws IOException {
    int readBytes = 0;
    while (readBytes < length) {
      int readBytesCurr = objectContent.read(dest,
              offset + readBytes,
              length - readBytes);
      readBytes +=readBytesCurr;
      if (readBytesCurr < 0) {
        throw new EOFException(FSExceptionMessages.EOF_IN_READ_FULLY);
      }
    }
  }

  /**
   * Read data from S3 using a http request with retries.
   * This also handles if file has been changed while the
   * http call is getting executed. If the file has been
   * changed RemoteFileChangedException is thrown.
   * @param operationName name of the operation for which get object on S3 is called.
   * @param position position of the object to be read from S3.
   * @param length length from position of the object to be read from S3.
   * @return S3Object result s3 object.
   * @throws IOException exception if any.
   */
  private S3Object getS3Object(String operationName, long position,
                               int length) throws IOException {
    final GetObjectRequest request = client.newGetRequest(key)
            .withRange(position, position + length - 1);
    changeTracker.maybeApplyConstraint(request);
    DurationTracker tracker = streamStatistics.initiateGetRequest();
    S3Object objectRange;
    Invoker invoker = context.getReadInvoker();
    try {
      objectRange = invoker.retry(operationName, pathStr, true,
        () -> {
          checkIfVectoredIOStopped();
          return client.getObject(request);
        });

    } catch (IOException ex) {
      tracker.failed();
      throw ex;
    } finally {
      tracker.close();
    }
    changeTracker.processResponse(objectRange, operationName,
            position);
    return objectRange;
  }

  /**
   * Check if vectored io operation has been stooped. This happens
   * when the stream is closed or unbuffer is called.
   * @throws InterruptedIOException throw InterruptedIOException such
   *                                that all running vectored io is
   *                                terminated thus releasing resources.
   */
  private void checkIfVectoredIOStopped() throws InterruptedIOException {
    if (stopVectoredIOOperations.get()) {
      throw new InterruptedIOException("Stream closed or unbuffer is called");
    }
  }

  /**
   * Access the input stream statistics.
   * This is for internal testing and may be removed without warning.
   * @return the statistics for this input stream
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public S3AInputStreamStatistics getS3AStreamStatistics() {
    return streamStatistics;
  }

  @Override
  public synchronized void setReadahead(Long readahead) {
    this.readahead = validateReadahead(readahead);
  }

  /**
   * Get the current readahead value.
   * @return a non-negative readahead value
   */
  public synchronized long getReadahead() {
    return readahead;
  }

  /**
   * Calculate the limit for a get request, based on input policy
   * and state of object.
   * @param inputPolicy input policy
   * @param targetPos position of the read
   * @param length length of bytes requested; if less than zero "unknown"
   * @param contentLength total length of file
   * @param readahead current readahead value
   * @return the absolute value of the limit of the request.
   */
  static long calculateRequestLimit(
      S3AInputPolicy inputPolicy,
      long targetPos,
      long length,
      long contentLength,
      long readahead) {
    long rangeLimit;
    switch (inputPolicy) {
    case Random:
      // positioned.
      // read either this block, or the here + readahead value.
      rangeLimit = (length < 0) ? contentLength
          : targetPos + Math.max(readahead, length);
      break;

    case Sequential:
      // sequential: plan for reading the entire object.
      rangeLimit = contentLength;
      break;

    case Normal:
      // normal is considered sequential until a backwards seek switches
      // it to 'Random'
    default:
      rangeLimit = contentLength;

    }
    // cannot read past the end of the object
    rangeLimit = Math.min(contentLength, rangeLimit);
    return rangeLimit;
  }

  /**
   * from a possibly null Long value, return a valid
   * readahead.
   * @param readahead new readahead
   * @return a natural number.
   * @throws IllegalArgumentException if the range is invalid.
   */
  public static long validateReadahead(@Nullable Long readahead) {
    if (readahead == null) {
      return Constants.DEFAULT_READAHEAD_RANGE;
    } else {
      Preconditions.checkArgument(readahead >= 0, E_NEGATIVE_READAHEAD_VALUE);
      return readahead;
    }
  }

  /**
   * Closes the underlying S3 stream, and merges the {@link #streamStatistics}
   * instance associated with the stream.
   * Also sets the {@code stopVectoredIOOperations} flag to true such that
   * active vectored read operations are terminated. However termination of
   * old vectored reads are not guaranteed if a new vectored read operation
   * is initiated after unbuffer is called.
   */
  @Override
  public synchronized void unbuffer() {
    try {
      stopVectoredIOOperations.set(true);
      closeStream("unbuffer()", false, false);
    } finally {
      streamStatistics.unbuffered();
      if (inputPolicy.isAdaptive()) {
        S3AInputPolicy policy = S3AInputPolicy.Random;
        LOG.debug("Switching to seek policy {} after unbuffer() invoked", policy);
        setInputPolicy(policy);
      }
    }
  }

  @Override
  public boolean hasCapability(String capability) {
    switch (toLowerCase(capability)) {
    case StreamCapabilities.IOSTATISTICS:
    case StreamCapabilities.IOSTATISTICS_CONTEXT:
    case StreamCapabilities.READAHEAD:
    case StreamCapabilities.UNBUFFER:
    case StreamCapabilities.VECTOREDIO:
      return true;
    default:
      return false;
    }
  }

  @VisibleForTesting
  boolean isObjectStreamOpen() {
    return wrappedStream != null;
  }

  @Override
  public IOStatistics getIOStatistics() {
    return ioStatistics;
  }

  /**
   * Callbacks for input stream IO.
   */
  public interface InputStreamCallbacks extends Closeable {

    /**
     * Create a GET request.
     * @param key object key
     * @return the request
     */
    GetObjectRequest newGetRequest(String key);

    /**
     * Execute the request.
     * @param request the request
     * @return the response
     */
    @Retries.OnceRaw
    S3Object getObject(GetObjectRequest request);

    /**
     * Submit some asynchronous work, for example, draining a stream.
     * @param operation operation to invoke
     * @param <T> return type
     * @return a future.
     */
    <T> CompletableFuture<T> submit(CallableRaisingIOE<T> operation);

  }

}

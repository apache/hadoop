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

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.impl.CombinedFileRange;
import org.apache.hadoop.fs.impl.FutureIOSupport;
import org.apache.hadoop.fs.impl.VectoredReadUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.s3a.impl.ChangeTracker;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.statistics.DurationTracker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.IntFunction;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.fs.impl.VectoredReadUtils.isOrderedDisjoint;
import static org.apache.hadoop.fs.impl.VectoredReadUtils.sliceTo;
import static org.apache.hadoop.fs.impl.VectoredReadUtils.sortAndMergeRanges;
import static org.apache.hadoop.util.StringUtils.toLowerCase;

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
  private final ThreadPoolExecutor unboundedThreadPool;
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
   * Create the stream.
   * This does not attempt to open it; that is only done on the first
   * actual read() operation.
   * @param ctx operation context
   * @param s3Attributes object attributes
   * @param client S3 client to use
   * @param unboundedThreadPool thread pool to use.
   */
  public S3AInputStream(S3AReadOpContext ctx,
                        S3ObjectAttributes s3Attributes,
                        InputStreamCallbacks client,
                        ThreadPoolExecutor unboundedThreadPool) {
    Preconditions.checkArgument(isNotEmpty(s3Attributes.getBucket()),
        "No Bucket");
    Preconditions.checkArgument(isNotEmpty(s3Attributes.getKey()), "No Key");
    long l = s3Attributes.getLen();
    Preconditions.checkArgument(l >= 0, "Negative content length");
    this.context = ctx;
    this.bucket = s3Attributes.getBucket();
    this.key = s3Attributes.getKey();
    this.pathStr = ctx.dstFileStatus.getPath().toString();
    this.contentLength = l;
    this.client = client;
    this.uri = "s3a://" + this.bucket + "/" + this.key;
    this.streamStatistics = ctx.getS3AStatisticsContext()
        .newInputStreamStatistics();
    this.ioStatistics = streamStatistics.getIOStatistics();
    this.changeTracker = new ChangeTracker(uri,
        ctx.getChangeDetectionPolicy(),
        streamStatistics.getChangeTrackerStatistics(),
        s3Attributes);
    setInputPolicy(ctx.getInputPolicy());
    setReadahead(ctx.getReadahead());
    this.unboundedThreadPool = unboundedThreadPool;
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
      closeStream("reopen(" + reason + ")", contentRangeFinish, forceAbort);
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

    DurationTracker tracker = streamStatistics.initiateGetRequest();
    try {
      object = Invoker.once(text, uri,
          () -> client.getObject(request));
    } catch(IOException e) {
      // input function failed: note it
      tracker.failed();
      // and rethrow
      throw e;
    } finally {
      // update the tracker.
      // this is called after any catch() call will have
      // set the failed flag.
      tracker.close();
    }

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
      if (inputPolicy.equals(S3AInputPolicy.Normal)) {
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
    closeStream("seekInStream()", this.contentRangeFinish, false);
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
    closeStream("failure recovery", contentRangeFinish, forceAbort);
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
        // close or abort the stream
        closeStream("close() operation", this.contentRangeFinish, false);
        LOG.debug("Statistics of stream {}\n{}", key, streamStatistics);
        // end the client+audit span.
        client.close();
        // this is actually a no-op
        super.close();
      } finally {
        // merge the statistics back into the FS statistics.
        streamStatistics.close();
      }
    }
  }

  /**
   * Close a stream: decide whether to abort or close, based on
   * the length of the stream and the current position.
   * If a close() is attempted and fails, the operation escalates to
   * an abort.
   *
   * This does not set the {@link #closed} flag.
   * @param reason reason for stream being closed; used in messages
   * @param length length of the stream.
   * @param forceAbort force an abort; used if explicitly requested.
   */
  @Retries.OnceRaw
  private void closeStream(String reason, long length, boolean forceAbort) {
    if (!isObjectStreamOpen()) {
      // steam is already closed
      return;
    }

    // if the amount of data remaining in the current request is greater
    // than the readahead value: abort.
    long remaining = remainingInCurrentRequest();
    LOG.debug("Closing stream {}: {}", reason,
        forceAbort ? "abort" : "soft");
    boolean shouldAbort = forceAbort || remaining > readahead;

    try {
      if (!shouldAbort) {
        try {
          // clean close. This will read to the end of the stream,
          // so, while cleaner, can be pathological on a multi-GB object

          // explicitly drain the stream
          long drained = 0;
          while (wrappedStream.read() >= 0) {
            drained++;
          }
          LOG.debug("Drained stream of {} bytes", drained);

          // now close it
          wrappedStream.close();
          // this MUST come after the close, so that if the IO operations fail
          // and an abort is triggered, the initial attempt's statistics
          // aren't collected.
          streamStatistics.streamClose(false, drained);
        } catch (Exception e) {
          // exception escalates to an abort
          LOG.debug("When closing {} stream for {}, will abort the stream",
              uri, reason, e);
          shouldAbort = true;
        }
      }
      if (shouldAbort) {
        // Abort, rather than just close, the underlying stream.  Otherwise, the
        // remaining object payload is read from S3 while closing the stream.
        LOG.debug("Aborting stream {}", uri);
        try {
          wrappedStream.abort();
        } catch (Exception e) {
          LOG.warn("When aborting {} stream after failing to close it for {}",
              uri, reason, e);
        }
        streamStatistics.streamClose(true, remaining);
      }
      LOG.debug("Stream {} {}: {}; remaining={} streamPos={},"
              + " nextReadPos={}," +
              " request range {}-{} length={}",
          uri, (shouldAbort ? "aborted" : "closed"), reason,
          remaining, pos, nextReadPos,
          contentRangeStart, contentRangeFinish,
          length);
    } finally {
      wrappedStream = null;
      object = null;
    }
  }

  /**
   * Forcibly reset the stream, by aborting the connection. The next
   * {@code read()} operation will trigger the opening of a new HTTPS
   * connection.
   *
   * This is potentially very inefficient, and should only be invoked
   * in extreme circumstances. It logs at info for this reason.
   * @return true if the connection was actually reset.
   * @throws IOException if invoked on a closed stream.
   */
  @InterfaceStability.Unstable
  public synchronized boolean resetConnection() throws IOException {
    checkNotClosed();
    if (isObjectStreamOpen()) {
      LOG.info("Forced reset of connection to {}", uri);
      closeStream("reset()", contentRangeFinish, true);
    }
    return isObjectStreamOpen();
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
    for (FileRange range : ranges) {
      validateRangeRequest(range);
      CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
      range.setData(result);
    }

    if (isOrderedDisjoint(ranges, 1, minSeekForVectorReads())) {
      LOG.debug("Not merging the ranges as they are disjoint");
      for(FileRange range: ranges) {
        ByteBuffer buffer = allocate.apply(range.getLength());
        unboundedThreadPool.submit(() -> readSingleRange(range, buffer));
      }
    } else {
      LOG.debug("Trying to merge the ranges as they are not disjoint");
      List<CombinedFileRange> combinedFileRanges = sortAndMergeRanges(ranges,
              1, minSeekForVectorReads(),
              maxReadSizeForVectorReads());
      LOG.debug("Number of original ranges size {} , Number of combined ranges {} ",
              ranges.size(), combinedFileRanges.size());
      for(CombinedFileRange combinedFileRange: combinedFileRanges) {
        CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
        ByteBuffer buffer = allocate.apply(combinedFileRange.getLength());
        combinedFileRange.setData(result);
        unboundedThreadPool.submit(
            () -> readCombinedRangeAndUpdateChildren(combinedFileRange, buffer));
      }
    }
    LOG.debug("Finished submitting vectored read to threadpool" +
            " on path {} for ranges {} ", pathStr, ranges);
  }

  /**
   * Read data in the combinedFileRange and update data in buffers
   * of all underlying ranges.
   * @param combinedFileRange combined range.
   * @param buffer combined buffer.
   */
  private void readCombinedRangeAndUpdateChildren(CombinedFileRange combinedFileRange,
                                                  ByteBuffer buffer) {
    // Not putting read single range call inside try block as
    // exception if any occurred during this call will be raised
    // during awaitFuture call while getting the combined buffer.
    readSingleRange(combinedFileRange, buffer);
    try {
      // In case of single range we return the original byte buffer else
      // we return slice byte buffers for each child ranges.
      ByteBuffer combinedBuffer = FutureIOSupport.awaitFuture(combinedFileRange.getData());
      if (combinedFileRange.getUnderlying().size() == 1) {
        combinedFileRange.getUnderlying().get(0).getData().complete(combinedBuffer);
      } else {
        for (FileRange child : combinedFileRange.getUnderlying()) {
          updateOriginalRange(child, combinedBuffer, combinedFileRange);
        }
      }
    } catch (Exception ex) {
      LOG.warn("Exception occurred while reading combined range from file {}", pathStr, ex);
      for(FileRange child : combinedFileRange.getUnderlying()) {
        child.getData().completeExceptionally(ex);
      }
    }
  }

  /**
   * Update data in child range from combined range.
   * @param child child range.
   * @param combinedBuffer combined buffer.
   * @param combinedFileRange combined range.
   */
  private void updateOriginalRange(FileRange child,
                                   ByteBuffer combinedBuffer,
                                   CombinedFileRange combinedFileRange) {
    LOG.trace("Start Filling original range [{}, {}) from combined range [{}, {}) ",
            child.getOffset(), child.getLength(),
            combinedFileRange.getOffset(), combinedFileRange.getLength());
    ByteBuffer childBuffer = sliceTo(combinedBuffer, combinedFileRange.getOffset(), child);
    child.getData().complete(childBuffer);
    LOG.trace("End Filling original range [{}, {}) from combined range [{}, {}) ",
            child.getOffset(), child.getLength(),
            combinedFileRange.getOffset(), combinedFileRange.getLength());
  }

  /**
   *  // Check if we can use contentLength returned by http GET request.
   * Validates range parameters.
   * @param range requested range.
   * @throws EOFException end of file exception.
   */
  private void validateRangeRequest(FileRange range) throws EOFException {
    VectoredReadUtils.validateRangeRequest(range);
    if(range.getOffset() + range.getLength() > contentLength) {
      LOG.warn("Requested range [{}, {}) is beyond EOF for path {}",
              range.getOffset(), range.getLength(), pathStr);
      throw new EOFException("Requested range [" + range.getOffset() +", "
              + range.getLength() + ") is beyond EOF for path " + pathStr);
    }
  }

  /**
   * TODO: Add retry in client.getObject(). not present in older reads why here??
   * Okay retry is being done in the top layer during read.
   * But if we do here in the top layer, one issue I am thinking is
   * what if there is some error which happened during filling the buffer
   * If we retry that old offsets of heap buffers can be overwritten ?
   * I think retry should be only added in {@link S3AInputStream#getS3Object}
   * Read data from S3 for this range and populate the bufffer.
   * @param range range of data to read.
   * @param buffer buffer to fill.
   */
  private void readSingleRange(FileRange range, ByteBuffer buffer) {
    LOG.debug("Start reading range {} from path {} ", range, pathStr);
    S3Object objectRange = null;
    S3ObjectInputStream objectContent = null;
    try {
      long position = range.getOffset();
      int length = range.getLength();
      final String operationName = "readRange";
      objectRange = getS3Object(operationName, position, length);
      objectContent = objectRange.getObjectContent();
      if (objectContent == null) {
        throw new PathIOException(uri,
                "Null IO stream received during " + operationName);
      }
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
      int readBytes = 0;
      int offset = 0;
      byte[] tmp = new byte[TMP_BUFFER_MAX_SIZE];
      while (readBytes < length) {
        int currentLength = readBytes + TMP_BUFFER_MAX_SIZE < length ?
                TMP_BUFFER_MAX_SIZE
                : length - readBytes;
        readByteArray(objectContent, tmp, 0, currentLength);
        buffer.put(tmp, 0, currentLength);
        offset = offset + currentLength;
        readBytes = readBytes + currentLength;
      }
      buffer.flip();
    } else {
      readByteArray(objectContent, buffer.array(), 0, length);
    }
  }

  public void readByteArray(S3ObjectInputStream objectContent,
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
   * Read data from S3 using a http request.
   * This also handles if file has been changed while http call
   * is getting executed. If file has been changed RemoteFileChangedException
   * is thrown.
   * @param operationName name of the operation for which get object on S3 is called.
   * @param position position of the object to be read from S3.
   * @param length length from position of the object to be read from S3.
   * @return S3Object
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
          () -> client.getObject(request));
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
   */
  @Override
  public synchronized void unbuffer() {
    try {
      closeStream("unbuffer()", contentRangeFinish, false);
    } finally {
      streamStatistics.unbuffered();
    }
  }

  @Override
  public boolean hasCapability(String capability) {
    switch (toLowerCase(capability)) {
    case StreamCapabilities.IOSTATISTICS:
    case StreamCapabilities.READAHEAD:
    case StreamCapabilities.UNBUFFER:
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

  }

}

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
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.impl.LeakReporter;
import org.apache.hadoop.fs.s3a.impl.streams.InputStreamType;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectInputStream;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectReadParameters;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.impl.CombinedFileRange;
import org.apache.hadoop.fs.VectoredReadUtils;
import org.apache.hadoop.fs.s3a.impl.ChangeTracker;
import org.apache.hadoop.fs.s3a.impl.InternalConstants;
import org.apache.hadoop.fs.s3a.impl.SDKStreamDrainer;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.io.IOUtils;


import static org.apache.hadoop.fs.VectoredReadUtils.isOrderedDisjoint;
import static org.apache.hadoop.fs.VectoredReadUtils.mergeSortedRanges;
import static org.apache.hadoop.fs.VectoredReadUtils.validateAndSortRanges;
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
public class S3AInputStream extends ObjectInputStream implements CanSetReadahead,
        CanUnbuffer, StreamCapabilities, IOStatisticsSource {

  public static final String E_NEGATIVE_READAHEAD_VALUE
      = "Negative readahead value";

  public static final String OPERATION_OPEN = "open";
  public static final String OPERATION_REOPEN = "re-open";

  /**
   * Switch for behavior on when wrappedStream.read()
   * returns -1 or raises an EOF; the original semantics
   * are that the stream is kept open.
   * Value {@value}.
   */
  private static final boolean CLOSE_WRAPPED_STREAM_ON_NEGATIVE_READ = true;

  /**
   * This is the maximum temporary buffer size we use while
   * populating the data in direct byte buffers during a vectored IO
   * operation. This is to ensure that when a big range of data is
   * requested in direct byte buffer doesn't leads to OOM errors.
   */
  private static final int TMP_BUFFER_MAX_SIZE = 64 * 1024;

  private static final Logger LOG =
      LoggerFactory.getLogger(S3AInputStream.class);

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
   * Input stream returned by a getObject call.
   */
  private ResponseInputStream<GetObjectResponse> wrappedStream;
  /**
   * Content length in format for vector IO.
   */
  private final Optional<Long> fileLength;


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
   * Threshold for stream reads to switch to
   * asynchronous draining.
   */
  private final long asyncDrainThreshold;

  /**
   * Create the stream.
   * This does not attempt to open it; that is only done on the first
   * actual read() operation.
   *
   * @param parameters creation parameters.
   */
  public S3AInputStream(ObjectReadParameters parameters) {

    super(InputStreamType.Classic, parameters);


    this.fileLength = Optional.of(getContentLength());
    S3AReadOpContext context = getContext();
    this.changeTracker = new ChangeTracker(getUri(),
        context.getChangeDetectionPolicy(),
        getS3AStreamStatistics().getChangeTrackerStatistics(),
        getObjectAttributes());
    setReadahead(context.getReadahead());
    this.asyncDrainThreshold = context.getAsyncDrainThreshold();
  }

  /**
   * Probe for stream being open.
   * Not synchronized; the flag is volatile.
   * @return true if the stream is still open.
   */
  @Override
  protected boolean isStreamOpen() {
    return !closed;
  }

  /**
   * Brute force stream close; invoked by {@link LeakReporter}.
   * All exceptions raised are ignored.
   */
  @Override
  protected void abortInFinalizer() {
    try {
      // stream was leaked: update statistic
      getS3AStreamStatistics().streamLeaked();
      // abort the stream. This merges statistics into the filesystem.
      closeStream("finalize()", true, true).get();
    } catch (InterruptedException | ExecutionException ignroed) {
      /* ignore this failure shutdown */
    }
  }

  /**
   * If the stream is in Adaptive mode, switch to random IO at this
   * point. Unsynchronized.
   */
  private void maybeSwitchToRandomIO() {
    if (getInputPolicy().isAdaptive()) {
      setInputPolicy(S3AInputPolicy.Random);
    }
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

    contentRangeFinish = calculateRequestLimit(getInputPolicy(), targetPos,
        length, getContentLength(), readahead);
    LOG.debug("reopen({}) for {} range[{}-{}], length={}," +
        " streamPosition={}, nextReadPosition={}, policy={}",
        getUri(), reason, targetPos, contentRangeFinish, length,  pos, nextReadPos,
        getInputPolicy());

    GetObjectRequest request = getCallbacks().newGetRequestBuilder(getKey())
        .range(S3AUtils.formatRange(targetPos, contentRangeFinish - 1))
        .applyMutation(changeTracker::maybeApplyConstraint)
        .build();
    long opencount = getS3AStreamStatistics().streamOpened();
    String operation = opencount == 0 ? OPERATION_OPEN : OPERATION_REOPEN;
    String text = String.format("%s %s at %d",
        operation, getUri(), targetPos);
    wrappedStream = onceTrackingDuration(text, getUri(),
        getS3AStreamStatistics().initiateGetRequest(), () ->
            getCallbacks().getObject(request));

    changeTracker.processResponse(wrappedStream.response(), operation,
        targetPos);

    contentRangeStart = targetPos;
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

    if (this.getContentLength() <= 0) {
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
          getUri(), positiveTargetPos, ioe);
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
    if (!isObjectStreamOpen()) {
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
        LOG.debug("Forward seek on {}, of {} bytes", getUri(), diff);
        long skipped = wrappedStream.skip(diff);
        if (skipped > 0) {
          pos += skipped;
        }
        getS3AStreamStatistics().seekForwards(diff, skipped);

        if (pos == targetPos) {
          // all is well
          LOG.debug("Now at {}: bytes remaining in current request: {}",
              pos, remainingInCurrentRequest());
          return;
        } else {
          // log a warning; continue to attempt to re-open
          LOG.warn("Failed to seek on {} to {}. Current position {}",
              getUri(), targetPos,  pos);
        }
      } else {
        // not attempting to read any bytes from the stream
        getS3AStreamStatistics().seekForwards(diff, 0);
      }
    } else if (diff < 0) {
      // backwards seek
      getS3AStreamStatistics().seekBackwards(diff);
      // if the stream is in "Normal" mode, switch to random IO at this
      // point, as it is indicative of columnar format IO
      maybeSwitchToRandomIO();
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
   * If an EOF Exception is raised there are two possibilities
   * <ol>
   *   <li>the stream is at the end of the file</li>
   *   <li>something went wrong with the network connection</li>
   * </ol>
   * This method does not attempt to distinguish; it assumes that an EOF
   * exception is always "end of file".
   * @param targetPos position from where data should be read
   * @param len length of the content that needs to be read
   * @throws RangeNotSatisfiableEOFException GET is out of range
   * @throws IOException anything else.
   */
  @Retries.RetryTranslated
  private void lazySeek(long targetPos, long len) throws IOException {

    Invoker invoker = getContext().getReadInvoker();
    invoker.retry("lazySeek to " + targetPos, getPathStr(), true,
        () -> {
          //For lazy seek
          seekInStream(targetPos, len);

          //re-open at specific location if needed
          if (!isObjectStreamOpen()) {
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
    getS3AStreamStatistics().bytesRead(bytesRead);
    if (getContext().stats != null && bytesRead > 0) {
      getContext().stats.incrementBytesRead(bytesRead);
    }
  }

  @Override
  @Retries.RetryTranslated
  public synchronized int read() throws IOException {
    checkNotClosed();
    if (this.getContentLength() == 0 || (nextReadPos >= getContentLength())) {
      return -1;
    }

    try {
      lazySeek(nextReadPos, 1);
    } catch (RangeNotSatisfiableEOFException e) {
      // attempt to GET beyond the end of the object
      LOG.debug("Downgrading 416 response attempt to read at {} to -1 response", nextReadPos);
      return -1;
    }

    Invoker invoker = getContext().getReadInvoker();
    int byteRead = invoker.retry("read", getPathStr(), true,
        () -> {
          int b;
          // When exception happens before re-setting wrappedStream in "reopen" called
          // by onReadFailure, then wrappedStream will be null. But the **retry** may
          // re-execute this block and cause NPE if we don't check wrappedStream
          if (!isObjectStreamOpen()) {
            reopen("failure recovery", getPos(), 1, false);
          }
          try {
            b = wrappedStream.read();
          } catch (HttpChannelEOFException | SocketTimeoutException e) {
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
      incrementBytesRead(1);
    } else {
      streamReadResultNegative();
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
    GetObjectResponse objectResponse = wrappedStream == null ? null : wrappedStream.response();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Got exception while trying to read from stream {}, " +
          "client: {} object: {}, trying to recover: ",
          getUri(), getCallbacks(), objectResponse, ioe);
    } else {
      LOG.info("Got exception while trying to read from stream {}, " +
          "client: {} object: {}, trying to recover: " + ioe,
          getUri(), getCallbacks(), objectResponse);
    }
    getS3AStreamStatistics().readException();
    closeStream("failure recovery", forceAbort, false);
  }

  /**
   * the read() call returned -1.
   * this means "the connection has gone past the end of the object" or
   * the stream has broken for some reason.
   * so close stream (without an abort).
   */
  private void streamReadResultNegative() {
    if (CLOSE_WRAPPED_STREAM_ON_NEGATIVE_READ) {
      closeStream("wrappedStream.read() returned -1", false, false);
    }
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

    if (this.getContentLength() == 0 || (nextReadPos >= getContentLength())) {
      return -1;
    }

    try {
      lazySeek(nextReadPos, len);
    } catch (RangeNotSatisfiableEOFException e) {
      // attempt to GET beyond the end of the object
      return -1;
    }

    Invoker invoker = getContext().getReadInvoker();

    getS3AStreamStatistics().readOperationStarted(nextReadPos, len);
    int bytesRead = invoker.retry("read", getPathStr(), true,
        () -> {
          int bytes;
          // When exception happens before re-setting wrappedStream in "reopen" called
          // by onReadFailure, then wrappedStream will be null. But the **retry** may
          // re-execute this block and cause NPE if we don't check wrappedStream
          if (!isObjectStreamOpen()) {
            reopen("failure recovery", getPos(), 1, false);
          }
          try {
            // read data; will block until there is data or the end of the stream is reached.
            // returns 0 for "stream is open but no data yet" and -1 for "end of stream".
            bytes = wrappedStream.read(buf, off, len);
          } catch (HttpChannelEOFException | SocketTimeoutException e) {
            onReadFailure(e, true);
            throw e;
          } catch (EOFException e) {
            LOG.debug("EOFException raised by http stream read(); downgrading to a -1 response", e);
            return -1;
          } catch (IOException e) {
            onReadFailure(e, false);
            throw e;
          }
          return bytes;
        });

    if (bytesRead > 0) {
      pos += bytesRead;
      nextReadPos += bytesRead;
      incrementBytesRead(bytesRead);
    } else {
      streamReadResultNegative();
    }
    getS3AStreamStatistics().readOperationCompleted(len, bytesRead);
    return bytesRead;
  }

  /**
   * Verify that the input stream is open. Non blocking; this gives
   * the last state of the volatile {@link #closed} field.
   * @throws IOException if the connection is closed.
   */
  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException(getUri() + ": " + FSExceptionMessages.STREAM_IS_CLOSED);
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
        getCallbacks().close();

      } finally {
        super.close();
      }
    }
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
    SDKStreamDrainer<ResponseInputStream<GetObjectResponse>> drainer = new SDKStreamDrainer<>(
        getUri(),
        wrappedStream,
        shouldAbort,
        (int) remaining,
        getS3AStreamStatistics(),
        reason);

    if (blocking || shouldAbort || remaining <= asyncDrainThreshold) {
      // don't bother with async IO if the caller plans to wait for
      // the result, there's an abort (which is fast), or
      // there is not much data to read.
      operation = CompletableFuture.completedFuture(drainer.apply());

    } else {
      LOG.debug("initiating asynchronous drain of {} bytes", remaining);
      // schedule an async drain/abort
      operation = getCallbacks().submit(drainer);
    }

    // either the stream is closed in the blocking call or the async call is
    // submitted with its own copy of the references
    wrappedStream = null;
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
    LOG.info("Forcing reset of connection to {}", getUri());
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
    return this.getContentLength() - this.pos;
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
    String s = getS3AStreamStatistics().toString();
    synchronized (this) {
      final StringBuilder sb = new StringBuilder(
          "S3AInputStream{");
      sb.append(super.toString()).append(" ");
      sb.append(getUri());
      sb.append(" wrappedStream=")
          .append(isObjectStreamOpen() ? "open" : "closed");
      sb.append(" read policy=").append(getInputPolicy());
      sb.append(" pos=").append(pos);
      sb.append(" nextReadPos=").append(nextReadPos);
      sb.append(" contentLength=").append(getContentLength());
      sb.append(" contentRangeStart=").append(contentRangeStart);
      sb.append(" contentRangeFinish=").append(contentRangeFinish);
      sb.append(" remainingInCurrentRequest=")
          .append(remainingInCurrentRequest());
      sb.append(" ").append(changeTracker);
      sb.append(" ").append(getVectoredIOContext());
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
    getS3AStreamStatistics().readFullyOperationStarted(position, length);
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
            // no attempt is currently made to recover from stream read problems;
            // a lazy seek to the offset is probably the solution.
            // but it will need more qualification against failure handling
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
  public synchronized void readVectored(List<? extends FileRange> ranges,
                           IntFunction<ByteBuffer> allocate) throws IOException {
    LOG.debug("Starting vectored read on path {} for ranges {} ", getPathStr(), ranges);
    checkNotClosed();
    if (stopVectoredIOOperations.getAndSet(false)) {
      LOG.debug("Reinstating vectored read operation for path {} ", getPathStr());
    }

    // prepare to read
    List<? extends FileRange> sortedRanges = validateAndSortRanges(ranges,
        fileLength);
    for (FileRange range : ranges) {
      CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
      range.setData(result);
    }
    // switch to random IO and close any open stream.
    // what happens if a read is in progress? bad things.
    // ...which is why this method is synchronized
    closeStream("readVectored()", false, false);
    maybeSwitchToRandomIO();

    if (isOrderedDisjoint(sortedRanges, 1, minSeekForVectorReads())) {
      LOG.debug("Not merging the ranges as they are disjoint");
      getS3AStreamStatistics().readVectoredOperationStarted(sortedRanges.size(),
          sortedRanges.size());
      for (FileRange range: sortedRanges) {
        ByteBuffer buffer = allocate.apply(range.getLength());
        getBoundedThreadPool().submit(() -> readSingleRange(range, buffer));
      }
    } else {
      LOG.debug("Trying to merge the ranges as they are not disjoint");
      List<CombinedFileRange> combinedFileRanges = mergeSortedRanges(sortedRanges,
              1, minSeekForVectorReads(),
              maxReadSizeForVectorReads());
      getS3AStreamStatistics().readVectoredOperationStarted(sortedRanges.size(),
          combinedFileRanges.size());
      LOG.debug("Number of original ranges size {} , Number of combined ranges {} ",
              ranges.size(), combinedFileRanges.size());
      for (CombinedFileRange combinedFileRange: combinedFileRanges) {
        getBoundedThreadPool().submit(
            () -> readCombinedRangeAndUpdateChildren(combinedFileRange, allocate));
      }
    }
    LOG.debug("Finished submitting vectored read to threadpool" +
            " on path {} for ranges {} ", getPathStr(), ranges);
  }

  /**
   * Read the data from S3 for the bigger combined file range and update all the
   * underlying ranges.
   * @param combinedFileRange big combined file range.
   * @param allocate method to create byte buffers to hold result data.
   */
  private void readCombinedRangeAndUpdateChildren(CombinedFileRange combinedFileRange,
                                                  IntFunction<ByteBuffer> allocate) {
    LOG.debug("Start reading {} from path {} ", combinedFileRange, getPathStr());
    ResponseInputStream<GetObjectResponse> rangeContent = null;
    try {
      rangeContent = getS3ObjectInputStream("readCombinedFileRange",
              combinedFileRange.getOffset(),
              combinedFileRange.getLength());
      populateChildBuffers(combinedFileRange, rangeContent, allocate);
    } catch (Exception ex) {
      LOG.debug("Exception while reading {} from path {} ", combinedFileRange, getPathStr(), ex);
      // complete exception all the underlying ranges which have not already
      // finished.
      for(FileRange child : combinedFileRange.getUnderlying()) {
        if (!child.getData().isDone()) {
          child.getData().completeExceptionally(ex);
        }
      }
    } finally {
      IOUtils.cleanupWithLogger(LOG, rangeContent);
    }
    LOG.debug("Finished reading {} from path {} ", combinedFileRange, getPathStr());
  }

  /**
   * Populate underlying buffers of the child ranges.
   * There is no attempt to recover from any read failures.
   * @param combinedFileRange big combined file range.
   * @param objectContent data from s3.
   * @param allocate method to allocate child byte buffers.
   * @throws IOException any IOE.
   * @throws EOFException if EOF if read() call returns -1
   * @throws InterruptedIOException if vectored IO operation is stopped.
   */
  private void populateChildBuffers(CombinedFileRange combinedFileRange,
                                    InputStream objectContent,
                                    IntFunction<ByteBuffer> allocate) throws IOException {
    // If the combined file range just contains a single child
    // range, we only have to fill that one child buffer else
    // we drain the intermediate data between consecutive ranges
    // and fill the buffers one by one.
    if (combinedFileRange.getUnderlying().size() == 1) {
      FileRange child = combinedFileRange.getUnderlying().get(0);
      ByteBuffer buffer = allocate.apply(child.getLength());
      populateBuffer(child, buffer, objectContent);
      child.getData().complete(buffer);
    } else {
      FileRange prev = null;
      for (FileRange child : combinedFileRange.getUnderlying()) {
        checkIfVectoredIOStopped();
        if (prev != null) {
          final long position = prev.getOffset() + prev.getLength();
          if (position < child.getOffset()) {
            // there's data to drain between the requests.
            // work out how much
            long drainQuantity = child.getOffset() - position;
            // and drain it.
            drainUnnecessaryData(objectContent, position, drainQuantity);
          }
        }
        ByteBuffer buffer = allocate.apply(child.getLength());
        populateBuffer(child, buffer, objectContent);
        child.getData().complete(buffer);
        prev = child;
      }
    }
  }

  /**
   * Drain unnecessary data in between ranges.
   * There's no attempt at recovery here; it should be done at a higher level.
   * @param objectContent s3 data stream.
   * @param position position in file, for logging
   * @param drainQuantity how many bytes to drain.
   * @throws IOException any IOE.
   * @throws EOFException if the end of stream was reached during the draining
   */
  @Retries.OnceTranslated
  private void drainUnnecessaryData(
      final InputStream objectContent,
      final long position,
      long drainQuantity) throws IOException {

    int drainBytes = 0;
    int readCount;
    byte[] drainBuffer;
    int size = (int)Math.min(InternalConstants.DRAIN_BUFFER_SIZE, drainQuantity);
    drainBuffer = new byte[size];
    LOG.debug("Draining {} bytes from stream from offset {}; buffer size={}",
        drainQuantity, position, size);
    try {
      long remaining = drainQuantity;
      while (remaining > 0) {
        checkIfVectoredIOStopped();
        readCount = objectContent.read(drainBuffer, 0, (int)Math.min(size, remaining));
        LOG.debug("Drained {} bytes from stream", readCount);
        if (readCount < 0) {
          // read request failed; often network issues.
          // no attempt is made to recover at this point.
          final String s = String.format(
              "End of stream reached draining data between ranges; expected %,d bytes;"
                  + " only drained %,d bytes before -1 returned (position=%,d)",
              drainQuantity, drainBytes, position + drainBytes);
          throw new EOFException(s);
        }
        drainBytes += readCount;
        remaining -= readCount;
      }
    } finally {
      getS3AStreamStatistics().readVectoredBytesDiscarded(drainBytes);
      LOG.debug("{} bytes drained from stream ", drainBytes);
    }
  }

  /**
   * Read data from S3 for this range and populate the buffer.
   * @param range range of data to read.
   * @param buffer buffer to fill.
   */
  private void readSingleRange(FileRange range, ByteBuffer buffer) {
    LOG.debug("Start reading {} from {} ", range, getPathStr());
    if (range.getLength() == 0) {
      // a zero byte read.
      buffer.flip();
      range.getData().complete(buffer);
      return;
    }
    ResponseInputStream<GetObjectResponse> objectRange = null;
    try {
      long position = range.getOffset();
      int length = range.getLength();
      objectRange = getS3ObjectInputStream("readSingleRange", position, length);
      populateBuffer(range, buffer, objectRange);
      range.getData().complete(buffer);
    } catch (Exception ex) {
      LOG.warn("Exception while reading a range {} from path {} ", range, getPathStr(), ex);
      range.getData().completeExceptionally(ex);
    } finally {
      IOUtils.cleanupWithLogger(LOG, objectRange);
    }
    LOG.debug("Finished reading range {} from path {} ", range, getPathStr());
  }

  /**
   * Get the s3 object input stream for S3 server for a specified range.
   * Also checks if the vectored io operation has been stopped before and after
   * the http get request such that we don't waste time populating the buffers.
   * @param operationName name of the operation for which get object on S3 is called.
   * @param position position of the object to be read from S3.
   * @param length length from position of the object to be read from S3.
   * @return result s3 object.
   * @throws IOException exception if any.
   * @throws InterruptedIOException if vectored io operation is stopped.
   */
  @Retries.RetryTranslated
  private ResponseInputStream<GetObjectResponse> getS3ObjectInputStream(
      final String operationName, final long position, final int length) throws IOException {
    checkIfVectoredIOStopped();
    ResponseInputStream<GetObjectResponse> objectRange =
        getS3Object(operationName, position, length);
    checkIfVectoredIOStopped();
    return objectRange;
  }

  /**
   * Populates the buffer with data from objectContent
   * till length. Handles both direct and heap byte buffers.
   * calls {@code buffer.flip()} on the buffer afterwards.
   * @param range vector range to populate.
   * @param buffer buffer to fill.
   * @param objectContent result retrieved from S3 store.
   * @throws IOException any IOE.
   * @throws EOFException if EOF if read() call returns -1
   * @throws InterruptedIOException if vectored IO operation is stopped.
   */
  private void populateBuffer(FileRange range,
                              ByteBuffer buffer,
                              InputStream objectContent) throws IOException {

    int length = range.getLength();
    if (buffer.isDirect()) {
      VectoredReadUtils.readInDirectBuffer(range, buffer,
          (position, tmp, offset, currentLength) -> {
            readByteArray(objectContent, range, tmp, offset, currentLength);
            return null;
          });
      buffer.flip();
    } else {
      // there is no use of a temp byte buffer, or buffer.put() calls,
      // so flip() is not needed.
      readByteArray(objectContent, range, buffer.array(), 0, length);
    }
  }

  /**
   * Read data into destination buffer from s3 object content.
   * Calls {@link #incrementBytesRead(long)} to update statistics
   * incrementally.
   * @param objectContent result from S3.
   * @param range range being read into
   * @param dest destination buffer.
   * @param offset start offset of dest buffer.
   * @param length number of bytes to fill in dest.
   * @throws IOException any IOE.
   * @throws EOFException if EOF if read() call returns -1
   * @throws InterruptedIOException if vectored IO operation is stopped.
   */
  private void readByteArray(InputStream objectContent,
                            final FileRange range,
                            byte[] dest,
                            int offset,
                            int length) throws IOException {
    LOG.debug("Reading {} bytes", length);
    int readBytes = 0;
    long position = range.getOffset();
    while (readBytes < length) {
      checkIfVectoredIOStopped();
      int readBytesCurr = objectContent.read(dest,
              offset + readBytes,
              length - readBytes);
      LOG.debug("read {} bytes from stream", readBytesCurr);
      if (readBytesCurr < 0) {
        throw new EOFException(
            String.format("HTTP stream closed before all bytes were read."
                    + " Expected %,d bytes but only read %,d bytes. Current position %,d"
                    + " (%s)",
                length, readBytes, position, range));
      }
      readBytes += readBytesCurr;
      position += readBytesCurr;

      // update io stats incrementally
      incrementBytesRead(readBytesCurr);
    }
  }

  /**
   * Read data from S3 with retries for the GET request
   * This also handles if file has been changed while the
   * http call is getting executed. If the file has been
   * changed RemoteFileChangedException is thrown.
   * @param operationName name of the operation for which get object on S3 is called.
   * @param position position of the object to be read from S3.
   * @param length length from position of the object to be read from S3.
   * @return S3Object result s3 object.
   * @throws IOException exception if any.
   * @throws InterruptedIOException if vectored io operation is stopped.
   * @throws RemoteFileChangedException if file has changed on the store.
   */
  @Retries.RetryTranslated
  private ResponseInputStream<GetObjectResponse> getS3Object(String operationName,
                                                             long position,
                                                             int length)
      throws IOException {
    final GetObjectRequest request = getCallbacks().newGetRequestBuilder(getKey())
        .range(S3AUtils.formatRange(position, position + length - 1))
        .applyMutation(changeTracker::maybeApplyConstraint)
        .build();
    DurationTracker tracker = getS3AStreamStatistics().initiateGetRequest();
    ResponseInputStream<GetObjectResponse> objectRange;
    Invoker invoker = getContext().getReadInvoker();
    try {
      objectRange = invoker.retry(operationName, getPathStr(), true,
        () -> {
          checkIfVectoredIOStopped();
          return getCallbacks().getObject(request);
        });

    } catch (IOException ex) {
      tracker.failed();
      throw ex;
    } finally {
      tracker.close();
    }
    changeTracker.processResponse(objectRange.response(), operationName,
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
      getS3AStreamStatistics().unbuffered();
      if (getInputPolicy().isAdaptive()) {
        S3AInputPolicy policy = S3AInputPolicy.Random;
        setInputPolicy(policy);
      }
    }
  }

  @Override
  public boolean hasCapability(String capability) {
    switch (toLowerCase(capability)) {
    case StreamCapabilities.IOSTATISTICS_CONTEXT:
    case StreamCapabilities.READAHEAD:
    case StreamCapabilities.UNBUFFER:
      return true;
    default:
      return super.hasCapability(capability);
    }
  }

  /**
   * Is the inner object stream open?
   * @return true if there is an active HTTP request to S3.
   */
  @VisibleForTesting
  public boolean isObjectStreamOpen() {
    return wrappedStream != null;
  }

  /**
   * Get the wrapped stream.
   * This is for testing only.
   *
   * @return the wrapped stream, or null if there is none.
   */
  @VisibleForTesting
  public ResponseInputStream<GetObjectResponse> getWrappedStream() {
    return wrappedStream;
  }

}

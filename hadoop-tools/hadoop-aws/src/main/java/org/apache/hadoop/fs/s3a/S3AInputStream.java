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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.SSECustomerKey;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
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

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketTimeoutException;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
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
  private final AmazonS3 client;
  private final String bucket;
  private final String key;
  private final String pathStr;
  private final long contentLength;
  private final String uri;
  private static final Logger LOG =
      LoggerFactory.getLogger(S3AInputStream.class);
  private final S3AInputStreamStatistics streamStatistics;
  private S3AEncryptionMethods serverSideEncryptionAlgorithm;
  private String serverSideEncryptionKey;
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
   */
  public S3AInputStream(S3AReadOpContext ctx,
      S3ObjectAttributes s3Attributes,
      AmazonS3 client) {
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
    this.serverSideEncryptionAlgorithm =
        s3Attributes.getServerSideEncryptionAlgorithm();
    this.serverSideEncryptionKey = s3Attributes.getServerSideEncryptionKey();
    this.changeTracker = new ChangeTracker(uri,
        ctx.getChangeDetectionPolicy(),
        streamStatistics.getChangeTrackerStatistics(),
        s3Attributes);
    setInputPolicy(ctx.getInputPolicy());
    setReadahead(ctx.getReadahead());
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
    GetObjectRequest request = new GetObjectRequest(bucket, key)
        .withRange(targetPos, contentRangeFinish - 1);
    if (S3AEncryptionMethods.SSE_C.equals(serverSideEncryptionAlgorithm) &&
        StringUtils.isNotBlank(serverSideEncryptionKey)){
      request.setSSECustomerKey(new SSECustomerKey(serverSideEncryptionKey));
    }
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

    // With S3Guard, the metadatastore gave us metadata for the file in
    // open(), so we use a slightly different retry policy, but only on initial
    // open.  After that, an exception generally means the file has changed
    // and there is no point retrying anymore.
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
  @Retries.RetryTranslated  // Some retries only happen w/ S3Guard, as intended.
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

    // With S3Guard, the metadatastore gave us metadata for the file in
    // open(), so we use a slightly different retry policy.
    // read() may not be likely to fail, but reopen() does a GET which
    // certainly could.
    Invoker invoker = context.getReadInvoker();
    int byteRead = invoker.retry("read", pathStr, true,
        () -> {
          int b;
          try {
            b = wrappedStream.read();
          } catch (EOFException e) {
            return -1;
          } catch (SocketTimeoutException e) {
            onReadFailure(e, 1, true);
            b = wrappedStream.read();
          } catch (IOException e) {
            onReadFailure(e, 1, false);
            b = wrappedStream.read();
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
   * Handle an IOE on a read by attempting to re-open the stream.
   * The filesystem's readException count will be incremented.
   * @param ioe exception caught.
   * @param length length of data being attempted to read
   * @throws IOException any exception thrown on the re-open attempt.
   */
  @Retries.OnceTranslated
  private void onReadFailure(IOException ioe, int length, boolean forceAbort)
          throws IOException {
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
    reopen("failure recovery", pos, length, forceAbort);
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
  @Retries.RetryTranslated  // Some retries only happen w/ S3Guard, as intended.
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

    // With S3Guard, the metadatastore gave us metadata for the file in
    // open(), so we use a slightly different retry policy.
    // read() may not be likely to fail, but reopen() does a GET which
    // certainly could.
    Invoker invoker = context.getReadInvoker();

    streamStatistics.readOperationStarted(nextReadPos, len);
    int bytesRead = invoker.retry("read", pathStr, true,
        () -> {
          int bytes;
          try {
            bytes = wrappedStream.read(buf, off, len);
          } catch (EOFException e) {
            // the base implementation swallows EOFs.
            return -1;
          } catch (SocketTimeoutException e) {
            onReadFailure(e, len, true);
            bytes = wrappedStream.read(buf, off, len);
          } catch (IOException e) {
            onReadFailure(e, len, false);
            bytes= wrappedStream.read(buf, off, len);
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
  @Retries.RetryTranslated  // Some retries only happen w/ S3Guard, as intended.
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
}

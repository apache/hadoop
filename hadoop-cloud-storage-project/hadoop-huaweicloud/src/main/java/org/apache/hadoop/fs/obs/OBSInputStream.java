/*
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

package org.apache.hadoop.fs.obs;

import org.apache.hadoop.util.Preconditions;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.GetObjectRequest;
import com.sun.istack.NotNull;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static org.apache.hadoop.fs.obs.OBSCommonUtils.translateException;

/**
 * Input stream for an OBS object.
 *
 * <p>As this stream seeks withing an object, it may close then re-open the
 * stream. When this happens, any updated stream data may be retrieved, and,
 * given the consistency model of Huawei OBS, outdated data may in fact be
 * picked up.
 *
 * <p>As a result, the outcome of reading from a stream of an object which is
 * actively manipulated during the read process is "undefined".
 *
 * <p>The class is marked as private as code should not be creating instances
 * themselves. Any extra feature (e.g instrumentation) should be considered
 * unstable.
 *
 * <p>Because it prints some of the state of the instrumentation, the output of
 * {@link #toString()} must also be considered unstable.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class OBSInputStream extends FSInputStream
    implements CanSetReadahead, ByteBufferReadable {
  /**
   * Class logger.
   */
  public static final Logger LOG = LoggerFactory.getLogger(
      OBSInputStream.class);

  /**
   * Read retry times.
   */
  private static final int READ_RETRY_TIME = 3;

  /**
   * Seek retry times.
   */
  private static final int SEEK_RETRY_TIME = 9;

  /**
   * Delay times.
   */
  private static final long DELAY_TIME = 10;

  /**
   * The statistics for OBS file system.
   */
  private final FileSystem.Statistics statistics;

  /**
   * Obs client.
   */
  private final ObsClient client;

  /**
   * Bucket name.
   */
  private final String bucket;

  /**
   * Bucket key.
   */
  private final String key;

  /**
   * Content length.
   */
  private final long contentLength;

  /**
   * Object uri.
   */
  private final String uri;

  /**
   * Obs file system instance.
   */
  private OBSFileSystem fs;

  /**
   * This is the public position; the one set in {@link #seek(long)} and
   * returned in {@link #getPos()}.
   */
  private long streamCurrentPos;

  /**
   * Closed bit. Volatile so reads are non-blocking. Updates must be in a
   * synchronized block to guarantee an atomic check and set
   */
  private volatile boolean closed;

  /**
   * Input stream.
   */
  private InputStream wrappedStream = null;

  /**
   * Read ahead range.
   */
  private long readAheadRange = OBSConstants.DEFAULT_READAHEAD_RANGE;

  /**
   * This is the actual position within the object, used by lazy seek to decide
   * whether to seek on the next read or not.
   */
  private long nextReadPos;

  /**
   * The end of the content range of the last request. This is an absolute value
   * of the range, not a length field.
   */
  private long contentRangeFinish;

  /**
   * The start of the content range of the last request.
   */
  private long contentRangeStart;

  OBSInputStream(
      final String bucketName,
      final String bucketKey,
      final long fileStatusLength,
      final ObsClient obsClient,
      final FileSystem.Statistics stats,
      final long readaheadRange,
      final OBSFileSystem obsFileSystem) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(bucketName),
        "No Bucket");
    Preconditions.checkArgument(StringUtils.isNotEmpty(bucketKey),
        "No Key");
    Preconditions.checkArgument(fileStatusLength >= 0,
        "Negative content length");
    this.bucket = bucketName;
    this.key = bucketKey;
    this.contentLength = fileStatusLength;
    this.client = obsClient;
    this.statistics = stats;
    this.uri = "obs://" + this.bucket + "/" + this.key;
    this.fs = obsFileSystem;
    setReadahead(readaheadRange);
  }

  /**
   * Calculate the limit for a get request, based on input policy and state of
   * object.
   *
   * @param targetPos     position of the read
   * @param length        length of bytes requested; if less than zero
   *                      "unknown"
   * @param contentLength total length of file
   * @param readahead     current readahead value
   * @return the absolute value of the limit of the request.
   */
  static long calculateRequestLimit(
      final long targetPos, final long length, final long contentLength,
      final long readahead) {
    // cannot read past the end of the object
    return Math.min(contentLength, length < 0 ? contentLength
        : targetPos + Math.max(readahead, length));
  }

  /**
   * Opens up the stream at specified target position and for given length.
   *
   * @param reason    reason for reopen
   * @param targetPos target position
   * @param length    length requested
   * @throws IOException on any failure to open the object
   */
  private synchronized void reopen(final String reason, final long targetPos,
      final long length)
      throws IOException {
    long startTime = System.currentTimeMillis();
    long threadId = Thread.currentThread().getId();
    if (wrappedStream != null) {
      closeStream("reopen(" + reason + ")", contentRangeFinish);
    }

    contentRangeFinish =
        calculateRequestLimit(targetPos, length, contentLength,
            readAheadRange);

    try {
      GetObjectRequest request = new GetObjectRequest(bucket, key);
      request.setRangeStart(targetPos);
      request.setRangeEnd(contentRangeFinish);
      if (fs.getSse().isSseCEnable()) {
        request.setSseCHeader(fs.getSse().getSseCHeader());
      }
      wrappedStream = client.getObject(request).getObjectContent();
      contentRangeStart = targetPos;
      if (wrappedStream == null) {
        throw new IOException(
            "Null IO stream from reopen of (" + reason + ") " + uri);
      }
    } catch (ObsException e) {
      throw translateException("Reopen at position " + targetPos, uri, e);
    }

    this.streamCurrentPos = targetPos;
    long endTime = System.currentTimeMillis();
    LOG.debug(
        "reopen({}) for {} range[{}-{}], length={},"
            + " streamPosition={}, nextReadPosition={}, thread={}, "
            + "timeUsedInMilliSec={}",
        uri,
        reason,
        targetPos,
        contentRangeFinish,
        length,
        streamCurrentPos,
        nextReadPos,
        threadId,
        endTime - startTime
    );
  }

  @Override
  public synchronized long getPos() {
    return nextReadPos < 0 ? 0 : nextReadPos;
  }

  @Override
  public synchronized void seek(final long targetPos) throws IOException {
    checkNotClosed();

    // Do not allow negative seek
    if (targetPos < 0) {
      throw new EOFException(
          FSExceptionMessages.NEGATIVE_SEEK + " " + targetPos);
    }

    if (this.contentLength <= 0) {
      return;
    }

    // Lazy seek
    nextReadPos = targetPos;
  }

  /**
   * Seek without raising any exception. This is for use in {@code finally}
   * clauses
   *
   * @param positiveTargetPos a target position which must be positive.
   */
  private void seekQuietly(final long positiveTargetPos) {
    try {
      seek(positiveTargetPos);
    } catch (IOException ioe) {
      LOG.debug("Ignoring IOE on seek of {} to {}", uri,
          positiveTargetPos, ioe);
    }
  }

  /**
   * Adjust the stream to a specific position.
   *
   * @param targetPos target seek position
   * @throws IOException on any failure to seek
   */
  private void seekInStream(final long targetPos) throws IOException {
    checkNotClosed();
    if (wrappedStream == null) {
      return;
    }
    // compute how much more to skip
    long diff = targetPos - streamCurrentPos;
    if (diff > 0) {
      // forward seek -this is where data can be skipped

      int available = wrappedStream.available();
      // always seek at least as far as what is available
      long forwardSeekRange = Math.max(readAheadRange, available);
      // work out how much is actually left in the stream
      // then choose whichever comes first: the range or the EOF
      long remainingInCurrentRequest = remainingInCurrentRequest();

      long forwardSeekLimit = Math.min(remainingInCurrentRequest,
          forwardSeekRange);
      boolean skipForward = remainingInCurrentRequest > 0
          && diff <= forwardSeekLimit;
      if (skipForward) {
        // the forward seek range is within the limits
        LOG.debug("Forward seek on {}, of {} bytes", uri, diff);
        long skippedOnce = wrappedStream.skip(diff);
        while (diff > 0 && skippedOnce > 0) {
          streamCurrentPos += skippedOnce;
          diff -= skippedOnce;
          incrementBytesRead(skippedOnce);
          skippedOnce = wrappedStream.skip(diff);
        }

        if (streamCurrentPos == targetPos) {
          // all is well
          return;
        } else {
          // log a warning; continue to attempt to re-open
          LOG.info("Failed to seek on {} to {}. Current position {}",
              uri, targetPos, streamCurrentPos);
        }
      }
    } else if (diff == 0 && remainingInCurrentRequest() > 0) {
      // targetPos == streamCurrentPos
      // if there is data left in the stream, keep going
      return;
    }

    // if the code reaches here, the stream needs to be reopened.
    // close the stream; if read the object will be opened at the
    // new streamCurrentPos
    closeStream("seekInStream()", this.contentRangeFinish);
    streamCurrentPos = targetPos;
  }

  @Override
  public boolean seekToNewSource(final long targetPos) {
    return false;
  }

  /**
   * Perform lazy seek and adjust stream to correct position for reading.
   *
   * @param targetPos position from where data should be read
   * @param len       length of the content that needs to be read
   * @throws IOException on any failure to lazy seek
   */
  private void lazySeek(final long targetPos, final long len)
      throws IOException {
    for (int i = 0; i < SEEK_RETRY_TIME; i++) {
      try {
        // For lazy seek
        seekInStream(targetPos);

        // re-open at specific location if needed
        if (wrappedStream == null) {
          reopen("read from new offset", targetPos, len);
        }

        break;
      } catch (IOException e) {
        if (wrappedStream != null) {
          closeStream("lazySeek() seekInStream has exception ",
              this.contentRangeFinish);
        }
        Throwable cause = e.getCause();
        if (cause instanceof ObsException) {
          ObsException obsException = (ObsException) cause;
          int status = obsException.getResponseCode();
          switch (status) {
          case OBSCommonUtils.UNAUTHORIZED_CODE:
          case OBSCommonUtils.FORBIDDEN_CODE:
          case OBSCommonUtils.NOT_FOUND_CODE:
          case OBSCommonUtils.GONE_CODE:
          case OBSCommonUtils.EOF_CODE:
            throw e;
          default:
            break;
          }
        }

        LOG.warn("IOException occurred in lazySeek, retry: {}", i, e);
        if (i == SEEK_RETRY_TIME - 1) {
          throw e;
        }
        try {
          Thread.sleep(DELAY_TIME);
        } catch (InterruptedException ie) {
          throw e;
        }
      }
    }
  }

  /**
   * Increment the bytes read counter if there is a stats instance and the
   * number of bytes read is more than zero.
   *
   * @param bytesRead number of bytes read
   */
  private void incrementBytesRead(final long bytesRead) {
    if (statistics != null && bytesRead > 0) {
      statistics.incrementBytesRead(bytesRead);
    }
  }

  private void sleepInLock() throws InterruptedException {
    long start = System.currentTimeMillis();
    long now = start;
    while (now - start < OBSInputStream.DELAY_TIME) {
      wait(start + OBSInputStream.DELAY_TIME - now);
      now = System.currentTimeMillis();
    }
  }

  @Override
  public synchronized int read() throws IOException {
    long startTime = System.currentTimeMillis();
    long threadId = Thread.currentThread().getId();
    checkNotClosed();
    if (this.contentLength == 0 || nextReadPos >= contentLength) {
      return -1;
    }

    int byteRead = -1;
    try {
      lazySeek(nextReadPos, 1);
    } catch (EOFException e) {
      onReadFailure(e, 1);
      return -1;
    }

    IOException exception = null;
    for (int retryTime = 1; retryTime <= READ_RETRY_TIME; retryTime++) {
      try {
        byteRead = wrappedStream.read();
        exception = null;
        break;
      } catch (EOFException e) {
        onReadFailure(e, 1);
        return -1;
      } catch (IOException e) {
        exception = e;
        onReadFailure(e, 1);
        LOG.warn(
            "read of [{}] failed, retry time[{}], due to exception[{}]",
            uri, retryTime, exception);
        if (retryTime < READ_RETRY_TIME) {
          try {
            sleepInLock();
          } catch (InterruptedException ie) {
            LOG.error(
                "read of [{}] failed, retry time[{}], due to "
                    + "exception[{}]",
                uri, retryTime,
                exception);
            throw exception;
          }
        }
      }
    }

    if (exception != null) {
      LOG.error(
          "read of [{}] failed, retry time[{}], due to exception[{}]",
          uri, READ_RETRY_TIME, exception);
      throw exception;
    }

    if (byteRead >= 0) {
      streamCurrentPos++;
      nextReadPos++;
    }

    if (byteRead >= 0) {
      incrementBytesRead(1);
    }

    long endTime = System.currentTimeMillis();
    LOG.debug(
        "read-0arg uri:{}, contentLength:{}, position:{}, readValue:{}, "
            + "thread:{}, timeUsedMilliSec:{}",
        uri, contentLength, byteRead >= 0 ? nextReadPos - 1 : nextReadPos,
        byteRead, threadId,
        endTime - startTime);
    return byteRead;
  }

  /**
   * Handle an IOE on a read by attempting to re-open the stream. The
   * filesystem's readException count will be incremented.
   *
   * @param ioe    exception caught.
   * @param length length of data being attempted to read
   * @throws IOException any exception thrown on the re-open attempt.
   */
  private void onReadFailure(final IOException ioe, final int length)
      throws IOException {
    LOG.debug(
        "Got exception while trying to read from stream {}"
            + " trying to recover: " + ioe, uri);
    int i = 1;
    while (true) {
      try {
        reopen("failure recovery", streamCurrentPos, length);
        return;
      } catch (OBSIOException e) {
        LOG.warn(
            "OBSIOException occurred in reopen for failure recovery, "
                + "the {} retry time",
            i, e);
        if (i == READ_RETRY_TIME) {
          throw e;
        }
        try {
          Thread.sleep(DELAY_TIME);
        } catch (InterruptedException ie) {
          throw e;
        }
      }
      i++;
    }
  }

  @Override
  public synchronized int read(final ByteBuffer byteBuffer)
      throws IOException {
    long startTime = System.currentTimeMillis();
    long threadId = Thread.currentThread().getId();
    LOG.debug("read byteBuffer: {}", byteBuffer.toString());
    checkNotClosed();

    int len = byteBuffer.remaining();
    if (len == 0) {
      return 0;
    }

    byte[] buf = new byte[len];

    if (this.contentLength == 0 || nextReadPos >= contentLength) {
      return -1;
    }

    try {
      lazySeek(nextReadPos, len);
    } catch (EOFException e) {
      onReadFailure(e, len);
      // the end of the file has moved
      return -1;
    }

    int bytesRead = 0;
    IOException exception = null;
    for (int retryTime = 1; retryTime <= READ_RETRY_TIME; retryTime++) {
      try {
        bytesRead = tryToReadFromInputStream(wrappedStream, buf, 0,
            len);
        if (bytesRead == -1) {
          return -1;
        }
        exception = null;
        break;
      } catch (EOFException e) {
        onReadFailure(e, len);
        return -1;
      } catch (IOException e) {
        exception = e;
        onReadFailure(e, len);
        LOG.warn(
            "read len[{}] of [{}] failed, retry time[{}], "
                + "due to exception[{}]",
            len, uri, retryTime, exception);
        if (retryTime < READ_RETRY_TIME) {
          try {
            sleepInLock();
          } catch (InterruptedException ie) {
            LOG.error(
                "read len[{}] of [{}] failed, retry time[{}], "
                    + "due to exception[{}]",
                len, uri, retryTime, exception);
            throw exception;
          }
        }
      }
    }

    if (exception != null) {
      LOG.error(
          "read len[{}] of [{}] failed, retry time[{}], "
              + "due to exception[{}]",
          len, uri, READ_RETRY_TIME, exception);
      throw exception;
    }

    if (bytesRead > 0) {
      streamCurrentPos += bytesRead;
      nextReadPos += bytesRead;
      byteBuffer.put(buf, 0, bytesRead);
    }
    incrementBytesRead(bytesRead);

    long endTime = System.currentTimeMillis();
    LOG.debug(
        "Read-ByteBuffer uri:{}, contentLength:{}, destLen:{}, readLen:{}, "
            + "position:{}, thread:{}, timeUsedMilliSec:{}",
        uri, contentLength, len, bytesRead,
        bytesRead >= 0 ? nextReadPos - bytesRead : nextReadPos, threadId,
        endTime - startTime);
    return bytesRead;
  }

  private int tryToReadFromInputStream(final InputStream in, final byte[] buf,
      final int off, final int len) throws IOException {
    int bytesRead = 0;
    while (bytesRead < len) {
      int bytes = in.read(buf, off + bytesRead, len - bytesRead);
      if (bytes == -1) {
        if (bytesRead == 0) {
          return -1;
        } else {
          break;
        }
      }
      bytesRead += bytes;
    }

    return bytesRead;
  }

  /**
   * {@inheritDoc}
   *
   * <p>This updates the statistics on read operations started and whether or
   * not the read operation "completed", that is: returned the exact number of
   * bytes requested.
   *
   * @throws IOException if there are other problems
   */
  @Override
  public synchronized int read(@NotNull final byte[] buf, final int off,
      final int len) throws IOException {
    long startTime = System.currentTimeMillis();
    long threadId = Thread.currentThread().getId();
    checkNotClosed();
    validatePositionedReadArgs(nextReadPos, buf, off, len);
    if (len == 0) {
      return 0;
    }

    if (this.contentLength == 0 || nextReadPos >= contentLength) {
      return -1;
    }

    try {
      lazySeek(nextReadPos, len);
    } catch (EOFException e) {
      onReadFailure(e, len);
      // the end of the file has moved
      return -1;
    }

    int bytesRead = 0;
    IOException exception = null;
    for (int retryTime = 1; retryTime <= READ_RETRY_TIME; retryTime++) {
      try {
        bytesRead = tryToReadFromInputStream(wrappedStream, buf, off,
            len);
        if (bytesRead == -1) {
          return -1;
        }
        exception = null;
        break;
      } catch (EOFException e) {
        onReadFailure(e, len);
        return -1;
      } catch (IOException e) {
        exception = e;
        onReadFailure(e, len);
        LOG.warn(
            "read offset[{}] len[{}] of [{}] failed, retry time[{}], "
                + "due to exception[{}]",
            off, len, uri, retryTime, exception);
        if (retryTime < READ_RETRY_TIME) {
          try {
            sleepInLock();
          } catch (InterruptedException ie) {
            LOG.error(
                "read offset[{}] len[{}] of [{}] failed, "
                    + "retry time[{}], due to exception[{}]",
                off, len, uri, retryTime, exception);
            throw exception;
          }
        }
      }
    }

    if (exception != null) {
      LOG.error(
          "read offset[{}] len[{}] of [{}] failed, retry time[{}], "
              + "due to exception[{}]",
          off, len, uri, READ_RETRY_TIME, exception);
      throw exception;
    }

    if (bytesRead > 0) {
      streamCurrentPos += bytesRead;
      nextReadPos += bytesRead;
    }
    incrementBytesRead(bytesRead);

    long endTime = System.currentTimeMillis();
    LOG.debug(
        "Read-3args uri:{}, contentLength:{}, destLen:{}, readLen:{}, "
            + "position:{}, thread:{}, timeUsedMilliSec:{}",
        uri, contentLength, len, bytesRead,
        bytesRead >= 0 ? nextReadPos - bytesRead : nextReadPos, threadId,
        endTime - startTime);
    return bytesRead;
  }

  /**
   * Verify that the input stream is open. Non blocking; this gives the last
   * state of the volatile {@link #closed} field.
   *
   * @throws IOException if the connection is closed.
   */
  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException(
          uri + ": " + FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }

  /**
   * Close the stream. This triggers publishing of the stream statistics back to
   * the filesystem statistics. This operation is synchronized, so that only one
   * thread can attempt to close the connection; all later/blocked calls are
   * no-ops.
   *
   * @throws IOException on any problem
   */
  @Override
  public synchronized void close() throws IOException {
    if (!closed) {
      closed = true;
      // close or abort the stream
      closeStream("close() operation", this.contentRangeFinish);
      // this is actually a no-op
      super.close();
    }
  }

  /**
   * Close a stream: decide whether to abort or close, based on the length of
   * the stream and the current position. If a close() is attempted and fails,
   * the operation escalates to an abort.
   *
   * <p>This does not set the {@link #closed} flag.
   *
   * @param reason reason for stream being closed; used in messages
   * @param length length of the stream
   * @throws IOException on any failure to close stream
   */
  private synchronized void closeStream(final String reason,
      final long length)
      throws IOException {
    if (wrappedStream != null) {
      try {
        wrappedStream.close();
      } catch (IOException e) {
        // exception escalates to an abort
        LOG.debug("When closing {} stream for {}", uri, reason, e);
        throw e;
      }

      LOG.debug(
          "Stream {} : {}; streamPos={}, nextReadPos={},"
              + " request range {}-{} length={}",
          uri,
          reason,
          streamCurrentPos,
          nextReadPos,
          contentRangeStart,
          contentRangeFinish,
          length);
      wrappedStream = null;
    }
  }

  @Override
  public synchronized int available() throws IOException {
    checkNotClosed();

    long remaining = remainingInFile();
    if (remaining > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int) remaining;
  }

  /**
   * Bytes left in stream.
   *
   * @return how many bytes are left to read
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public synchronized long remainingInFile() {
    return this.contentLength - this.streamCurrentPos;
  }

  /**
   * Bytes left in the current request. Only valid if there is an active
   * request.
   *
   * @return how many bytes are left to read in the current GET.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public synchronized long remainingInCurrentRequest() {
    return this.contentRangeFinish - this.streamCurrentPos;
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  /**
   * String value includes statistics as well as stream state. <b>Important:
   * there are no guarantees as to the stability of this value.</b>
   *
   * @return a string value for printing in logs/diagnostics
   */
  @Override
  @InterfaceStability.Unstable
  public String toString() {
    synchronized (this) {
      return "OBSInputStream{" + uri
          + " wrappedStream=" + (wrappedStream != null
          ? "open"
          : "closed")
          + " streamCurrentPos=" + streamCurrentPos
          + " nextReadPos=" + nextReadPos
          + " contentLength=" + contentLength
          + " contentRangeStart=" + contentRangeStart
          + " contentRangeFinish=" + contentRangeFinish
          + " remainingInCurrentRequest=" + remainingInCurrentRequest()
          + '}';
    }
  }

  /**
   * Subclass {@code readFully()} operation which only seeks at the start of the
   * series of operations; seeking back at the end.
   *
   * <p>This is significantly higher performance if multiple read attempts
   * are needed to fetch the data, as it does not break the HTTP connection.
   *
   * <p>To maintain thread safety requirements, this operation is
   * synchronized for the duration of the sequence. {@inheritDoc}
   */
  @Override
  public void readFully(final long position, final byte[] buffer,
      final int offset,
      final int length)
      throws IOException {
    long startTime = System.currentTimeMillis();
    long threadId = Thread.currentThread().getId();
    checkNotClosed();
    validatePositionedReadArgs(position, buffer, offset, length);
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
            throw new EOFException(
                FSExceptionMessages.EOF_IN_READ_FULLY);
          }
          nread += nbytes;
        }
      } finally {
        seekQuietly(oldPos);
      }
    }

    long endTime = System.currentTimeMillis();
    LOG.debug(
        "ReadFully uri:{}, contentLength:{}, destLen:{}, readLen:{}, "
            + "position:{}, thread:{}, timeUsedMilliSec:{}",
        uri, contentLength, length, nread, position, threadId,
        endTime - startTime);
  }

  /**
   * Read bytes starting from the specified position.
   *
   * @param position start read from this position
   * @param buffer   read buffer
   * @param offset   offset into buffer
   * @param length   number of bytes to read
   * @return actual number of bytes read
   * @throws IOException on any failure to read
   */
  @Override
  public int read(final long position, final byte[] buffer, final int offset,
      final int length)
      throws IOException {
    int len = length;
    checkNotClosed();
    validatePositionedReadArgs(position, buffer, offset, len);
    if (position < 0 || position >= contentLength) {
      return -1;
    }
    if ((position + len) > contentLength) {
      len = (int) (contentLength - position);
    }

    if (fs.isReadTransformEnabled()) {
      return super.read(position, buffer, offset, len);
    }

    return randomReadWithNewInputStream(position, buffer, offset, len);
  }

  private int randomReadWithNewInputStream(final long position,
      final byte[] buffer, final int offset, final int length)
      throws IOException {
    long startTime = System.currentTimeMillis();
    long threadId = Thread.currentThread().getId();
    int bytesRead = 0;
    InputStream inputStream = null;
    IOException exception = null;
    GetObjectRequest request = new GetObjectRequest(bucket, key);
    request.setRangeStart(position);
    request.setRangeEnd(position + length);
    if (fs.getSse().isSseCEnable()) {
      request.setSseCHeader(fs.getSse().getSseCHeader());
    }

    for (int retryTime = 1; retryTime <= READ_RETRY_TIME; retryTime++) {
      try {
        inputStream = client.getObject(request).getObjectContent();
        if (inputStream == null) {
          break;
        }
        bytesRead = tryToReadFromInputStream(inputStream, buffer,
            offset, length);
        if (bytesRead == -1) {
          return -1;
        }

        exception = null;
        break;
      } catch (ObsException | IOException e) {
        if (e instanceof ObsException) {
          exception = translateException(
              "Read at position " + position, uri, (ObsException) e);
        } else {
          exception = (IOException) e;
        }
        LOG.warn(
            "read position[{}] destLen[{}] offset[{}] readLen[{}] "
                + "of [{}] failed, retry time[{}], due to "
                + "exception[{}] e[{}]",
            position, length, offset, bytesRead, uri, retryTime,
            exception, e);
        if (retryTime < READ_RETRY_TIME) {
          try {
            Thread.sleep(DELAY_TIME);
          } catch (InterruptedException ie) {
            LOG.error(
                "read position[{}] destLen[{}] offset[{}] "
                    + "readLen[{}] of [{}] failed, retry time[{}], "
                    + "due to exception[{}] e[{}]",
                position, length, offset, bytesRead, uri, retryTime,
                exception, e);
            throw exception;
          }
        }
      } finally {
        if (inputStream != null) {
          inputStream.close();
        }
      }
    }

    if (inputStream == null || exception != null) {
      LOG.error(
          "read position[{}] destLen[{}] offset[{}] len[{}] failed, "
              + "retry time[{}], due to exception[{}]",
          position, length, offset, bytesRead, READ_RETRY_TIME,
          exception);
      throw new IOException("read failed of " + uri + ", inputStream is "
          + (inputStream == null ? "null" : "not null"), exception);

    }

    long endTime = System.currentTimeMillis();
    LOG.debug(
        "Read-4args uri:{}, contentLength:{}, destLen:{}, readLen:{}, "
            + "position:{}, thread:{}, timeUsedMilliSec:{}",
        uri, contentLength, length, bytesRead, position, threadId,
        endTime - startTime);
    return bytesRead;
  }

  @Override
  public synchronized void setReadahead(final Long newReadaheadRange) {
    if (newReadaheadRange == null) {
      this.readAheadRange = OBSConstants.DEFAULT_READAHEAD_RANGE;
    } else {
      Preconditions.checkArgument(newReadaheadRange >= 0,
          "Negative readahead value");
      this.readAheadRange = newReadaheadRange;
    }
  }
}

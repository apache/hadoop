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

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

import org.slf4j.Logger;

import java.io.EOFException;
import java.io.IOException;

import static org.apache.hadoop.fs.s3a.S3AUtils.*;

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
public class S3AInputStream extends FSInputStream implements CanSetReadahead {
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
  private S3ObjectInputStream wrappedStream;
  private final FileSystem.Statistics stats;
  private final AmazonS3Client client;
  private final String bucket;
  private final String key;
  private final long contentLength;
  private final String uri;
  public static final Logger LOG = S3AFileSystem.LOG;
  public static final long CLOSE_THRESHOLD = 4096;
  private final S3AInstrumentation.InputStreamStatistics streamStatistics;
  private long readahead;

  /**
   * This is the actual position within the object, used by
   * lazy seek to decide whether to seek on the next read or not.
   */
  private long nextReadPos;

  /* Amount of data desired from the request */
  private long requestedStreamLen;

  public S3AInputStream(String bucket,
      String key,
      long contentLength,
      AmazonS3Client client,
      FileSystem.Statistics stats,
      S3AInstrumentation instrumentation,
      long readahead) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(bucket), "No Bucket");
    Preconditions.checkArgument(StringUtils.isNotEmpty(key), "No Key");
    Preconditions.checkArgument(contentLength >= 0 , "Negative content length");
    this.bucket = bucket;
    this.key = key;
    this.contentLength = contentLength;
    this.client = client;
    this.stats = stats;
    this.uri = "s3a://" + this.bucket + "/" + this.key;
    this.streamStatistics = instrumentation.newInputStreamStatistics();
    setReadahead(readahead);
  }

  /**
   * Opens up the stream at specified target position and for given length.
   *
   * @param reason reason for reopen
   * @param targetPos target position
   * @param length length requested
   * @throws IOException on any failure to open the object
   */
  private synchronized void reopen(String reason, long targetPos, long length)
      throws IOException {
    requestedStreamLen = this.contentLength;

    if (wrappedStream != null) {
      closeStream("reopen(" + reason + ")", requestedStreamLen);
    }
    LOG.debug("reopen({}) for {} at targetPos={}, length={}," +
        " requestedStreamLen={}, streamPosition={}, nextReadPosition={}",
        uri, reason, targetPos, length, requestedStreamLen, pos, nextReadPos);

    streamStatistics.streamOpened();
    try {
      GetObjectRequest request = new GetObjectRequest(bucket, key)
          .withRange(targetPos, requestedStreamLen);
      wrappedStream = client.getObject(request).getObjectContent();

      if (wrappedStream == null) {
        throw new IOException("Null IO stream from reopen of (" + reason +  ") "
            + uri);
      }
    } catch (AmazonClientException e) {
      throw translateException("Reopen at position " + targetPos, uri, e);
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
      long forwardSeekLimit = Math.min(remaining(), forwardSeekRange);
      if (diff <= forwardSeekLimit) {
        // the forward seek range is within the limits
        LOG.debug("Forward seek on {}, of {} bytes", uri, diff);
        streamStatistics.seekForwards(diff);
        long skipped = wrappedStream.skip(diff);
        if (skipped > 0) {
          pos += skipped;
          // as these bytes have been read, they are included in the counter
          incrementBytesRead(diff);
        }

        if (pos == targetPos) {
          // all is well
          return;
        } else {
          // log a warning; continue to attempt to re-open
          LOG.warn("Failed to seek on {} to {}. Current position {}",
              uri, targetPos,  pos);
        }
      }
    } else if (diff < 0) {
      // backwards seek
      streamStatistics.seekBackwards(diff);
    } else {
      // targetPos == pos
      // this should never happen as the caller filters it out.
      // Retained just in case
      LOG.debug("Ignoring seek {} to {} as target position == current",
          uri, targetPos);
    }

    // close the stream; if read the object will be opened at the new pos
    closeStream("seekInStream()", this.requestedStreamLen);
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
  private void lazySeek(long targetPos, long len) throws IOException {
    //For lazy seek
    if (targetPos != this.pos) {
      seekInStream(targetPos, len);
    }

    //re-open at specific location if needed
    if (wrappedStream == null) {
      reopen("read from new offset", targetPos, len);
    }
  }

  /**
   * Increment the bytes read counter if there is a stats instance
   * and the number of bytes read is more than zero.
   * @param bytesRead number of bytes read
   */
  private void incrementBytesRead(long bytesRead) {
    streamStatistics.bytesRead(bytesRead);
    if (stats != null && bytesRead > 0) {
      stats.incrementBytesRead(bytesRead);
    }
  }

  @Override
  public synchronized int read() throws IOException {
    checkNotClosed();
    if (this.contentLength == 0 || (nextReadPos >= contentLength)) {
      return -1;
    }


    int byteRead;
    try {
      lazySeek(nextReadPos, 1);
      byteRead = wrappedStream.read();
    } catch (EOFException e) {
      return -1;
    } catch (IOException e) {
      onReadFailure(e, 1);
      byteRead = wrappedStream.read();
    }

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
  private void onReadFailure(IOException ioe, int length) throws IOException {
    LOG.info("Got exception while trying to read from stream {}"
        + " trying to recover: "+ ioe, uri);
    LOG.debug("While trying to read from stream {}", uri, ioe);
    streamStatistics.readException();
    reopen("failure recovery", pos, length);
  }

  /**
   * {@inheritDoc}
   *
   * This updates the statistics on read operations started and whether
   * or not the read operation "completed", that is: returned the exact
   * number of bytes requested.
   * @throws EOFException if there is no more data
   * @throws IOException if there are other problems
   */
  @Override
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

    int bytesRead;
    try {
      streamStatistics.readOperationStarted(nextReadPos, len);
      bytesRead = wrappedStream.read(buf, off, len);
    } catch (EOFException e) {
      throw e;
    } catch (IOException e) {
      onReadFailure(e, len);
      bytesRead = wrappedStream.read(buf, off, len);
    }

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
        closeStream("close() operation", this.contentLength);
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
   *
   * @param reason reason for stream being closed; used in messages
   * @param length length of the stream.
   */
  private void closeStream(String reason, long length) {
    if (wrappedStream != null) {
      boolean shouldAbort = length - pos > CLOSE_THRESHOLD;
      if (!shouldAbort) {
        try {
          // clean close. This will read to the end of the stream,
          // so, while cleaner, can be pathological on a multi-GB object
          wrappedStream.close();
          streamStatistics.streamClose(false);
        } catch (IOException e) {
          // exception escalates to an abort
          LOG.debug("When closing {} stream for {}", uri, reason, e);
          shouldAbort = true;
        }
      }
      if (shouldAbort) {
        // Abort, rather than just close, the underlying stream.  Otherwise, the
        // remaining object payload is read from S3 while closing the stream.
        wrappedStream.abort();
        streamStatistics.streamClose(true);
      }
      LOG.debug("Stream {} {}: {}; streamPos={}, nextReadPos={}," +
          " length={}",
          uri, (shouldAbort ? "aborted":"closed"), reason, pos, nextReadPos,
          length);
      wrappedStream = null;
    }
  }

  @Override
  public synchronized int available() throws IOException {
    checkNotClosed();

    long remaining = remaining();
    if (remaining > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int)remaining;
  }

  /**
   * Bytes left in stream.
   * @return how many bytes are left to read
   */
  protected long remaining() {
    return this.contentLength - this.pos;
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
    final StringBuilder sb = new StringBuilder(
        "S3AInputStream{");
    sb.append(uri);
    sb.append(" pos=").append(pos);
    sb.append(" nextReadPos=").append(nextReadPos);
    sb.append(" contentLength=").append(contentLength);
    sb.append(" ").append(streamStatistics.toString());
    sb.append('}');
    return sb.toString();
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
  public S3AInstrumentation.InputStreamStatistics getS3AStreamStatistics() {
    return streamStatistics;
  }

  @Override
  public void setReadahead(Long readahead) {
    if (readahead == null) {
      this.readahead = Constants.DEFAULT_READAHEAD_RANGE;
    } else {
      Preconditions.checkArgument(readahead >= 0, "Negative readahead value");
      this.readahead = readahead;
    }
  }

  /**
   * Get the current readahead value.
   * @return a non-negative readahead value
   */
  public long getReadahead() {
    return readahead;
  }
}

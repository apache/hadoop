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

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

import org.slf4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.SocketException;

public class S3AInputStream extends FSInputStream {
  private long pos;
  private boolean closed;
  private S3ObjectInputStream wrappedStream;
  private final FileSystem.Statistics stats;
  private final AmazonS3Client client;
  private final String bucket;
  private final String key;
  private final long contentLength;
  private final String uri;
  public static final Logger LOG = S3AFileSystem.LOG;
  public static final long CLOSE_THRESHOLD = 4096;

  // Used by lazy seek
  private long nextReadPos;

  //Amount of data requested from the request
  private long requestedStreamLen;

  public S3AInputStream(String bucket, String key, long contentLength,
      AmazonS3Client client, FileSystem.Statistics stats) {
    this.bucket = bucket;
    this.key = key;
    this.contentLength = contentLength;
    this.client = client;
    this.stats = stats;
    this.pos = 0;
    this.nextReadPos = 0;
    this.closed = false;
    this.wrappedStream = null;
    this.uri = "s3a://" + this.bucket + "/" + this.key;
  }

  /**
   * Opens up the stream at specified target position and for given length.
   *
   * @param targetPos target position
   * @param length length requested
   * @throws IOException
   */
  private synchronized void reopen(long targetPos, long length)
      throws IOException {
    requestedStreamLen = (length < 0) ? this.contentLength :
        Math.max(this.contentLength, (CLOSE_THRESHOLD + (targetPos + length)));

    if (wrappedStream != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Closing the previous stream");
      }
      closeStream(requestedStreamLen);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Requesting for "
          + "targetPos=" + targetPos
          + ", length=" + length
          + ", requestedStreamLen=" + requestedStreamLen
          + ", streamPosition=" + pos
          + ", nextReadPosition=" + nextReadPos
      );
    }

    GetObjectRequest request = new GetObjectRequest(bucket, key)
        .withRange(targetPos, requestedStreamLen);
    wrappedStream = client.getObject(request).getObjectContent();

    if (wrappedStream == null) {
      throw new IOException("Null IO stream");
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
    if (targetPos > pos) {
      if ((diff + length) <= wrappedStream.available()) {
        // already available in buffer
        pos += wrappedStream.skip(diff);
        if (pos != targetPos) {
          throw new IOException("Failed to seek to " + targetPos
              + ". Current position " + pos);
        }
        return;
      }
    }

    // close the stream; if read the object will be opened at the new pos
    closeStream(this.requestedStreamLen);
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
      reopen(targetPos, len);
    }
  }

  @Override
  public synchronized int read() throws IOException {
    checkNotClosed();
    if (this.contentLength == 0 || (nextReadPos >= contentLength)) {
      return -1;
    }

    lazySeek(nextReadPos, 1);

    int byteRead;
    try {
      byteRead = wrappedStream.read();
    } catch (SocketTimeoutException | SocketException e) {
      LOG.info("Got exception while trying to read from stream,"
          + " trying to recover " + e);
      reopen(pos, 1);
      byteRead = wrappedStream.read();
    } catch (EOFException e) {
      return -1;
    }

    if (byteRead >= 0) {
      pos++;
      nextReadPos++;
    }

    if (stats != null && byteRead >= 0) {
      stats.incrementBytesRead(1);
    }
    return byteRead;
  }

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

    lazySeek(nextReadPos, len);

    int byteRead;
    try {
      byteRead = wrappedStream.read(buf, off, len);
    } catch (SocketTimeoutException | SocketException e) {
      LOG.info("Got exception while trying to read from stream,"
          + " trying to recover " + e);
      reopen(pos, len);
      byteRead = wrappedStream.read(buf, off, len);
    }

    if (byteRead > 0) {
      pos += byteRead;
      nextReadPos += byteRead;
    }

    if (stats != null && byteRead > 0) {
      stats.incrementBytesRead(byteRead);
    }

    return byteRead;
  }

  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    super.close();
    closed = true;
    closeStream(this.contentLength);
  }

  /**
   * Close a stream: decide whether to abort or close, based on
   * the length of the stream and the current position.
   *
   * This does not set the {@link #closed} flag.
   * @param length length of the stream.
   * @throws IOException
   */
  private void closeStream(long length) throws IOException {
    if (wrappedStream != null) {
      String reason = null;
      boolean shouldAbort = length - pos > CLOSE_THRESHOLD;
      if (!shouldAbort) {
        try {
          reason = "Closed stream";
          wrappedStream.close();
        } catch (IOException e) {
          // exception escalates to an abort
          LOG.debug("When closing stream", e);
          shouldAbort = true;
        }
      }
      if (shouldAbort) {
        // Abort, rather than just close, the underlying stream.  Otherwise, the
        // remaining object payload is read from S3 while closing the stream.
        wrappedStream.abort();
        reason = "Closed stream with abort";
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(reason + "; streamPos=" + pos
            + ", nextReadPos=" + nextReadPos
            + ", contentLength=" + length);
      }
      wrappedStream = null;
    }
  }

  @Override
  public synchronized int available() throws IOException {
    checkNotClosed();

    long remaining = this.contentLength - this.pos;
    if (remaining > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int)remaining;
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "S3AInputStream{");
    sb.append(uri);
    sb.append(" pos=").append(pos);
    sb.append(" nextReadPos=").append(nextReadPos);
    sb.append(" contentLength=").append(contentLength);
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
        seek(oldPos);
      }
    }
  }
}

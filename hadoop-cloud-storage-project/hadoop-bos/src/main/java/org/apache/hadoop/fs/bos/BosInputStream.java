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

package org.apache.hadoop.fs.bos;

import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

/**
 * Input stream implementation for reading objects from BOS.
 * Supports lazy seeking and readahead buffering.
 */
public class BosInputStream extends FSInputStream
    implements CanSetReadahead {

  private static final Logger LOG =
      LoggerFactory.getLogger(BosInputStream.class);

  private FileSystem.Statistics statistics;
  private BosNativeFileSystemStore store;
  private static final long DEFAULT_READAHEAD_LEN =
      1024 * 1024;
  private volatile boolean closed;
  private final String key;
  private final long contentLength;
  private InputStream in = null;
  private long pos = 0;
  private FileMetadata fileMetaData;
  private long readahead = DEFAULT_READAHEAD_LEN;

  /**
   * This is the actual position within the object, used by
   * lazy seek to decide whether to seek on the next read or
   * not.
   */
  private long nextReadPos = 0;

  /**
   * The start of the content range of the last request.
   */
  private long contentRangeStart = 0;

  /**
   * The end of the content range of the last request.
   * This is an absolute value of the range, not a length
   * field.
   */
  private long contentRangeFinish = 0;

  /**
   * Constructs a BosInputStream for the given object key.
   *
   * @param key        the object key in BOS
   * @param fileMetaData the file metadata
   * @param store      the native file system store
   * @param statistics the file system statistics
   */
  public BosInputStream(
      String key, FileMetadata fileMetaData,
      BosNativeFileSystemStore store,
      FileSystem.Statistics statistics) {
    this.key = key;
    this.fileMetaData = fileMetaData;
    this.contentLength = fileMetaData.getLength();
    this.store = store;
    this.statistics = statistics;
  }

  /**
   * Reads a single byte from the stream.
   *
   * @return the byte read, or -1 if end of stream
   * @throws IOException if an I/O error occurs
   */
  public synchronized int read() throws IOException {
    checkNotClosed();

    if (this.contentLength == 0
        || (nextReadPos >= contentLength)) {
      return -1;
    }

    int byteRead;
    try {
      lazySeek(nextReadPos, 1);
      byteRead = in.read();
    } catch (EOFException e) {
      return -1;
    } catch (IOException e) {
      onReadFailure(e, 1);
      byteRead = in.read();
    }

    if (byteRead >= 0) {
      pos++;
      nextReadPos++;
    }

    if (statistics != null && byteRead >= 0) {
      statistics.incrementBytesRead(1);
    }
    return byteRead;
  }

  /**
   * Reads bytes into a buffer from the stream.
   *
   * @param buf the buffer to read into
   * @param off the start offset in the buffer
   * @param len the maximum number of bytes to read
   * @return the number of bytes read, or -1 if end of stream
   * @throws IOException if an I/O error occurs
   */
  public synchronized int read(
      byte[] buf, int off, int len) throws IOException {
    checkNotClosed();

    if (len == 0) {
      return 0;
    }

    if (this.contentLength == 0
        || (nextReadPos >= contentLength)) {
      return -1;
    }

    // Validate and adjust parameters
    if (off < 0 || len < 0
        || len > buf.length - off) {
      throw new IndexOutOfBoundsException(
          String.format(
              "Invalid read parameters:"
                  + " buf.length=%d, off=%d, len=%d",
              buf.length, off, len));
    }

    try {
      lazySeek(nextReadPos, len);
    } catch (EOFException e) {
      // the end of the file has moved
      return -1;
    }

    int bytesRead;
    try {
      bytesRead = in.read(buf, off, len);
    } catch (EOFException e) {
      onReadFailure(e, len);
      // the base implementation swallows EOFs.
      return -1;
    } catch (IOException e) {
      onReadFailure(e, len);
      bytesRead = in.read(buf, off, len);
    } catch (Exception e) {
      int intervalSeconds = 3;
      int retry = 3;
      while (true) {
        try {
          LOG.info(
              "Read exception occur:"
                  + " retry at most {} times.",
              retry);
          // Reopen the stream at the current position
          // to recover from the exception
          onReadFailure(e, len);
          // Read from the reopened stream
          bytesRead = in.read(buf, off, len);
          break;
        } catch (EOFException eof) {
          return -1;
        } catch (IOException ioe) {
          // For IOException, try to reopen once more
          // and retry
          LOG.info(
              "IOException during retry,"
                  + " attempting to reopen stream");
          onReadFailure(ioe, len);
          bytesRead = in.read(buf, off, len);
          break;
        } catch (Exception ex) {
          if (retry <= 0) {
            String errorMsg =
                ex.getMessage() != null
                    ? ex.getMessage()
                    : ex.getClass().getSimpleName();
            throw new IOException(
                "Retry " + retry
                    + " times to read still"
                    + " exception: " + errorMsg,
                ex);
          }

          try {
            TimeUnit.SECONDS.sleep(
                intervalSeconds);
          } catch (InterruptedException ite) {
            Thread.currentThread().interrupt();
            throw new IOException(
                "Thread interrupted during retry", ite);
          }
          intervalSeconds *= 2;
          retry--;
          // Update e to ex for the next iteration
          e = ex;
        }
      }
    }

    if (bytesRead > 0) {
      pos += bytesRead;
      nextReadPos += bytesRead;
    }

    if (statistics != null && bytesRead >= 0) {
      statistics.incrementBytesRead(bytesRead);
    }

    return bytesRead;
  }

  /**
   * Closes this input stream and releases resources.
   *
   * @throws IOException if an I/O error occurs
   */
  public synchronized void close() throws IOException {
    if (!closed) {
      closed = true;

      // close or abort the stream
      closeStream("close() operation");
      // this is actually a no-op
      super.close();
    }
  }

  /**
   * Seeks to the specified position in the stream. This
   * performs a lazy seek; the actual stream repositioning
   * happens on the next read.
   *
   * @param targetPos the target position to seek to
   * @throws IOException if an I/O error occurs
   */
  public synchronized void seek(long targetPos)
      throws IOException {
    checkNotClosed();

    // Do not allow negative seek
    if (targetPos < 0) {
      throw new EOFException(
          FSExceptionMessages.NEGATIVE_SEEK
              + " " + targetPos);
    }
    if (targetPos > contentLength) {
      throw new EOFException(
          FSExceptionMessages.CANNOT_SEEK_PAST_EOF
              + " " + targetPos);
    }

    if (this.contentLength <= 0) {
      return;
    }

    // Lazy seek
    nextReadPos = targetPos;
  }

  /**
   * Returns the current position in the stream.
   *
   * @return the current position
   * @throws IOException if an I/O error occurs
   */
  public synchronized long getPos() throws IOException {
    return nextReadPos;
  }

  /**
   * Seeks to a new source. Not supported; always returns
   * false.
   *
   * @param targetPos the target position
   * @return false always
   * @throws IOException if an I/O error occurs
   */
  public boolean seekToNewSource(long targetPos)
      throws IOException {
    return false;
  }

  /**
   * Sets the readahead value for this stream.
   *
   * @param readahead the readahead length, or null to reset
   *                  to default
   */
  public synchronized void setReadahead(Long readahead) {
    if (readahead == null) {
      this.readahead = DEFAULT_READAHEAD_LEN;
    } else {
      this.readahead = Math.max(
          readahead, DEFAULT_READAHEAD_LEN);
    }
  }

  /**
   * Verify that the input stream is open. Non blocking; this
   * gives the last state of the volatile {@link #closed}
   * field.
   *
   * @throws IOException if the connection is closed.
   */
  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException(
          key + ": "
              + FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }

  /**
   * Perform lazy seek and adjust stream to correct position
   * for reading.
   *
   * @param targetPos position from where data should be read
   * @param len length of the content that needs to be read
   * @throws IOException if an I/O error occurs
   */
  private void lazySeek(long targetPos, long len)
      throws IOException {
    // For lazy seek
    try {
      seekInStream(targetPos, len);
    } catch (Exception e) {
      closeStream("seekInStream failed");
    }
    // re-open at specific location if needed
    if (in == null) {
      LOG.debug(
          "re-open at specific location: {}",
          targetPos);
      reopen("read from new offset", targetPos, len);
    }
  }

  /**
   * Adjust the stream to a specific position.
   *
   * @param targetPos target seek position
   * @param length length of content that needs to be read
   *               from targetPos
   * @throws IOException if an I/O error occurs
   */
  private void seekInStream(
      long targetPos, long length) throws IOException {
    long diff = targetPos - pos;
    checkNotClosed();

    if (in == null) {
      return;
    }
    // compute how much more to skip
    if (diff > 0) {
      // if seek then reopen the stream

      // forward seek - this is where data can be skipped
      int available = in.available();
      // always seek at least as far as what is available
      long forwardSeekRange =
          Math.max(readahead, available);
      // work out how much is actually left in the stream
      // then choose whichever comes first: the range or
      // the EOF
      long remainingInCurrentRequest =
          remainingInCurrentRequest();

      long forwardSeekLimit = Math.min(
          remainingInCurrentRequest, forwardSeekRange);

      boolean skipForward =
          remainingInCurrentRequest > 0
              && diff <= forwardSeekLimit;

      if (skipForward) {
        // the forward seek range is within the limits
        LOG.debug(
            "Forward seek on {} of {}bytes",
            key, diff);

        long skipped = in.skip(diff);
        if (skipped > 0) {
          pos += skipped;
          // as these bytes have been read, they are
          // included in the counter
          statistics.incrementBytesRead(skipped);
        }

        if (pos == targetPos) {
          // all is well
          return;
        } else {
          // log a warning; continue to attempt to
          // re-open
          LOG.debug(
              "Failed to seek on {} to {}."
                  + " Current position {}",
              key, targetPos, pos);
        }
      }
    } else if (diff < 0) {
      // backwards seek
      LOG.debug(
          "seek on {} backwards {}."
              + " Current position {}",
          key, diff, pos);
    } else {
      // targetPos == pos
      if (remainingInCurrentRequest() > 0) {
        // if there is data left in the stream, keep
        // going
        return;
      }
    }
    // if the code reaches here, the stream needs to be
    // reopened. close the stream; if read the object will
    // be opened at the new pos
    closeStream("seekInStream()");
    pos = targetPos;
  }

  /**
   * Bytes left in the current request.
   * Only valid if there is an active request.
   *
   * @return how many bytes are left to read in the current
   *         GET.
   */
  public synchronized long remainingInCurrentRequest() {
    // contentRangeFinish is inclusive (last byte index),
    // so add 1
    return this.contentRangeFinish - this.pos + 1;
  }

  /**
   * Close a stream.
   *
   * This does not set the {@link #closed} flag.
   *
   * @param reason reason for stream being closed; used in
   *               messages
   */
  private void closeStream(String reason) {
    if (in != null) {
      try {
        in.close();
      } catch (IOException e) {
        LOG.debug(
            "When closing {} stream for {}",
            key, reason, e);
      }
    }
    long remaining = remainingInCurrentRequest();
    LOG.debug(
        "Stream {} : {}; remaining={} streamPos={},"
            + " nextReadPos={}, request range {}-{}",
        key, reason, remaining, pos,
        nextReadPos, contentRangeStart,
        contentRangeFinish);

    in = null;
  }

  /**
   * Handle an IOE on a read by attempting to re-open the
   * stream. The filesystem's readException count will be
   * incremented.
   *
   * @param ioe exception caught.
   * @param length length of data being attempted to read
   * @throws IOException any exception thrown on the re-open
   *                     attempt.
   */
  private void onReadFailure(
      Exception ioe, int length) throws IOException {
    LOG.info(
        "Got exception while trying to read from"
            + " stream {} trying to recover from"
            + " position: {}",
        key, pos);
    LOG.info(
        "While trying to read from stream {}"
            + " exception: {}",
        key, ioe);
    reopen("failure recovery", pos, length);
  }

  /**
   * Opens up the stream at specified target position and for
   * given length.
   *
   * @param reason reason for reopen
   * @param targetPos target position
   * @param length length requested
   * @throws IOException on any failure to open the object
   */
  private synchronized void reopen(
      String reason, long targetPos, long length)
      throws IOException {
    if (in != null) {
      closeStream("reopen(" + reason + ")");
    }

    contentRangeFinish = calculateRequestLimit(
        targetPos, length, contentLength, readahead);

    LOG.info(
        "reopen({}) for {} range[{}-{}], length={},"
            + " contentLength={}, streamPosition={},"
            + " nextReadPosition={}",
        key, reason, targetPos, contentRangeFinish,
        length, contentLength, pos, nextReadPos);
    try {
      in = store.retrieve(
          key, targetPos, contentRangeFinish,
          fileMetaData);
      contentRangeStart = targetPos;
    } catch (Exception e) {
      LOG.info(
          "native store retrieve failed reason : {}",
          e.getMessage());
      closeStream("native store retrieve failed");
    }
    if (in == null) {
      throw new IOException(
          "Null IO stream from reopen of ("
              + reason + ") " + key);
    }
    this.pos = targetPos;
  }

  /**
   * Calculate the limit for a get request.
   *
   * @param targetPos position of the read
   * @param length length of bytes requested; if less than
   *               zero "unknown"
   * @param fileContentLength total length of file
   * @param readaheadLen current readahead value
   * @return the absolute value of the limit of the request
   *         (inclusive, last byte index).
   */
  private long calculateRequestLimit(
      long targetPos,
      long length,
      long fileContentLength,
      long readaheadLen) {
    if (length < 0) {
      // For unknown length, read to end of file
      // (last byte index)
      return fileContentLength - 1;
    }

    return Math.min(
        fileContentLength,
        targetPos + Math.max(readaheadLen, length)) - 1;
  }
}

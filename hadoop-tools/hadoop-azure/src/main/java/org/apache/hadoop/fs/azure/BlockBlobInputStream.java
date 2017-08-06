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

package org.apache.hadoop.fs.azure;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobRequestOptions;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.azure.StorageInterface.CloudBlockBlobWrapper;

/**
 * Encapsulates the BlobInputStream used by block blobs and adds support for
 * random access and seek. Random access performance is improved by several
 * orders of magnitude.
 */
final class BlockBlobInputStream extends InputStream implements Seekable {
  private final CloudBlockBlobWrapper blob;
  private final BlobRequestOptions options;
  private final OperationContext opContext;
  private InputStream blobInputStream = null;
  private int minimumReadSizeInBytes = 0;
  private long streamPositionAfterLastRead = -1;
  // position of next network read within stream
  private long streamPosition = 0;
  // length of stream
  private long streamLength = 0;
  private boolean closed = false;
  // internal buffer, re-used for performance optimization
  private byte[] streamBuffer;
  // zero-based offset within streamBuffer of current read position
  private int streamBufferPosition;
  // length of data written to streamBuffer, streamBuffer may be larger
  private int streamBufferLength;

  /**
   * Creates a seek-able stream for reading from block blobs.
   * @param blob a block blob reference.
   * @param options the blob request options.
   * @param opContext the blob operation context.
   * @throws IOException IO failure
   */
  BlockBlobInputStream(CloudBlockBlobWrapper blob,
      BlobRequestOptions options,
      OperationContext opContext) throws IOException {
    this.blob = blob;
    this.options = options;
    this.opContext = opContext;

    this.minimumReadSizeInBytes = blob.getStreamMinimumReadSizeInBytes();

    try {
      this.blobInputStream = blob.openInputStream(options, opContext);
    } catch (StorageException e) {
      throw new IOException(e);
    }

    this.streamLength = blob.getProperties().getLength();
  }

  private void checkState() throws IOException {
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }

  /**
   * Reset the internal stream buffer but do not release the memory.
   * The buffer can be reused to avoid frequent memory allocations of
   * a large buffer.
   */
  private void resetStreamBuffer() {
    streamBufferPosition = 0;
    streamBufferLength = 0;
  }

  /**
   * Gets the read position of the stream.
   * @return the zero-based byte offset of the read position.
   * @throws IOException IO failure
   */
  @Override
  public synchronized long getPos() throws IOException {
    checkState();
    return (streamBuffer != null)
        ? streamPosition - streamBufferLength + streamBufferPosition
        : streamPosition;
  }

  /**
   * Sets the read position of the stream.
   * @param pos a zero-based byte offset in the stream.
   * @throws EOFException if read is out of range
   */
  @Override
  public synchronized void seek(long pos) throws IOException {
    checkState();
    if (pos < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK + " " + pos);
    }
    if (pos > streamLength) {
      throw new EOFException(
          FSExceptionMessages.CANNOT_SEEK_PAST_EOF + " " + pos);
    }

    // calculate offset between the target and current position in the stream
    long offset = pos - getPos();

    if (offset == 0) {
      // no=op, no state change
      return;
    }

    if (offset > 0) {
      // forward seek, data can be skipped as an optimization
      if (skip(offset) != offset) {
        throw new EOFException(FSExceptionMessages.EOF_IN_READ_FULLY);
      }
      return;
    }

    // reverse seek, offset is negative
    if (streamBuffer != null) {
      if (streamBufferPosition + offset >= 0) {
        // target position is inside the stream buffer,
        // only need to move backwards within the stream buffer
        streamBufferPosition += offset;
      } else {
        // target position is outside the stream buffer,
        // need to reset stream buffer and move position for next network read
        resetStreamBuffer();
        streamPosition = pos;
      }
    } else {
      streamPosition = pos;
    }

    // close BlobInputStream after seek is invoked because BlobInputStream
    // does not support seek
    closeBlobInputStream();
  }

  /**
   * Seeks an secondary copy of the data.  This method is not supported.
   * @param targetPos a zero-based byte offset in the stream.
   * @return false
   * @throws IOException IO failure
   */
  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  /**
   * Gets the number of bytes that can be read (or skipped over) without
   * performing a network operation.
   * @throws IOException IO failure
   */
  @Override
  public synchronized int available() throws IOException {
    checkState();
    if (blobInputStream != null) {
      return blobInputStream.available();
    } else {
      return (streamBuffer == null)
          ? 0
          : streamBufferLength - streamBufferPosition;
    }
  }

  private void closeBlobInputStream() throws IOException {
    if (blobInputStream != null) {
      try {
        blobInputStream.close();
      } finally {
        blobInputStream = null;
      }
    }
  }

  /**
   * Closes this stream and releases any system resources associated with it.
   * @throws IOException IO failure
   */
  @Override
  public synchronized void close() throws IOException {
    closed = true;
    closeBlobInputStream();
    streamBuffer = null;
    streamBufferPosition = 0;
    streamBufferLength = 0;
  }

  private int doNetworkRead(byte[] buffer, int offset, int len)
      throws IOException {
    MemoryOutputStream outputStream;
    boolean needToCopy = false;

    if (streamPositionAfterLastRead == streamPosition) {
      // caller is reading sequentially, so initialize the stream buffer
      if (streamBuffer == null) {
        streamBuffer = new byte[(int) Math.min(minimumReadSizeInBytes,
            streamLength)];
      }
      resetStreamBuffer();
      outputStream = new MemoryOutputStream(streamBuffer, streamBufferPosition,
          streamBuffer.length);
      needToCopy = true;
    } else {
      outputStream = new MemoryOutputStream(buffer, offset, len);
    }

    long bytesToRead = Math.min(
        minimumReadSizeInBytes,
        Math.min(
            outputStream.capacity(),
            streamLength - streamPosition));

    try {
      blob.downloadRange(streamPosition, bytesToRead, outputStream, options,
          opContext);
    } catch (StorageException e) {
      throw new IOException(e);
    }

    int bytesRead = outputStream.size();
    if (bytesRead > 0) {
      streamPosition += bytesRead;
      streamPositionAfterLastRead = streamPosition;
      int count = Math.min(bytesRead, len);
      if (needToCopy) {
        streamBufferLength = bytesRead;
        System.arraycopy(streamBuffer, streamBufferPosition, buffer, offset,
            count);
        streamBufferPosition += count;
      }
      return count;
    } else {
      // This may happen if the blob was modified after the length was obtained.
      throw new EOFException("End of stream reached unexpectedly.");
    }
  }

  /**
   * Reads up to <code>len</code> bytes of data from the input stream into an
   * array of bytes.
   * @param b a buffer into which the data is written.
   * @param offset a start offset into {@code buffer} where the data is written.
   * @param len the maximum number of bytes to be read.
   * @return the number of bytes written into {@code buffer}, or -1.
   * @throws IOException IO failure
   */
  @Override
  public synchronized int read(byte[] b, int offset, int len)
      throws IOException {
    checkState();
    NativeAzureFileSystemHelper.validateReadArgs(b, offset, len);
    if (blobInputStream != null) {
      int numberOfBytesRead = blobInputStream.read(b, offset, len);
      streamPosition += numberOfBytesRead;
      return numberOfBytesRead;
    } else {
      if (offset < 0 || len < 0 || len > b.length - offset) {
        throw new IndexOutOfBoundsException("read arguments out of range");
      }
      if (len == 0) {
        return 0;
      }

      int bytesRead = 0;
      int available = available();
      if (available > 0) {
        bytesRead = Math.min(available, len);
        System.arraycopy(streamBuffer, streamBufferPosition, b, offset,
            bytesRead);
        streamBufferPosition += bytesRead;
      }

      if (len == bytesRead) {
        return len;
      }
      if (streamPosition >= streamLength) {
        return (bytesRead > 0) ? bytesRead : -1;
      }

      offset += bytesRead;
      len -= bytesRead;

      return bytesRead + doNetworkRead(b, offset, len);
    }
  }

  /**
   * Reads the next byte of data from the stream.
   * @return the next byte of data, or -1
   * @throws IOException IO failure
   */
  @Override
  public int read() throws IOException {
    byte[] buffer = new byte[1];
    int numberOfBytesRead = read(buffer, 0, 1);
    return (numberOfBytesRead < 1) ? -1 : buffer[0];
  }

  /**
   * Skips over and discards n bytes of data from this input stream.
   * @param n the number of bytes to be skipped.
   * @return the actual number of bytes skipped.
   * @throws IOException IO failure
   * @throws IndexOutOfBoundsException if n is negative or if the sum of n
   * and the current value of getPos() is greater than the length of the stream.
   */
  @Override
  public synchronized long skip(long n) throws IOException {
    checkState();

    if (blobInputStream != null) {
      // blobInput stream is open; delegate the work to it
      long skipped = blobInputStream.skip(n);
      // update position to the actual skip value
      streamPosition += skipped;
      return skipped;
    }

    // no blob stream; implement the skip logic directly
    if (n < 0 || n > streamLength - getPos()) {
      throw new IndexOutOfBoundsException("skip range");
    }

    if (streamBuffer != null) {
      // there's a buffer, so seek with it
      if (n < streamBufferLength - streamBufferPosition) {
        // new range is in the buffer, so just update the buffer position
        // skip within the buffer.
        streamBufferPosition += (int) n;
      } else {
        // skip is out of range, so move position to ne value and reset
        // the buffer ready for the next read()
        streamPosition = getPos() + n;
        resetStreamBuffer();
      }
    } else {
      // no stream buffer; increment the stream position ready for
      // the next triggered connection & read
      streamPosition += n;
    }
    return n;
  }

  /**
   * An <code>OutputStream</code> backed by a user-supplied buffer.
   */
  static class MemoryOutputStream extends OutputStream {
    private final byte[] buffer;
    private final int offset;
    private final int length;
    private int writePosition;

    /**
     * Creates a <code>MemoryOutputStream</code> from a user-supplied buffer.
     * @param buffer an array of bytes.
     * @param offset a starting offset in <code>buffer</code> where the data
     * will be written.
     * @param length the maximum number of bytes to be written to the stream.
     */
    MemoryOutputStream(byte[] buffer, int offset, int length) {
      if (buffer == null) {
        throw new NullPointerException("buffer");
      }
      if (offset < 0 || length < 0 || length > buffer.length - offset) {
        throw new IndexOutOfBoundsException("offset out of range of buffer");
      }
      this.buffer = buffer;
      this.offset = offset;
      this.length = length;
      this.writePosition = offset;
    }

    /**
     * Gets the current size of the stream.
     */
    public synchronized int size() {
      return writePosition - offset;
    }

    /**
     * Gets the current capacity of the stream.
     */
    public synchronized int capacity() {
      return length;
    }

    /**
     * Writes the next byte to the stream.
     * @param b the byte to be written.
     * @throws IOException IO failure
     */
    public synchronized void write(int b) throws IOException {
      if (size() > length - 1) {
        throw new IOException("No space for more writes");
      }
      buffer[writePosition++] = (byte) b;
    }

    /**
     * Writes a range of bytes to the stream.
     * @param b a byte array.
     * @param off the start offset in <code>buffer</code> from which the data
     * is read.
     * @param length the number of bytes to be written.
     * @throws IOException IO failure
     */
    public synchronized void write(byte[] b, int off, int length)
        throws IOException {
      if (b == null) {
        throw new NullPointerException("Null buffer argument");
      }
      if (off < 0 || length < 0 || length > b.length - off) {
        throw new IndexOutOfBoundsException("array write offset");
      }
      System.arraycopy(b, off, buffer, writePosition, length);
      writePosition += length;
    }
  }
}

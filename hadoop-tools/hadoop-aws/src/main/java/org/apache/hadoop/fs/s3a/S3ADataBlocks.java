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

package org.apache.hadoop.fs.s3a;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.util.DirectBufferPool;

import static org.apache.hadoop.fs.s3a.S3ADataBlocks.DataBlock.DestState.*;

/**
 * Set of classes to support output streaming into blocks which are then
 * uploaded as partitions.
 */
final class S3ADataBlocks {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3ADataBlocks.class);

  private S3ADataBlocks() {
  }

  /**
   * Validate args to a write command. These are the same validation checks
   * expected for any implementation of {@code OutputStream.write()}.
   * @param b byte array containing data
   * @param off offset in array where to start
   * @param len number of bytes to be written
   * @throws NullPointerException for a null buffer
   * @throws IndexOutOfBoundsException if indices are out of range
   */
  static void validateWriteArgs(byte[] b, int off, int len)
      throws IOException {
    Preconditions.checkNotNull(b);
    if ((off < 0) || (off > b.length) || (len < 0) ||
        ((off + len) > b.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException(
          "write (b[" + b.length + "], " + off + ", " + len + ')');
    }
  }

  /**
   * Create a factory.
   * @param owner factory owner
   * @param name factory name -the option from {@link Constants}.
   * @return the factory, ready to be initialized.
   * @throws IllegalArgumentException if the name is unknown.
   */
  static BlockFactory createFactory(S3AFileSystem owner,
      String name) {
    switch (name) {
    case Constants.FAST_UPLOAD_BUFFER_ARRAY:
      return new ArrayBlockFactory(owner);
    case Constants.FAST_UPLOAD_BUFFER_DISK:
      return new DiskBlockFactory(owner);
    case Constants.FAST_UPLOAD_BYTEBUFFER:
      return new ByteBufferBlockFactory(owner);
    default:
      throw new IllegalArgumentException("Unsupported block buffer" +
          " \"" + name + '"');
    }
  }

  /**
   * Base class for block factories.
   */
  static abstract class BlockFactory implements Closeable {

    private final S3AFileSystem owner;

    protected BlockFactory(S3AFileSystem owner) {
      this.owner = owner;
    }


    /**
     * Create a block.
     * @param limit limit of the block.
     * @return a new block.
     */
    abstract DataBlock create(int limit) throws IOException;

    /**
     * Implement any close/cleanup operation.
     * Base class is a no-op
     * @throws IOException -ideally, it shouldn't.
     */
    @Override
    public void close() throws IOException {
    }

    /**
     * Owner.
     */
    protected S3AFileSystem getOwner() {
      return owner;
    }
  }

  /**
   * This represents a block being uploaded.
   */
  static abstract class DataBlock implements Closeable {

    enum DestState {Writing, Upload, Closed}

    private volatile DestState state = Writing;

    /**
     * Atomically enter a state, verifying current state.
     * @param current current state. null means "no check"
     * @param next next state
     * @throws IllegalStateException if the current state is not as expected
     */
    protected synchronized final void enterState(DestState current,
        DestState next)
        throws IllegalStateException {
      verifyState(current);
      LOG.debug("{}: entering state {}", this, next);
      state = next;
    }

    /**
     * Verify that the block is in the declared state.
     * @param expected expected state.
     * @throws IllegalStateException if the DataBlock is in the wrong state
     */
    protected final void verifyState(DestState expected)
        throws IllegalStateException {
      if (expected != null && state != expected) {
        throw new IllegalStateException("Expected stream state " + expected
            + " -but actual state is " + state + " in " + this);
      }
    }

    /**
     * Current state.
     * @return the current state.
     */
    final DestState getState() {
      return state;
    }

    /**
     * Return the current data size.
     * @return the size of the data
     */
    abstract int dataSize();

    /**
     * Predicate to verify that the block has the capacity to write
     * the given set of bytes.
     * @param bytes number of bytes desired to be written.
     * @return true if there is enough space.
     */
    abstract boolean hasCapacity(long bytes);

    /**
     * Predicate to check if there is data in the block.
     * @return true if there is
     */
    boolean hasData() {
      return dataSize() > 0;
    }

    /**
     * The remaining capacity in the block before it is full.
     * @return the number of bytes remaining.
     */
    abstract int remainingCapacity();

    /**
     * Write a series of bytes from the buffer, from the offset.
     * Returns the number of bytes written.
     * Only valid in the state {@code Writing}.
     * Base class verifies the state but does no writing.
     * @param buffer buffer
     * @param offset offset
     * @param length length of write
     * @return number of bytes written
     * @throws IOException trouble
     */
    int write(byte[] buffer, int offset, int length) throws IOException {
      verifyState(Writing);
      Preconditions.checkArgument(buffer != null, "Null buffer");
      Preconditions.checkArgument(length >= 0, "length is negative");
      Preconditions.checkArgument(offset >= 0, "offset is negative");
      Preconditions.checkArgument(
          !(buffer.length - offset < length),
          "buffer shorter than amount of data to write");
      return 0;
    }

    /**
     * Flush the output.
     * Only valid in the state {@code Writing}.
     * In the base class, this is a no-op
     * @throws IOException any IO problem.
     */
    void flush() throws IOException {
      verifyState(Writing);
    }

    /**
     * Switch to the upload state and return a stream for uploading.
     * Base class calls {@link #enterState(DestState, DestState)} to
     * manage the state machine.
     * @return the stream
     * @throws IOException trouble
     */
    InputStream startUpload() throws IOException {
      LOG.debug("Start datablock upload");
      enterState(Writing, Upload);
      return null;
    }

    /**
     * Enter the closed state.
     * @return true if the class was in any other state, implying that
     * the subclass should do its close operations
     */
    protected synchronized boolean enterClosedState() {
      if (!state.equals(Closed)) {
        enterState(null, Closed);
        return true;
      } else {
        return false;
      }
    }

    @Override
    public void close() throws IOException {
      if (enterClosedState()) {
        LOG.debug("Closed {}", this);
        innerClose();
      }
    }

    /**
     * Inner close logic for subclasses to implement.
     */
    protected void innerClose() throws IOException {

    }

  }

  // ====================================================================

  /**
   * Use byte arrays on the heap for storage.
   */
  static class ArrayBlockFactory extends BlockFactory {

    ArrayBlockFactory(S3AFileSystem owner) {
      super(owner);
    }

    @Override
    DataBlock create(int limit) throws IOException {
      return new ByteArrayBlock(limit);
    }

  }

  static class S3AByteArrayOutputStream extends ByteArrayOutputStream {

    S3AByteArrayOutputStream(int size) {
      super(size);
    }

    /**
     * InputStream backed by the internal byte array
     *
     * @return
     */
    ByteArrayInputStream getInputStream() {
      ByteArrayInputStream bin = new ByteArrayInputStream(this.buf, 0, count);
      this.reset();
      this.buf = null;
      return bin;
    }
  }

  /**
   * Stream to memory via a {@code ByteArrayOutputStream}.
   *
   * This was taken from {@code S3AFastOutputStream} and has the
   * same problem which surfaced there: it can consume a lot of heap space
   * proportional to the mismatch between writes to the stream and
   * the JVM-wide upload bandwidth to the S3 endpoint.
   * The memory consumption can be limited by tuning the filesystem settings
   * to restrict the number of queued/active uploads.
   */

  static class ByteArrayBlock extends DataBlock {
    private S3AByteArrayOutputStream buffer;
    private final int limit;
    // cache data size so that it is consistent after the buffer is reset.
    private Integer dataSize;

    ByteArrayBlock(int limit) {
      this.limit = limit;
      buffer = new S3AByteArrayOutputStream(limit);
    }

    /**
     * Get the amount of data; if there is no buffer then the size is 0.
     * @return the amount of data available to upload.
     */
    @Override
    int dataSize() {
      return dataSize != null ? dataSize : buffer.size();
    }

    @Override
    InputStream startUpload() throws IOException {
      super.startUpload();
      dataSize = buffer.size();
      ByteArrayInputStream bufferData = buffer.getInputStream();
      buffer = null;
      return bufferData;
    }

    @Override
    boolean hasCapacity(long bytes) {
      return dataSize() + bytes <= limit;
    }

    @Override
    int remainingCapacity() {
      return limit - dataSize();
    }

    @Override
    int write(byte[] b, int offset, int len) throws IOException {
      super.write(b, offset, len);
      int written = Math.min(remainingCapacity(), len);
      buffer.write(b, offset, written);
      return written;
    }

    @Override
    protected void innerClose() {
      buffer = null;
    }

    @Override
    public String toString() {
      return "ByteArrayBlock{" +
          "state=" + getState() +
          ", limit=" + limit +
          ", dataSize=" + dataSize +
          '}';
    }
  }

  // ====================================================================

  /**
   * Stream via Direct ByteBuffers; these are allocated off heap
   * via {@link DirectBufferPool}.
   * This is actually the most complex of all the block factories,
   * due to the need to explicitly recycle buffers; in comparison, the
   * {@link DiskBlock} buffer delegates the work of deleting files to
   * the {@link DiskBlock.FileDeletingInputStream}. Here the
   * input stream {@link ByteBufferInputStream} has a similar task, along
   * with the foundational work of streaming data from a byte array.
   */

  static class ByteBufferBlockFactory extends BlockFactory {

    private final DirectBufferPool bufferPool = new DirectBufferPool();
    private final AtomicInteger buffersOutstanding = new AtomicInteger(0);

    ByteBufferBlockFactory(S3AFileSystem owner) {
      super(owner);
    }

    @Override
    ByteBufferBlock create(int limit) throws IOException {
      return new ByteBufferBlock(limit);
    }

    private ByteBuffer requestBuffer(int limit) {
      LOG.debug("Requesting buffer of size {}", limit);
      buffersOutstanding.incrementAndGet();
      return bufferPool.getBuffer(limit);
    }

    private void releaseBuffer(ByteBuffer buffer) {
      LOG.debug("Releasing buffer");
      bufferPool.returnBuffer(buffer);
      buffersOutstanding.decrementAndGet();
    }

    /**
     * Get count of outstanding buffers.
     * @return the current buffer count
     */
    public int getOutstandingBufferCount() {
      return buffersOutstanding.get();
    }

    @Override
    public String toString() {
      return "ByteBufferBlockFactory{"
          + "buffersOutstanding=" + buffersOutstanding +
          '}';
    }

    /**
     * A DataBlock which requests a buffer from pool on creation; returns
     * it when the output stream is closed.
     */
    class ByteBufferBlock extends DataBlock {
      private ByteBuffer buffer;
      private final int bufferSize;
      // cache data size so that it is consistent after the buffer is reset.
      private Integer dataSize;

      /**
       * Instantiate. This will request a ByteBuffer of the desired size.
       * @param bufferSize buffer size
       */
      ByteBufferBlock(int bufferSize) {
        this.bufferSize = bufferSize;
        buffer = requestBuffer(bufferSize);
      }

      /**
       * Get the amount of data; if there is no buffer then the size is 0.
       * @return the amount of data available to upload.
       */
      @Override
      int dataSize() {
        return dataSize != null ? dataSize : bufferCapacityUsed();
      }

      @Override
      ByteBufferInputStream startUpload() throws IOException {
        super.startUpload();
        dataSize = bufferCapacityUsed();
        // set the buffer up from reading from the beginning
        buffer.limit(buffer.position());
        buffer.position(0);
        return new ByteBufferInputStream(dataSize, buffer);
      }

      @Override
      public boolean hasCapacity(long bytes) {
        return bytes <= remainingCapacity();
      }

      @Override
      public int remainingCapacity() {
        return buffer != null ? buffer.remaining() : 0;
      }

      private int bufferCapacityUsed() {
        return buffer.capacity() - buffer.remaining();
      }

      @Override
      int write(byte[] b, int offset, int len) throws IOException {
        super.write(b, offset, len);
        int written = Math.min(remainingCapacity(), len);
        buffer.put(b, offset, written);
        return written;
      }

      @Override
      protected void innerClose() {
        buffer = null;
      }

      @Override
      public String toString() {
        return "ByteBufferBlock{"
            + "state=" + getState() +
            ", dataSize=" + dataSize() +
            ", limit=" + bufferSize +
            ", remainingCapacity=" + remainingCapacity() +
            '}';
      }

    }

    /**
     * Provide an input stream from a byte buffer; supporting
     * {@link #mark(int)}, which is required to enable replay of failed
     * PUT attempts.
     * This input stream returns the buffer to the pool afterwards.
     */
    class ByteBufferInputStream extends InputStream {

      private final int size;
      private ByteBuffer byteBuffer;

      ByteBufferInputStream(int size, ByteBuffer byteBuffer) {
        LOG.debug("Creating ByteBufferInputStream of size {}", size);
        this.size = size;
        this.byteBuffer = byteBuffer;
      }

      /**
       * Return the buffer to the pool after the stream is closed.
       */
      @Override
      public synchronized void close() {
        if (byteBuffer != null) {
          LOG.debug("releasing buffer");
          releaseBuffer(byteBuffer);
          byteBuffer = null;
        }
      }

      /**
       * Verify that the stream is open.
       * @throws IOException if the stream is closed
       */
      private void verifyOpen() throws IOException {
        if (byteBuffer == null) {
          throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
      }

      public synchronized int read() throws IOException {
        if (available() > 0) {
          return byteBuffer.get() & 0xFF;
        } else {
          return -1;
        }
      }

      @Override
      public synchronized long skip(long offset) throws IOException {
        verifyOpen();
        long newPos = position() + offset;
        if (newPos < 0) {
          throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
        }
        if (newPos > size) {
          throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
        }
        byteBuffer.position((int) newPos);
        return newPos;
      }

      @Override
      public synchronized int available() {
        Preconditions.checkState(byteBuffer != null,
            FSExceptionMessages.STREAM_IS_CLOSED);
        return byteBuffer.remaining();
      }

      /**
       * Get the current buffer position.
       * @return the buffer position
       */
      public synchronized int position() {
        return byteBuffer.position();
      }

      /**
       * Check if there is data left.
       * @return true if there is data remaining in the buffer.
       */
      public synchronized boolean hasRemaining() {
        return byteBuffer.hasRemaining();
      }

      @Override
      public synchronized void mark(int readlimit) {
        LOG.debug("mark at {}", position());
        byteBuffer.mark();
      }

      @Override
      public synchronized void reset() throws IOException {
        LOG.debug("reset");
        byteBuffer.reset();
      }

      @Override
      public boolean markSupported() {
        return true;
      }

      /**
       * Read in data.
       * @param buffer destination buffer
       * @param offset offset within the buffer
       * @param length length of bytes to read
       * @throws EOFException if the position is negative
       * @throws IndexOutOfBoundsException if there isn't space for the
       * amount of data requested.
       * @throws IllegalArgumentException other arguments are invalid.
       */
      @SuppressWarnings("NullableProblems")
      public synchronized int read(byte[] buffer, int offset, int length)
          throws IOException {
        Preconditions.checkArgument(length >= 0, "length is negative");
        Preconditions.checkArgument(buffer != null, "Null buffer");
        if (buffer.length - offset < length) {
          throw new IndexOutOfBoundsException(
              FSExceptionMessages.TOO_MANY_BYTES_FOR_DEST_BUFFER
                  + ": request length =" + length
                  + ", with offset =" + offset
                  + "; buffer capacity =" + (buffer.length - offset));
        }
        verifyOpen();
        if (!hasRemaining()) {
          return -1;
        }

        int toRead = Math.min(length, available());
        byteBuffer.get(buffer, offset, toRead);
        return toRead;
      }

      @Override
      public String toString() {
        final StringBuilder sb = new StringBuilder(
            "ByteBufferInputStream{");
        sb.append("size=").append(size);
        ByteBuffer buffer = this.byteBuffer;
        if (buffer != null) {
          sb.append(", available=").append(buffer.remaining());
        }
        sb.append('}');
        return sb.toString();
      }
    }
  }

  // ====================================================================

  /**
   * Buffer blocks to disk.
   */
  static class DiskBlockFactory extends BlockFactory {

    DiskBlockFactory(S3AFileSystem owner) {
      super(owner);
    }

    /**
     * Create a temp file and a block which writes to it.
     * @param limit limit of the block.
     * @return the new block
     * @throws IOException IO problems
     */
    @Override
    DataBlock create(int limit) throws IOException {
      File destFile = getOwner()
          .createTmpFileForWrite("s3ablock", limit, getOwner().getConf());
      return new DiskBlock(destFile, limit);
    }
  }

  /**
   * Stream to a file.
   * This will stop at the limit; the caller is expected to create a new block
   */
  static class DiskBlock extends DataBlock {

    private int bytesWritten;
    private final File bufferFile;
    private final int limit;
    private BufferedOutputStream out;
    private InputStream uploadStream;

    DiskBlock(File bufferFile, int limit)
        throws FileNotFoundException {
      this.limit = limit;
      this.bufferFile = bufferFile;
      out = new BufferedOutputStream(new FileOutputStream(bufferFile));
    }

    @Override
    int dataSize() {
      return bytesWritten;
    }

    @Override
    boolean hasCapacity(long bytes) {
      return dataSize() + bytes <= limit;
    }

    @Override
    int remainingCapacity() {
      return limit - bytesWritten;
    }

    @Override
    int write(byte[] b, int offset, int len) throws IOException {
      super.write(b, offset, len);
      int written = Math.min(remainingCapacity(), len);
      out.write(b, offset, written);
      bytesWritten += written;
      return written;
    }

    @Override
    InputStream startUpload() throws IOException {
      super.startUpload();
      try {
        out.flush();
      } finally {
        out.close();
        out = null;
      }
      uploadStream = new FileInputStream(bufferFile);
      return new FileDeletingInputStream(uploadStream);
    }

    /**
     * The close operation will delete the destination file if it still
     * exists.
     * @throws IOException IO problems
     */
    @Override
    protected void innerClose() throws IOException {
      final DestState state = getState();
      LOG.debug("Closing {}", this);
      switch (state) {
      case Writing:
        if (bufferFile.exists()) {
          // file was not uploaded
          LOG.debug("Deleting buffer file as upload did not start");
          boolean deleted = bufferFile.delete();
          if (!deleted && bufferFile.exists()) {
            LOG.warn("Failed to delete buffer file {}", bufferFile);
          }
        }
        break;

      case Upload:
        LOG.debug("Buffer file {} exists —close upload stream", bufferFile);
        break;

      case Closed:
        // no-op
        break;

      default:
        // this state can never be reached, but checkstyle complains, so
        // it is here.
      }
    }

    /**
     * Flush operation will flush to disk.
     * @throws IOException IOE raised on FileOutputStream
     */
    @Override
    void flush() throws IOException {
      super.flush();
      out.flush();
    }

    @Override
    public String toString() {
      String sb = "FileBlock{"
          + "destFile=" + bufferFile +
          ", state=" + getState() +
          ", dataSize=" + dataSize() +
          ", limit=" + limit +
          '}';
      return sb;
    }

    /**
     * An input stream which deletes the buffer file when closed.
     */
    private final class FileDeletingInputStream extends FilterInputStream {
      private final AtomicBoolean closed = new AtomicBoolean(false);

      FileDeletingInputStream(InputStream source) {
        super(source);
      }

      /**
       * Delete the input file when closed.
       * @throws IOException IO problem
       */
      @Override
      public void close() throws IOException {
        try {
          super.close();
        } finally {
          if (!closed.getAndSet(true)) {
            if (!bufferFile.delete()) {
              LOG.warn("delete({}) returned false",
                  bufferFile.getAbsoluteFile());
            }
          }
        }
      }
    }
  }

}

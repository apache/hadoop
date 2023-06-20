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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.s3a.statistics.BlockOutputStreamStatistics;
import org.apache.hadoop.util.DirectBufferPool;

import static org.apache.hadoop.fs.s3a.S3ADataBlocks.DataBlock.DestState.*;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;

/**
 * Set of classes to support output streaming into blocks which are then
 * uploaded as to S3 as a single PUT, or as part of a multipart request.
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
   * The output information for an upload.
   * It can be one of a file or an input stream.
   * When closed, any stream is closed. Any source file is untouched.
   */
  static final class BlockUploadData implements Closeable {
    private final File file;
    private final InputStream uploadStream;

    /**
     * File constructor; input stream will be null.
     * @param file file to upload
     */
    BlockUploadData(File file) {
      Preconditions.checkArgument(file.exists(), "No file: " + file);
      this.file = file;
      this.uploadStream = null;
    }

    /**
     * Stream constructor, file field will be null.
     * @param uploadStream stream to upload
     */
    BlockUploadData(InputStream uploadStream) {
      Preconditions.checkNotNull(uploadStream, "rawUploadStream");
      this.uploadStream = uploadStream;
      this.file = null;
    }

    /**
     * Predicate: does this instance contain a file reference.
     * @return true if there is a file.
     */
    boolean hasFile() {
      return file != null;
    }

    /**
     * Get the file, if there is one.
     * @return the file for uploading, or null.
     */
    File getFile() {
      return file;
    }

    /**
     * Get the raw upload stream, if the object was
     * created with one.
     * @return the upload stream or null.
     */
    InputStream getUploadStream() {
      return uploadStream;
    }

    /**
     * Close: closes any upload stream provided in the constructor.
     * @throws IOException inherited exception
     */
    @Override
    public void close() throws IOException {
      cleanupWithLogger(LOG, uploadStream);
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
     *
     * @param index index of block
     * @param limit limit of the block.
     * @param statistics stats to work with
     * @return a new block.
     */
    abstract DataBlock create(long index, long limit,
        BlockOutputStreamStatistics statistics)
        throws IOException;

    /**
     * Implement any close/cleanup operation.
     * Base class is a no-op
     * @throws IOException Inherited exception; implementations should
     * avoid raising it.
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
    protected final long index;
    private final BlockOutputStreamStatistics statistics;

    protected DataBlock(long index,
        BlockOutputStreamStatistics statistics) {
      this.index = index;
      this.statistics = statistics;
    }

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
    abstract long dataSize();

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
    abstract long remainingCapacity();

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
    BlockUploadData startUpload() throws IOException {
      LOG.debug("Start datablock[{}] upload", index);
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

    /**
     * A block has been allocated.
     */
    protected void blockAllocated() {
      if (statistics != null) {
        statistics.blockAllocated();
      }
    }

    /**
     * A block has been released.
     */
    protected void blockReleased() {
      if (statistics != null) {
        statistics.blockReleased();
      }
    }

    protected BlockOutputStreamStatistics getStatistics() {
      return statistics;
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
    DataBlock create(long index, long limit,
        BlockOutputStreamStatistics statistics)
        throws IOException {
      Preconditions.checkArgument(limit > 0,
          "Invalid block size: %d", limit);
      return new ByteArrayBlock(0, limit, statistics);
    }

  }

  static class S3AByteArrayOutputStream extends ByteArrayOutputStream {

    S3AByteArrayOutputStream(int size) {
      super(size);
    }

    /**
     * InputStream backed by the internal byte array.
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

    ByteArrayBlock(long index,
        long limit,
        BlockOutputStreamStatistics statistics) {
      super(index, statistics);
      this.limit = (limit > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) limit;
      buffer = new S3AByteArrayOutputStream(this.limit);
      blockAllocated();
    }

    /**
     * Get the amount of data; if there is no buffer then the size is 0.
     * @return the amount of data available to upload.
     */
    @Override
    long dataSize() {
      return dataSize != null ? dataSize : buffer.size();
    }

    @Override
    BlockUploadData startUpload() throws IOException {
      super.startUpload();
      dataSize = buffer.size();
      ByteArrayInputStream bufferData = buffer.getInputStream();
      buffer = null;
      return new BlockUploadData(bufferData);
    }

    @Override
    boolean hasCapacity(long bytes) {
      return dataSize() + bytes <= limit;
    }

    @Override
    long remainingCapacity() {
      return limit - dataSize();
    }

    @Override
    int write(byte[] b, int offset, int len) throws IOException {
      super.write(b, offset, len);
      int written = (int) Math.min(remainingCapacity(), len);
      buffer.write(b, offset, written);
      return written;
    }

    @Override
    protected void innerClose() {
      buffer = null;
      blockReleased();
    }

    @Override
    public String toString() {
      return "ByteArrayBlock{"
          +"index=" + index +
          ", state=" + getState() +
          ", limit=" + limit +
          ", dataSize=" + dataSize +
          '}';
    }
  }

  // ====================================================================

  /**
   * Stream via Direct ByteBuffers; these are allocated off heap
   * via {@link DirectBufferPool}.
   */

  static class ByteBufferBlockFactory extends BlockFactory {

    private final DirectBufferPool bufferPool = new DirectBufferPool();
    private final AtomicInteger buffersOutstanding = new AtomicInteger(0);

    ByteBufferBlockFactory(S3AFileSystem owner) {
      super(owner);
    }

    @Override
    ByteBufferBlock create(long index, long limit,
        BlockOutputStreamStatistics statistics)
        throws IOException {
      Preconditions.checkArgument(limit > 0,
          "Invalid block size: %d", limit);
      return new ByteBufferBlock(index, limit, statistics);
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
     * it when it is closed.
     */
    class ByteBufferBlock extends DataBlock {
      private ByteBuffer blockBuffer;
      private final int bufferSize;
      // cache data size so that it is consistent after the buffer is reset.
      private Integer dataSize;

      /**
       * Instantiate. This will request a ByteBuffer of the desired size.
       * @param index block index
       * @param bufferSize buffer size
       * @param statistics statistics to update
       */
      ByteBufferBlock(long index,
          long bufferSize,
          BlockOutputStreamStatistics statistics) {
        super(index, statistics);
        this.bufferSize = bufferSize > Integer.MAX_VALUE ?
            Integer.MAX_VALUE : (int) bufferSize;
        blockBuffer = requestBuffer(this.bufferSize);
        blockAllocated();
      }

      /**
       * Get the amount of data; if there is no buffer then the size is 0.
       * @return the amount of data available to upload.
       */
      @Override
      long dataSize() {
        return dataSize != null ? dataSize : bufferCapacityUsed();
      }

      @Override
      BlockUploadData startUpload() throws IOException {
        super.startUpload();
        dataSize = bufferCapacityUsed();
        // set the buffer up from reading from the beginning
        blockBuffer.limit(blockBuffer.position());
        blockBuffer.position(0);
        return new BlockUploadData(
            new ByteBufferInputStream(dataSize, blockBuffer));
      }

      @Override
      public boolean hasCapacity(long bytes) {
        return bytes <= remainingCapacity();
      }

      @Override
      public long remainingCapacity() {
        return blockBuffer != null ? blockBuffer.remaining() : 0;
      }

      private int bufferCapacityUsed() {
        return blockBuffer.capacity() - blockBuffer.remaining();
      }

      @Override
      int write(byte[] b, int offset, int len) throws IOException {
        super.write(b, offset, len);
        int written = (int) Math.min(remainingCapacity(), len);
        blockBuffer.put(b, offset, written);
        return written;
      }

      /**
       * Closing the block will release the buffer.
       */
      @Override
      protected void innerClose() {
        if (blockBuffer != null) {
          blockReleased();
          releaseBuffer(blockBuffer);
          blockBuffer = null;
        }
      }

      @Override
      public String toString() {
        return "ByteBufferBlock{"
            + "index=" + index +
            ", state=" + getState() +
            ", dataSize=" + dataSize() +
            ", limit=" + bufferSize +
            ", remainingCapacity=" + remainingCapacity() +
            '}';
      }

      /**
       * Provide an input stream from a byte buffer; supporting
       * {@link #mark(int)}, which is required to enable replay of failed
       * PUT attempts.
       */
      class ByteBufferInputStream extends InputStream {

        private final int size;
        private ByteBuffer byteBuffer;

        ByteBufferInputStream(int size,
            ByteBuffer byteBuffer) {
          LOG.debug("Creating ByteBufferInputStream of size {}", size);
          this.size = size;
          this.byteBuffer = byteBuffer;
        }

        /**
         * After the stream is closed, set the local reference to the byte
         * buffer to null; this guarantees that future attempts to use
         * stream methods will fail.
         */
        @Override
        public synchronized void close() {
          LOG.debug("ByteBufferInputStream.close() for {}",
              ByteBufferBlock.super.toString());
          byteBuffer = null;
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
         * @param b destination buffer
         * @param offset offset within the buffer
         * @param length length of bytes to read
         * @throws EOFException if the position is negative
         * @throws IndexOutOfBoundsException if there isn't space for the
         * amount of data requested.
         * @throws IllegalArgumentException other arguments are invalid.
         */
        @SuppressWarnings("NullableProblems")
        public synchronized int read(byte[] b, int offset, int length)
            throws IOException {
          Preconditions.checkArgument(length >= 0, "length is negative");
          Preconditions.checkArgument(b != null, "Null buffer");
          if (b.length - offset < length) {
            throw new IndexOutOfBoundsException(
                FSExceptionMessages.TOO_MANY_BYTES_FOR_DEST_BUFFER
                    + ": request length =" + length
                    + ", with offset =" + offset
                    + "; buffer capacity =" + (b.length - offset));
          }
          verifyOpen();
          if (!hasRemaining()) {
            return -1;
          }

          int toRead = Math.min(length, available());
          byteBuffer.get(b, offset, toRead);
          return toRead;
        }

        @Override
        public String toString() {
          final StringBuilder sb = new StringBuilder(
              "ByteBufferInputStream{");
          sb.append("size=").append(size);
          ByteBuffer buf = this.byteBuffer;
          if (buf != null) {
            sb.append(", available=").append(buf.remaining());
          }
          sb.append(", ").append(ByteBufferBlock.super.toString());
          sb.append('}');
          return sb.toString();
        }
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
     * Create a temp file and a {@link DiskBlock} instance to manage it.
     *
     * @param index block index
     * @param limit limit of the block. -1 means "no limit"
     * @param statistics statistics to update
     * @return the new block
     * @throws IOException IO problems
     */
    @Override
    DataBlock create(long index,
        long limit,
        BlockOutputStreamStatistics statistics)
        throws IOException {
      Preconditions.checkArgument(limit != 0,
          "Invalid block size: %d", limit);
      File destFile = getOwner()
          .createTmpFileForWrite(String.format("s3ablock-%04d-", index),
              limit, getOwner().getConf());
      return new DiskBlock(destFile, limit, index, statistics);
    }
  }

  /**
   * Stream to a file.
   * This will stop at the limit; the caller is expected to create a new block.
   */
  static class DiskBlock extends DataBlock {

    private long bytesWritten;
    private final File bufferFile;
    private final long limit;
    private BufferedOutputStream out;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    DiskBlock(File bufferFile,
        long limit,
        long index,
        BlockOutputStreamStatistics statistics)
        throws FileNotFoundException {
      super(index, statistics);
      this.limit = limit;
      this.bufferFile = bufferFile;
      blockAllocated();
      out = new BufferedOutputStream(new FileOutputStream(bufferFile));
    }

    @Override
    long dataSize() {
      return bytesWritten;
    }

    /**
     * Does this block have unlimited space?
     * @return true if a block with no size limit was created.
     */
    private boolean unlimited() {
      return limit < 0;
    }

    @Override
    boolean hasCapacity(long bytes) {
      return unlimited() || dataSize() + bytes <= limit;
    }

    /**
     * {@inheritDoc}.
     * If there is no limit to capacity, return MAX_VALUE.
     * @return capacity in the block.
     */
    @Override
    long remainingCapacity() {
      return unlimited()
          ? Integer.MAX_VALUE
          : limit - bytesWritten;
    }

    @Override
    int write(byte[] b, int offset, int len) throws IOException {
      super.write(b, offset, len);
      int written = (int) Math.min(remainingCapacity(), len);
      out.write(b, offset, written);
      bytesWritten += written;
      return written;
    }

    @Override
    BlockUploadData startUpload() throws IOException {
      super.startUpload();
      try {
        out.flush();
      } finally {
        out.close();
        out = null;
      }
      return new BlockUploadData(bufferFile);
    }

    /**
     * The close operation will delete the destination file if it still
     * exists.
     * @throws IOException IO problems
     */
    @SuppressWarnings("UnnecessaryDefault")
    @Override
    protected void innerClose() throws IOException {
      final DestState state = getState();
      LOG.debug("Closing {}", this);
      switch (state) {
      case Writing:
        if (bufferFile.exists()) {
          // file was not uploaded
          LOG.debug("Block[{}]: Deleting buffer file as upload did not start",
              index);
          closeBlock();
        }
        break;

      case Upload:
        LOG.debug("Block[{}]: Buffer file {} exists —close upload stream",
            index, bufferFile);
        break;

      case Closed:
        closeBlock();
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
          + "index=" + index
          + ", destFile=" + bufferFile +
          ", state=" + getState() +
          ", dataSize=" + dataSize() +
          ", limit=" + limit +
          '}';
      return sb;
    }

    /**
     * Close the block.
     * This will delete the block's buffer file if the block has
     * not previously been closed.
     */
    void closeBlock() {
      LOG.debug("block[{}]: closeBlock()", index);
      if (!closed.getAndSet(true)) {
        blockReleased();
        if (!bufferFile.delete() && bufferFile.exists()) {
          LOG.warn("delete({}) returned false",
              bufferFile.getAbsoluteFile());
        }
      } else {
        LOG.debug("block[{}]: skipping re-entrant closeBlock()", index);
      }
    }
  }

}

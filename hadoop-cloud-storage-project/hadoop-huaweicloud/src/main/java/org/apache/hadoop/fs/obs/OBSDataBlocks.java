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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.util.DirectBufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * Set of classes to support output streaming into blocks which are then
 * uploaded as to OBS as a single PUT, or as part of a multipart request.
 */
final class OBSDataBlocks {

  /**
   * Class logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      OBSDataBlocks.class);

  private OBSDataBlocks() {
  }

  /**
   * Validate args to a write command. These are the same validation checks
   * expected for any implementation of {@code OutputStream.write()}.
   *
   * @param b   byte array containing data
   * @param off offset in array where to start
   * @param len number of bytes to be written
   * @throws NullPointerException      for a null buffer
   * @throws IndexOutOfBoundsException if indices are out of range
   */
  static void validateWriteArgs(final byte[] b, final int off,
      final int len) {
    Preconditions.checkNotNull(b);
    if (off < 0 || off > b.length || len < 0 || off + len > b.length
        || off + len < 0) {
      throw new IndexOutOfBoundsException(
          "write (b[" + b.length + "], " + off + ", " + len + ')');
    }
  }

  /**
   * Create a factory.
   *
   * @param owner factory owner
   * @param name  factory name -the option from {@link OBSConstants}.
   * @return the factory, ready to be initialized.
   * @throws IllegalArgumentException if the name is unknown.
   */
  static BlockFactory createFactory(final OBSFileSystem owner,
      final String name) {
    switch (name) {
    case OBSConstants.FAST_UPLOAD_BUFFER_ARRAY:
      return new ByteArrayBlockFactory(owner);
    case OBSConstants.FAST_UPLOAD_BUFFER_DISK:
      return new DiskBlockFactory(owner);
    case OBSConstants.FAST_UPLOAD_BYTEBUFFER:
      return new ByteBufferBlockFactory(owner);
    default:
      throw new IllegalArgumentException(
          "Unsupported block buffer" + " \"" + name + '"');
    }
  }

  /**
   * Base class for block factories.
   */
  abstract static class BlockFactory {
    /**
     * OBS file system type.
     */
    private final OBSFileSystem owner;

    protected BlockFactory(final OBSFileSystem obsFileSystem) {
      this.owner = obsFileSystem;
    }

    /**
     * Create a block.
     *
     * @param index index of block
     * @param limit limit of the block.
     * @return a new block.
     * @throws IOException on any failure to create block
     */
    abstract DataBlock create(long index, int limit) throws IOException;

    /**
     * Owner.
     *
     * @return obsFileSystem instance
     */
    protected OBSFileSystem getOwner() {
      return owner;
    }
  }

  /**
   * This represents a block being uploaded.
   */
  abstract static class DataBlock implements Closeable {

    /**
     * Data block index.
     */
    private final long index;

    /**
     * Dest state can be : writing/upload/closed.
     */
    private volatile DestState state = DestState.Writing;

    protected DataBlock(final long dataIndex) {
      this.index = dataIndex;
    }

    /**
     * Atomically enter a state, verifying current state.
     *
     * @param current current state. null means "no check"
     * @param next    next state
     * @throws IllegalStateException if the current state is not as expected
     */
    protected final synchronized void enterState(final DestState current,
        final DestState next)
        throws IllegalStateException {
      verifyState(current);
      LOG.debug("{}: entering state {}", this, next);
      state = next;
    }

    /**
     * Verify that the block is in the declared state.
     *
     * @param expected expected state.
     * @throws IllegalStateException if the DataBlock is in the wrong state
     */
    protected final void verifyState(final DestState expected)
        throws IllegalStateException {
      if (expected != null && state != expected) {
        throw new IllegalStateException(
            "Expected stream state " + expected
                + " -but actual state is " + state + " in " + this);
      }
    }

    /**
     * Current state.
     *
     * @return the current state.
     */
    protected final DestState getState() {
      return state;
    }

    protected long getIndex() {
      return index;
    }

    /**
     * Return the current data size.
     *
     * @return the size of the data
     */
    abstract int dataSize();

    /**
     * Predicate to verify that the block has the capacity to write the given
     * set of bytes.
     *
     * @param bytes number of bytes desired to be written.
     * @return true if there is enough space.
     */
    abstract boolean hasCapacity(long bytes);

    /**
     * Predicate to check if there is data in the block.
     *
     * @return true if there is
     */
    boolean hasData() {
      return dataSize() > 0;
    }

    /**
     * The remaining capacity in the block before it is full.
     *
     * @return the number of bytes remaining.
     */
    abstract int remainingCapacity();

    /**
     * Write a series of bytes from the buffer, from the offset. Returns the
     * number of bytes written. Only valid in the state {@code Writing}. Base
     * class verifies the state but does no writing.
     *
     * @param buffer buffer
     * @param offset offset
     * @param length length of write
     * @return number of bytes written
     * @throws IOException trouble
     */
    int write(final byte[] buffer, final int offset, final int length)
        throws IOException {
      verifyState(DestState.Writing);
      Preconditions.checkArgument(buffer != null, "Null buffer");
      Preconditions.checkArgument(length >= 0, "length is negative");
      Preconditions.checkArgument(offset >= 0, "offset is negative");
      Preconditions.checkArgument(
          !(buffer.length - offset < length),
          "buffer shorter than amount of data to write");
      return 0;
    }

    /**
     * Flush the output. Only valid in the state {@code Writing}. In the base
     * class, this is a no-op
     *
     * @throws IOException any IO problem.
     */
    void flush() throws IOException {
      verifyState(DestState.Writing);
    }

    /**
     * Switch to the upload state and return a stream for uploading. Base class
     * calls {@link #enterState(DestState, DestState)} to manage the state
     * machine.
     *
     * @return the stream
     * @throws IOException trouble
     */
    Object startUpload() throws IOException {
      LOG.debug("Start datablock[{}] upload", index);
      enterState(DestState.Writing, DestState.Upload);
      return null;
    }

    /**
     * Enter the closed state.
     *
     * @return true if the class was in any other state, implying that the
     * subclass should do its close operations
     */
    protected synchronized boolean enterClosedState() {
      if (!state.equals(DestState.Closed)) {
        enterState(null, DestState.Closed);
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
     *
     * @throws IOException on any failure to close
     */
    protected abstract void innerClose() throws IOException;

    /**
     * Destination state definition for a data block.
     */
    enum DestState {
      /**
       * destination state : writing.
       */
      Writing,
      /**
       * destination state : upload.
       */
      Upload,
      /**
       * destination state : closed.
       */
      Closed
    }
  }

  /**
   * Use byte arrays on the heap for storage.
   */
  static class ByteArrayBlockFactory extends BlockFactory {
    ByteArrayBlockFactory(final OBSFileSystem owner) {
      super(owner);
    }

    @Override
    DataBlock create(final long index, final int limit) {
      int firstBlockSize = super.owner.getConf()
          .getInt(OBSConstants.FAST_UPLOAD_BUFFER_ARRAY_FIRST_BLOCK_SIZE,
              OBSConstants
                  .FAST_UPLOAD_BUFFER_ARRAY_FIRST_BLOCK_SIZE_DEFAULT);
      return new ByteArrayBlock(0, limit, firstBlockSize);
    }
  }

  /**
   * OBS specific byte array output stream.
   */
  static class OBSByteArrayOutputStream extends ByteArrayOutputStream {
    OBSByteArrayOutputStream(final int size) {
      super(size);
    }

    /**
     * InputStream backed by the internal byte array.
     *
     * @return input stream
     */
    ByteArrayInputStream getInputStream() {
      ByteArrayInputStream bin = new ByteArrayInputStream(this.buf, 0,
          count);
      this.reset();
      this.buf = null;
      return bin;
    }
  }

  /**
   * Stream to memory via a {@code ByteArrayOutputStream}.
   *
   * <p>This was taken from {@code OBSBlockOutputStream} and has the same
   * problem which surfaced there: it can consume a lot of heap space
   * proportional to the mismatch between writes to the stream and the JVM-wide
   * upload bandwidth to the OBS endpoint. The memory consumption can be limited
   * by tuning the filesystem settings to restrict the number of queued/active
   * uploads.
   */
  static class ByteArrayBlock extends DataBlock {
    /**
     * Memory limit.
     */
    private final int limit;

    /**
     * Output stream.
     */
    private OBSByteArrayOutputStream buffer;

    /**
     * Cache data size so that it is consistent after the buffer is reset.
     */
    private Integer dataSize;

    /**
     * Block first size.
     */
    private int firstBlockSize;

    /**
     * Input stream.
     */
    private ByteArrayInputStream inputStream = null;

    ByteArrayBlock(final long index, final int limitBlockSize,
        final int blockSize) {
      super(index);
      this.limit = limitBlockSize;
      this.buffer = new OBSByteArrayOutputStream(blockSize);
      this.firstBlockSize = blockSize;
    }

    /**
     * Returns the block first block size.
     *
     * @return the block first block size
     */
    @VisibleForTesting
    public int firstBlockSize() {
      return this.firstBlockSize;
    }

    /**
     * Get the amount of data; if there is no buffer then the size is 0.
     *
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
      inputStream = buffer.getInputStream();
      return inputStream;
    }

    @Override
    boolean hasCapacity(final long bytes) {
      return dataSize() + bytes <= limit;
    }

    @Override
    int remainingCapacity() {
      return limit - dataSize();
    }

    @Override
    int write(final byte[] b, final int offset, final int len)
        throws IOException {
      super.write(b, offset, len);
      int written = Math.min(remainingCapacity(), len);
      buffer.write(b, offset, written);
      return written;
    }

    @Override
    protected void innerClose() throws IOException {
      if (buffer != null) {
        buffer.close();
        buffer = null;
      }

      if (inputStream != null) {
        inputStream.close();
        inputStream = null;
      }
    }

    @Override
    public String toString() {
      return "ByteArrayBlock{"
          + "index="
          + getIndex()
          + ", state="
          + getState()
          + ", limit="
          + limit
          + ", dataSize="
          + dataSize
          + '}';
    }
  }

  /**
   * Stream via Direct ByteBuffers; these are allocated off heap via {@link
   * DirectBufferPool}.
   */
  static class ByteBufferBlockFactory extends BlockFactory {

    /**
     * The directory buffer pool.
     */
    private static final DirectBufferPool BUFFER_POOL
        = new DirectBufferPool();

    /**
     * Count of outstanding buffers.
     */
    private static final AtomicInteger BUFFERS_OUTSTANDING
        = new AtomicInteger(0);

    ByteBufferBlockFactory(final OBSFileSystem owner) {
      super(owner);
    }

    @Override
    ByteBufferBlock create(final long index, final int limit) {
      return new ByteBufferBlock(index, limit);
    }

    public static ByteBuffer requestBuffer(final int limit) {
      LOG.debug("Requesting buffer of size {}", limit);
      BUFFERS_OUTSTANDING.incrementAndGet();
      return BUFFER_POOL.getBuffer(limit);
    }

    public static void releaseBuffer(final ByteBuffer buffer) {
      LOG.debug("Releasing buffer");
      BUFFER_POOL.returnBuffer(buffer);
      BUFFERS_OUTSTANDING.decrementAndGet();
    }

    /**
     * Get count of outstanding buffers.
     *
     * @return the current buffer count
     */
    public int getOutstandingBufferCount() {
      return BUFFERS_OUTSTANDING.get();
    }

    @Override
    public String toString() {
      return "ByteBufferBlockFactory{" + "buffersOutstanding="
          + BUFFERS_OUTSTANDING + '}';
    }
  }

  /**
   * A DataBlock which requests a buffer from pool on creation; returns it when
   * it is closed.
   */
  static class ByteBufferBlock extends DataBlock {
    /**
     * Set the buffer size.
     */
    private final int bufferSize;

    /**
     * Create block buffer.
     */
    private ByteBuffer blockBuffer;

    /**
     * Cache data size so that it is consistent after the buffer is reset.
     */
    private Integer dataSize;

    /**
     * Create input stream.
     */
    private ByteBufferInputStream inputStream;

    /**
     * Instantiate. This will request a ByteBuffer of the desired size.
     *
     * @param index          block index
     * @param initBufferSize buffer size
     */
    ByteBufferBlock(final long index, final int initBufferSize) {
      super(index);
      this.bufferSize = initBufferSize;
      blockBuffer = ByteBufferBlockFactory.requestBuffer(initBufferSize);
    }

    /**
     * Get the amount of data; if there is no buffer then the size is 0.
     *
     * @return the amount of data available to upload.
     */
    @Override
    int dataSize() {
      return dataSize != null ? dataSize : bufferCapacityUsed();
    }

    @Override
    InputStream startUpload() throws IOException {
      super.startUpload();
      dataSize = bufferCapacityUsed();
      // set the buffer up from reading from the beginning
      blockBuffer.limit(blockBuffer.position());
      blockBuffer.position(0);
      inputStream = new ByteBufferInputStream(dataSize, blockBuffer);
      return inputStream;
    }

    @Override
    public boolean hasCapacity(final long bytes) {
      return bytes <= remainingCapacity();
    }

    @Override
    public int remainingCapacity() {
      return blockBuffer != null ? blockBuffer.remaining() : 0;
    }

    private int bufferCapacityUsed() {
      return blockBuffer.capacity() - blockBuffer.remaining();
    }

    @Override
    int write(final byte[] b, final int offset, final int len)
        throws IOException {
      super.write(b, offset, len);
      int written = Math.min(remainingCapacity(), len);
      blockBuffer.put(b, offset, written);
      return written;
    }

    /**
     * Closing the block will release the buffer.
     */
    @Override
    protected void innerClose() {
      if (blockBuffer != null) {
        ByteBufferBlockFactory.releaseBuffer(blockBuffer);
        blockBuffer = null;
      }
      if (inputStream != null) {
        inputStream.close();
        inputStream = null;
      }
    }

    @Override
    public String toString() {
      return "ByteBufferBlock{"
          + "index="
          + getIndex()
          + ", state="
          + getState()
          + ", dataSize="
          + dataSize()
          + ", limit="
          + bufferSize
          + ", remainingCapacity="
          + remainingCapacity()
          + '}';
    }

    /**
     * Provide an input stream from a byte buffer; supporting {@link
     * #mark(int)}, which is required to enable replay of failed PUT attempts.
     */
    class ByteBufferInputStream extends InputStream {

      /**
       * Set the input stream size.
       */
      private final int size;

      /**
       * Set the byte buffer.
       */
      private ByteBuffer byteBuffer;

      ByteBufferInputStream(final int streamSize,
          final ByteBuffer streamByteBuffer) {
        LOG.debug("Creating ByteBufferInputStream of size {}",
            streamSize);
        this.size = streamSize;
        this.byteBuffer = streamByteBuffer;
      }

      /**
       * After the stream is closed, set the local reference to the byte buffer
       * to null; this guarantees that future attempts to use stream methods
       * will fail.
       */
      @Override
      public synchronized void close() {
        LOG.debug("ByteBufferInputStream.close() for {}",
            ByteBufferBlock.super.toString());
        byteBuffer = null;
      }

      /**
       * Verify that the stream is open.
       *
       * @throws IOException if the stream is closed
       */
      private void verifyOpen() throws IOException {
        if (byteBuffer == null) {
          throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
      }

      public synchronized int read() {
        if (available() > 0) {
          return byteBuffer.get() & OBSCommonUtils.BYTE_TO_INT_MASK;
        } else {
          return -1;
        }
      }

      @Override
      public synchronized long skip(final long offset)
          throws IOException {
        verifyOpen();
        long newPos = position() + offset;
        if (newPos < 0) {
          throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
        }
        if (newPos > size) {
          throw new EOFException(
              FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
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
       *
       * @return the buffer position
       */
      public synchronized int position() {
        return byteBuffer.position();
      }

      /**
       * Check if there is data left.
       *
       * @return true if there is data remaining in the buffer.
       */
      public synchronized boolean hasRemaining() {
        return byteBuffer.hasRemaining();
      }

      @Override
      public synchronized void mark(final int readlimit) {
        LOG.debug("mark at {}", position());
        byteBuffer.mark();
      }

      @Override
      public synchronized void reset() {
        LOG.debug("reset");
        byteBuffer.reset();
      }

      @Override
      public boolean markSupported() {
        return true;
      }

      /**
       * Read in data.
       *
       * @param b      destination buffer
       * @param offset offset within the buffer
       * @param length length of bytes to read
       * @return read size
       * @throws EOFException              if the position is negative
       * @throws IndexOutOfBoundsException if there isn't space for the amount
       *                                   of data requested.
       * @throws IllegalArgumentException  other arguments are invalid.
       */
      public synchronized int read(final byte[] b, final int offset,
          final int length)
          throws IOException {
        Preconditions.checkArgument(length >= 0, "length is negative");
        Preconditions.checkArgument(b != null, "Null buffer");
        if (b.length - offset < length) {
          throw new IndexOutOfBoundsException(
              FSExceptionMessages.TOO_MANY_BYTES_FOR_DEST_BUFFER
                  + ": request length ="
                  + length
                  + ", with offset ="
                  + offset
                  + "; buffer capacity ="
                  + (b.length - offset));
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

  /**
   * Buffer blocks to disk.
   */
  static class DiskBlockFactory extends BlockFactory {
    /**
     * Allocator the local directory.
     */
    private static LocalDirAllocator directoryAllocator;

    DiskBlockFactory(final OBSFileSystem owner) {
      super(owner);
    }

    /**
     * Create a temp file and a {@link DiskBlock} instance to manage it.
     *
     * @param index block index
     * @param limit limit of the block.
     * @return the new block
     * @throws IOException IO problems
     */
    @Override
    DataBlock create(final long index, final int limit) throws IOException {
      File destFile = createTmpFileForWrite(
          String.format("obs-block-%04d-", index), limit,
          getOwner().getConf());
      return new DiskBlock(destFile, limit, index);
    }

    /**
     * Demand create the directory allocator, then create a temporary file.
     * {@link LocalDirAllocator#createTmpFileForWrite(String, long,
     * Configuration)}.
     *
     * @param pathStr prefix for the temporary file
     * @param size    the size of the file that is going to be written
     * @param conf    the Configuration object
     * @return a unique temporary file
     * @throws IOException IO problems
     */
    static synchronized File createTmpFileForWrite(final String pathStr,
        final long size, final Configuration conf)
        throws IOException {
      if (directoryAllocator == null) {
        String bufferDir = conf.get(OBSConstants.BUFFER_DIR) != null
            ? OBSConstants.BUFFER_DIR
            : "hadoop.tmp.dir";
        directoryAllocator = new LocalDirAllocator(bufferDir);
      }
      return directoryAllocator.createTmpFileForWrite(pathStr, size,
          conf);
    }
  }

  /**
   * Stream to a file. This will stop at the limit; the caller is expected to
   * create a new block.
   */
  static class DiskBlock extends DataBlock {

    /**
     * Create buffer file.
     */
    private final File bufferFile;

    /**
     * Buffer size limit.
     */
    private final int limit;

    /**
     * Verify block has closed or not.
     */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Written bytes count.
     */
    private int bytesWritten;

    /**
     * Out put stream buffer.
     */
    private BufferedOutputStream out;

    DiskBlock(final File destBufferFile, final int limitSize,
        final long index)
        throws FileNotFoundException {
      super(index);
      this.limit = limitSize;
      this.bufferFile = destBufferFile;
      out = new BufferedOutputStream(
          new FileOutputStream(destBufferFile));
    }

    @Override
    int dataSize() {
      return bytesWritten;
    }

    @Override
    boolean hasCapacity(final long bytes) {
      return dataSize() + bytes <= limit;
    }

    @Override
    int remainingCapacity() {
      return limit - bytesWritten;
    }

    @Override
    int write(final byte[] b, final int offset, final int len)
        throws IOException {
      super.write(b, offset, len);
      int written = Math.min(remainingCapacity(), len);
      out.write(b, offset, written);
      bytesWritten += written;
      return written;
    }

    @Override
    File startUpload() throws IOException {
      super.startUpload();
      try {
        out.flush();
      } finally {
        out.close();
        out = null;
      }
      return bufferFile;
    }

    /**
     * The close operation will delete the destination file if it still exists.
     */
    @Override
    protected void innerClose() {
      final DestState state = getState();
      LOG.debug("Closing {}", this);
      switch (state) {
      case Writing:
        if (bufferFile.exists()) {
          // file was not uploaded
          LOG.debug(
              "Block[{}]: Deleting buffer file as upload "
                  + "did not start",
              getIndex());
          closeBlock();
        }
        break;

      case Upload:
        LOG.debug(
            "Block[{}]: Buffer file {} exists close upload stream",
            getIndex(), bufferFile);
        break;

      case Closed:
        closeBlock();
        break;

      default:
        // this state can never be reached, but checkstyle
        // complains, so it is here.
      }
    }

    /**
     * Flush operation will flush to disk.
     *
     * @throws IOException IOE raised on FileOutputStream
     */
    @Override
    void flush() throws IOException {
      super.flush();
      out.flush();
    }

    @Override
    public String toString() {
      return "FileBlock{index=" + getIndex() + ", destFile=" + bufferFile
          + ", state=" + getState() + ", dataSize="
          + dataSize() + ", limit=" + limit + '}';
    }

    /**
     * Close the block. This will delete the block's buffer file if the block
     * has not previously been closed.
     */
    void closeBlock() {
      LOG.debug("block[{}]: closeBlock()", getIndex());
      if (!closed.getAndSet(true)) {
        if (!bufferFile.delete() && bufferFile.exists()) {
          LOG.warn("delete({}) returned false",
              bufferFile.getAbsoluteFile());
        }
      } else {
        LOG.debug("block[{}]: skipping re-entrant closeBlock()",
            getIndex());
      }
    }
  }
}

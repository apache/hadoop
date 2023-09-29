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

package org.apache.hadoop.fs.store;

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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.DirectBufferPool;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_TMP_DIR;
import static org.apache.hadoop.fs.store.DataBlocks.DataBlock.DestState.Closed;
import static org.apache.hadoop.fs.store.DataBlocks.DataBlock.DestState.Upload;
import static org.apache.hadoop.fs.store.DataBlocks.DataBlock.DestState.Writing;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;

/**
 * A class to provide disk, byteBuffer and byteArray option for Filesystem
 * OutputStreams.
 * <ul>
 *   <li>
 *     Disk: Uses Disk space to write the blocks. Is suited best to avoid
 *     OutOfMemory Errors in Java heap space.
 *   </li>
 *   <li>
 *     byteBuffer: Uses DirectByteBuffer to allocate memory off-heap to
 *     provide faster writing of DataBlocks with some risk of running
 *     OutOfMemory.
 *   </li>
 *   <li>
 *     byteArray: Uses byte[] to write a block of data. On heap and does have
 *     risk of running OutOfMemory fairly easily.
 *   </li>
 * </ul>
 * <p>
 * Implementation of DataBlocks taken from HADOOP-13560 to support huge file
 * uploads in S3A with different options rather than one.
 */
public final class DataBlocks {

  private static final Logger LOG =
      LoggerFactory.getLogger(DataBlocks.class);

  /**
   * Buffer blocks to disk.
   * Capacity is limited to available disk space.
   */
  public static final String DATA_BLOCKS_BUFFER_DISK = "disk";

  /**
   * Use a byte buffer.
   */
  public static final String DATA_BLOCKS_BYTEBUFFER = "bytebuffer";

  /**
   * Use an in-memory array. Fast but will run of heap rapidly.
   */
  public static final String DATA_BLOCKS_BUFFER_ARRAY = "array";

  private DataBlocks() {
  }

  /**
   * Validate args to a write command. These are the same validation checks
   * expected for any implementation of {@code OutputStream.write()}.
   *
   * @param b   byte array containing data.
   * @param off offset in array where to start.
   * @param len number of bytes to be written.
   * @throws NullPointerException      for a null buffer
   * @throws IndexOutOfBoundsException if indices are out of range
   * @throws IOException raised on errors performing I/O.
   */
  public static void validateWriteArgs(byte[] b, int off, int len)
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
   *
   * @param keyToBufferDir Key to buffer directory config for a FS.
   * @param configuration  factory configurations.
   * @param name           factory name -the option from {@link CommonConfigurationKeys}.
   * @return the factory, ready to be initialized.
   * @throws IllegalArgumentException if the name is unknown.
   */
  public static BlockFactory createFactory(String keyToBufferDir,
      Configuration configuration,
      String name) {
    LOG.debug("Creating DataFactory of type : {}", name);
    switch (name) {
    case DATA_BLOCKS_BUFFER_ARRAY:
      return new ArrayBlockFactory(keyToBufferDir, configuration);
    case DATA_BLOCKS_BUFFER_DISK:
      return new DiskBlockFactory(keyToBufferDir, configuration);
    case DATA_BLOCKS_BYTEBUFFER:
      return new ByteBufferBlockFactory(keyToBufferDir, configuration);
    default:
      throw new IllegalArgumentException("Unsupported block buffer" +
          " \"" + name + '"');
    }
  }

  /**
   * The output information for an upload.
   * It can be one of a file, an input stream or a byteArray.
   * {@link #toByteArray()} method to be used to convert the data into byte
   * array to be done in this class as well.
   * When closed, any stream is closed. Any source file is untouched.
   */
  public static final class BlockUploadData implements Closeable {
    private final File file;
    private InputStream uploadStream;
    private byte[] byteArray;
    private boolean isClosed;

    /**
     * Constructor for byteArray upload data block. File and uploadStream
     * would be null.
     *
     * @param byteArray byteArray used to construct BlockUploadData.
     */
    public BlockUploadData(byte[] byteArray) {
      this.file = null;
      this.uploadStream = null;

      this.byteArray = requireNonNull(byteArray);
    }

    /**
     * File constructor; input stream and byteArray will be null.
     *
     * @param file file to upload
     */
    BlockUploadData(File file) {
      Preconditions.checkArgument(file.exists(), "No file: %s", file);
      this.file = file;
      this.uploadStream = null;
      this.byteArray = null;
    }

    /**
     * Stream constructor, file and byteArray field will be null.
     *
     * @param uploadStream stream to upload.
     */
    BlockUploadData(InputStream uploadStream) {
      requireNonNull(uploadStream, "rawUploadStream");
      this.uploadStream = uploadStream;
      this.file = null;
      this.byteArray = null;
    }

    /**
     * Predicate: does this instance contain a file reference.
     *
     * @return true if there is a file.
     */
    boolean hasFile() {
      return file != null;
    }

    /**
     * Get the file, if there is one.
     *
     * @return the file for uploading, or null.
     */
    File getFile() {
      return file;
    }

    /**
     * Get the raw upload stream, if the object was
     * created with one.
     *
     * @return the upload stream or null.
     */
    InputStream getUploadStream() {
      return uploadStream;
    }

    /**
     * Convert to a byte array.
     * If the data is stored in a file, it will be read and returned.
     * If the data was passed in via an input stream (which happens if the
     * data is stored in a bytebuffer) then it will be converted to a byte
     * array -which will then be cached for any subsequent use.
     *
     * @return byte[] after converting the uploadBlock.
     * @throws IOException throw if an exception is caught while reading
     *                     File/InputStream or closing InputStream.
     */
    public byte[] toByteArray() throws IOException {
      Preconditions.checkState(!isClosed, "Block is closed");
      if (byteArray != null) {
        return byteArray;
      }
      if (file != null) {
        // Need to save byteArray here so that we don't read File if
        // byteArray() is called more than once on the same file.
        byteArray = FileUtils.readFileToByteArray(file);
        return byteArray;
      }
      byteArray = IOUtils.toByteArray(uploadStream);
      IOUtils.close(uploadStream);
      uploadStream = null;
      return byteArray;
    }

    /**
     * Close: closes any upload stream and byteArray provided in the
     * constructor.
     *
     * @throws IOException inherited exception.
     */
    @Override
    public void close() throws IOException {
      isClosed = true;
      cleanupWithLogger(LOG, uploadStream);
      byteArray = null;
      if (file != null) {
        LOG.debug("File deleted in BlockUploadData close: {}", file.delete());
      }
    }
  }

  /**
   * Base class for block factories.
   */
  public static abstract class BlockFactory implements Closeable {

    private final String keyToBufferDir;
    private final Configuration conf;

    protected BlockFactory(String keyToBufferDir, Configuration conf) {
      this.keyToBufferDir = keyToBufferDir;
      this.conf = conf;
    }

    /**
     * Create a block.
     *
     * @param index      index of block
     * @param limit      limit of the block.
     * @param statistics stats to work with
     * @return a new block.
     * @throws IOException raised on errors performing I/O.
     */
    public abstract DataBlock create(long index, int limit,
        BlockUploadStatistics statistics)
        throws IOException;

    /**
     * Implement any close/cleanup operation.
     * Base class is a no-op.
     *
     * @throws IOException Inherited exception; implementations should
     *                     avoid raising it.
     */
    @Override
    public void close() throws IOException {
    }

    /**
     * Configuration.
     *
     * @return config passed to create the factory.
     */
    protected Configuration getConf() {
      return conf;
    }

    /**
     * Key to Buffer Directory config for a FS instance.
     *
     * @return String containing key to Buffer dir.
     */
    public String getKeyToBufferDir() {
      return keyToBufferDir;
    }
  }

  /**
   * This represents a block being uploaded.
   */
  public static abstract class DataBlock implements Closeable {

    public enum DestState {Writing, Upload, Closed}

    private volatile DestState state = Writing;
    private final long index;
    private final BlockUploadStatistics statistics;

    protected DataBlock(long index,
        BlockUploadStatistics statistics) {
      this.index = index;
      this.statistics = statistics;
    }

    /**
     * Atomically enter a state, verifying current state.
     *
     * @param current current state. null means "no check"
     * @param next    next state
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
     *
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
     *
     * @return the current state.
     */
    public final DestState getState() {
      return state;
    }

    /**
     * Return the current data size.
     *
     * @return the size of the data.
     */
    public abstract int dataSize();

    /**
     * Predicate to verify that the block has the capacity to write
     * the given set of bytes.
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
    public boolean hasData() {
      return dataSize() > 0;
    }

    /**
     * The remaining capacity in the block before it is full.
     *
     * @return the number of bytes remaining.
     */
    public abstract int remainingCapacity();

    /**
     * Write a series of bytes from the buffer, from the offset.
     * Returns the number of bytes written.
     * Only valid in the state {@code Writing}.
     * Base class verifies the state but does no writing.
     *
     * @param buffer buffer.
     * @param offset offset.
     * @param length length of write.
     * @return number of bytes written.
     * @throws IOException trouble
     */
    public int write(byte[] buffer, int offset, int length) throws IOException {
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
     *
     * @throws IOException any IO problem.
     */
    public void flush() throws IOException {
      verifyState(Writing);
    }

    /**
     * Switch to the upload state and return a stream for uploading.
     * Base class calls {@link #enterState(DestState, DestState)} to
     * manage the state machine.
     *
     * @return the stream.
     * @throws IOException trouble
     */
    public BlockUploadData startUpload() throws IOException {
      LOG.debug("Start datablock[{}] upload", index);
      enterState(Writing, Upload);
      return null;
    }

    /**
     * Enter the closed state.
     *
     * @return true if the class was in any other state, implying that
     * the subclass should do its close operations.
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
     *
     * @throws IOException raised on errors performing I/O.
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

    protected BlockUploadStatistics getStatistics() {
      return statistics;
    }

    public long getIndex() {
      return index;
    }
  }

  // ====================================================================

  /**
   * Use byte arrays on the heap for storage.
   */
  static class ArrayBlockFactory extends BlockFactory {

    ArrayBlockFactory(String keyToBufferDir, Configuration conf) {
      super(keyToBufferDir, conf);
    }

    @Override
    public DataBlock create(long index, int limit,
        BlockUploadStatistics statistics)
        throws IOException {
      return new ByteArrayBlock(0, limit, statistics);
    }

  }

  static class DataBlockByteArrayOutputStream extends ByteArrayOutputStream {

    DataBlockByteArrayOutputStream(int size) {
      super(size);
    }

    /**
     * InputStream backed by the internal byte array.
     *
     * @return ByteArrayInputStream instance.
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
   * <p>
   * It can consume a lot of heap space
   * proportional to the mismatch between writes to the stream and
   * the JVM-wide upload bandwidth to a Store's endpoint.
   * The memory consumption can be limited by tuning the filesystem settings
   * to restrict the number of queued/active uploads.
   */

  static class ByteArrayBlock extends DataBlock {
    private DataBlockByteArrayOutputStream buffer;
    private final int limit;
    // cache data size so that it is consistent after the buffer is reset.
    private Integer dataSize;

    ByteArrayBlock(long index,
        int limit,
        BlockUploadStatistics statistics) {
      super(index, statistics);
      this.limit = limit;
      this.buffer = new DataBlockByteArrayOutputStream(limit);
      blockAllocated();
    }

    /**
     * Get the amount of data; if there is no buffer then the size is 0.
     *
     * @return the amount of data available to upload.
     */
    @Override
    public int dataSize() {
      return dataSize != null ? dataSize : buffer.size();
    }

    @Override
    public BlockUploadData startUpload() throws IOException {
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
    public int remainingCapacity() {
      return limit - dataSize();
    }

    @Override
    public int write(byte[] b, int offset, int len) throws IOException {
      super.write(b, offset, len);
      int written = Math.min(remainingCapacity(), len);
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
          + "index=" + getIndex() +
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

    ByteBufferBlockFactory(String keyToBufferDir, Configuration conf) {
      super(keyToBufferDir, conf);
    }

    @Override public ByteBufferBlock create(long index, int limit,
        BlockUploadStatistics statistics)
        throws IOException {
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
     *
     * @return the current buffer count.
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
       *
       * @param index      block index.
       * @param bufferSize buffer size.
       * @param statistics statistics to update.
       */
      ByteBufferBlock(long index,
          int bufferSize,
          BlockUploadStatistics statistics) {
        super(index, statistics);
        this.bufferSize = bufferSize;
        this.blockBuffer = requestBuffer(bufferSize);
        blockAllocated();
      }

      /**
       * Get the amount of data; if there is no buffer then the size is 0.
       *
       * @return the amount of data available to upload.
       */
      @Override public int dataSize() {
        return dataSize != null ? dataSize : bufferCapacityUsed();
      }

      @Override
      public BlockUploadData startUpload() throws IOException {
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
      public int remainingCapacity() {
        return blockBuffer != null ? blockBuffer.remaining() : 0;
      }

      private int bufferCapacityUsed() {
        return blockBuffer.capacity() - blockBuffer.remaining();
      }

      @Override
      public int write(byte[] b, int offset, int len) throws IOException {
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
          blockReleased();
          releaseBuffer(blockBuffer);
          blockBuffer = null;
        }
      }

      @Override
      public String toString() {
        return "ByteBufferBlock{"
            + "index=" + getIndex() +
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
         *
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
         *
         * @param b      destination buffer.
         * @param offset offset within the buffer.
         * @param length length of bytes to read.
         * @throws EOFException              if the position is negative
         * @throws IndexOutOfBoundsException if there isn't space for the
         *                                   amount of data requested.
         * @throws IllegalArgumentException  other arguments are invalid.
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

    private LocalDirAllocator directoryAllocator;

    DiskBlockFactory(String keyToBufferDir, Configuration conf) {
      super(keyToBufferDir, conf);
      String bufferDir = conf.get(keyToBufferDir) != null
          ? keyToBufferDir : HADOOP_TMP_DIR;
      directoryAllocator = new LocalDirAllocator(bufferDir);
    }

    /**
     * Create a temp file and a {@link DiskBlock} instance to manage it.
     *
     * @param index      block index.
     * @param limit      limit of the block.
     * @param statistics statistics to update.
     * @return the new block.
     * @throws IOException IO problems
     */
    @Override
    public DataBlock create(long index,
        int limit,
        BlockUploadStatistics statistics)
        throws IOException {
      File destFile = createTmpFileForWrite(String.format("datablock-%04d-",
          index),
          limit, getConf());

      return new DiskBlock(destFile, limit, index, statistics);
    }

    /**
     * Demand create the directory allocator, then create a temporary file.
     * This does not mark the file for deletion when a process exits.
     * {@link LocalDirAllocator#createTmpFileForWrite(String, long, Configuration)}.
     *
     * @param pathStr prefix for the temporary file.
     * @param size    the size of the file that is going to be written.
     * @param conf    the Configuration object.
     * @return a unique temporary file.
     * @throws IOException IO problems
     */
    File createTmpFileForWrite(String pathStr, long size,
        Configuration conf) throws IOException {
      Path path = directoryAllocator.getLocalPathForWrite(pathStr,
          size, conf);
      File dir = new File(path.getParent().toUri().getPath());
      String prefix = path.getName();
      // create a temp file on this directory
      return File.createTempFile(prefix, null, dir);
    }
  }

  /**
   * Stream to a file.
   * This will stop at the limit; the caller is expected to create a new block.
   */
  static class DiskBlock extends DataBlock {

    private int bytesWritten;
    private final File bufferFile;
    private final int limit;
    private BufferedOutputStream out;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    DiskBlock(File bufferFile,
        int limit,
        long index,
        BlockUploadStatistics statistics)
        throws FileNotFoundException {
      super(index, statistics);
      this.limit = limit;
      this.bufferFile = bufferFile;
      blockAllocated();
      out = new BufferedOutputStream(new FileOutputStream(bufferFile));
    }

    @Override public int dataSize() {
      return bytesWritten;
    }

    @Override
    boolean hasCapacity(long bytes) {
      return dataSize() + bytes <= limit;
    }

    @Override public int remainingCapacity() {
      return limit - bytesWritten;
    }

    @Override
    public int write(byte[] b, int offset, int len) throws IOException {
      super.write(b, offset, len);
      int written = Math.min(remainingCapacity(), len);
      out.write(b, offset, written);
      bytesWritten += written;
      return written;
    }

    @Override
    public BlockUploadData startUpload() throws IOException {
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
     *
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
              getIndex());
          closeBlock();
        }
        break;

      case Upload:
        LOG.debug("Block[{}]: Buffer file {} exists —close upload stream",
            getIndex(), bufferFile);
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
     *
     * @throws IOException IOE raised on FileOutputStream
     */
    @Override public void flush() throws IOException {
      super.flush();
      out.flush();
    }

    @Override
    public String toString() {
      String sb = "FileBlock{"
          + "index=" + getIndex()
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
      LOG.debug("block[{}]: closeBlock()", getIndex());
      if (!closed.getAndSet(true)) {
        blockReleased();
        if (!bufferFile.delete() && bufferFile.exists()) {
          LOG.warn("delete({}) returned false",
              bufferFile.getAbsoluteFile());
        }
      } else {
        LOG.debug("block[{}]: skipping re-entrant closeBlock()", getIndex());
      }
    }
  }
}

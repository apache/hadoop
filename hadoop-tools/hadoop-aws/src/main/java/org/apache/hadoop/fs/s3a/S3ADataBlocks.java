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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.apache.hadoop.fs.s3a.S3ADataBlocks.DataBlock.DestState.*;

/**
 * Set of classes to support output streaming into blocks which are then
 * uploaded as partitions.
 */
class S3ADataBlocks {
  static final Logger LOG = LoggerFactory.getLogger(S3ADataBlocks.class);

  static abstract class AbstractBlockFactory implements Closeable {

    protected S3AFileSystem owner;

    /**
     * Bind to the factory owner.
     * @param fs owner filesystem
     */
    void init(S3AFileSystem fs) {
      this.owner = fs;
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
  }

  static class MemoryBlockFactory extends AbstractBlockFactory {

    @Override
    void init(S3AFileSystem fs) {
      super.init(fs);
    }

    @Override
    DataBlock create(int limit) throws IOException {
      return new ByteArrayBlock(limit);
    }


  }

  /**
   * Buffer blocks to disk.
   */
  static class DiskBlockFactory extends AbstractBlockFactory {

    @Override
    void init(S3AFileSystem fs) {
      super.init(fs);
    }

    @Override
    DataBlock create(int limit) throws IOException {
      File destFile = owner.getDirectoryAllocator()
          .createTmpFileForWrite("s3ablock", limit, owner.getConf());
      return new FileBlock(destFile, limit);
    }
  }

  static abstract class DataBlock implements Closeable {

    enum DestState {Writing, Upload, Closed}

    private volatile DestState state = Writing;

    protected synchronized void enterState(DestState current, DestState next) {
      verifyState(current);
      LOG.debug("{}: entering state {}" , this, next);
      state = next;
    }

    protected void verifyState(DestState current) {
      Preconditions.checkState(state == current,
          "Expected stream state " + current + " -but actual state is " + state
              + " in " + this);
    }

    DestState getState() {
      return state;
    }

    /**
     * Return the current data size.
     * @return the size of the data
     */
    abstract int dataSize();

    abstract boolean hasCapacity(long bytes);

    /**
     * Is there data in the block.
     * @return true if there is
     */
    boolean hasData() {
      return dataSize() > 0;
    }

    abstract int remainingCapacity();

    /**
     * Write a series of bytes from the buffer, from the offset.
     * Returns the number of bytes written:
     * @param b buffer
     * @param off offset
     * @param len length of write
     * @return number of bytes written
     * @throws IOException trouble
     */
    int write(byte b[], int off, int len) throws IOException {
      verifyState(Writing);
      return 0;
    }

    void flush() throws IOException {

    }

    /**
     * Switch to the upload state and return a stream for uploading.
     * @return the stream
     * @throws IOException trouble
     */
    InputStream openForUpload() throws IOException {
      enterState(Writing, Upload);
      return null;
    }

    /**
     * actions to take on block upload completion.
     * @throws IOException Any failure
     */
    void blockUploadCompleted() throws IOException {

    }

    /**
     * Enter the closed state.
     * @return true if the class was in any other state, implying that
     * the subclass should do its close operations
     */
    protected synchronized boolean enterClosedState() {
      if (!state.equals(Closed)) {
        enterState(state, Closed);
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * Stream to memory via a {@code ByteArrayOutputStream}.
   *
   * This was taken from {@link S3AFastOutputStream} and has the
   * same problem which surfaced there: it consumes heap space
   * proportional to the mismatch between writes to the stream and
   * the JVM-wide upload bandwidth to the S3 endpoint.
   */

  static class ByteArrayBlock extends DataBlock {
    private ByteArrayOutputStream buffer;
    private int limit;
    // cache data size so that it is consistent after the buffer is reset.
    private Integer dataSize;

    public ByteArrayBlock(int limit) {
      this.limit = limit;
      buffer = new ByteArrayOutputStream();
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
    InputStream openForUpload() throws IOException {
      super.openForUpload();
      dataSize = buffer.size();
      ByteArrayInputStream bufferData = new ByteArrayInputStream(
          buffer.toByteArray());
      buffer.reset();
      buffer = null;
      return bufferData;
    }

    @Override
    public boolean hasCapacity(long bytes) {
      return dataSize() + bytes <= limit;
    }

    @Override
    public int remainingCapacity() {
      return limit - dataSize();
    }

    @Override
    int write(byte[] b, int off, int len) throws IOException {
      super.write(b, off, len);
      int written = Math.min(remainingCapacity(), len);
      buffer.write(b, off, written);
      return written;
    }

    @Override
    public void close() throws IOException {
      if (enterClosedState()) {
        LOG.debug("Closed {}", this);
        buffer = null;
      }
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "ByteArrayBlock{");
      sb.append("state=").append(getState());
      sb.append(", dataSize=").append(dataSize());
      sb.append(", limit=").append(limit);
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Stream to a file.
   * This will stop at the limit; the caller is expected to create a new block
   */
  static class FileBlock extends DataBlock {

    private File bufferFile;

    private int limit;
    protected int bytesWritten;

    private BufferedOutputStream out;
    private InputStream uploadStream;

    public FileBlock(File bufferFile, int limit)
        throws FileNotFoundException {
      this.limit = limit;
      this.bufferFile = bufferFile;
      out = new BufferedOutputStream(
          new FileOutputStream(bufferFile));
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
    int write(byte[] b, int off, int len) throws IOException {
      super.write(b, off, len);
      int written = Math.min(remainingCapacity(), len);
      out.write(b, off, written);
      bytesWritten += written;
      return written;
    }

    @Override
    InputStream openForUpload() throws IOException {
      super.openForUpload();
      try {
        out.flush();
      } finally {
        out.close();
        out = null;
      }
      uploadStream = new FileInputStream(bufferFile);
      return new FileDeletingInputStream(uploadStream);
    }

    @Override
    public synchronized void close() throws IOException {
      final DestState state = getState();
      LOG.debug("Closing {}", this);
      enterClosedState();
      final boolean bufferExists = bufferFile.exists();
      switch (state) {
      case Writing:
        if (bufferExists) {
          // file was not uploaded
          LOG.debug("Deleting buffer file as upload did not start");
          bufferFile.delete();
        }
        break;
      case Upload:
        LOG.debug("Buffer file {} exists —close upload stream", bufferFile);
        break;

      case Closed:
        // no-op
      }
    }

    @Override
    void flush() throws IOException {
      verifyState(Writing);
      out.flush();
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "FileBlock{");
      sb.append("destFile=").append(bufferFile);
      sb.append(", state=").append(getState());
      sb.append(", dataSize=").append(dataSize());
      sb.append(", limit=").append(limit);
      sb.append('}');
      return sb.toString();
    }

    /**
     * An input stream which deletes the buffer file when closed.
     */
    private class FileDeletingInputStream extends ForwardingInputStream {

      FileDeletingInputStream(InputStream source) {
        super(source);
      }

      @Override
      public void close() throws IOException {
        super.close();
        bufferFile.delete();
      }
    }
  }

  /**
   * Stream which forwards everything to its inner class.
   * For ease of subclassing.
   */
  @SuppressWarnings({
      "NullableProblems",
      "NonSynchronizedMethodOverridesSynchronizedMethod"
  })
  static class ForwardingInputStream extends InputStream {

    protected final InputStream source;

    public ForwardingInputStream(InputStream source) {
      this.source = source;
    }

    public int read() throws IOException {
      return source.read();
    }

    public int read(byte[] b) throws IOException {
      return source.read(b);
    }

    public int read(byte[] b, int off, int len) throws IOException {
      return source.read(b, off, len);
    }

    public long skip(long n) throws IOException {
      return source.skip(n);
    }

    public int available() throws IOException {
      return source.available();
    }

    public void close() throws IOException {
      LOG.debug("Closing inner stream");
      source.close();
    }

    public void mark(int readlimit) {
      source.mark(readlimit);
    }

    public void reset() throws IOException {
      source.reset();
    }

    public boolean markSupported() {
      return source.markSupported();
    }

  }

  /**
   * Validate args to write command.
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
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Create a factory.
   * @param name factory name -the option from {@link Constants}.
   * @return the factory, ready to be initialized.
   * @throws IllegalArgumentException if the name is unknown.
   */
  static AbstractBlockFactory createFactory(String name) {
    switch (name) {
    case Constants.BLOCK_OUTPUT_BUFFER_ARRAY:
      return new MemoryBlockFactory();
    case Constants.BLOCK_OUTPUT_BUFFER_DISK:
      return new DiskBlockFactory();
    default:
      throw new IllegalArgumentException("Unsupported block buffer" +
          " \"" + name + "\"");
    }
  }
}

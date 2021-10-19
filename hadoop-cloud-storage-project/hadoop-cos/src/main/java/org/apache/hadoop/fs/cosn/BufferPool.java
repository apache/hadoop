/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.cosn;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

/**
 * BufferPool class is used to manage the buffers during program execution.
 * It is provided in a thread-safe singleton mode,and
 * keeps the program's memory and disk consumption at a stable value.
 */
public final class BufferPool {
  private static final Logger LOG =
      LoggerFactory.getLogger(BufferPool.class);

  private static BufferPool ourInstance = new BufferPool();

  /**
   * Use this method to get the instance of BufferPool.
   *
   * @return the instance of BufferPool
   */
  public static BufferPool getInstance() {
    return ourInstance;
  }

  private BlockingQueue<ByteBuffer> bufferPool = null;
  private long singleBufferSize = 0;
  private File diskBufferDir = null;

  private AtomicBoolean isInitialize = new AtomicBoolean(false);

  private BufferPool() {
  }

  private File createDir(String dirPath) throws IOException {
    File dir = new File(dirPath);
    if (!dir.exists()) {
      LOG.debug("Buffer dir: [{}] does not exists. create it first.",
          dirPath);
      if (dir.mkdirs()) {
        if (!dir.setWritable(true) || !dir.setReadable(true)
            || !dir.setExecutable(true)) {
          LOG.warn("Set the buffer dir: [{}]'s permission [writable,"
              + "readable, executable] failed.", dir.getAbsolutePath());
        }
        LOG.debug("Buffer dir: [{}] is created successfully.",
            dir.getAbsolutePath());
      } else {
        // Once again, check if it has been created successfully.
        // Prevent problems created by multiple processes at the same time.
        if (!dir.exists()) {
          throw new IOException("buffer dir:" + dir.getAbsolutePath()
              + " is created unsuccessfully");
        }
      }
    } else {
      LOG.debug("buffer dir: {} already exists.", dirPath);
    }

    return dir;
  }

  /**
   * Create buffers correctly by reading the buffer file directory,
   * buffer pool size,and file block size in the configuration.
   *
   * @param conf Provides configurations for the Hadoop runtime
   * @throws IOException Configuration errors,
   *                     insufficient or no access for memory or
   *                     disk space may cause this exception
   */
  public synchronized void initialize(Configuration conf)
      throws IOException {
    if (this.isInitialize.get()) {
      return;
    }
    this.singleBufferSize = conf.getLong(CosNConfigKeys.COSN_BLOCK_SIZE_KEY,
        CosNConfigKeys.DEFAULT_BLOCK_SIZE);

    // The block size of CosN can only support up to 2GB.
    if (this.singleBufferSize < Constants.MIN_PART_SIZE
        || this.singleBufferSize > Constants.MAX_PART_SIZE) {
      String exceptionMsg = String.format(
          "The block size of CosN is limited to %d to %d",
          Constants.MIN_PART_SIZE, Constants.MAX_PART_SIZE);
      throw new IOException(exceptionMsg);
    }

    long memoryBufferLimit = conf.getLong(
        CosNConfigKeys.COSN_UPLOAD_BUFFER_SIZE_KEY,
        CosNConfigKeys.DEFAULT_UPLOAD_BUFFER_SIZE);

    this.diskBufferDir = this.createDir(conf.get(
        CosNConfigKeys.COSN_BUFFER_DIR_KEY,
        CosNConfigKeys.DEFAULT_BUFFER_DIR));

    int bufferPoolSize = (int) (memoryBufferLimit / this.singleBufferSize);
    if (0 == bufferPoolSize) {
      throw new IOException(
          String.format("The total size of the buffer [%d] is " +
                  "smaller than a single block [%d]."
                  + "please consider increase the buffer size " +
                  "or decrease the block size",
              memoryBufferLimit, this.singleBufferSize));
    }
    this.bufferPool = new LinkedBlockingQueue<>(bufferPoolSize);
    for (int i = 0; i < bufferPoolSize; i++) {
      this.bufferPool.add(ByteBuffer.allocateDirect(
          (int) this.singleBufferSize));
    }

    this.isInitialize.set(true);
  }

  /**
   * Check if the buffer pool has been initialized.
   *
   * @throws IOException if the buffer pool is not initialized
   */
  private void checkInitialize() throws IOException {
    if (!this.isInitialize.get()) {
      throw new IOException(
          "The buffer pool has not been initialized yet");
    }
  }

  /**
   * Obtain a buffer from this buffer pool through the method.
   *
   * @param bufferSize expected buffer size to get
   * @return a buffer wrapper that satisfies the bufferSize.
   * @throws IOException if the buffer pool not initialized,
   *                     or the bufferSize parameter is not within
   *                     the range[1MB to the single buffer size]
   */
  public ByteBufferWrapper getBuffer(int bufferSize) throws IOException {
    this.checkInitialize();
    if (bufferSize > 0 && bufferSize <= this.singleBufferSize) {
      ByteBufferWrapper byteBufferWrapper = this.getByteBuffer();
      if (null == byteBufferWrapper) {
        // Use a disk buffer when the memory buffer is not enough
        byteBufferWrapper = this.getMappedBuffer();
      }
      return byteBufferWrapper;
    } else {
      String exceptionMsg = String.format(
          "Parameter buffer size out of range: 1048576 to %d",
          this.singleBufferSize
      );
      throw new IOException(exceptionMsg);
    }
  }

  /**
   * Get a ByteBufferWrapper from the buffer pool.
   *
   * @return a new byte buffer wrapper
   * @throws IOException if the buffer pool is not initialized
   */
  private ByteBufferWrapper getByteBuffer() throws IOException {
    this.checkInitialize();
    ByteBuffer buffer = this.bufferPool.poll();
    return buffer == null ? null : new ByteBufferWrapper(buffer);
  }

  /**
   * Get a mapped buffer from the buffer pool.
   *
   * @return a new mapped buffer
   * @throws IOException If the buffer pool is not initialized.
   *                     or some I/O error occurs
   */
  private ByteBufferWrapper getMappedBuffer() throws IOException {
    this.checkInitialize();
    File tmpFile = File.createTempFile(Constants.BLOCK_TMP_FILE_PREFIX,
        Constants.BLOCK_TMP_FILE_SUFFIX, this.diskBufferDir);
    tmpFile.deleteOnExit();
    RandomAccessFile raf = new RandomAccessFile(tmpFile, "rw");
    raf.setLength(this.singleBufferSize);
    MappedByteBuffer buf = raf.getChannel().map(
        FileChannel.MapMode.READ_WRITE, 0, this.singleBufferSize);
    return new ByteBufferWrapper(buf, raf, tmpFile);
  }

  /**
   * return the byte buffer wrapper to the buffer pool.
   *
   * @param byteBufferWrapper the byte buffer wrapper getting from the pool
   * @throws InterruptedException if interrupted while waiting
   * @throws IOException          some io error occurs
   */
  public void returnBuffer(ByteBufferWrapper byteBufferWrapper)
      throws InterruptedException, IOException {
    if (null == this.bufferPool || null == byteBufferWrapper) {
      return;
    }

    if (byteBufferWrapper.isDiskBuffer()) {
      byteBufferWrapper.close();
    } else {
      ByteBuffer byteBuffer = byteBufferWrapper.getByteBuffer();
      if (null != byteBuffer) {
        byteBuffer.clear();
        LOG.debug("Return the buffer to the buffer pool.");
        if (!this.bufferPool.offer(byteBuffer)) {
          LOG.error("Return the buffer to buffer pool failed.");
        }
      }
    }
  }
}

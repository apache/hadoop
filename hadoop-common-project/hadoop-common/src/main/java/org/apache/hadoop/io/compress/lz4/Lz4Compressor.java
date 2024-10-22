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

package org.apache.hadoop.io.compress.lz4;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4Compressor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Compressor} based on the lz4 compression algorithm.
 * http://code.google.com/p/lz4/
 */
public class Lz4Compressor implements Compressor {
  private static final Logger LOG =
      LoggerFactory.getLogger(Lz4Compressor.class.getName());
  private static final int DEFAULT_DIRECT_BUFFER_SIZE = 64 * 1024;
  private static final int DEFAULT_HC_LEVEL = 9;

  private int directBufferSize;
  private Buffer compressedDirectBuf = null;
  private int uncompressedDirectBufLen;
  private Buffer uncompressedDirectBuf = null;
  private byte[] userBuf = null;
  private int userBufOff = 0, userBufLen = 0;
  private boolean finish, finished;
  private int dstCapacity;

  private long bytesRead = 0L;
  private long bytesWritten = 0L;

  private final LZ4Compressor lz4Compressor;

  /**
   * Creates a new compressor.
   *
   * @param directBufferSize size of the direct buffer to be used.
   * @param useLz4HC use high compression ratio version of lz4, 
   *                 which trades CPU for compression ratio.
   */
  public Lz4Compressor(int directBufferSize, boolean useLz4HC, int compressionLevel) {
    this.directBufferSize = directBufferSize;

    try {
      LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
      if (useLz4HC) {
        lz4Compressor = lz4Factory.highCompressor(compressionLevel);
      } else {
        lz4Compressor = lz4Factory.fastCompressor();
      }
    } catch (AssertionError t) {
      throw new RuntimeException("lz4-java library is not available: " +
              "Lz4Compressor has not been loaded. You need to add " +
              "lz4-java.jar to your CLASSPATH. " + t, t);
    }

    uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);

    // Compression is guaranteed to succeed if 'dstCapacity' >=
    // LZ4_compressBound(srcSize)
    // whereas LZ4_compressBound(isize) is (isize) + ((isize)/255) + 16)
    this.dstCapacity = (directBufferSize) + ((directBufferSize) / 255) + 16;

    compressedDirectBuf = ByteBuffer.allocateDirect(this.dstCapacity);
    compressedDirectBuf.position(this.dstCapacity);
  }

  /**
   * Creates a new compressor.
   *
   * @param directBufferSize size of the direct buffer to be used.
   */
  public Lz4Compressor(int directBufferSize) {
    this(directBufferSize, false, DEFAULT_HC_LEVEL);
  }

  /**
   * Creates a new compressor with the default buffer size.
   */
  public Lz4Compressor() {
    this(DEFAULT_DIRECT_BUFFER_SIZE);
  }

  /**
   * Sets input data for compression.
   * This should be called whenever #needsInput() returns
   * <code>true</code> indicating that more input data is required.
   *
   * @param b   Input data
   * @param off Start offset
   * @param len Length
   */
  @Override
  public synchronized void setInput(byte[] b, int off, int len) {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
    finished = false;

    if (len > uncompressedDirectBuf.remaining()) {
      // save data; now !needsInput
      this.userBuf = b;
      this.userBufOff = off;
      this.userBufLen = len;
    } else {
      ((ByteBuffer) uncompressedDirectBuf).put(b, off, len);
      uncompressedDirectBufLen = uncompressedDirectBuf.position();
    }

    bytesRead += len;
  }

  /**
   * If a write would exceed the capacity of the direct buffers, it is set
   * aside to be loaded by this function while the compressed data are
   * consumed.
   */
  synchronized void setInputFromSavedData() {
    if (0 >= userBufLen) {
      return;
    }
    finished = false;

    uncompressedDirectBufLen = Math.min(userBufLen, directBufferSize);
    ((ByteBuffer) uncompressedDirectBuf).put(userBuf, userBufOff,
        uncompressedDirectBufLen);

    // Note how much data is being fed to lz4
    userBufOff += uncompressedDirectBufLen;
    userBufLen -= uncompressedDirectBufLen;
  }

  /**
   * Does nothing.
   */
  @Override
  public synchronized void setDictionary(byte[] b, int off, int len) {
    // do nothing
  }

  /**
   * Returns true if the input data buffer is empty and
   * #setInput() should be called to provide more input.
   *
   * @return <code>true</code> if the input data buffer is empty and
   *         #setInput() should be called in order to provide more input.
   */
  @Override
  public synchronized boolean needsInput() {
    return !(compressedDirectBuf.remaining() > 0
        || uncompressedDirectBuf.remaining() == 0 || userBufLen > 0);
  }

  /**
   * When called, indicates that compression should end
   * with the current contents of the input buffer.
   */
  @Override
  public synchronized void finish() {
    finish = true;
  }

  /**
   * Returns true if the end of the compressed
   * data output stream has been reached.
   *
   * @return <code>true</code> if the end of the compressed
   *         data output stream has been reached.
   */
  @Override
  public synchronized boolean finished() {
    // Check if all uncompressed data has been consumed
    return (finish && finished && compressedDirectBuf.remaining() == 0);
  }

  /**
   * Fills specified buffer with compressed data. Returns actual number
   * of bytes of compressed data. A return value of 0 indicates that
   * needsInput() should be called in order to determine if more input
   * data is required.
   *
   * @param b   Buffer for the compressed data
   * @param off Start offset of the data
   * @param len Size of the buffer
   * @return The actual number of bytes of compressed data.
   */
  @Override
  public synchronized int compress(byte[] b, int off, int len)
      throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    // Check if there is compressed data
    int n = compressedDirectBuf.remaining();
    if (n > 0) {
      n = Math.min(n, len);
      ((ByteBuffer) compressedDirectBuf).get(b, off, n);
      bytesWritten += n;
      return n;
    }

    // Re-initialize the lz4's output direct-buffer
    compressedDirectBuf.clear();
    compressedDirectBuf.limit(0);
    if (0 == uncompressedDirectBuf.position()) {
      // No compressed data, so we should have !needsInput or !finished
      setInputFromSavedData();
      if (0 == uncompressedDirectBuf.position()) {
        // Called without data; write nothing
        finished = true;
        return 0;
      }
    }

    // Compress data
    n = compressDirectBuf();
    compressedDirectBuf.limit(n);
    uncompressedDirectBuf.clear(); // lz4 consumes all buffer input

    // Set 'finished' if snapy has consumed all user-data
    if (0 == userBufLen) {
      finished = true;
    }

    // Get atmost 'len' bytes
    n = Math.min(n, len);
    bytesWritten += n;
    ((ByteBuffer) compressedDirectBuf).get(b, off, n);

    return n;
  }

  /**
   * Resets compressor so that a new set of input data can be processed.
   */
  @Override
  public synchronized void reset() {
    finish = false;
    finished = false;
    uncompressedDirectBuf.clear();
    uncompressedDirectBufLen = 0;
    compressedDirectBuf.clear();
    compressedDirectBuf.limit(0);
    userBufOff = userBufLen = 0;
    bytesRead = bytesWritten = 0L;
  }

  /**
   * Prepare the compressor to be used in a new stream with settings defined in
   * the given Configuration
   *
   * @param conf Configuration from which new setting are fetched
   */
  @Override
  public synchronized void reinit(Configuration conf) {
    reset();
  }

  /**
   * Return number of bytes given to this compressor since last reset.
   */
  @Override
  public synchronized long getBytesRead() {
    return bytesRead;
  }

  /**
   * Return number of bytes consumed by callers of compress since last reset.
   */
  @Override
  public synchronized long getBytesWritten() {
    return bytesWritten;
  }

  /**
   * Closes the compressor and discards any unprocessed input.
   */
  @Override
  public synchronized void end() {
  }

  private int compressDirectBuf() {
    if (uncompressedDirectBufLen == 0) {
      return 0;
    } else {
      // Set the position and limit of `uncompressedDirectBuf` for reading
      uncompressedDirectBuf.limit(uncompressedDirectBufLen).position(0);
      compressedDirectBuf.clear();
      lz4Compressor.compress((ByteBuffer) uncompressedDirectBuf,
              (ByteBuffer) compressedDirectBuf);
      uncompressedDirectBufLen = 0;
      uncompressedDirectBuf.limit(directBufferSize).position(0);
      int size = compressedDirectBuf.position();
      compressedDirectBuf.position(0);
      return size;
    }
  }
}

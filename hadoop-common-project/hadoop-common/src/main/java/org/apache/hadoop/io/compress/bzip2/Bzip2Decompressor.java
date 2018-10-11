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

package org.apache.hadoop.io.compress.bzip2;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.compress.Decompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Decompressor} based on the popular 
 * bzip2 compression algorithm.
 * http://www.bzip2.org/
 * 
 */
public class Bzip2Decompressor implements Decompressor {
  private static final int DEFAULT_DIRECT_BUFFER_SIZE = 64*1024;
  
  private static final Logger LOG =
      LoggerFactory.getLogger(Bzip2Decompressor.class);

  private long stream;
  private boolean conserveMemory;
  private int directBufferSize;
  private Buffer compressedDirectBuf = null;
  private int compressedDirectBufOff, compressedDirectBufLen;
  private Buffer uncompressedDirectBuf = null;
  private byte[] userBuf = null;
  private int userBufOff = 0, userBufLen = 0;
  private boolean finished;

  /**
   * Creates a new decompressor.
   */
  public Bzip2Decompressor(boolean conserveMemory, int directBufferSize) {
    this.conserveMemory = conserveMemory;
    this.directBufferSize = directBufferSize;
    compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
    
    stream = init(conserveMemory ? 1 : 0);
  }
  
  public Bzip2Decompressor() {
    this(false, DEFAULT_DIRECT_BUFFER_SIZE);
  }

  @Override
  public synchronized void setInput(byte[] b, int off, int len) {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
  
    this.userBuf = b;
    this.userBufOff = off;
    this.userBufLen = len;
    
    setInputFromSavedData();
    
    // Reinitialize bzip2's output direct buffer.
    uncompressedDirectBuf.limit(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
  }
  
  synchronized void setInputFromSavedData() {
    compressedDirectBufOff = 0;
    compressedDirectBufLen = userBufLen;
    if (compressedDirectBufLen > directBufferSize) {
      compressedDirectBufLen = directBufferSize;
    }

    // Reinitialize bzip2's input direct buffer.
    compressedDirectBuf.rewind();
    ((ByteBuffer)compressedDirectBuf).put(userBuf, userBufOff, 
                                          compressedDirectBufLen);
    
    // Note how much data is being fed to bzip2.
    userBufOff += compressedDirectBufLen;
    userBufLen -= compressedDirectBufLen;
  }

  @Override
  public synchronized void setDictionary(byte[] b, int off, int len) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized boolean needsInput() {
    // Consume remaining compressed data?
    if (uncompressedDirectBuf.remaining() > 0) {
      return false;
    }
    
    // Check if bzip2 has consumed all input.
    if (compressedDirectBufLen <= 0) {
      // Check if we have consumed all user-input.
      if (userBufLen <= 0) {
        return true;
      } else {
        setInputFromSavedData();
      }
    }
    
    return false;
  }

  @Override
  public synchronized boolean needsDictionary() {
    return false;
  }

  @Override
  public synchronized boolean finished() {
    // Check if bzip2 says it has finished and
    // all compressed data has been consumed.
    return (finished && uncompressedDirectBuf.remaining() == 0);
  }

  @Override
  public synchronized int decompress(byte[] b, int off, int len) 
    throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
    
    // Check if there is uncompressed data.
    int n = uncompressedDirectBuf.remaining();
    if (n > 0) {
      n = Math.min(n, len);
      ((ByteBuffer)uncompressedDirectBuf).get(b, off, n);
      return n;
    }
    
    // Re-initialize bzip2's output direct buffer.
    uncompressedDirectBuf.rewind();
    uncompressedDirectBuf.limit(directBufferSize);

    // Decompress the data.
    n = finished ? 0 : inflateBytesDirect();
    uncompressedDirectBuf.limit(n);

    // Get at most 'len' bytes.
    n = Math.min(n, len);
    ((ByteBuffer)uncompressedDirectBuf).get(b, off, n);

    return n;
  }
  
  /**
   * Returns the total number of uncompressed bytes output so far.
   *
   * @return the total (non-negative) number of uncompressed bytes output so far
   */
  public synchronized long getBytesWritten() {
    checkStream();
    return getBytesWritten(stream);
  }

  /**
   * Returns the total number of compressed bytes input so far.
   *
   * @return the total (non-negative) number of compressed bytes input so far
   */
  public synchronized long getBytesRead() {
    checkStream();
    return getBytesRead(stream);
  }

  /**
   * Returns the number of bytes remaining in the input buffers; normally
   * called when finished() is true to determine amount of post-gzip-stream
   * data.
   *
   * @return the total (non-negative) number of unprocessed bytes in input
   */
  @Override
  public synchronized int getRemaining() {
    checkStream();
    return userBufLen + getRemaining(stream);  // userBuf + compressedDirectBuf
  }

  /**
   * Resets everything including the input buffers (user and direct).
   */
  @Override
  public synchronized void reset() {
    checkStream();
    end(stream);
    stream = init(conserveMemory ? 1 : 0);
    finished = false;
    compressedDirectBufOff = compressedDirectBufLen = 0;
    uncompressedDirectBuf.limit(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
    userBufOff = userBufLen = 0;
  }

  @Override
  public synchronized void end() {
    if (stream != 0) {
      end(stream);
      stream = 0;
    }
  }

  static void initSymbols(String libname) {
    initIDs(libname);
  }

  private void checkStream() {
    if (stream == 0)
      throw new NullPointerException();
  }
  
  private native static void initIDs(String libname);
  private native static long init(int conserveMemory);
  private native int inflateBytesDirect();
  private native static long getBytesRead(long strm);
  private native static long getBytesWritten(long strm);
  private native static int getRemaining(long strm);
  private native static void end(long strm);
}

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

package org.apache.hadoop.io.compress.lzo;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.NativeCodeLoader;

/**
 * A {@link Compressor} based on the lzo algorithm.
 * http://www.oberhumer.com/opensource/lzo/
 * 
 */
public class LzoCompressor implements Compressor {
  private static final Log LOG = 
    LogFactory.getLog(LzoCompressor.class.getName());

  private int directBufferSize;
  private byte[] userBuf = null;
  private int userBufOff = 0, userBufLen = 0;
  private Buffer uncompressedDirectBuf = null;
  private int uncompressedDirectBufLen = 0;
  private Buffer compressedDirectBuf = null;
  private boolean finish, finished;
  
  private CompressionStrategy strategy; // The lzo compression algorithm.
  private long lzoCompressor = 0;       // The actual lzo compression function.
  private int workingMemoryBufLen = 0;  // The length of 'working memory' buf.
  private Buffer workingMemoryBuf;      // The 'working memory' for lzo.
  
  /**
   * The compression algorithm for lzo library.
   */
  public static enum CompressionStrategy {
    /**
     * lzo1 algorithms.
     */
    LZO1 (0),
    LZO1_99 (1),
    
    /**
     * lzo1a algorithms.
     */
    LZO1A (2),
    LZO1A_99 (3),
    
    /**
     * lzo1b algorithms.
     */
    LZO1B (4),
    LZO1B_BEST_COMPRESSION(5),
    LZO1B_BEST_SPEED(6),
    LZO1B_1 (7),
    LZO1B_2 (8),
    LZO1B_3 (9),
    LZO1B_4 (10),
    LZO1B_5 (11),
    LZO1B_6 (12),
    LZO1B_7 (13),
    LZO1B_8 (14),
    LZO1B_9 (15),
    LZO1B_99 (16),
    LZO1B_999 (17),

    /**
     * lzo1c algorithms.
     */
    LZO1C (18),
    LZO1C_BEST_COMPRESSION(19),
    LZO1C_BEST_SPEED(20),
    LZO1C_1 (21),
    LZO1C_2 (22),
    LZO1C_3 (23),
    LZO1C_4 (24),
    LZO1C_5 (25),
    LZO1C_6 (26),
    LZO1C_7 (27),
    LZO1C_8 (28),
    LZO1C_9 (29),
    LZO1C_99 (30),
    LZO1C_999 (31),
    
    /**
     * lzo1f algorithms.
     */
    LZO1F_1 (32),
    LZO1F_999 (33),
    
    /**
     * lzo1x algorithms.
     */
    LZO1X_1 (34),
    LZO1X_11 (35),
    LZO1X_12 (36),
    LZO1X_15 (37),
    LZO1X_999 (38),
    
    /**
     * lzo1y algorithms.
     */
    LZO1Y_1 (39),
    LZO1Y_999 (40),
    
    /**
     * lzo1z algorithms.
     */
    LZO1Z_999 (41),
    
    /**
     * lzo2a algorithms.
     */
    LZO2A_999 (42);
    
    private final int compressor;

    private CompressionStrategy(int compressor) {
      this.compressor = compressor;
    }
    
    int getCompressor() {
      return compressor;
    }
  }; // CompressionStrategy

  private static boolean nativeLzoLoaded = false;
  
  static {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      // Initialize the native library
      initIDs();
      nativeLzoLoaded = true;
    } else {
      LOG.error("Cannot load " + LzoCompressor.class.getName() + 
                " without native-hadoop library!");
    }
  }
  
  /**
   * Check if lzo compressors are loaded and initialized.
   * 
   * @return <code>true</code> if lzo compressors are loaded & initialized,
   *         else <code>false</code> 
   */
  public static boolean isNativeLzoLoaded() {
    return nativeLzoLoaded;
  }

  /** 
   * Creates a new compressor using the specified {@link CompressionStrategy}.
   * 
   * @param strategy lzo compression algorithm to use
   * @param directBufferSize size of the direct buffer to be used.
   */
  public LzoCompressor(CompressionStrategy strategy, int directBufferSize) {
    this.strategy = strategy;
    this.directBufferSize = directBufferSize;
    uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
    
    /**
     * Initialize {@link #lzoCompress} and {@link #workingMemoryBufLen}
     */
    init(this.strategy.getCompressor());
    workingMemoryBuf = ByteBuffer.allocateDirect(workingMemoryBufLen);
  }
  
  /**
   * Creates a new compressor with the default lzo1x_1 compression.
   */
  public LzoCompressor() {
    this(CompressionStrategy.LZO1X_1, 64*1024);
  }
  
  public synchronized void setInput(byte[] b, int off, int len) {
    if (b== null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    this.userBuf = b;
    this.userBufOff = off;
    this.userBufLen = len;

    // Reinitialize lzo's output direct-buffer 
    compressedDirectBuf.limit(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
  }

  synchronized void setInputFromSavedData() {
    uncompressedDirectBufLen = userBufLen;
    if (uncompressedDirectBufLen > directBufferSize) {
      uncompressedDirectBufLen = directBufferSize;
    }

    // Reinitialize lzo's input direct buffer
    uncompressedDirectBuf.rewind();
    ((ByteBuffer)uncompressedDirectBuf).put(userBuf, userBufOff,  
                                            uncompressedDirectBufLen);

    // Note how much data is being fed to lzo
    userBufOff += uncompressedDirectBufLen;
    userBufLen -= uncompressedDirectBufLen;
  }

  public synchronized void setDictionary(byte[] b, int off, int len) {
    // nop
  }

  public boolean needsInput() {
    // Consume remaining compressed data?
    if (compressedDirectBuf.remaining() > 0) {
      return false;
    }

    // Check if lzo has consumed all input
    if (uncompressedDirectBufLen <= 0) {
      // Check if we have consumed all user-input
      if (userBufLen <= 0) {
        return true;
      } else {
        setInputFromSavedData();
      }
    }
    
    return false;
  }
  
  public synchronized void finish() {
    finish = true;
  }
  
  public synchronized boolean finished() {
    // Check if 'lzo' says its 'finished' and
    // all compressed data has been consumed
    return (finish && finished && compressedDirectBuf.remaining() == 0); 
  }

  public synchronized int compress(byte[] b, int off, int len) 
    throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
    
    int n = 0;
    
    // Check if there is compressed data
    n = compressedDirectBuf.remaining();
    if (n > 0) {
      n = Math.min(n, len);
      ((ByteBuffer)compressedDirectBuf).get(b, off, n);
      return n;
    }

    // Re-initialize the lzo's output direct-buffer
    compressedDirectBuf.rewind();
    compressedDirectBuf.limit(directBufferSize);

    // Compress data
    n = compressBytesDirect(strategy.getCompressor());
    compressedDirectBuf.limit(n);
    
    // Set 'finished' if lzo has consumed all user-data
    if (userBufLen <= 0) {
      finished = true;
    }
    
    // Get atmost 'len' bytes
    n = Math.min(n, len);
    ((ByteBuffer)compressedDirectBuf).get(b, off, n);

    return n;
  }

  public synchronized void reset() {
    finish = false;
    finished = false;
    uncompressedDirectBuf.rewind();
    uncompressedDirectBufLen = 0;
    compressedDirectBuf.limit(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
    userBufOff = userBufLen = 0;
  }
  
  public synchronized void end() {
    // nop
  }
  
  private native static void initIDs();
  private native void init(int compressor);
  private native int compressBytesDirect(int compressor);
}

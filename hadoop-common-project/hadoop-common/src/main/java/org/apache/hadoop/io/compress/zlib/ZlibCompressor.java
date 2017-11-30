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

package org.apache.hadoop.io.compress.zlib;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.NativeCodeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Compressor} based on the popular 
 * zlib compression algorithm.
 * http://www.zlib.net/
 * 
 */
public class ZlibCompressor implements Compressor {

  private static final Logger LOG =
      LoggerFactory.getLogger(ZlibCompressor.class);

  private static final int DEFAULT_DIRECT_BUFFER_SIZE = 64*1024;

  private long stream;
  private CompressionLevel level;
  private CompressionStrategy strategy;
  private final CompressionHeader windowBits;
  private int directBufferSize;
  private byte[] userBuf = null;
  private int userBufOff = 0, userBufLen = 0;
  private Buffer uncompressedDirectBuf = null;
  private int uncompressedDirectBufOff = 0, uncompressedDirectBufLen = 0;
  private boolean keepUncompressedBuf = false;
  private Buffer compressedDirectBuf = null;
  private boolean finish, finished;

  /**
   * The compression level for zlib library.
   */
  public enum CompressionLevel {
    /**
     * Compression level for no compression.
     */
    NO_COMPRESSION (0),
    
    /**
     * Compression level for fastest compression.
     */
    BEST_SPEED (1),

    /**
     * Compression level 2.
     */
    TWO (2),


    /**
     * Compression level 3.
     */
    THREE (3),


    /**
     * Compression level 4.
     */
    FOUR (4),


    /**
     * Compression level 5.
     */
    FIVE (5),


    /**
     * Compression level 6.
     */
    SIX (6),

    /**
     * Compression level 7.
     */
    SEVEN (7),

    /**
     * Compression level 8.
     */
    EIGHT (8),


    /**
     * Compression level for best compression.
     */
    BEST_COMPRESSION (9),
    
    /**
     * Default compression level.
     */
    DEFAULT_COMPRESSION (-1);
    
    
    private final int compressionLevel;
    
    CompressionLevel(int level) {
      compressionLevel = level;
    }
    
    int compressionLevel() {
      return compressionLevel;
    }
  };
  
  /**
   * The compression level for zlib library.
   */
  public enum CompressionStrategy {
    /**
     * Compression strategy best used for data consisting mostly of small
     * values with a somewhat random distribution. Forces more Huffman coding
     * and less string matching.
     */
    FILTERED (1),
    
    /**
     * Compression strategy for Huffman coding only.
     */
    HUFFMAN_ONLY (2),
    
    /**
     * Compression strategy to limit match distances to one
     * (run-length encoding).
     */
    RLE (3),

    /**
     * Compression strategy to prevent the use of dynamic Huffman codes, 
     * allowing for a simpler decoder for special applications.
     */
    FIXED (4),

    /**
     * Default compression strategy.
     */
    DEFAULT_STRATEGY (0);
    
    
    private final int compressionStrategy;
    
    CompressionStrategy(int strategy) {
      compressionStrategy = strategy;
    }
    
    int compressionStrategy() {
      return compressionStrategy;
    }
  };

  /**
   * The type of header for compressed data.
   */
  public enum CompressionHeader {
    /**
     * No headers/trailers/checksums.
     */
    NO_HEADER (-15),
    
    /**
     * Default headers/trailers/checksums.
     */
    DEFAULT_HEADER (15),
    
    /**
     * Simple gzip headers/trailers.
     */
    GZIP_FORMAT (31);

    private final int windowBits;
    
    CompressionHeader(int windowBits) {
      this.windowBits = windowBits;
    }
    
    public int windowBits() {
      return windowBits;
    }
  }
  
  private static boolean nativeZlibLoaded = false;
  
  static {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      try {
        // Initialize the native library
        initIDs();
        nativeZlibLoaded = true;
      } catch (Throwable t) {
        // Ignore failure to load/initialize native-zlib
      }
    }
  }
  
  static boolean isNativeZlibLoaded() {
    return nativeZlibLoaded;
  }

  protected final void construct(CompressionLevel level, CompressionStrategy strategy,
      CompressionHeader header, int directBufferSize) {
  }

  /**
   * Creates a new compressor with the default compression level.
   * Compressed data will be generated in ZLIB format.
   */
  public ZlibCompressor() {
    this(CompressionLevel.DEFAULT_COMPRESSION,
         CompressionStrategy.DEFAULT_STRATEGY,
         CompressionHeader.DEFAULT_HEADER,
         DEFAULT_DIRECT_BUFFER_SIZE);
  }

  /**
   * Creates a new compressor, taking settings from the configuration.
   */
  public ZlibCompressor(Configuration conf) {
    this(ZlibFactory.getCompressionLevel(conf),
         ZlibFactory.getCompressionStrategy(conf),
         CompressionHeader.DEFAULT_HEADER,
         DEFAULT_DIRECT_BUFFER_SIZE);
  }

  /** 
   * Creates a new compressor using the specified compression level.
   * Compressed data will be generated in ZLIB format.
   * 
   * @param level Compression level #CompressionLevel
   * @param strategy Compression strategy #CompressionStrategy
   * @param header Compression header #CompressionHeader
   * @param directBufferSize Size of the direct buffer to be used.
   */
  public ZlibCompressor(CompressionLevel level, CompressionStrategy strategy, 
                        CompressionHeader header, int directBufferSize) {
    this.level = level;
    this.strategy = strategy;
    this.windowBits = header;
    stream = init(this.level.compressionLevel(), 
                  this.strategy.compressionStrategy(), 
                  this.windowBits.windowBits());

    this.directBufferSize = directBufferSize;
    uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
  }

  /**
   * Prepare the compressor to be used in a new stream with settings defined in
   * the given Configuration. It will reset the compressor's compression level
   * and compression strategy.
   * 
   * @param conf Configuration storing new settings
   */
  @Override
  public void reinit(Configuration conf) {
    reset();
    if (conf == null) {
      return;
    }
    end(stream);
    level = ZlibFactory.getCompressionLevel(conf);
    strategy = ZlibFactory.getCompressionStrategy(conf);
    stream = init(level.compressionLevel(), 
                  strategy.compressionStrategy(), 
                  windowBits.windowBits());
    if(LOG.isDebugEnabled()) {
      LOG.debug("Reinit compressor with new compression configuration");
    }
  }

  @Override
  public void setInput(byte[] b, int off, int len) {
    if (b== null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
    
    this.userBuf = b;
    this.userBufOff = off;
    this.userBufLen = len;
    uncompressedDirectBufOff = 0;
    setInputFromSavedData();
    
    // Reinitialize zlib's output direct buffer 
    compressedDirectBuf.limit(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
  }
  
  //copy enough data from userBuf to uncompressedDirectBuf
  void setInputFromSavedData() {
    int len = Math.min(userBufLen, uncompressedDirectBuf.remaining());
    ((ByteBuffer)uncompressedDirectBuf).put(userBuf, userBufOff, len);
    userBufLen -= len;
    userBufOff += len;
    uncompressedDirectBufLen = uncompressedDirectBuf.position();
  }

  @Override
  public void setDictionary(byte[] b, int off, int len) {
    if (stream == 0 || b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
    setDictionary(stream, b, off, len);
  }

  @Override
  public boolean needsInput() {
    // Consume remaining compressed data?
    if (compressedDirectBuf.remaining() > 0) {
      return false;
    }

    // Check if zlib has consumed all input
    // compress should be invoked if keepUncompressedBuf true
    if (keepUncompressedBuf && uncompressedDirectBufLen > 0)
      return false;
    
    if (uncompressedDirectBuf.remaining() > 0) {
      // Check if we have consumed all user-input
      if (userBufLen <= 0) {
        return true;
      } else {
        // copy enough data from userBuf to uncompressedDirectBuf
        setInputFromSavedData();
        if (uncompressedDirectBuf.remaining() > 0) // uncompressedDirectBuf is not full
          return true;
        else 
          return false;
      }
    }
    
    return false;
  }
  
  @Override
  public void finish() {
    finish = true;
  }
  
  @Override
  public boolean finished() {
    // Check if 'zlib' says its 'finished' and
    // all compressed data has been consumed
    return (finished && compressedDirectBuf.remaining() == 0);
  }

  @Override
  public int compress(byte[] b, int off, int len) 
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

    // Re-initialize the zlib's output direct buffer
    compressedDirectBuf.rewind();
    compressedDirectBuf.limit(directBufferSize);

    // Compress data
    n = deflateBytesDirect();
    compressedDirectBuf.limit(n);
    
    // Check if zlib consumed all input buffer
    // set keepUncompressedBuf properly
    if (uncompressedDirectBufLen <= 0) { // zlib consumed all input buffer
      keepUncompressedBuf = false;
      uncompressedDirectBuf.clear();
      uncompressedDirectBufOff = 0;
      uncompressedDirectBufLen = 0;
    } else { // zlib did not consume all input buffer
      keepUncompressedBuf = true;
    }
    
    // Get atmost 'len' bytes
    n = Math.min(n, len);
    ((ByteBuffer)compressedDirectBuf).get(b, off, n);

    return n;
  }

  /**
   * Returns the total number of compressed bytes output so far.
   *
   * @return the total (non-negative) number of compressed bytes output so far
   */
  @Override
  public long getBytesWritten() {
    checkStream();
    return getBytesWritten(stream);
  }

  /**
   * Returns the total number of uncompressed bytes input so far.</p>
   *
   * @return the total (non-negative) number of uncompressed bytes input so far
   */
  @Override
  public long getBytesRead() {
    checkStream();
    return getBytesRead(stream);
  }

  @Override
  public void reset() {
    checkStream();
    reset(stream);
    finish = false;
    finished = false;
    uncompressedDirectBuf.rewind();
    uncompressedDirectBufOff = uncompressedDirectBufLen = 0;
    keepUncompressedBuf = false;
    compressedDirectBuf.limit(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
    userBufOff = userBufLen = 0;
  }
  
  @Override
  public void end() {
    if (stream != 0) {
      end(stream);
      stream = 0;
    }
  }
  
  private void checkStream() {
    if (stream == 0)
      throw new NullPointerException();
  }
  
  private native static void initIDs();
  private native static long init(int level, int strategy, int windowBits);
  private native static void setDictionary(long strm, byte[] b, int off,
                                           int len);
  private native int deflateBytesDirect();
  private native static long getBytesRead(long strm);
  private native static long getBytesWritten(long strm);
  private native static void reset(long strm);
  private native static void end(long strm);

  public native static String getLibraryName();
}

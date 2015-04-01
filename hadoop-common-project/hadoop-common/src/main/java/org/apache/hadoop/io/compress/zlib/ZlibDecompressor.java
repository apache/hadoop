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

import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DirectDecompressor;
import org.apache.hadoop.util.NativeCodeLoader;

/**
 * A {@link Decompressor} based on the popular 
 * zlib compression algorithm.
 * http://www.zlib.net/
 * 
 */
public class ZlibDecompressor implements Decompressor {
  private static final int DEFAULT_DIRECT_BUFFER_SIZE = 64*1024;

  private long stream;
  private CompressionHeader header;
  private int directBufferSize;
  private Buffer compressedDirectBuf = null;
  private int compressedDirectBufOff, compressedDirectBufLen;
  private Buffer uncompressedDirectBuf = null;
  private byte[] userBuf = null;
  private int userBufOff = 0, userBufLen = 0;
  private boolean finished;
  private boolean needDict;

  /**
   * The headers to detect from compressed data.
   */
  public static enum CompressionHeader {
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
    GZIP_FORMAT (31),
    
    /**
     * Autodetect gzip/zlib headers/trailers.
     */
    AUTODETECT_GZIP_ZLIB (47);

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

  /**
   * Creates a new decompressor.
   */
  public ZlibDecompressor(CompressionHeader header, int directBufferSize) {
    this.header = header;
    this.directBufferSize = directBufferSize;    
    compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
    
    stream = init(this.header.windowBits());
  }
  
  public ZlibDecompressor() {
    this(CompressionHeader.DEFAULT_HEADER, DEFAULT_DIRECT_BUFFER_SIZE);
  }

  @Override
  public void setInput(byte[] b, int off, int len) {
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
    
    // Reinitialize zlib's output direct buffer 
    uncompressedDirectBuf.limit(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
  }
  
  void setInputFromSavedData() {
    compressedDirectBufOff = 0;
    compressedDirectBufLen = userBufLen;
    if (compressedDirectBufLen > directBufferSize) {
      compressedDirectBufLen = directBufferSize;
    }

    // Reinitialize zlib's input direct buffer
    compressedDirectBuf.rewind();
    ((ByteBuffer)compressedDirectBuf).put(userBuf, userBufOff, 
                                          compressedDirectBufLen);
    
    // Note how much data is being fed to zlib
    userBufOff += compressedDirectBufLen;
    userBufLen -= compressedDirectBufLen;
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
    needDict = false;
  }

  @Override
  public boolean needsInput() {
    // Consume remaining compressed data?
    if (uncompressedDirectBuf.remaining() > 0) {
      return false;
    }
    
    // Check if zlib has consumed all input
    if (compressedDirectBufLen <= 0) {
      // Check if we have consumed all user-input
      if (userBufLen <= 0) {
        return true;
      } else {
        setInputFromSavedData();
      }
    }
    
    return false;
  }

  @Override
  public boolean needsDictionary() {
    return needDict;
  }

  @Override
  public boolean finished() {
    // Check if 'zlib' says it's 'finished' and
    // all compressed data has been consumed
    return (finished && uncompressedDirectBuf.remaining() == 0);
  }

  @Override
  public int decompress(byte[] b, int off, int len) 
    throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
    
    int n = 0;
    
    // Check if there is uncompressed data
    n = uncompressedDirectBuf.remaining();
    if (n > 0) {
      n = Math.min(n, len);
      ((ByteBuffer)uncompressedDirectBuf).get(b, off, n);
      return n;
    }
    
    // Re-initialize the zlib's output direct buffer
    uncompressedDirectBuf.rewind();
    uncompressedDirectBuf.limit(directBufferSize);

    // Decompress data
    n = inflateBytesDirect();
    uncompressedDirectBuf.limit(n);

    // Get at most 'len' bytes
    n = Math.min(n, len);
    ((ByteBuffer)uncompressedDirectBuf).get(b, off, n);

    return n;
  }
  
  /**
   * Returns the total number of uncompressed bytes output so far.
   *
   * @return the total (non-negative) number of uncompressed bytes output so far
   */
  public long getBytesWritten() {
    checkStream();
    return getBytesWritten(stream);
  }

  /**
   * Returns the total number of compressed bytes input so far.</p>
   *
   * @return the total (non-negative) number of compressed bytes input so far
   */
  public long getBytesRead() {
    checkStream();
    return getBytesRead(stream);
  }

  /**
   * Returns the number of bytes remaining in the input buffers; normally
   * called when finished() is true to determine amount of post-gzip-stream
   * data.</p>
   *
   * @return the total (non-negative) number of unprocessed bytes in input
   */
  @Override
  public int getRemaining() {
    checkStream();
    return userBufLen + getRemaining(stream);  // userBuf + compressedDirectBuf
  }

  /**
   * Resets everything including the input buffers (user and direct).</p>
   */
  @Override
  public void reset() {
    checkStream();
    reset(stream);
    finished = false;
    needDict = false;
    compressedDirectBufOff = compressedDirectBufLen = 0;
    uncompressedDirectBuf.limit(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
    userBufOff = userBufLen = 0;
  }

  @Override
  public void end() {
    if (stream != 0) {
      end(stream);
      stream = 0;
    }
  }

  @Override
  protected void finalize() {
    end();
  }
  
  private void checkStream() {
    if (stream == 0)
      throw new NullPointerException();
  }
  
  private native static void initIDs();
  private native static long init(int windowBits);
  private native static void setDictionary(long strm, byte[] b, int off,
                                           int len);
  private native int inflateBytesDirect();
  private native static long getBytesRead(long strm);
  private native static long getBytesWritten(long strm);
  private native static int getRemaining(long strm);
  private native static void reset(long strm);
  private native static void end(long strm);
    
  int inflateDirect(ByteBuffer src, ByteBuffer dst) throws IOException {
    assert (this instanceof ZlibDirectDecompressor);
    
    ByteBuffer presliced = dst;
    if (dst.position() > 0) {
      presliced = dst;
      dst = dst.slice();
    }

    Buffer originalCompressed = compressedDirectBuf;
    Buffer originalUncompressed = uncompressedDirectBuf;
    int originalBufferSize = directBufferSize;
    compressedDirectBuf = src;
    compressedDirectBufOff = src.position();
    compressedDirectBufLen = src.remaining();
    uncompressedDirectBuf = dst;
    directBufferSize = dst.remaining();
    int n = 0;
    try {
      n = inflateBytesDirect();
      presliced.position(presliced.position() + n);
      if (compressedDirectBufLen > 0) {
        src.position(compressedDirectBufOff);
      } else {
        src.position(src.limit());
      }
    } finally {
      compressedDirectBuf = originalCompressed;
      uncompressedDirectBuf = originalUncompressed;
      compressedDirectBufOff = 0;
      compressedDirectBufLen = 0;
      directBufferSize = originalBufferSize;
    }
    return n;
  }
  
  public static class ZlibDirectDecompressor 
      extends ZlibDecompressor implements DirectDecompressor {
    public ZlibDirectDecompressor() {
      super(CompressionHeader.DEFAULT_HEADER, 0);
    }

    public ZlibDirectDecompressor(CompressionHeader header, int directBufferSize) {
      super(header, directBufferSize);
    }
    
    @Override
    public boolean finished() {
      return (endOfInput && super.finished());
    }
    
    @Override
    public void reset() {
      super.reset();
      endOfInput = true;
    }
    
    private boolean endOfInput;

    @Override
    public void decompress(ByteBuffer src, ByteBuffer dst)
        throws IOException {
      assert dst.isDirect() : "dst.isDirect()";
      assert src.isDirect() : "src.isDirect()";
      assert dst.remaining() > 0 : "dst.remaining() > 0";      
      this.inflateDirect(src, dst);
      endOfInput = !src.hasRemaining();
    }

    @Override
    public void setDictionary(byte[] b, int off, int len) {
      throw new UnsupportedOperationException(
          "byte[] arrays are not supported for DirectDecompressor");
    }

    @Override
    public int decompress(byte[] b, int off, int len) {
      throw new UnsupportedOperationException(
          "byte[] arrays are not supported for DirectDecompressor");
    }
  }
}

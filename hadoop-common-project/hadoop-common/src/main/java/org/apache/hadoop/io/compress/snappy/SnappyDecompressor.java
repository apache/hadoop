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

package org.apache.hadoop.io.compress.snappy;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DirectDecompressor;
import org.apache.hadoop.util.NativeCodeLoader;

/**
 * A {@link Decompressor} based on the snappy compression algorithm.
 * http://code.google.com/p/snappy/
 */
public class SnappyDecompressor implements Decompressor {
  private static final Log LOG =
      LogFactory.getLog(SnappyDecompressor.class.getName());
  private static final int DEFAULT_DIRECT_BUFFER_SIZE = 64 * 1024;

  private int directBufferSize;
  private Buffer compressedDirectBuf = null;
  private int compressedDirectBufLen;
  private Buffer uncompressedDirectBuf = null;
  private byte[] userBuf = null;
  private int userBufOff = 0, userBufLen = 0;
  private boolean finished;

  private static boolean nativeSnappyLoaded = false;

  static {
    if (NativeCodeLoader.isNativeCodeLoaded() &&
        NativeCodeLoader.buildSupportsSnappy()) {
      try {
        initIDs();
        nativeSnappyLoaded = true;
      } catch (Throwable t) {
        LOG.error("failed to load SnappyDecompressor", t);
      }
    }
  }
  
  public static boolean isNativeCodeLoaded() {
    return nativeSnappyLoaded;
  }
  
  /**
   * Creates a new compressor.
   *
   * @param directBufferSize size of the direct buffer to be used.
   */
  public SnappyDecompressor(int directBufferSize) {
    this.directBufferSize = directBufferSize;

    compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);

  }

  /**
   * Creates a new decompressor with the default buffer size.
   */
  public SnappyDecompressor() {
    this(DEFAULT_DIRECT_BUFFER_SIZE);
  }

  /**
   * Sets input data for decompression.
   * This should be called if and only if {@link #needsInput()} returns
   * <code>true</code> indicating that more input data is required.
   * (Both native and non-native versions of various Decompressors require
   * that the data passed in via <code>b[]</code> remain unmodified until
   * the caller is explicitly notified--via {@link #needsInput()}--that the
   * buffer may be safely modified.  With this requirement, an extra
   * buffer-copy can be avoided.)
   *
   * @param b   Input data
   * @param off Start offset
   * @param len Length
   */
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

    // Reinitialize snappy's output direct-buffer
    uncompressedDirectBuf.limit(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
  }

  /**
   * If a write would exceed the capacity of the direct buffers, it is set
   * aside to be loaded by this function while the compressed data are
   * consumed.
   */
  void setInputFromSavedData() {
    compressedDirectBufLen = Math.min(userBufLen, directBufferSize);

    // Reinitialize snappy's input direct buffer
    compressedDirectBuf.rewind();
    ((ByteBuffer) compressedDirectBuf).put(userBuf, userBufOff,
        compressedDirectBufLen);

    // Note how much data is being fed to snappy
    userBufOff += compressedDirectBufLen;
    userBufLen -= compressedDirectBufLen;
  }

  /**
   * Does nothing.
   */
  @Override
  public void setDictionary(byte[] b, int off, int len) {
    // do nothing
  }

  /**
   * Returns true if the input data buffer is empty and
   * {@link #setInput(byte[], int, int)} should be called to
   * provide more input.
   *
   * @return <code>true</code> if the input data buffer is empty and
   *         {@link #setInput(byte[], int, int)} should be called in
   *         order to provide more input.
   */
  @Override
  public boolean needsInput() {
    // Consume remaining compressed data?
    if (uncompressedDirectBuf.remaining() > 0) {
      return false;
    }

    // Check if snappy has consumed all input
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

  /**
   * Returns <code>false</code>.
   *
   * @return <code>false</code>.
   */
  @Override
  public boolean needsDictionary() {
    return false;
  }

  /**
   * Returns true if the end of the decompressed
   * data output stream has been reached.
   *
   * @return <code>true</code> if the end of the decompressed
   *         data output stream has been reached.
   */
  @Override
  public boolean finished() {
    return (finished && uncompressedDirectBuf.remaining() == 0);
  }

  /**
   * Fills specified buffer with uncompressed data. Returns actual number
   * of bytes of uncompressed data. A return value of 0 indicates that
   * {@link #needsInput()} should be called in order to determine if more
   * input data is required.
   *
   * @param b   Buffer for the compressed data
   * @param off Start offset of the data
   * @param len Size of the buffer
   * @return The actual number of bytes of compressed data.
   * @throws IOException
   */
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
      ((ByteBuffer) uncompressedDirectBuf).get(b, off, n);
      return n;
    }
    if (compressedDirectBufLen > 0) {
      // Re-initialize the snappy's output direct buffer
      uncompressedDirectBuf.rewind();
      uncompressedDirectBuf.limit(directBufferSize);

      // Decompress data
      n = decompressBytesDirect();
      uncompressedDirectBuf.limit(n);

      if (userBufLen <= 0) {
        finished = true;
      }

      // Get atmost 'len' bytes
      n = Math.min(n, len);
      ((ByteBuffer) uncompressedDirectBuf).get(b, off, n);
    }

    return n;
  }

  /**
   * Returns <code>0</code>.
   *
   * @return <code>0</code>.
   */
  @Override
  public int getRemaining() {
    // Never use this function in BlockDecompressorStream.
    return 0;
  }

  @Override
  public void reset() {
    finished = false;
    compressedDirectBufLen = 0;
    uncompressedDirectBuf.limit(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
    userBufOff = userBufLen = 0;
  }

  /**
   * Resets decompressor and input and output buffers so that a new set of
   * input data can be processed.
   */
  @Override
  public void end() {
    // do nothing
  }

  private native static void initIDs();

  private native int decompressBytesDirect();
  
  int decompressDirect(ByteBuffer src, ByteBuffer dst) throws IOException {
    assert (this instanceof SnappyDirectDecompressor);
    
    ByteBuffer presliced = dst;
    if (dst.position() > 0) {
      presliced = dst;
      dst = dst.slice();
    }

    Buffer originalCompressed = compressedDirectBuf;
    Buffer originalUncompressed = uncompressedDirectBuf;
    int originalBufferSize = directBufferSize;
    compressedDirectBuf = src.slice();
    compressedDirectBufLen = src.remaining();
    uncompressedDirectBuf = dst;
    directBufferSize = dst.remaining();
    int n = 0;
    try {
      n = decompressBytesDirect();
      presliced.position(presliced.position() + n);
      // SNAPPY always consumes the whole buffer or throws an exception
      src.position(src.limit());
      finished = true;
    } finally {
      compressedDirectBuf = originalCompressed;
      uncompressedDirectBuf = originalUncompressed;
      compressedDirectBufLen = 0;
      directBufferSize = originalBufferSize;
    }
    return n;
  }
  
  public static class SnappyDirectDecompressor extends SnappyDecompressor implements
      DirectDecompressor {
    
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
      this.decompressDirect(src, dst);
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

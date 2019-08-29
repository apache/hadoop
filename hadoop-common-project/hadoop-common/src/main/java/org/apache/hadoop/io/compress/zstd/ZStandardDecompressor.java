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

package org.apache.hadoop.io.compress.zstd;

import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DirectDecompressor;
import org.apache.hadoop.util.NativeCodeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@link Decompressor} based on the zStandard compression algorithm.
 * https://github.com/facebook/zstd
 */
public class ZStandardDecompressor implements Decompressor {
  private static final Logger LOG =
      LoggerFactory.getLogger(ZStandardDecompressor.class);

  private long stream;
  private int directBufferSize;
  private ByteBuffer compressedDirectBuf = null;
  private int compressedDirectBufOff, bytesInCompressedBuffer;
  private ByteBuffer uncompressedDirectBuf = null;
  private byte[] userBuf = null;
  private int userBufOff = 0, userBufferBytesToConsume = 0;
  private boolean finished;
  private int remaining = 0;

  private static boolean nativeZStandardLoaded = false;

  static {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      try {
        // Initialize the native library
        initIDs();
        nativeZStandardLoaded = true;
      } catch (Throwable t) {
        LOG.warn("Error loading zstandard native libraries: " + t);
      }
    }
  }

  public static boolean isNativeCodeLoaded() {
    return nativeZStandardLoaded;
  }

  public static int getRecommendedBufferSize() {
    return getStreamSize();
  }

  public ZStandardDecompressor() {
    this(getStreamSize());
  }

  /**
   * Creates a new decompressor.
   */
  public ZStandardDecompressor(int bufferSize) {
    this.directBufferSize = bufferSize;
    compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
    stream = create();
    reset();
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
    this.userBufferBytesToConsume = len;

    setInputFromSavedData();

    uncompressedDirectBuf.limit(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
  }

  private void setInputFromSavedData() {
    compressedDirectBufOff = 0;
    bytesInCompressedBuffer = userBufferBytesToConsume;
    if (bytesInCompressedBuffer > directBufferSize) {
      bytesInCompressedBuffer = directBufferSize;
    }

    compressedDirectBuf.rewind();
    compressedDirectBuf.put(
        userBuf, userBufOff, bytesInCompressedBuffer);

    userBufOff += bytesInCompressedBuffer;
    userBufferBytesToConsume -= bytesInCompressedBuffer;
  }

  // dictionary is not supported
  @Override
  public void setDictionary(byte[] b, int off, int len) {
    throw new UnsupportedOperationException(
        "Dictionary support is not enabled");
  }

  @Override
  public boolean needsInput() {
    // Consume remaining compressed data?
    if (uncompressedDirectBuf.remaining() > 0) {
      return false;
    }

    // Check if we have consumed all input
    if (bytesInCompressedBuffer - compressedDirectBufOff <= 0) {
      // Check if we have consumed all user-input
      if (userBufferBytesToConsume <= 0) {
        return true;
      } else {
        setInputFromSavedData();
      }
    }
    return false;
  }

  // dictionary is not supported.
  @Override
  public boolean needsDictionary() {
    return false;
  }

  @Override
  public boolean finished() {
    // finished == true if ZSTD_decompressStream() returns 0
    // also check we have nothing left in our buffer
    return (finished && uncompressedDirectBuf.remaining() == 0);
  }

  @Override
  public int decompress(byte[] b, int off, int len)
      throws IOException {
    checkStream();
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    // Check if there is uncompressed data
    int n = uncompressedDirectBuf.remaining();
    if (n > 0) {
      return populateUncompressedBuffer(b, off, len, n);
    }

    // Re-initialize the output direct buffer
    uncompressedDirectBuf.rewind();
    uncompressedDirectBuf.limit(directBufferSize);

    // Decompress data
    n = inflateBytesDirect(
        compressedDirectBuf,
        compressedDirectBufOff,
        bytesInCompressedBuffer,
        uncompressedDirectBuf,
        0,
        directBufferSize
    );
    uncompressedDirectBuf.limit(n);

    // Get at most 'len' bytes
    return populateUncompressedBuffer(b, off, len, n);
  }

  /**
   * <p>Returns the number of bytes remaining in the input buffers;
   * normally called when finished() is true to determine amount of post-stream
   * data.</p>
   *
   * @return the total (non-negative) number of unprocessed bytes in input
   */
  @Override
  public int getRemaining() {
    checkStream();
    // userBuf + compressedDirectBuf
    return userBufferBytesToConsume + remaining;
  }

  /**
   * Resets everything including the input buffers (user and direct).
   */
  @Override
  public void reset() {
    checkStream();
    init(stream);
    remaining = 0;
    finished = false;
    compressedDirectBufOff = 0;
    bytesInCompressedBuffer = 0;
    uncompressedDirectBuf.limit(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
    userBufOff = 0;
    userBufferBytesToConsume = 0;
  }

  @Override
  public void end() {
    if (stream != 0) {
      free(stream);
      stream = 0;
    }
  }

  @Override
  protected void finalize() {
    reset();
  }

  private void checkStream() {
    if (stream == 0) {
      throw new NullPointerException("Stream not initialized");
    }
  }

  private int populateUncompressedBuffer(byte[] b, int off, int len, int n) {
    n = Math.min(n, len);
    uncompressedDirectBuf.get(b, off, n);
    return n;
  }

  private native static void initIDs();
  private native static long create();
  private native static void init(long stream);
  private native int inflateBytesDirect(ByteBuffer src, int srcOffset,
      int srcLen, ByteBuffer dst, int dstOffset, int dstLen);
  private native static void free(long strm);
  private native static int getStreamSize();

  int inflateDirect(ByteBuffer src, ByteBuffer dst) throws IOException {
    assert
        (this instanceof ZStandardDecompressor.ZStandardDirectDecompressor);

    int originalPosition = dst.position();
    int n = inflateBytesDirect(
        src, src.position(), src.limit(), dst, dst.position(),
        dst.limit()
    );
    dst.position(originalPosition + n);
    if (bytesInCompressedBuffer > 0) {
      src.position(compressedDirectBufOff);
    } else {
      src.position(src.limit());
    }
    return n;
  }

  /**
   * A {@link DirectDecompressor} for ZStandard
   * https://github.com/facebook/zstd.
   */
  public static class ZStandardDirectDecompressor
      extends ZStandardDecompressor implements DirectDecompressor {

    public ZStandardDirectDecompressor(int directBufferSize) {
      super(directBufferSize);
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

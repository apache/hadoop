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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.ZStandardCodec;
import org.apache.hadoop.util.NativeCodeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@link Compressor} based on the zStandard compression algorithm.
 * https://github.com/facebook/zstd
 */
public class ZStandardCompressor implements Compressor {

  private static final Logger LOG =
      LoggerFactory.getLogger(ZStandardCompressor.class);

  private long stream;
  private int level;
  private int directBufferSize;
  private byte[] userBuf = null;
  private int userBufOff = 0, userBufLen = 0;
  private ByteBuffer uncompressedDirectBuf = null;
  private int uncompressedDirectBufOff = 0, uncompressedDirectBufLen = 0;
  private boolean keepUncompressedBuf = false;
  private ByteBuffer compressedDirectBuf = null;
  private boolean finish, finished;
  private long bytesRead = 0;
  private long bytesWritten = 0;

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

  @VisibleForTesting
  ZStandardCompressor() {
    this(CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_LEVEL_DEFAULT,
        CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
  }

  /**
   * Creates a new compressor with the default compression level.
   * Compressed data will be generated in ZStandard format.
   */
  public ZStandardCompressor(int level, int bufferSize) {
    this(level, bufferSize, bufferSize);
  }

  @VisibleForTesting
  ZStandardCompressor(int level, int inputBufferSize, int outputBufferSize) {
    this.level = level;
    stream = create();
    this.directBufferSize = outputBufferSize;
    uncompressedDirectBuf = ByteBuffer.allocateDirect(inputBufferSize);
    compressedDirectBuf = ByteBuffer.allocateDirect(outputBufferSize);
    compressedDirectBuf.position(outputBufferSize);
    reset();
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
    if (conf == null) {
      return;
    }
    level = ZStandardCodec.getCompressionLevel(conf);
    reset();
    LOG.debug("Reinit compressor with new compression configuration");
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
    uncompressedDirectBufOff = 0;
    setInputFromSavedData();

    compressedDirectBuf.limit(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
  }

  //copy enough data from userBuf to uncompressedDirectBuf
  private void setInputFromSavedData() {
    int len = Math.min(userBufLen, uncompressedDirectBuf.remaining());
    uncompressedDirectBuf.put(userBuf, userBufOff, len);
    userBufLen -= len;
    userBufOff += len;
    uncompressedDirectBufLen = uncompressedDirectBuf.position();
  }

  @Override
  public void setDictionary(byte[] b, int off, int len) {
    throw new UnsupportedOperationException(
        "Dictionary support is not enabled");
  }

  @Override
  public boolean needsInput() {
    // Consume remaining compressed data?
    if (compressedDirectBuf.remaining() > 0) {
      return false;
    }

    // have we consumed all input
    if (keepUncompressedBuf && uncompressedDirectBufLen > 0) {
      return false;
    }

    if (uncompressedDirectBuf.remaining() > 0) {
      // Check if we have consumed all user-input
      if (userBufLen <= 0) {
        return true;
      } else {
        // copy enough data from userBuf to uncompressedDirectBuf
        setInputFromSavedData();
        // uncompressedDirectBuf is not full
        return uncompressedDirectBuf.remaining() > 0;
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
    // Check if 'zstd' says its 'finished' and all compressed
    // data has been consumed
    return (finished && compressedDirectBuf.remaining() == 0);
  }

  @Override
  public int compress(byte[] b, int off, int len) throws IOException {
    checkStream();
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
      compressedDirectBuf.get(b, off, n);
      return n;
    }

    // Re-initialize the output direct buffer
    compressedDirectBuf.rewind();
    compressedDirectBuf.limit(directBufferSize);

    // Compress data
    n = deflateBytesDirect(
        uncompressedDirectBuf,
        uncompressedDirectBufOff,
        uncompressedDirectBufLen,
        compressedDirectBuf,
        directBufferSize
    );
    compressedDirectBuf.limit(n);

    // Check if we have consumed all input buffer
    if (uncompressedDirectBufLen <= 0) {
      // consumed all input buffer
      keepUncompressedBuf = false;
      uncompressedDirectBuf.clear();
      uncompressedDirectBufOff = 0;
      uncompressedDirectBufLen = 0;
    } else {
      //  did not consume all input buffer
      keepUncompressedBuf = true;
    }

    // Get at most 'len' bytes
    n = Math.min(n, len);
    compressedDirectBuf.get(b, off, n);
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
    return bytesWritten;
  }

  /**
   * <p>Returns the total number of uncompressed bytes input so far.</p>
   *
   * @return the total (non-negative) number of uncompressed bytes input so far
   */
  @Override
  public long getBytesRead() {
    checkStream();
    return bytesRead;
  }

  @Override
  public void reset() {
    checkStream();
    init(level, stream);
    finish = false;
    finished = false;
    bytesRead = 0;
    bytesWritten = 0;
    uncompressedDirectBuf.rewind();
    uncompressedDirectBufOff = 0;
    uncompressedDirectBufLen = 0;
    keepUncompressedBuf = false;
    compressedDirectBuf.limit(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
    userBufOff = 0;
    userBufLen = 0;
  }

  @Override
  public void end() {
    if (stream != 0) {
      end(stream);
      stream = 0;
    }
  }

  private void checkStream() {
    if (stream == 0) {
      throw new NullPointerException();
    }
  }

  private native static long create();
  private native static void init(int level, long stream);
  private native int deflateBytesDirect(ByteBuffer src, int srcOffset,
      int srcLen, ByteBuffer dst, int dstLen);
  private static native int getStreamSize();
  private native static void end(long strm);
  private native static void initIDs();
  public native static String getLibraryName();
}

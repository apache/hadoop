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

import com.github.luben.zstd.EndDirective;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdOutputStream;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.ZStandardCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@link Compressor} based on the Zstandard compression algorithm,
 * backed by the <a href="https://github.com/luben/zstd-jni">zstd-jni</a> library.
 */
public class ZStandardCompressor implements Compressor {

  private static final Logger LOG =
      LoggerFactory.getLogger(ZStandardCompressor.class);

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

  private ZstdCompressCtx zstdJniCtx = null;

  public static int getRecommendedBufferSize() {
    // zstd-jni recommended output size for streaming (~128 KB)
    return (int) ZstdOutputStream.recommendedCOutSize();
  }

  @VisibleForTesting
  ZStandardCompressor() {
    this(CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_LEVEL_DEFAULT,
        CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
  }

  /**
   * Creates a new compressor with the default compression level.
   * Compressed data will be generated in ZStandard format.
   * @param level level.
   * @param bufferSize bufferSize.
   */
  public ZStandardCompressor(int level, int bufferSize) {
    this(level, bufferSize, bufferSize);
  }

  @VisibleForTesting
  ZStandardCompressor(int level, int inputBufferSize, int outputBufferSize) {
    this.level = level;
    zstdJniCtx = new ZstdCompressCtx();
    uncompressedDirectBuf = ByteBuffer.allocateDirect(inputBufferSize);
    directBufferSize = outputBufferSize;
    compressedDirectBuf = ByteBuffer.allocateDirect(outputBufferSize);
    compressedDirectBuf.position(directBufferSize);
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
    if (keepUncompressedBuf && uncompressedDirectBufLen - uncompressedDirectBufOff > 0) {
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

    // Always invoke the streaming API — even with empty input — so internally
    // buffered bytes continue to be drained, matching native ZSTD_flushStream.
    // Use END only when finish=true, no more user data, and all direct-buffer
    // data consumed (mirrors ZSTD_endStream); otherwise FLUSH (mirrors
    // ZSTD_compressStream + ZSTD_flushStream).
    boolean allConsumed = (uncompressedDirectBufLen - uncompressedDirectBufOff <= 0);
    boolean shouldEnd = finish && userBufLen == 0 && allConsumed;

    uncompressedDirectBuf.position(uncompressedDirectBufOff);
    uncompressedDirectBuf.limit(uncompressedDirectBufLen);
    compressedDirectBuf.position(0);
    compressedDirectBuf.limit(directBufferSize);

    EndDirective endOp = shouldEnd ? EndDirective.END : EndDirective.FLUSH;
    boolean done = zstdJniCtx.compressDirectByteBufferStream(
        compressedDirectBuf, uncompressedDirectBuf, endOp);

    int newOff = uncompressedDirectBuf.position();
    n = compressedDirectBuf.position();

    bytesRead += newOff - uncompressedDirectBufOff;
    bytesWritten += n;

    uncompressedDirectBufOff = newOff;
    if (uncompressedDirectBufLen - uncompressedDirectBufOff <= 0) {
      keepUncompressedBuf = false;
      uncompressedDirectBuf.clear();
      uncompressedDirectBufOff = 0;
      uncompressedDirectBufLen = 0;
    } else {
      keepUncompressedBuf = true;
    }

    if (endOp == EndDirective.END && done) {
      finished = true;
    }

    compressedDirectBuf.position(0);
    compressedDirectBuf.limit(n);

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
    zstdJniCtx.reset();
    zstdJniCtx.setLevel(level);
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
    if (zstdJniCtx != null) {
      zstdJniCtx.close();
      zstdJniCtx = null;
    }
  }

  @Override
  protected void finalize() {
    end();
  }

  private void checkStream() {
    if (zstdJniCtx == null) {
      throw new NullPointerException("ZstdCompressCtx is not initialized");
    }
  }
}

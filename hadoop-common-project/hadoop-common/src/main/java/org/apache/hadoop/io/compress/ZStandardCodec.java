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

package org.apache.hadoop.io.compress;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.compress.zstd.ZStandardCompressor;
import org.apache.hadoop.io.compress.zstd.ZStandardDecompressor;
import org.apache.hadoop.util.NativeCodeLoader;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_BUFFER_SIZE_KEY;

/**
 * This class creates zstd compressors/decompressors.
 */
public class ZStandardCodec implements
    Configurable, CompressionCodec, DirectDecompressionCodec  {
  private Configuration conf;

  /**
   * Set the configuration to be used by this object.
   *
   * @param conf the configuration object.
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Return the configuration used by this object.
   *
   * @return the configuration object used by this object.
   */
  @Override
  public Configuration getConf() {
    return conf;
  }

  public static void checkNativeCodeLoaded() {
    if (!NativeCodeLoader.isNativeCodeLoaded() ||
        !NativeCodeLoader.buildSupportsZstd()) {
      throw new RuntimeException("native zStandard library "
          + "not available: this version of libhadoop was built "
          + "without zstd support.");
    }
    if (!ZStandardCompressor.isNativeCodeLoaded()) {
      throw new RuntimeException("native zStandard library not "
          + "available: ZStandardCompressor has not been loaded.");
    }
    if (!ZStandardDecompressor.isNativeCodeLoaded()) {
      throw new RuntimeException("native zStandard library not "
          + "available: ZStandardDecompressor has not been loaded.");
    }
  }

  public static boolean isNativeCodeLoaded() {
    return ZStandardCompressor.isNativeCodeLoaded()
        && ZStandardDecompressor.isNativeCodeLoaded();
  }

  public static String getLibraryName() {
    return ZStandardCompressor.getLibraryName();
  }

  public static int getCompressionLevel(Configuration conf) {
    return conf.getInt(
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_LEVEL_KEY,
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_LEVEL_DEFAULT);
  }

  public static int getCompressionBufferSize(Configuration conf) {
    int bufferSize = getBufferSize(conf);
    return bufferSize == 0 ?
        ZStandardCompressor.getRecommendedBufferSize() :
        bufferSize;
  }

  public static int getDecompressionBufferSize(Configuration conf) {
    int bufferSize = getBufferSize(conf);
    return bufferSize == 0 ?
        ZStandardDecompressor.getRecommendedBufferSize() :
        bufferSize;
  }

  private static int getBufferSize(Configuration conf) {
    return conf.getInt(IO_COMPRESSION_CODEC_ZSTD_BUFFER_SIZE_KEY,
        IO_COMPRESSION_CODEC_ZSTD_BUFFER_SIZE_DEFAULT);
  }

  /**
   * Create a {@link CompressionOutputStream} that will write to the given
   * {@link OutputStream}.
   *
   * @param out the location for the final output stream
   * @return a stream the user can write uncompressed data to have compressed
   * @throws IOException
   */
  @Override
  public CompressionOutputStream createOutputStream(OutputStream out)
      throws IOException {
    return Util.
        createOutputStreamWithCodecPool(this, conf, out);
  }

  /**
   * Create a {@link CompressionOutputStream} that will write to the given
   * {@link OutputStream} with the given {@link Compressor}.
   *
   * @param out        the location for the final output stream
   * @param compressor compressor to use
   * @return a stream the user can write uncompressed data to have compressed
   * @throws IOException
   */
  @Override
  public CompressionOutputStream createOutputStream(OutputStream out,
      Compressor compressor)
      throws IOException {
    checkNativeCodeLoaded();
    return new CompressorStream(out, compressor,
        getCompressionBufferSize(conf));
  }

  /**
   * Get the type of {@link Compressor} needed by this {@link CompressionCodec}.
   *
   * @return the type of compressor needed by this codec.
   */
  @Override
  public Class<? extends Compressor> getCompressorType() {
    checkNativeCodeLoaded();
    return ZStandardCompressor.class;
  }

  /**
   * Create a new {@link Compressor} for use by this {@link CompressionCodec}.
   *
   * @return a new compressor for use by this codec
   */
  @Override
  public Compressor createCompressor() {
    checkNativeCodeLoaded();
    return new ZStandardCompressor(
        getCompressionLevel(conf), getCompressionBufferSize(conf));
  }


  /**
   * Create a {@link CompressionInputStream} that will read from the given
   * input stream.
   *
   * @param in the stream to read compressed bytes from
   * @return a stream to read uncompressed bytes from
   * @throws IOException
   */
  @Override
  public CompressionInputStream createInputStream(InputStream in)
      throws IOException {
    return Util.
        createInputStreamWithCodecPool(this, conf, in);
  }

  /**
   * Create a {@link CompressionInputStream} that will read from the given
   * {@link InputStream} with the given {@link Decompressor}.
   *
   * @param in           the stream to read compressed bytes from
   * @param decompressor decompressor to use
   * @return a stream to read uncompressed bytes from
   * @throws IOException
   */
  @Override
  public CompressionInputStream createInputStream(InputStream in,
                                                  Decompressor decompressor)
      throws IOException {
    checkNativeCodeLoaded();
    return new DecompressorStream(in, decompressor,
        getDecompressionBufferSize(conf));
  }

  /**
   * Get the type of {@link Decompressor} needed by
   * this {@link CompressionCodec}.
   *
   * @return the type of decompressor needed by this codec.
   */
  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    checkNativeCodeLoaded();
    return ZStandardDecompressor.class;
  }

  /**
   * Create a new {@link Decompressor} for use by this {@link CompressionCodec}.
   *
   * @return a new decompressor for use by this codec
   */
  @Override
  public Decompressor createDecompressor() {
    checkNativeCodeLoaded();
    return new ZStandardDecompressor(getDecompressionBufferSize(conf));
  }

  /**
   * Get the default filename extension for this kind of compression.
   *
   * @return <code>.zst</code>.
   */
  @Override
  public String getDefaultExtension() {
    return ".zst";
  }

  @Override
  public DirectDecompressor createDirectDecompressor() {
    return new ZStandardDecompressor.ZStandardDirectDecompressor(
        getDecompressionBufferSize(conf)
    );
  }
}

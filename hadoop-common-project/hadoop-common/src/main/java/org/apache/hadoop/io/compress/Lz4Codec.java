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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.lz4.Lz4Compressor;
import org.apache.hadoop.io.compress.lz4.Lz4Decompressor;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.NativeCodeLoader;

/**
 * This class creates lz4 compressors/decompressors.
 */
public class Lz4Codec implements Configurable, CompressionCodec {

  static {
    NativeCodeLoader.isNativeCodeLoaded();
  }

  Configuration conf;

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
   * @return the configuration object used by this objec.
   */
  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Are the native lz4 libraries loaded & initialized?
   *
   * @return true if loaded & initialized, otherwise false
   */
  public static boolean isNativeCodeLoaded() {
    return NativeCodeLoader.isNativeCodeLoaded();
  }

  public static String getLibraryName() {
    return Lz4Compressor.getLibraryName();
  }

  /**
   * Create a {@link CompressionOutputStream} that will write to the given
   * {@link OutputStream}.
   *
   * @param out the location for the final output stream
   * @return a stream the user can write uncompressed data to have it compressed
   * @throws IOException
   */
  @Override
  public CompressionOutputStream createOutputStream(OutputStream out)
      throws IOException {
    return createOutputStream(out, createCompressor());
  }

  /**
   * Create a {@link CompressionOutputStream} that will write to the given
   * {@link OutputStream} with the given {@link Compressor}.
   *
   * @param out        the location for the final output stream
   * @param compressor compressor to use
   * @return a stream the user can write uncompressed data to have it compressed
   * @throws IOException
   */
  @Override
  public CompressionOutputStream createOutputStream(OutputStream out,
                                                    Compressor compressor)
      throws IOException {
    if (!isNativeCodeLoaded()) {
      throw new RuntimeException("native lz4 library not available");
    }
    int bufferSize = conf.getInt(
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_KEY,
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_DEFAULT);

    int compressionOverhead = bufferSize/255 + 16;

    return new BlockCompressorStream(out, compressor, bufferSize,
        compressionOverhead);
  }

  /**
   * Get the type of {@link Compressor} needed by this {@link CompressionCodec}.
   *
   * @return the type of compressor needed by this codec.
   */
  @Override
  public Class<? extends Compressor> getCompressorType() {
    if (!isNativeCodeLoaded()) {
      throw new RuntimeException("native lz4 library not available");
    }

    return Lz4Compressor.class;
  }

  /**
   * Create a new {@link Compressor} for use by this {@link CompressionCodec}.
   *
   * @return a new compressor for use by this codec
   */
  @Override
  public Compressor createCompressor() {
    if (!isNativeCodeLoaded()) {
      throw new RuntimeException("native lz4 library not available");
    }
    int bufferSize = conf.getInt(
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_KEY,
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_DEFAULT);
    boolean useLz4HC = conf.getBoolean(
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_USELZ4HC_KEY,
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_USELZ4HC_DEFAULT);
    return new Lz4Compressor(bufferSize, useLz4HC);
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
    return createInputStream(in, createDecompressor());
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
    if (!isNativeCodeLoaded()) {
      throw new RuntimeException("native lz4 library not available");
    }

    return new BlockDecompressorStream(in, decompressor, conf.getInt(
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_KEY,
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_DEFAULT));
  }

  /**
   * Get the type of {@link Decompressor} needed by this {@link CompressionCodec}.
   *
   * @return the type of decompressor needed by this codec.
   */
  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    if (!isNativeCodeLoaded()) {
      throw new RuntimeException("native lz4 library not available");
    }

    return Lz4Decompressor.class;
  }

  /**
   * Create a new {@link Decompressor} for use by this {@link CompressionCodec}.
   *
   * @return a new decompressor for use by this codec
   */
  @Override
  public Decompressor createDecompressor() {
    if (!isNativeCodeLoaded()) {
      throw new RuntimeException("native lz4 library not available");
    }
    int bufferSize = conf.getInt(
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_KEY,
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_DEFAULT);
    return new Lz4Decompressor(bufferSize);
  }

  /**
   * Get the default filename extension for this kind of compression.
   *
   * @return <code>.lz4</code>.
   */
  @Override
  public String getDefaultExtension() {
    return ".lz4";
  }
}

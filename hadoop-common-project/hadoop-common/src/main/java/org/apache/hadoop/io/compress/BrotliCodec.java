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

import com.aayushatharva.brotli4j.Brotli4jLoader;
import com.aayushatharva.brotli4j.encoder.Encoder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.compress.brotli.BrotliCompressor;
import org.apache.hadoop.io.compress.brotli.BrotliDecompressor;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * This class provides output and input streams for Brotli compression
 * and decompression. It uses the native brotli library provided as a
 * Maven dependency.
 *
 * Brotli compression does not support splittability!
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class BrotliCodec extends Configured implements CompressionCodec {

  public static final String MODE_PROP = "compression.brotli.is-text";
  public static final String QUALITY_LEVEL_PROP = "compression.brotli.quality";
  public static final String LZ_WINDOW_SIZE_PROP = "compression.brotli.lzwin";
  public static final Encoder.Mode DEFAULT_MODE = Encoder.Mode.GENERIC;
  public static final int DEFAULT_QUALITY = -1;
  public static final int DEFAULT_LZ_WINDOW_SIZE = -1;

  static {
    loadNatives();
  }

  public static void loadNatives() {
    Brotli4jLoader.ensureAvailability();
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out)
      throws IOException {
    return createOutputStream(out, createCompressor());
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out,
                                                    Compressor compressor)
      throws IOException {
    return new BrotliCompressorStream(out, compressor);
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    return BrotliCompressor.class;
  }

  @Override
  public Compressor createCompressor() {
    return new BrotliCompressor(getConf());
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in)
      throws IOException {
    return createInputStream(in, createDecompressor());
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in,
                                                  Decompressor decompressor)
      throws IOException {
    return new BrotliDecompressorStream(in, decompressor);
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return BrotliDecompressor.class;
  }

  @Override
  public Decompressor createDecompressor() {
    return new BrotliDecompressor();
  }

  @Override
  public String getDefaultExtension() {
    return ".br";
  }

  private static final class BrotliCompressorStream extends CompressorStream {
    private BrotliCompressorStream(OutputStream out,
                                   Compressor compressor,
                                   int bufferSize) {
      super(out, compressor, bufferSize);
    }

    private BrotliCompressorStream(OutputStream out,
                                   Compressor compressor) {
      super(out, compressor);
    }

    @Override
    public void close() throws IOException {
      super.close();
      compressor.end();
    }
  }

  private static final class BrotliDecompressorStream
          extends DecompressorStream {

    private BrotliDecompressorStream(InputStream in,
                                     Decompressor decompressor,
                                     int bufferSize)
        throws IOException {
      super(in, decompressor, bufferSize);
    }

    private BrotliDecompressorStream(InputStream in, Decompressor decompressor)
        throws IOException {
      super(in, decompressor);
    }

    @Override
    public void close() throws IOException {
      super.close();
      decompressor.end();
    }
  }
}

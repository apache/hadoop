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
import java.io.OutputStream;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.lzo.*;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * A {@link org.apache.hadoop.io.compress.CompressionCodec} for a streaming
 * <b>lzo</b> compression/decompression pair.
 * http://www.oberhumer.com/opensource/lzo/
 * 
 */
public class LzoCodec implements Configurable, CompressionCodec {
  
  private static final Log LOG = LogFactory.getLog(LzoCodec.class.getName());

  private Configuration conf;

  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  public Configuration getConf() {
    return conf;
  }

  private static boolean nativeLzoLoaded = false;
  
  static {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      nativeLzoLoaded = LzoCompressor.isNativeLzoLoaded() &&
        LzoDecompressor.isNativeLzoLoaded();
      
      if (nativeLzoLoaded) {
        LOG.info("Successfully loaded & initialized native-lzo library");
      } else {
        LOG.error("Failed to load/initialize native-lzo library");
      }
    } else {
      LOG.error("Cannot load native-lzo without native-hadoop");
    }
  }

  /**
   * Check if native-lzo library is loaded & initialized.
   * 
   * @param conf configuration
   * @return <code>true</code> if native-lzo library is loaded & initialized;
   *         else <code>false</code>
   */
  public static boolean isNativeLzoLoaded(Configuration conf) {
    return nativeLzoLoaded && conf.getBoolean("hadoop.native.lib", true);
  }

  public CompressionOutputStream createOutputStream(OutputStream out) 
    throws IOException {
    return createOutputStream(out, createCompressor());
  }
  
  public CompressionOutputStream createOutputStream(OutputStream out, 
      Compressor compressor) throws IOException {
    // Ensure native-lzo library is loaded & initialized
    if (!isNativeLzoLoaded(conf)) {
      throw new RuntimeException("native-lzo library not available");
    }
    
    /**
     * <b>http://www.oberhumer.com/opensource/lzo/lzofaq.php</b>
     *
     * How much can my data expand during compression ?
     * ================================================
     * LZO will expand incompressible data by a little amount.
     * I still haven't computed the exact values, but I suggest using
     * these formulas for a worst-case expansion calculation:
     * 
     * Algorithm LZO1, LZO1A, LZO1B, LZO1C, LZO1F, LZO1X, LZO1Y, LZO1Z:
     * ----------------------------------------------------------------
     * output_block_size = input_block_size + (input_block_size / 16) + 64 + 3
     * 
     * This is about 106% for a large block size.
     * 
     * Algorithm LZO2A:
     * ----------------
     * output_block_size = input_block_size + (input_block_size / 8) + 128 + 3
     */

    // Create the lzo output-stream
    LzoCompressor.CompressionStrategy strategy = 
      LzoCompressor.CompressionStrategy.valueOf(
          conf.get("io.compression.codec.lzo.compressor",
            LzoCompressor.CompressionStrategy.LZO1X_1.name()));
    int bufferSize =
      conf.getInt("io.compression.codec.lzo.buffersize", 64*1024);
    int compressionOverhead = strategy.name().contains("LZO1")
      ? (bufferSize >> 4) + 64 + 3
      : (bufferSize >> 3) + 128 + 3;

    return new BlockCompressorStream(out, compressor, bufferSize,
                                     compressionOverhead);
  }

  public Class<? extends Compressor> getCompressorType() {
    // Ensure native-lzo library is loaded & initialized
    if (!isNativeLzoLoaded(conf)) {
      throw new RuntimeException("native-lzo library not available");
    }
    return LzoCompressor.class;
  }

  public Compressor createCompressor() {
    // Ensure native-lzo library is loaded & initialized
    if (!isNativeLzoLoaded(conf)) {
      throw new RuntimeException("native-lzo library not available");
    }
    
    LzoCompressor.CompressionStrategy strategy = 
      LzoCompressor.CompressionStrategy.valueOf(
          conf.get("io.compression.codec.lzo.compressor",
            LzoCompressor.CompressionStrategy.LZO1X_1.name()));
    int bufferSize =
      conf.getInt("io.compression.codec.lzo.buffersize", 64*1024);

    return new LzoCompressor(strategy, bufferSize);
  }

  public CompressionInputStream createInputStream(InputStream in)
      throws IOException {
    return createInputStream(in, createDecompressor());
  }

  public CompressionInputStream createInputStream(InputStream in, 
                                                  Decompressor decompressor) 
  throws IOException {
    // Ensure native-lzo library is loaded & initialized
    if (!isNativeLzoLoaded(conf)) {
      throw new RuntimeException("native-lzo library not available");
    }
    return new BlockDecompressorStream(in, decompressor, 
        conf.getInt("io.compression.codec.lzo.buffersize", 64*1024));
  }

  public Class<? extends Decompressor> getDecompressorType() {
    // Ensure native-lzo library is loaded & initialized
    if (!isNativeLzoLoaded(conf)) {
      throw new RuntimeException("native-lzo library not available");
    }
    return LzoDecompressor.class;
  }

  public Decompressor createDecompressor() {
    // Ensure native-lzo library is loaded & initialized
    if (!isNativeLzoLoaded(conf)) {
      throw new RuntimeException("native-lzo library not available");
    }
    
    LzoDecompressor.CompressionStrategy strategy = 
      LzoDecompressor.CompressionStrategy.valueOf(
          conf.get("io.compression.codec.lzo.decompressor",
            LzoDecompressor.CompressionStrategy.LZO1X.name()));
    int bufferSize =
      conf.getInt("io.compression.codec.lzo.buffersize", 64*1024);

    return new LzoDecompressor(strategy, bufferSize); 
  }

  /**
   * Get the default filename extension for this kind of compression.
   * @return the extension including the '.'
   */
  public String getDefaultExtension() {
    return ".lzo_deflate";
  }
}

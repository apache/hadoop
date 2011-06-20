/**
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyError;

public class SnappyCodec implements Configurable, CompressionCodec {
  private static final Log logger = LogFactory.getLog(SnappyCodec.class
      .getName());
  private static boolean nativeSnappyLoaded = false;
  private Configuration conf;

  public static final String SNAPPY_BUFFER_SIZE_KEY = "io.compression.codec.snappy.buffersize";
  public static final int DEFAULT_SNAPPY_BUFFER_SIZE = 256 * 1024;

  public SnappyCodec() {

  }

  public SnappyCodec(Configuration conf) {
    setConf(conf);
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return conf;
  }

  static {
    try {
      if (Snappy.getNativeLibraryVersion() != null) {
        logger
            .info("Successfully loaded & initialized native-snappy library [snappy-java rev "
                + Snappy.getNativeLibraryVersion() + "]");

        nativeSnappyLoaded = true;
      } else {
        logger.info("Failed to load native-snappy library");
      }

    } catch (SnappyError e) {
      logger.error("Native Snappy load error: ", e);
    }
  }

  public static boolean isNativeSnappyLoaded(Configuration conf) {
    return nativeSnappyLoaded;
  }

  public CompressionOutputStream createOutputStream(OutputStream out)
      throws IOException {
    return createOutputStream(out, createCompressor());
  }

  public CompressionOutputStream createOutputStream(OutputStream out,
      Compressor compressor) throws IOException {

    if (!isNativeSnappyLoaded(conf)) {
      throw new RuntimeException("native-snappy library not available");
    }

    int bufferSize = conf.getInt(SNAPPY_BUFFER_SIZE_KEY,
        DEFAULT_SNAPPY_BUFFER_SIZE);

    int compressionOverhead = Snappy.maxCompressedLength(bufferSize) - bufferSize; 

    return new BlockCompressorStream(out, compressor, bufferSize,
        compressionOverhead);
  }

  public Class<? extends Compressor> getCompressorType() {
    if (!isNativeSnappyLoaded(conf)) {
      throw new RuntimeException("native-snappy library not available");
    }
    return SnappyCompressor.class;
  }

  public Compressor createCompressor() {
    if (!isNativeSnappyLoaded(conf)) {
      throw new RuntimeException("native-snappy library not available");
    }

    return new SnappyCompressor(conf.getInt(SNAPPY_BUFFER_SIZE_KEY,
        DEFAULT_SNAPPY_BUFFER_SIZE));
  }

  public CompressionInputStream createInputStream(InputStream in)
      throws IOException {
    return createInputStream(in, createDecompressor());
  }

  public CompressionInputStream createInputStream(InputStream in,
      Decompressor decompressor) throws IOException {
    if (!isNativeSnappyLoaded(conf)) {
      throw new RuntimeException("native-snappy library not available");
    }
    return new BlockDecompressorStream(in, decompressor, conf.getInt(
        SNAPPY_BUFFER_SIZE_KEY, DEFAULT_SNAPPY_BUFFER_SIZE));
  }

  public Class<? extends Decompressor> getDecompressorType() {
    if (!isNativeSnappyLoaded(conf)) {
      throw new RuntimeException("native-snappy library not available");
    }
    return SnappyDecompressor.class;
  }

  public Decompressor createDecompressor() {
    if (!isNativeSnappyLoaded(conf)) {
      throw new RuntimeException("native-snappy library not available");
    }

    return new SnappyDecompressor(conf.getInt(SNAPPY_BUFFER_SIZE_KEY,
        DEFAULT_SNAPPY_BUFFER_SIZE));
  }

  public String getDefaultExtension() {
    return ".snappy";
  }
}

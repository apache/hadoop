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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DirectDecompressor;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy;
import org.apache.hadoop.util.NativeCodeLoader;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collection of factories to create the right 
 * zlib/gzip compressor/decompressor instances.
 * 
 */
public class ZlibFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(ZlibFactory.class);

  private static boolean nativeZlibLoaded = false;
  
  static {
    loadNativeZLib();
  }

  /**
   * Load native library and set the flag whether to use native library. The
   * method is also used for reset the flag modified by setNativeZlibLoaded
   */
  @VisibleForTesting
  public static void loadNativeZLib() {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      nativeZlibLoaded = ZlibCompressor.isNativeZlibLoaded() &&
        ZlibDecompressor.isNativeZlibLoaded();
      
      if (nativeZlibLoaded) {
        LOG.info("Successfully loaded & initialized native-zlib library");
      } else {
        LOG.warn("Failed to load/initialize native-zlib library");
      }
    }
  }
  
  /**
   * Set the flag whether to use native library. Used for testing non-native
   * libraries
   *
   */
  @VisibleForTesting
  public static void setNativeZlibLoaded(final boolean isLoaded) {
    ZlibFactory.nativeZlibLoaded = isLoaded;
  }
  /**
   * Check if native-zlib code is loaded & initialized correctly and 
   * can be loaded for this job.
   * 
   * @param conf configuration
   * @return <code>true</code> if native-zlib is loaded & initialized 
   *         and can be loaded for this job, else <code>false</code>
   */
  public static boolean isNativeZlibLoaded(Configuration conf) {
    return nativeZlibLoaded;
  }

  public static String getLibraryName() {
    return ZlibCompressor.getLibraryName();
  }

  /**
   * Return the appropriate type of the zlib compressor. 
   * 
   * @param conf configuration
   * @return the appropriate type of the zlib compressor.
   */
  public static Class<? extends Compressor> 
  getZlibCompressorType(Configuration conf) {
    return (isNativeZlibLoaded(conf)) ? 
            ZlibCompressor.class : BuiltInZlibDeflater.class;
  }
  
  /**
   * Return the appropriate implementation of the zlib compressor. 
   * 
   * @param conf configuration
   * @return the appropriate implementation of the zlib compressor.
   */
  public static Compressor getZlibCompressor(Configuration conf) {
    return (isNativeZlibLoaded(conf)) ? 
      new ZlibCompressor(conf) :
      new BuiltInZlibDeflater(ZlibFactory.getCompressionLevel(conf).compressionLevel());
  }

  /**
   * Return the appropriate type of the zlib decompressor. 
   * 
   * @param conf configuration
   * @return the appropriate type of the zlib decompressor.
   */
  public static Class<? extends Decompressor> 
  getZlibDecompressorType(Configuration conf) {
    return (isNativeZlibLoaded(conf)) ? 
            ZlibDecompressor.class : BuiltInZlibInflater.class;
  }
  
  /**
   * Return the appropriate implementation of the zlib decompressor. 
   * 
   * @param conf configuration
   * @return the appropriate implementation of the zlib decompressor.
   */
  public static Decompressor getZlibDecompressor(Configuration conf) {
    return (isNativeZlibLoaded(conf)) ? 
      new ZlibDecompressor() : new BuiltInZlibInflater(); 
  }
  
  /**
   * Return the appropriate implementation of the zlib direct decompressor. 
   * 
   * @param conf configuration
   * @return the appropriate implementation of the zlib decompressor.
   */
  public static DirectDecompressor getZlibDirectDecompressor(Configuration conf) {
    return (isNativeZlibLoaded(conf)) ? 
      new ZlibDecompressor.ZlibDirectDecompressor() : null; 
  }

  public static void setCompressionStrategy(Configuration conf,
      CompressionStrategy strategy) {
    conf.setEnum("zlib.compress.strategy", strategy);
  }

  public static CompressionStrategy getCompressionStrategy(Configuration conf) {
    return conf.getEnum("zlib.compress.strategy",
        CompressionStrategy.DEFAULT_STRATEGY);
  }

  public static void setCompressionLevel(Configuration conf,
      CompressionLevel level) {
    conf.setEnum("zlib.compress.level", level);
  }

  public static CompressionLevel getCompressionLevel(Configuration conf) {
    return conf.getEnum("zlib.compress.level",
        CompressionLevel.DEFAULT_COMPRESSION);
  }

}

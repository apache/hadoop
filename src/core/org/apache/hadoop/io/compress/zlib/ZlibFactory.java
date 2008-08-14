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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.NativeCodeLoader;

/**
 * A collection of factories to create the right 
 * zlib/gzip compressor/decompressor instances.
 * 
 */
public class ZlibFactory {
  private static final Log LOG =
    LogFactory.getLog(ZlibFactory.class);

  private static boolean nativeZlibLoaded = false;
  
  static {
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
   * Check if native-zlib code is loaded & initialized correctly and 
   * can be loaded for this job.
   * 
   * @param conf configuration
   * @return <code>true</code> if native-zlib is loaded & initialized 
   *         and can be loaded for this job, else <code>false</code>
   */
  public static boolean isNativeZlibLoaded(Configuration conf) {
    return nativeZlibLoaded && conf.getBoolean("hadoop.native.lib", true); 
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
      new ZlibCompressor() : new BuiltInZlibDeflater(); 
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
  
}

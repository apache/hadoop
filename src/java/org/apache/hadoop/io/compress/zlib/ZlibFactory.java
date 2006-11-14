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
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.NativeCodeLoader;

/**
 * A collection of factories to create the right 
 * zlib/gzip compressor/decompressor instances.
 * 
 * @author Arun C Murthy
 */
public class ZlibFactory {
  private static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.io.compress.zlib.ZlibFactory");

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
   * Check if native-zlib code is loaded and initialized correctly.
   * 
   * @return <code>true</code> if native-zlib is loaded and initialized, 
   *         else <code>false</code>
   */
  public static boolean isNativeZlibLoaded() {
    return nativeZlibLoaded; 
  }
  
  /**
   * Return the appropriate implementation of the zlib compressor. 
   * 
   * @return the appropriate implementation of the zlib compressor.
   */
  public static Compressor getZlibCompressor() {
    return (nativeZlibLoaded) ? 
        new ZlibCompressor() : new BuiltInZlibDeflater(); 
  }

  /**
   * Return the appropriate implementation of the zlib decompressor. 
   * 
   * @return the appropriate implementation of the zlib decompressor.
   */
  public static Decompressor getZlibDecompressor() {
    return (nativeZlibLoaded) ? 
        new ZlibDecompressor() : new BuiltInZlibInflater(); 
  }
  
}

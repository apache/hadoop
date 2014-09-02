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
package org.apache.hadoop.crypto.random;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.NativeCodeLoader;

import com.google.common.base.Preconditions;
import org.apache.hadoop.util.PerformanceAdvisory;

/**
 * OpenSSL secure random using JNI.
 * This implementation is thread-safe.
 * <p/>
 * 
 * If using an Intel chipset with RDRAND, the high-performance hardware 
 * random number generator will be used and it's much faster than
 * {@link java.security.SecureRandom}. If RDRAND is unavailable, default
 * OpenSSL secure random generator will be used. It's still faster
 * and can generate strong random bytes.
 * <p/>
 * @see https://wiki.openssl.org/index.php/Random_Numbers
 * @see http://en.wikipedia.org/wiki/RdRand
 */
@InterfaceAudience.Private
public class OpensslSecureRandom extends Random {
  private static final long serialVersionUID = -7828193502768789584L;
  private static final Log LOG =
      LogFactory.getLog(OpensslSecureRandom.class.getName());
  
  /** If native SecureRandom unavailable, use java SecureRandom */
  private java.security.SecureRandom fallback = null;
  private static boolean nativeEnabled = false;
  static {
    if (NativeCodeLoader.isNativeCodeLoaded() &&
        NativeCodeLoader.buildSupportsOpenssl()) {
      try {
        initSR();
        nativeEnabled = true;
      } catch (Throwable t) {
        LOG.error("Failed to load Openssl SecureRandom", t);
      }
    }
  }
  
  public static boolean isNativeCodeLoaded() {
    return nativeEnabled;
  }
  
  public OpensslSecureRandom() {
    if (!nativeEnabled) {
      PerformanceAdvisory.LOG.debug("Build does not support openssl, " +
          "falling back to Java SecureRandom.");
      fallback = new java.security.SecureRandom();
    }
  }
  
  /**
   * Generates a user-specified number of random bytes.
   * It's thread-safe.
   * 
   * @param bytes the array to be filled in with random bytes.
   */
  @Override
  public void nextBytes(byte[] bytes) {
    if (!nativeEnabled || !nextRandBytes(bytes)) {
      fallback.nextBytes(bytes);
    }
  }
  
  @Override
  public void setSeed(long seed) {
    // Self-seeding.
  }
  
  /**
   * Generates an integer containing the user-specified number of
   * random bits (right justified, with leading zeros).
   *
   * @param numBits number of random bits to be generated, where
   * 0 <= <code>numBits</code> <= 32.
   *
   * @return int an <code>int</code> containing the user-specified number
   * of random bits (right justified, with leading zeros).
   */
  @Override
  final protected int next(int numBits) {
    Preconditions.checkArgument(numBits >= 0 && numBits <= 32);
    int numBytes = (numBits + 7) / 8;
    byte b[] = new byte[numBytes];
    int next = 0;
    
    nextBytes(b);
    for (int i = 0; i < numBytes; i++) {
      next = (next << 8) + (b[i] & 0xFF);
    }
    
    return next >>> (numBytes * 8 - numBits);
  }
  
  private native static void initSR();
  private native boolean nextRandBytes(byte[] bytes); 
}

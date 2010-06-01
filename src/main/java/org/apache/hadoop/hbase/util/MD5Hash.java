/**
 * Copyright 2010 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Utility class for MD5
 * MD5 hash produces a 128-bit digest.
 */
public class MD5Hash {
  private static final Log LOG = LogFactory.getLog(MD5Hash.class);

  /**
   * Given a byte array, returns in MD5 hash as a hex string.
   * @param key
   * @return SHA1 hash as a 32 character hex string.
   */
  public static String getMD5AsHex(byte[] key) {
    return getMD5AsHex(key, 0, key.length);
  }
  
  /**
   * Given a byte array, returns its MD5 hash as a hex string.
   * Only "length" number of bytes starting at "offset" within the
   * byte array are used.
   *
   * @param key the key to hash (variable length byte array)
   * @param offset
   * @param length 
   * @return MD5 hash as a 32 character hex string.
   */
  public static String getMD5AsHex(byte[] key, int offset, int length) {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      md.update(key, offset, length);
      byte[] digest = md.digest();
      return new String(Hex.encodeHex(digest));
    } catch (NoSuchAlgorithmException e) {
      // this should never happen unless the JDK is messed up.
      throw new RuntimeException("Error computing MD5 hash", e);
    }
  }
}

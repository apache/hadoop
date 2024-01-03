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

package org.apache.hadoop.fs.azurebfs.security;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Utility class for managing encryption of bytes or base64String conversion of
 * bytes.
 */
public final class EncodingHelper {

  private EncodingHelper() {

  }

  public static byte[] getSHA256Hash(byte[] key) {
    try {
      final MessageDigest digester = MessageDigest.getInstance("SHA-256");
      return digester.digest(key);
    } catch (NoSuchAlgorithmException noSuchAlgorithmException) {
      /*This exception can be ignored. Reason being SHA-256 is a valid algorithm,
       and it is constant for all method calls.*/
      throw new RuntimeException("SHA-256 algorithm not found in MessageDigest",
          noSuchAlgorithmException);
    }
  }

  public static String getBase64EncodedString(byte[] bytes) {
    return Base64.getEncoder().encodeToString(bytes);
  }
}

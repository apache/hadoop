/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.security;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.ozone.OzoneConsts;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

/**
 * Symmetric Key structure for GDPR.
 */
public class GDPRSymmetricKey {

  private SecretKeySpec secretKey;
  private Cipher cipher;
  private int length;
  private String algorithm;
  private String secret;

  public SecretKeySpec getSecretKey() {
    return secretKey;
  }

  public Cipher getCipher() {
    return cipher;
  }

  /**
   * Default constructor creates key with default values.
   * @throws UnsupportedEncodingException
   * @throws NoSuchAlgorithmException
   * @throws NoSuchPaddingException
   */
  public GDPRSymmetricKey()
      throws UnsupportedEncodingException, NoSuchAlgorithmException,
      NoSuchPaddingException {
    length = OzoneConsts.GDPR_RANDOM_SECRET_LENGTH;
    algorithm = OzoneConsts.GDPR_ALGORITHM_NAME;
    secret = RandomStringUtils.randomAlphabetic(length);
    byte[] key = fixSecret(secret, length);
    this.secretKey = new SecretKeySpec(key, algorithm);
    this.cipher = Cipher.getInstance(algorithm);
  }

  /**
   * Overloaded constructor creates key with specified values.
   * @throws UnsupportedEncodingException
   * @throws NoSuchAlgorithmException
   * @throws NoSuchPaddingException
   */
  public GDPRSymmetricKey(String secret, int len, String algorithm)
      throws UnsupportedEncodingException, NoSuchAlgorithmException,
      NoSuchPaddingException {
    this.secret = secret;
    this.length = len;
    this.algorithm = algorithm;
    byte[] key = fixSecret(secret, length);
    this.secretKey = new SecretKeySpec(key, algorithm);
    this.cipher = Cipher.getInstance(algorithm);
  }

  private byte[] fixSecret(String s, int len)
      throws UnsupportedEncodingException {
    if (s.length() < len) {
      int missingLength = len - s.length();
      for (int i = 0; i < missingLength; i++) {
        s += " ";
      }
    }
    return s.substring(0, len).getBytes(OzoneConsts.GDPR_CHARSET);
  }

  public Map<String, String> getKeyDetails() {
    Map<String, String> keyDetail = new HashMap<>();
    keyDetail.put(OzoneConsts.GDPR_SECRET, this.secret);
    keyDetail.put(OzoneConsts.GDPR_ALGORITHM, this.algorithm);
    keyDetail.put(OzoneConsts.GDPR_LENGTH, String.valueOf(this.length));
    return keyDetail;
  }

}

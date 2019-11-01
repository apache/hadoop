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

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.ozone.OzoneConsts;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

/**
 * Symmetric Key structure for GDPR.
 */
public class GDPRSymmetricKey {

  private SecretKeySpec secretKey;
  private Cipher cipher;
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
   * @throws Exception
   */
  public GDPRSymmetricKey(SecureRandom secureRandom) throws Exception {
    algorithm = OzoneConsts.GDPR_ALGORITHM_NAME;
    secret = RandomStringUtils.random(
        OzoneConsts.GDPR_DEFAULT_RANDOM_SECRET_LENGTH,
        0, 0, true, true, null, secureRandom);
    this.secretKey = new SecretKeySpec(
        secret.getBytes(OzoneConsts.GDPR_CHARSET), algorithm);
    this.cipher = Cipher.getInstance(algorithm);
  }

  /**
   * Overloaded constructor creates key with specified values.
   * @throws Exception
   */
  public GDPRSymmetricKey(String secret, String algorithm) throws Exception {
    Preconditions.checkNotNull(secret, "Secret cannot be null");
    //TODO: When we add feature to allow users to customize the secret length,
    // we need to update this length check Precondition
    Preconditions.checkArgument(secret.length() == 16,
        "Secret must be exactly 16 characters");
    Preconditions.checkNotNull(algorithm, "Algorithm cannot be null");
    this.secret = secret;
    this.algorithm = algorithm;
    this.secretKey = new SecretKeySpec(
        secret.getBytes(OzoneConsts.GDPR_CHARSET), algorithm);
    this.cipher = Cipher.getInstance(algorithm);
  }

  public Map<String, String> getKeyDetails() {
    Map<String, String> keyDetail = new HashMap<>();
    keyDetail.put(OzoneConsts.GDPR_SECRET, this.secret);
    keyDetail.put(OzoneConsts.GDPR_ALGORITHM, this.algorithm);
    return keyDetail;
  }

}

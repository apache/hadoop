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

package org.apache.hadoop.fs.s3a.impl;

import org.apache.hadoop.conf.Configuration;

/**
 * This class is for storing information about key type and corresponding key
 * to be used for client side encryption.
 */
public class CSEMaterials {

  /**
   * Enum for CSE key types.
   */
  public enum CSEKeyType {
    /**
     * AWS KMS keys are used of encryption and decryption.
     */
    KMS,
    /**
     * Custom cryptographic manager class is used for encryption and decryption.
     */
    CUSTOM
  }

  /**
   * The KMS key Id.
   */
  private String kmsKeyId;

  /**
   * Custom cryptographic manager class name.
   */
  private String customKeyringClassName;

  private Configuration conf;

  /**
   * The CSE key type to use.
   */
  private CSEKeyType cseKeyType;

  /**
   * Kms key id to use.
   * @param value new value
   * @return the builder
   */
  public CSEMaterials withKmsKeyId(
      final String value) {
    kmsKeyId = value;
    return this;
  }

  /**
   * Custom cryptographic class name to use.
   * @param value cryptographic manager class name
   * @return the builder
   */
  public CSEMaterials withCustomCryptographicClassName(
      final String value) {
    customKeyringClassName = value;
    return this;
  }

  /**
   * Configuration.
   * @param value configuration
   * @return the builder
   */
  public CSEMaterials withConf(
      final Configuration value) {
    conf = value;
    return this;
  }


  /**
   * Get the Kms key id to use.
   * @return the kms key id.
   */
  public String getKmsKeyId() {
    return kmsKeyId;
  }

  public Configuration getConf() {
    return conf;
  }

  /**
   * Get the custom cryptographic class name.
   * @return custom keyring class name
   */
  public String getCustomKeyringClassName() {
    return customKeyringClassName;
  }

  /**
   * CSE key type to use.
   * @param value new value
   * @return the builder
   */
  public CSEMaterials withCSEKeyType(
      final CSEKeyType value) {
    cseKeyType = value;
    return this;
  }

  /**
   * Get the CSE key type.
   * @return CSE key type
   */
  public CSEKeyType getCseKeyType() {
    return cseKeyType;
  }
}

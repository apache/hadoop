/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.swift.auth;

/**
 * Class for Keystone authentication.
 * Used when {@link ApiKeyCredentials} is not applicable
 * THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON.
 * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 */
public class KeystoneApiKeyCredentials {

  /**
   * User access key
   */
  private String accessKey;

  /**
   * User access secret
   */
  private String secretKey;

  public KeystoneApiKeyCredentials(String accessKey, String secretKey) {
    this.accessKey = accessKey;
    this.secretKey = secretKey;
  }

  public String getAccessKey() {
    return accessKey;
  }

  public void setAccessKey(String accessKey) {
    this.accessKey = accessKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public void setSecretKey(String secretKey) {
    this.secretKey = secretKey;
  }

  @Override
  public String toString() {
    return "user " +
            "'" + accessKey + '\'' +
            " with key of length " + ((secretKey == null) ? 0 : secretKey.length());
  }
}

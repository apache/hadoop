/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;

/**
 * Encryption key info for bucket encryption key.
 */
public class BucketEncryptionKeyInfo {
  private final CryptoProtocolVersion version;
  private final CipherSuite suite;
  private final String keyName;

  public BucketEncryptionKeyInfo(
      CryptoProtocolVersion version, CipherSuite suite,
      String keyName) {
    this.version = version;
    this.suite = suite;
    this.keyName = keyName;
  }

  public String getKeyName() {
    return keyName;
  }

  public CipherSuite getSuite() {
    return suite;
  }

  public CryptoProtocolVersion getVersion() {
    return version;
  }

  /**
   * Builder for BucketEncryptionKeyInfo.
   */
  public static class Builder {
    private CryptoProtocolVersion version;
    private CipherSuite suite;
    private String keyName;

    public Builder setKeyName(String name) {
      this.keyName = name;
      return this;
    }

    public Builder setSuite(CipherSuite cs) {
      this.suite = cs;
      return this;
    }

    public Builder setVersion(CryptoProtocolVersion ver) {
      this.version = ver;
      return this;
    }

    public BucketEncryptionKeyInfo build() {
      return new BucketEncryptionKeyInfo(version, suite, keyName);
    }
  }
}
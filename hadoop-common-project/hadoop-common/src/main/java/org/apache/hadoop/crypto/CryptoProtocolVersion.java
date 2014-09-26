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
package org.apache.hadoop.crypto;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Versions of the client/server protocol used for HDFS encryption.
 */
@InterfaceAudience.Private
public enum CryptoProtocolVersion {
  UNKNOWN("Unknown", 1),
  ENCRYPTION_ZONES("Encryption zones", 2);

  private final String description;
  private final int version;
  private Integer unknownValue = null;

  private static CryptoProtocolVersion[] supported = {ENCRYPTION_ZONES};

  /**
   * @return Array of supported protocol versions.
   */
  public static CryptoProtocolVersion[] supported() {
    return supported;
  }

  CryptoProtocolVersion(String description, int version) {
    this.description = description;
    this.version = version;
  }

  /**
   * Returns if a given protocol version is supported.
   *
   * @param version version number
   * @return true if the version is supported, else false
   */
  public static boolean supports(CryptoProtocolVersion version) {
    if (version.getVersion() == UNKNOWN.getVersion()) {
      return false;
    }
    for (CryptoProtocolVersion v : CryptoProtocolVersion.values()) {
      if (v.getVersion() == version.getVersion()) {
        return true;
      }
    }
    return false;
  }

  public void setUnknownValue(int unknown) {
    this.unknownValue = unknown;
  }

  public int getUnknownValue() {
    return unknownValue;
  }

  public String getDescription() {
    return description;
  }

  public int getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return "CryptoProtocolVersion{" +
        "description='" + description + '\'' +
        ", version=" + version +
        ", unknownValue=" + unknownValue +
        '}';
  }
}

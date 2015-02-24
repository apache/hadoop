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
 * Defines properties of a CipherSuite. Modeled after the ciphers in
 * {@link javax.crypto.Cipher}.
 */
@InterfaceAudience.Private
public enum CipherSuite {
  UNKNOWN("Unknown", 0),
  AES_CTR_NOPADDING("AES/CTR/NoPadding", 16);

  private final String name;
  private final int algoBlockSize;

  private Integer unknownValue = null;

  CipherSuite(String name, int algoBlockSize) {
    this.name = name;
    this.algoBlockSize = algoBlockSize;
  }

  public void setUnknownValue(int unknown) {
    this.unknownValue = unknown;
  }

  public int getUnknownValue() {
    return unknownValue;
  }

  /**
   * @return name of cipher suite, as in {@link javax.crypto.Cipher}
   */
  public String getName() {
    return name;
  }

  /**
   * @return size of an algorithm block in bytes
   */
  public int getAlgorithmBlockSize() {
    return algoBlockSize;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("{");
    builder.append("name: " + name);
    builder.append(", algorithmBlockSize: " + algoBlockSize);
    if (unknownValue != null) {
      builder.append(", unknownValue: " + unknownValue);
    }
    builder.append("}");
    return builder.toString();
  }
  
  /**
   * Convert to CipherSuite from name, {@link #algoBlockSize} is fixed for
   * certain cipher suite, just need to compare the name.
   * @param name cipher suite name
   * @return CipherSuite cipher suite
   */
  public static CipherSuite convert(String name) {
    CipherSuite[] suites = CipherSuite.values();
    for (CipherSuite suite : suites) {
      if (suite.getName().equals(name)) {
        return suite;
      }
    }
    throw new IllegalArgumentException("Invalid cipher suite name: " + name);
  }
  
  /**
   * Returns suffix of cipher suite configuration.
   * @return String configuration suffix
   */
  public String getConfigSuffix() {
    String[] parts = name.split("/");
    StringBuilder suffix = new StringBuilder();
    for (String part : parts) {
      suffix.append(".").append(part.toLowerCase());
    }
    
    return suffix.toString();
  }
}

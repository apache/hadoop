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
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Defines properties of a CipherSuite. Modeled after the ciphers in
 * {@link javax.crypto.Cipher}.
 */
@InterfaceAudience.Private
public enum CipherSuite {
  AES_CTR_NOPADDING("AES/CTR/NoPadding", 128);

  private final String name;
  private final int blockBits;

  CipherSuite(String name, int blockBits) {
    this.name = name;
    this.blockBits = blockBits;
  }

  /**
   * @return name of cipher suite, as in {@link javax.crypto.Cipher}
   */
  public String getName() {
    return name;
  }

  /**
   * @return size of an algorithm block in bits
   */
  public int getNumberBlockBits() {
    return blockBits;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("{");
    builder.append("name: " + getName() + ", ");
    builder.append("numBlockBits: " + getNumberBlockBits());
    builder.append("}");
    return builder.toString();
  }
}

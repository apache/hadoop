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
package org.apache.hadoop.fs;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.crypto.CipherSuite;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * FileEncryptionInfo encapsulates all the encryption-related information for
 * an encrypted file.
 */
@InterfaceAudience.Private
public class FileEncryptionInfo {

  private final CipherSuite cipherSuite;
  private final byte[] key;
  private final byte[] iv;

  public FileEncryptionInfo(CipherSuite suite, byte[] key, byte[] iv) {
    checkNotNull(suite);
    checkNotNull(key);
    checkNotNull(iv);
    checkArgument(key.length == suite.getNumberBlockBits() / 8,
        "Unexpected key length");
    checkArgument(iv.length == suite.getNumberBlockBits() / 8,
        "Unexpected IV length");
    this.cipherSuite = suite;
    this.key = key;
    this.iv = iv;
  }

  /**
   * @return {@link org.apache.hadoop.crypto.CipherSuite} used to encrypt
   * the file.
   */
  public CipherSuite getCipherSuite() {
    return cipherSuite;
  }

  /**
   * @return encrypted data encryption key for the file
   */
  public byte[] getEncryptedDataEncryptionKey() {
    return key;
  }

  /**
   * @return initialization vector for the cipher used to encrypt the file
   */
  public byte[] getIV() {
    return iv;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("{");
    builder.append("cipherSuite: " + cipherSuite);
    builder.append(", key: " + Hex.encodeHexString(key));
    builder.append(", iv: " + Hex.encodeHexString(iv));
    builder.append("}");
    return builder.toString();
  }
}

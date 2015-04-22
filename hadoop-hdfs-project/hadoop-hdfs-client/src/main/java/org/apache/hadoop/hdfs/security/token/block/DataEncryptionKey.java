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
package org.apache.hadoop.hdfs.security.token.block;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A little struct class to contain all fields required to perform encryption of
 * the DataTransferProtocol.
 */
@InterfaceAudience.Private
public class DataEncryptionKey {
  public final int keyId;
  public final String blockPoolId;
  public final byte[] nonce;
  public final byte[] encryptionKey;
  public final long expiryDate;
  public final String encryptionAlgorithm;

  public DataEncryptionKey(int keyId, String blockPoolId, byte[] nonce,
      byte[] encryptionKey, long expiryDate, String encryptionAlgorithm) {
    this.keyId = keyId;
    this.blockPoolId = blockPoolId;
    this.nonce = nonce;
    this.encryptionKey = encryptionKey;
    this.expiryDate = expiryDate;
    this.encryptionAlgorithm = encryptionAlgorithm;
  }

  @Override
  public String toString() {
    return keyId + "/" + blockPoolId + "/" + nonce.length + "/" +
        encryptionKey.length;
  }
}

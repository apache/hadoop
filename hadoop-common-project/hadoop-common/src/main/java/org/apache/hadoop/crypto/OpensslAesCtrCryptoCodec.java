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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.GeneralSecurityException;

/**
 * Implement the AES-CTR crypto codec using JNI into OpenSSL.
 */
@InterfaceAudience.Private
public class OpensslAesCtrCryptoCodec extends OpensslCtrCryptoCodec {

  private static final Logger LOG =
          LoggerFactory.getLogger(OpensslAesCtrCryptoCodec.class.getName());

  public OpensslAesCtrCryptoCodec() {
    String loadingFailureReason = OpensslCipher.getLoadingFailureReason();
    if (loadingFailureReason != null) {
      throw new RuntimeException(loadingFailureReason);
    }
  }

  @Override
  public Logger getLogger() {
    return LOG;
  }

  @Override
  public CipherSuite getCipherSuite() {
    return CipherSuite.AES_CTR_NOPADDING;
  }

  @Override
  public void calculateIV(byte[] initIV, long counter, byte[] iv) {
    super.calculateIV(initIV, counter, iv,
            getCipherSuite().getAlgorithmBlockSize());
  }

  @Override
  public Encryptor createEncryptor() throws GeneralSecurityException {
    return new OpensslCtrCipher(OpensslCipher.ENCRYPT_MODE,
            getCipherSuite());
  }

  @Override
  public Decryptor createDecryptor() throws GeneralSecurityException {
    return new OpensslCtrCipher(OpensslCipher.DECRYPT_MODE,
            getCipherSuite());
  }
}

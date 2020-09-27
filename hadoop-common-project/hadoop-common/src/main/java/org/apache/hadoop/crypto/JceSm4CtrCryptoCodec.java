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
import javax.crypto.Cipher;
import java.security.GeneralSecurityException;

/**
 * Implement the SM4-CTR crypto codec using JCE provider.
 */
@InterfaceAudience.Private
public class JceSm4CtrCryptoCodec extends JceCtrCryptoCodec {

  private static final Logger LOG =
      LoggerFactory.getLogger(JceSm4CtrCryptoCodec.class.getName());

  public JceSm4CtrCryptoCodec() {
  }

  @Override
  public Logger getLogger() {
    return LOG;
  }

  @Override
  public CipherSuite getCipherSuite() {
    return CipherSuite.SM4_CTR_NOPADDING;
  }

  @Override
  public void calculateIV(byte[] initIV, long counter, byte[] iv) {
    super.calculateIV(initIV, counter, iv,
            getCipherSuite().getAlgorithmBlockSize());
  }

  @Override
  public Encryptor createEncryptor() throws GeneralSecurityException {
    return new JceCtrCipher(Cipher.ENCRYPT_MODE, getProvider(),
            getCipherSuite(), "SM4");
  }

  @Override
  public Decryptor createDecryptor() throws GeneralSecurityException {
    return new JceCtrCipher(Cipher.DECRYPT_MODE, getProvider(),
            getCipherSuite(), "SM4");
  }
}

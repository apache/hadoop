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

import java.security.GeneralSecurityException;

import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY;

/**
 * Implement the AES-CTR crypto codec using JCE provider.
 */
public class JCEAESCTRCryptoCodec extends AESCTRCryptoCodec {
  private Configuration conf;
  private String provider;

  public JCEAESCTRCryptoCodec() {
  }
  
  @Override
  public Configuration getConf() {
    return conf;
  }
  
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    provider = conf.get(HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY);
  }

  @Override
  public Encryptor getEncryptor() throws GeneralSecurityException {
    return new JCEAESCTREncryptor(provider);
  }

  @Override
  public Decryptor getDecryptor() throws GeneralSecurityException {
    return new JCEAESCTRDecryptor(provider);
  }
}

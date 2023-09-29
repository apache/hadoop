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

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.BeforeClass;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.
    HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY;

public class TestCryptoStreamsWithJceSm4CtrCryptoCodec extends
    TestCryptoStreams {

  @BeforeClass
  public static void init() throws Exception {
    Configuration conf = new Configuration();
    conf.set(HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY, "SM4/CTR/NoPadding");
    conf.set(HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY,
            BouncyCastleProvider.PROVIDER_NAME);
    conf.set(
         CommonConfigurationKeysPublic.
             HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_SM4_CTR_NOPADDING_KEY,
         JceSm4CtrCryptoCodec.class.getName());
    codec = CryptoCodec.getInstance(conf);
    assertThat(JceSm4CtrCryptoCodec.class.getCanonicalName()).
        isEqualTo(codec.getClass().getCanonicalName());
  }
}

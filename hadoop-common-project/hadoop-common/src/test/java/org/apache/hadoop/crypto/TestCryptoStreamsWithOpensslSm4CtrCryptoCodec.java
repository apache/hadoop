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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.random.OsSecureRandom;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.
    HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.
    HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_SM4_CTR_NOPADDING_KEY;

public class TestCryptoStreamsWithOpensslSm4CtrCryptoCodec
    extends TestCryptoStreams {

  @BeforeClass
  public static void init() throws Exception {
    GenericTestUtils.assumeInNativeProfile();
    Configuration conf = new Configuration();
    conf.set(HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY, "SM4/CTR/NoPadding");
    conf.set(HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_SM4_CTR_NOPADDING_KEY,
            OpensslSm4CtrCryptoCodec.class.getName());
    codec = CryptoCodec.getInstance(conf);
    assertNotNull("Unable to instantiate codec " +
            OpensslSm4CtrCryptoCodec.class.getName() + ", is the required "
            + "version of OpenSSL installed?", codec);
    assertEquals(OpensslSm4CtrCryptoCodec.class.getCanonicalName(),
            codec.getClass().getCanonicalName());
  }

  @Test
  public void testCodecClosesRandom() throws Exception {
    GenericTestUtils.assumeInNativeProfile();
    Configuration conf = new Configuration();
    conf.set(HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY, "SM4/CTR/NoPadding");
    conf.set(HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_SM4_CTR_NOPADDING_KEY,
            OpensslSm4CtrCryptoCodec.class.getName());
    conf.set(
            CommonConfigurationKeysPublic.
                    HADOOP_SECURITY_SECURE_RANDOM_IMPL_KEY,
            OsSecureRandom.class.getName());
    CryptoCodec codecWithRandom = CryptoCodec.getInstance(conf);
    assertNotNull("Unable to instantiate codec " +
            OpensslSm4CtrCryptoCodec.class.getName() + ", is the required "
            + "version of OpenSSL installed?", codecWithRandom);
    OsSecureRandom random = (OsSecureRandom)
            ((OpensslSm4CtrCryptoCodec) codecWithRandom).getRandom();
    // trigger the OsSecureRandom to create an internal FileInputStream
    random.nextBytes(new byte[10]);
    assertFalse(random.isClosed());
    // verify closing the codec closes the codec's random's stream.
    codecWithRandom.close();
    assertTrue(random.isClosed());
  }
}

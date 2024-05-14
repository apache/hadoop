/*
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
import org.apache.hadoop.test.GenericTestUtils;
import org.assertj.core.api.Assertions;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.event.Level;

import java.security.Provider;
import java.security.Security;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_AUTO_ADD_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_AUTO_ADD_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY;

/** Test {@link CryptoUtils}. */
public class TestCryptoUtils {
  static {
    GenericTestUtils.setLogLevel(CryptoUtils.LOG, Level.TRACE);
  }

  @Test(timeout = 1_000)
  public void testProviderName() {
    Assert.assertEquals(CryptoUtils.BOUNCY_CASTLE_PROVIDER_NAME, BouncyCastleProvider.PROVIDER_NAME);
  }

  static void assertRemoveProvider() {
    Security.removeProvider(BouncyCastleProvider.PROVIDER_NAME);
    Assert.assertNull(Security.getProvider(BouncyCastleProvider.PROVIDER_NAME));
  }

  static void assertSetProvider(Configuration conf) {
    conf.set(HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY, CryptoUtils.BOUNCY_CASTLE_PROVIDER_NAME);
    final String providerFromConf = CryptoUtils.getJceProvider(conf);
    Assert.assertEquals(CryptoUtils.BOUNCY_CASTLE_PROVIDER_NAME, providerFromConf);
  }

  @Test(timeout = 5_000)
  public void testAutoAddDisabled() {
    assertRemoveProvider();

    final Configuration conf = new Configuration();
    conf.setBoolean(HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_AUTO_ADD_KEY, false);

    assertSetProvider(conf);

    Assert.assertNull(Security.getProvider(BouncyCastleProvider.PROVIDER_NAME));
  }

  @Test(timeout = 5_000)
  public void testAutoAddEnabled() {
    assertRemoveProvider();

    final Configuration conf = new Configuration();
    Assertions.assertThat(conf.get(HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_AUTO_ADD_KEY))
        .describedAs("conf: " + HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_AUTO_ADD_KEY)
        .isEqualToIgnoringCase("true");
    Assert.assertTrue(HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_AUTO_ADD_DEFAULT);

    conf.set(HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY, CryptoUtils.BOUNCY_CASTLE_PROVIDER_NAME);
    final String providerFromConf = CryptoUtils.getJceProvider(conf);
    Assert.assertEquals(CryptoUtils.BOUNCY_CASTLE_PROVIDER_NAME, providerFromConf);

    final Provider provider = Security.getProvider(BouncyCastleProvider.PROVIDER_NAME);
    Assertions.assertThat(provider)
        .isInstanceOf(BouncyCastleProvider.class);

    assertRemoveProvider();
  }
}

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
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.event.Level;

import java.security.Provider;
import java.security.Security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_AUTO_ADD_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_AUTO_ADD_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY;

/** Test {@link CryptoUtils}. */
public class TestCryptoUtils {
  static {
    GenericTestUtils.setLogLevel(CryptoUtils.LOG, Level.TRACE);
  }

  @Test
  @Timeout(value = 1)
  public void testProviderName() {
    assertEquals(CryptoUtils.BOUNCY_CASTLE_PROVIDER_NAME, BouncyCastleProvider.PROVIDER_NAME);
  }

  static void assertRemoveProvider() {
    Security.removeProvider(BouncyCastleProvider.PROVIDER_NAME);
    assertNull(Security.getProvider(BouncyCastleProvider.PROVIDER_NAME));
  }

  static void assertSetProvider(Configuration conf) {
    conf.set(HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY, CryptoUtils.BOUNCY_CASTLE_PROVIDER_NAME);
    final String providerFromConf = CryptoUtils.getJceProvider(conf);
    assertEquals(CryptoUtils.BOUNCY_CASTLE_PROVIDER_NAME, providerFromConf);
  }

  @Test
  @Timeout(value = 5)
  public void testAutoAddDisabled() {
    assertRemoveProvider();

    final Configuration conf = new Configuration();
    conf.setBoolean(HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_AUTO_ADD_KEY, false);

    assertSetProvider(conf);

    assertNull(Security.getProvider(BouncyCastleProvider.PROVIDER_NAME));
  }

  @Test
  @Timeout(value = 5)
  public void testAutoAddEnabled() {
    assertRemoveProvider();

    final Configuration conf = new Configuration();
    assertThat(conf.get(HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_AUTO_ADD_KEY))
        .describedAs("conf: " + HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_AUTO_ADD_KEY)
        .isEqualToIgnoringCase("true");
    assertTrue(HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_AUTO_ADD_DEFAULT);

    conf.set(HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY, CryptoUtils.BOUNCY_CASTLE_PROVIDER_NAME);
    final String providerFromConf = CryptoUtils.getJceProvider(conf);
    assertEquals(CryptoUtils.BOUNCY_CASTLE_PROVIDER_NAME, providerFromConf);

    final Provider provider = Security.getProvider(BouncyCastleProvider.PROVIDER_NAME);
    assertThat(provider).isInstanceOf(BouncyCastleProvider.class);

    assertRemoveProvider();
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.security.token;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestSecurityManagerConfig {

  private final String defaultAlgorithm =
      CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_GENERATOR_ALGORITHM_DEFAULT;
  private final int defaultLength =
      CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_LENGTH_DEFAULT;
  private final String strongAlgorithm = "HmacSHA256";
  private final int strongLength = 256;

  @Test
  public void testDefaults() {
    assertEquals(defaultAlgorithm, SecretManagerConfig.getSelectedAlgorithm());
    assertEquals(defaultLength, SecretManagerConfig.getSelectedLength());
  }

  @Test
  public void testUpdateByConfig() {
    SecretManagerConfig.update(createConfiguration(strongAlgorithm, strongLength));
    assertEquals(strongAlgorithm, SecretManagerConfig.getSelectedAlgorithm());
    assertEquals(strongLength, SecretManagerConfig.getSelectedLength());
  }

  @Test
  public void testMacCreation() {
    SecretManagerConfig.update(createConfiguration(strongAlgorithm, strongLength));
    Mac mac = SecretManagerConfig.createMac();
    assertEquals(strongAlgorithm, mac.getAlgorithm());
  }

  @Test
  public void testMacCreationUnknownAlgorithm() {
    SecretManagerConfig.update(
        createConfiguration("testMacCreationUnknownAlgorithm_NO_ALG", defaultLength));
    assertThrows(IllegalArgumentException.class, SecretManagerConfig::createMac);
  }

  @Test
  public void testKeygenCreation() {
    SecretManagerConfig.update(createConfiguration(strongAlgorithm, strongLength));
    KeyGenerator keyGenerator = SecretManagerConfig.createKeyGenerator();
    assertEquals(strongAlgorithm, keyGenerator.getAlgorithm());
  }

  @Test
  public void testKeygenCreationUnknownAlgorithm() {
    SecretManagerConfig.update(
        createConfiguration("testKeygenCreationUnknownAlgorithm_NO_ALG", defaultLength));
    assertThrows(IllegalArgumentException.class, SecretManagerConfig::createKeyGenerator);
  }

  @Test
  public void testConfigUpdateAfterKeygenCreation() {
    SecretManagerConfig.update(createConfiguration(strongAlgorithm, strongLength));
    KeyGenerator keyGenerator = SecretManagerConfig.createKeyGenerator();
    SecretManagerConfig.update(createConfiguration(defaultAlgorithm, defaultLength));
    assertEquals(strongAlgorithm, keyGenerator.getAlgorithm());
  }

  @AfterEach
  public void tearDown() {
    SecretManagerConfig.update(createConfiguration(defaultAlgorithm, defaultLength));
  }

  private Configuration createConfiguration(String algorithm, int length) {
    Configuration conf = new Configuration();
    conf.set(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_GENERATOR_ALGORITHM_KEY,
        algorithm);
    conf.setInt(CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_LENGTH_KEY,
        length);
    return conf;
  }
}

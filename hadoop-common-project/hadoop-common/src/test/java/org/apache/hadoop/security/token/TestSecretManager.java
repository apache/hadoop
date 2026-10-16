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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestSecretManager {

  private final String defaultAlgorithm =
      CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_GENERATOR_ALGORITHM_DEFAULT;
  private final int defaultLength =
      CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_LENGTH_DEFAULT;
  private final String strongAlgorithm = "HmacSHA256";
  private final int strongLength = 256;
  private SecretManager<TokenIdentifier> secretManager;

  @Test
  public void testDefaults() {
    assertKey(secretManager.generateSecret(), defaultAlgorithm, defaultLength);
  }

  @Test
  public void testUpdate() {
    SecretManager.update(createConfiguration(strongAlgorithm, strongLength));
    assertKey(secretManager.generateSecret(), strongAlgorithm, strongLength);
  }

  @Test
  public void testUnknownAlgorithm() {
    SecretManager.update(createConfiguration("testUnknownAlgorithm_NO_ALG", strongLength));
    assertThrows(IllegalArgumentException.class, secretManager::generateSecret);
  }

  @Test
  public void testUpdateAfterInitialisation() {
    SecretKey oldSecretKey = secretManager.generateSecret();
    SecretManager.update(createConfiguration(strongAlgorithm, strongLength));
    SecretKey newSecretKey = secretManager.generateSecret();
    assertKey(oldSecretKey, defaultAlgorithm, defaultLength);
    assertKey(newSecretKey, defaultAlgorithm, defaultLength);
  }

  @BeforeEach
  public void setUp() {
    secretManager = new SecretManager<TokenIdentifier>() {
      @Override
      protected byte[] createPassword(TokenIdentifier identifier) {
        return new byte[0];
      }

      @Override
      public byte[] retrievePassword(TokenIdentifier identifier) throws InvalidToken {
        return new byte[0];
      }

      @Override
      public TokenIdentifier createIdentifier() {
        return null;
      }
    };
  }

  @AfterEach
  public void tearDown() {
    SecretManager.update(createConfiguration(defaultAlgorithm, defaultLength));
  }

  private void assertKey(SecretKey secretKey, String algorithm, int length) {
    assertEquals(algorithm, secretKey.getAlgorithm(),
        "Algorithm of created key is not as expected.");
    assertEquals(length, secretKey.getEncoded().length * 8,
        "Length of created key is not as expected.");
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

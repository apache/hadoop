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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import java.security.NoSuchAlgorithmException;

/**
 * Provides configuration and utility methods for managing cryptographic key generation
 * and message authentication code (MAC) generation using specified algorithms and key lengths.
 * <p>
 * This class supports static access to the selected cryptographic algorithm and key length,
 * and provides methods to create configured {@link javax.crypto.KeyGenerator} and {@link javax.crypto.Mac} instances.
 * The configuration is initialized statically from a provided {@link Configuration} object.
 * <p>
 * The {@link SecretManager} has some static method, so static configuration is required
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class SecretManagerConfig {
  private static final Logger LOG = LoggerFactory.getLogger(SecretManagerConfig.class);
  private static String SELECTED_ALGORITHM;
  private static int SELECTED_LENGTH;
  private static boolean INITIALIZED;

  static {
    update(new Configuration());
  }

  /**
   * Updates the selected cryptographic algorithm and key length using the provided
   * Hadoop {@link Configuration}. This method reads the values for
   * {@code HADOOP_SECURITY_SECRET_MANAGER_KEY_GENERATOR_ALGORITHM_KEY} and
   * {@code HADOOP_SECURITY_SECRET_MANAGER_KEY_LENGTH_KEY}, or uses default values if not set.
   *
   * @param conf the configuration object containing cryptographic settings
   */
  public static synchronized void update(Configuration conf) {
    if (INITIALIZED) {
      LOG.warn(
          "Keygen or Mac was already initialized with older configuration, those will not be updated");
    }
    SELECTED_ALGORITHM = conf.get(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_GENERATOR_ALGORITHM_KEY,
        CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_GENERATOR_ALGORITHM_DEFAULT);
    LOG.debug("Selected hash algorithm: {}", SELECTED_ALGORITHM);
    SELECTED_LENGTH =
        conf.getInt(CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_LENGTH_KEY,
            CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_LENGTH_DEFAULT);
    LOG.debug("Selected hash key length: {}", SELECTED_LENGTH);
  }

  /**
   * Returns the currently selected cryptographic algorithm.
   *
   * @return the name of the selected algorithm
   */
  public static synchronized String getSelectedAlgorithm() {
    return SELECTED_ALGORITHM;
  }

  /**
   * Returns the currently selected key length in bits.
   *
   * @return the selected key length
   */
  public static synchronized int getSelectedLength() {
    return SELECTED_LENGTH;
  }

  /**
   * Creates a new {@link KeyGenerator} instance configured with the currently selected
   * algorithm and key length.
   *
   * @return a new {@code KeyGenerator} instance
   * @throws IllegalArgumentException if the specified algorithm is not available
   */
  public static synchronized KeyGenerator createKeyGenerator() {
    LOG.debug("Creating key generator instance {}, {}", SELECTED_ALGORITHM, SELECTED_LENGTH);
    INITIALIZED = true;
    try {
      KeyGenerator keyGen = KeyGenerator.getInstance(SELECTED_ALGORITHM);
      keyGen.init(SELECTED_LENGTH);
      return keyGen;
    } catch (NoSuchAlgorithmException nsa) {
      throw new IllegalArgumentException("Can't find " + SELECTED_ALGORITHM, nsa);
    }
  }

  /**
   * Creates a new {@link Mac} instance using the currently selected algorithm.
   *
   * @return a new {@code Mac} instance
   * @throws IllegalArgumentException if the specified algorithm is not available
   */
  public static synchronized Mac createMac() {
    LOG.debug("Creating mac instance {}", SELECTED_ALGORITHM);
    INITIALIZED = true;
    try {
      return Mac.getInstance(SELECTED_ALGORITHM);
    } catch (NoSuchAlgorithmException nsa) {
      throw new IllegalArgumentException("Can't find " + SELECTED_ALGORITHM, nsa);
    }
  }
}

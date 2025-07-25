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
import java.util.Map;
import java.util.WeakHashMap;

/**
 * Provides configuration and utility methods for managing cryptographic key generation
 * and message authentication code (MAC) generation using specified algorithms and key lengths.
 * <p>
 * This class supports static access to the selected cryptographic algorithm and key length,
 * and provides methods to create configured {@link javax.crypto.KeyGenerator}
 * and {@link javax.crypto.Mac} instances.
 * The configuration is initialized statically from a provided {@link Configuration} object.
 * <p>
 * The {@link SecretManager} has some static method, so static configuration is required
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class SecretManagerConfig {
  private static final Logger LOG = LoggerFactory.getLogger(SecretManagerConfig.class);
  private static String selectedAlgorithm;
  private static int selectedLength;

  private static final Map<Thread, KeyGenerator> KEYGENS = new WeakHashMap<>();
  private static final Map<Thread, Mac> MACS = new WeakHashMap<>();

  static {
    update(new Configuration());
  }

  private SecretManagerConfig() {
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
    if (!KEYGENS.isEmpty()) {
      LOG.warn("Keygen was already initialized with older config, those will not be updated." +
          "Hint: If you turn on debug log you can see when it happened. Keygens: {}", KEYGENS);
    }
    if (!MACS.isEmpty()) {
      LOG.warn("Mac was already initialized with older config, those will not be updated." +
          "Hint: If you turn on debug log you can see when it happened. Macs: {}", MACS);
    }
    selectedAlgorithm = conf.get(
      CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_GENERATOR_ALGORITHM_KEY,
      CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_GENERATOR_ALGORITHM_DEFAULT);
    LOG.debug("Selected hash algorithm: {}", selectedAlgorithm);
    selectedLength = conf.getInt(
      CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_LENGTH_KEY,
      CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_LENGTH_DEFAULT);
    LOG.debug("Selected hash key length: {}", selectedLength);
  }

  /**
   * Returns the currently selected cryptographic algorithm.
   *
   * @return the name of the selected algorithm
   */
  public static synchronized String getSelectedAlgorithm() {
    return selectedAlgorithm;
  }

  /**
   * Returns the currently selected key length in bits.
   *
   * @return the selected key length
   */
  public static synchronized int getSelectedLength() {
    return selectedLength;
  }

  /**
   * Creates a new {@link KeyGenerator} instance configured with the currently selected
   * algorithm and key length.
   *
   * @return a new {@code KeyGenerator} instance
   * @throws IllegalArgumentException if the specified algorithm is not available
   */
  public static synchronized KeyGenerator createKeyGenerator() {
    LOG.debug("Creating key generator instance {} - {} bit with thread {}",
        selectedAlgorithm, selectedLength, Thread.currentThread());
    try {
      KeyGenerator keyGen = KeyGenerator.getInstance(selectedAlgorithm);
      keyGen.init(selectedLength);
      KEYGENS.put(Thread.currentThread(), keyGen);
      return keyGen;
    } catch (NoSuchAlgorithmException nsa) {
      throw new IllegalArgumentException("Can't find " + selectedAlgorithm, nsa);
    }
  }

  /**
   * Creates a new {@link Mac} instance using the currently selected algorithm.
   *
   * @return a new {@code Mac} instance
   * @throws IllegalArgumentException if the specified algorithm is not available
   */
  public static synchronized Mac createMac() {
    LOG.debug("Creating mac instance {} with thread {}", selectedAlgorithm, Thread.currentThread());
    try {
      Mac mac = Mac.getInstance(selectedAlgorithm);
      MACS.put(Thread.currentThread(), mac);
      return mac;
    } catch (NoSuchAlgorithmException nsa) {
      throw new IllegalArgumentException("Can't find " + selectedAlgorithm, nsa);
    }
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.hdds.security.x509.keys;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;

/**
 * A class to generate Key Pair for use with Certificates.
 */
public class HDDSKeyGenerator {
  private static final Logger LOG =
      LoggerFactory.getLogger(HDDSKeyGenerator.class);
  private final SecurityConfig securityConfig;

  /**
   * Constructor for HDDSKeyGenerator.
   *
   * @param configuration - config
   */
  public HDDSKeyGenerator(Configuration configuration) {
    this.securityConfig = new SecurityConfig(configuration);
  }

  /**
   * Constructor that takes a SecurityConfig as the Argument.
   *
   * @param config - SecurityConfig
   */
  public HDDSKeyGenerator(SecurityConfig config) {
    this.securityConfig = config;
  }

  /**
   * Returns the Security config used for this object.
   *
   * @return SecurityConfig
   */
  public SecurityConfig getSecurityConfig() {
    return securityConfig;
  }

  /**
   * Use Config to generate key.
   *
   * @return KeyPair
   * @throws NoSuchProviderException  - On Error, due to missing Java
   *                                  dependencies.
   * @throws NoSuchAlgorithmException - On Error,  due to missing Java
   *                                  dependencies.
   */
  public KeyPair generateKey() throws NoSuchProviderException,
      NoSuchAlgorithmException {
    return generateKey(securityConfig.getSize(),
        securityConfig.getKeyAlgo(), securityConfig.getProvider());
  }

  /**
   * Specify the size -- all other parameters are used from config.
   *
   * @param size - int, valid key sizes.
   * @return KeyPair
   * @throws NoSuchProviderException  - On Error, due to missing Java
   *                                  dependencies.
   * @throws NoSuchAlgorithmException - On Error,  due to missing Java
   *                                  dependencies.
   */
  public KeyPair generateKey(int size) throws
      NoSuchProviderException, NoSuchAlgorithmException {
    return generateKey(size,
        securityConfig.getKeyAlgo(), securityConfig.getProvider());
  }

  /**
   * Custom Key Generation, all values are user provided.
   *
   * @param size - Key Size
   * @param algorithm - Algorithm to use
   * @param provider - Security provider.
   * @return KeyPair.
   * @throws NoSuchProviderException  - On Error, due to missing Java
   *                                  dependencies.
   * @throws NoSuchAlgorithmException - On Error,  due to missing Java
   *                                  dependencies.
   */
  public KeyPair generateKey(int size, String algorithm, String provider)
      throws NoSuchProviderException, NoSuchAlgorithmException {
    LOG.debug("Generating key pair using size:{}, Algorithm:{}, Provider:{}",
        size, algorithm, provider);
    KeyPairGenerator generator = KeyPairGenerator
        .getInstance(algorithm, provider);
    generator.initialize(size);
    return generator.generateKeyPair();
  }
}

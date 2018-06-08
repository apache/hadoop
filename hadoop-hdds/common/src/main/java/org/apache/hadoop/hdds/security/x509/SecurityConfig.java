/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.apache.hadoop.hdds.security.x509;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Provider;
import java.security.Security;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DEFAULT_KEY_LEN;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DEFAULT_KEY_ALGORITHM;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DEFAULT_SECURITY_PROVIDER;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_KEY_ALGORITHM;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_KEY_DIR_NAME;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_KEY_DIR_NAME_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_KEY_LEN;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_METADATA_DIR_NAME;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PRIVATE_KEY_FILE_NAME;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PRIVATE_KEY_FILE_NAME_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PUBLIC_KEY_FILE_NAME;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PUBLIC_KEY_FILE_NAME_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECURITY_PROVIDER;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;

/**
 * A class that deals with all Security related configs in HDDDS.
 * It is easier to have all Java code related to config in a single place.
 */
public class SecurityConfig {
  private static final Logger LOG =
      LoggerFactory.getLogger(SecurityConfig.class);
  private static volatile Provider provider;
  private final Configuration configuration;
  private final int size;
  private final String algo;
  private final String providerString;
  private final String metadatDir;
  private final String keyDir;
  private final String privateKeyName;
  private final String publicKeyName;

  /**
   * Constructs a HDDSKeyGenerator.
   *
   * @param configuration - HDDS Configuration
   */
  public SecurityConfig(Configuration configuration) {
    Preconditions.checkNotNull(configuration, "Configuration cannot be null");
    this.configuration = configuration;
    this.size = this.configuration.getInt(HDDS_KEY_LEN, HDDS_DEFAULT_KEY_LEN);
    this.algo = this.configuration.get(HDDS_KEY_ALGORITHM,
        HDDS_DEFAULT_KEY_ALGORITHM);
    this.providerString = this.configuration.get(HDDS_SECURITY_PROVIDER,
            HDDS_DEFAULT_SECURITY_PROVIDER);

    // Please Note: To make it easy for our customers we will attempt to read
    // HDDS metadata dir and if that is not set, we will use Ozone directory.
    // TODO: We might want to fix this later.
    this.metadatDir = this.configuration.get(HDDS_METADATA_DIR_NAME,
        configuration.get(OZONE_METADATA_DIRS));

    Preconditions.checkNotNull(this.metadatDir, "Metadata directory can't be"
        + " null. Please check configs.");
    this.keyDir = this.configuration.get(HDDS_KEY_DIR_NAME,
        HDDS_KEY_DIR_NAME_DEFAULT);
    this.privateKeyName = this.configuration.get(HDDS_PRIVATE_KEY_FILE_NAME,
        HDDS_PRIVATE_KEY_FILE_NAME_DEFAULT);
    this.publicKeyName =  this.configuration.get(HDDS_PUBLIC_KEY_FILE_NAME,
        HDDS_PUBLIC_KEY_FILE_NAME_DEFAULT);

    // First Startup -- if the provider is null, check for the provider.
    if (SecurityConfig.provider == null) {
      synchronized (SecurityConfig.class) {
        provider = Security.getProvider(this.providerString);
        if (SecurityConfig.provider == null) {
          // Provider not found, let us try to Dynamically initialize the
          // provider.
          provider = initSecurityProvider(this.providerString);
        }
      }
    }
  }

  /**
   * Returns the Provider name.
   * @return String Provider name.
   */
  public String getProviderString() {
    return providerString;
  }

  /**
   * Returns the public key file name.
   * @return String, File name used for public keys.
   */
  public String getPublicKeyName() {
    return publicKeyName;
  }

  /**
   * Returns the private key file name.
   * @return String, File name used for private keys.
   */
  public String getPrivateKeyName() {
    return privateKeyName;
  }

  /**
   * Returns the File path to where keys are stored.
   * @return  String Key location.
   */
  public Path getKeyLocation() {
    return Paths.get(metadatDir, keyDir);
  }

  /**
   * Gets the Key Size.
   *
   * @return key size.
   */
  public int getSize() {
    return size;
  }

  /**
   * Gets provider.
   *
   * @return String Provider name.
   */
  public String getProvider() {
    return providerString;
  }

  /**
   * Returns the Key generation Algorithm used.
   *
   * @return String Algo.
   */
  public String getAlgo() {
    return algo;
  }

  /**
   * Returns the Configuration used for initializing this SecurityConfig.
   * @return  Configuration
   */
  public Configuration getConfiguration() {
    return configuration;
  }


  /**
   * Adds a security provider dynamically if it is not loaded already.
   *
   * @param providerName - name of the provider.
   */
  private Provider initSecurityProvider(String providerName) {
    switch (providerName) {
    case "BC":
      Security.addProvider(new BouncyCastleProvider());
      return Security.getProvider(providerName);
    default:
      LOG.error("Security Provider:{} is unknown", provider);
      throw new SecurityException("Unknown security provider:" + provider);
    }
  }
}

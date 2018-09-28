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
import java.time.Duration;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DEFAULT_KEY_ALGORITHM;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DEFAULT_KEY_LEN;
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
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_MAX_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_MAX_DURATION_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_SIGNATURE_ALGO;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_SIGNATURE_ALGO_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;

/**
 * A class that deals with all Security related configs in HDDS.
 *
 * This class allows security configs to be read and used consistently across
 * all of security related code base.
 */
public class SecurityConfig {
  private static final Logger LOG =
      LoggerFactory.getLogger(SecurityConfig.class);
  private static volatile Provider provider;
  private final Configuration configuration;
  private final int size;
  private final String keyAlgo;
  private final String providerString;
  private final String metadatDir;
  private final String keyDir;
  private final String privateKeyFileName;
  private final String publicKeyFileName;
  private final Duration certDuration;
  private final String x509SignatureAlgo;

  /**
   * Constructs a SecurityConfig.
   *
   * @param configuration - HDDS Configuration
   */
  public SecurityConfig(Configuration configuration) {
    Preconditions.checkNotNull(configuration, "Configuration cannot be null");
    this.configuration = configuration;
    this.size = this.configuration.getInt(HDDS_KEY_LEN, HDDS_DEFAULT_KEY_LEN);
    this.keyAlgo = this.configuration.get(HDDS_KEY_ALGORITHM,
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
    this.privateKeyFileName = this.configuration.get(HDDS_PRIVATE_KEY_FILE_NAME,
        HDDS_PRIVATE_KEY_FILE_NAME_DEFAULT);
    this.publicKeyFileName = this.configuration.get(HDDS_PUBLIC_KEY_FILE_NAME,
        HDDS_PUBLIC_KEY_FILE_NAME_DEFAULT);

    String durationString = this.configuration.get(HDDS_X509_MAX_DURATION,
        HDDS_X509_MAX_DURATION_DEFAULT);
    this.certDuration = Duration.parse(durationString);
    this.x509SignatureAlgo = this.configuration.get(HDDS_X509_SIGNATURE_ALGO,
        HDDS_X509_SIGNATURE_ALGO_DEFAULT);

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
   * Returns the public key file name, This is used for storing the public
   * keys on disk.
   *
   * @return String, File name used for public keys.
   */
  public String getPublicKeyFileName() {
    return publicKeyFileName;
  }

  /**
   * Returns the private key file name.This is used for storing the private
   * keys on disk.
   *
   * @return String, File name used for private keys.
   */
  public String getPrivateKeyFileName() {
    return privateKeyFileName;
  }

  /**
   * Returns the File path to where keys are stored.
   *
   * @return String Key location.
   */
  public Path getKeyLocation() {
    return Paths.get(metadatDir, keyDir);
  }

  /**
   * Gets the Key Size, The default key size is 2048, since the default
   * algorithm used is RSA. User can change this by setting the "hdds.key
   * .len" in configuration.
   *
   * @return key size.
   */
  public int getSize() {
    return size;
  }

  /**
   * Returns the Provider name. SCM defaults to using Bouncy Castle and will
   * return "BC".
   *
   * @return String Provider name.
   */
  public String getProvider() {
    return providerString;
  }

  /**
   * Returns the Key generation Algorithm used.  User can change this by
   * setting the "hdds.key.algo" in configuration.
   *
   * @return String Algo.
   */
  public String getKeyAlgo() {
    return keyAlgo;
  }

  /**
   * Returns the X.509 Signature Algorithm used. This can be changed by setting
   * "hdds.x509.signature.algorithm" to the new name. The default algorithm
   * is SHA256withRSA.
   *
   * @return String
   */
  public String getSignatureAlgo() {
    return x509SignatureAlgo;
  }

  /**
   * Returns the Configuration used for initializing this SecurityConfig.
   *
   * @return Configuration
   */
  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * Returns the maximum length a certificate can be valid in SCM. The
   * default value is 5 years. This can be changed by setting
   * "hdds.x509.max.duration" in configuration. The formats accepted are
   * based on the ISO-8601 duration format PnDTnHnMn.nS
   *
   * Default value is 5 years and written as P1865D.
   *
   * @return Duration.
   */
  public Duration getMaxCertificateDuration() {
    return this.certDuration;
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

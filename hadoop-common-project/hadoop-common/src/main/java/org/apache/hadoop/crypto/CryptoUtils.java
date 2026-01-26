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

import java.security.Provider;
import java.security.Security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.store.LogExactlyOnce;

/** Utility methods for the crypto related features. */
@InterfaceAudience.Private
public final class CryptoUtils {
  static final Logger LOG = LoggerFactory.getLogger(CryptoUtils.class);
  private static final LogExactlyOnce LOG_FAILED_TO_LOAD_CLASS = new LogExactlyOnce(LOG);
  private static final LogExactlyOnce LOG_FAILED_TO_ADD_PROVIDER = new LogExactlyOnce(LOG);

  private static final String BOUNCY_CASTLE_PROVIDER_CLASS
      = "org.bouncycastle.jce.provider.BouncyCastleProvider";
  static final String BOUNCY_CASTLE_PROVIDER_NAME = "BC";

  /**
   * Get the security provider value specified in
   * {@link CommonConfigurationKeysPublic#HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY}
   * from the given conf.
   *
   * @param conf the configuration
   * @return the configured provider, if there is any; otherwise, return an empty string.
   */
  public static String getJceProvider(Configuration conf) {
    final String provider = conf.getTrimmed(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY, "");
    final boolean autoAdd = conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_AUTO_ADD_KEY,
        CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_AUTO_ADD_DEFAULT);

    // For backward compatible, auto-add BOUNCY_CASTLE_PROVIDER_CLASS when the provider is "BC".
    if (autoAdd && BOUNCY_CASTLE_PROVIDER_NAME.equals(provider)) {
      try {
        // Use reflection in order to avoid statically loading the class.
        final Class<?> clazz = Class.forName(BOUNCY_CASTLE_PROVIDER_CLASS);
        Security.addProvider((Provider) clazz.getConstructor().newInstance());
        LOG.debug("Successfully added security provider {}", provider);
        if (LOG.isTraceEnabled()) {
          LOG.trace("Trace", new Throwable());
        }
      } catch (ClassNotFoundException e) {
        LOG_FAILED_TO_LOAD_CLASS.warn("Failed to load " + BOUNCY_CASTLE_PROVIDER_CLASS, e);
      } catch (Exception e) {
        LOG_FAILED_TO_ADD_PROVIDER.warn("Failed to add security provider for {}", provider, e);
      }
    }
    return provider;
  }

  private CryptoUtils() { }
}

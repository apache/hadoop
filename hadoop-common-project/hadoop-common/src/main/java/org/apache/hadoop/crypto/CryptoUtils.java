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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.store.LogExactlyOnce;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.security.Provider;
import java.security.Security;

/** Utility methods for the crypto related features. */
@InterfaceAudience.Private
public class CryptoUtils {
  static final Logger LOG = LoggerFactory.getLogger(CryptoUtils.class);
  private static final LogExactlyOnce LOG_FAILED_TO_LOAD_CLASS = new LogExactlyOnce(LOG);
  private static final LogExactlyOnce LOG_FAILED_TO_GET_FIELD = new LogExactlyOnce(LOG);
  private static final LogExactlyOnce LOG_FAILED_TO_ADD_PROVIDER = new LogExactlyOnce(LOG);

  private static final String BOUNCY_CASTLE_PROVIDER_CLASS
      = "org.bouncycastle.jce.provider.BouncyCastleProvider";
  private static final String PROVIDER_NAME_FIELD = "PROVIDER_NAME";

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

    // For backward compatible, auto-add BOUNCY_CASTLE_PROVIDER_CLASS.
    if (autoAdd && !provider.isEmpty()) {
      try {
        // Use reflection in order to avoid statically loading the class.
        final Class<?> clazz = Class.forName(BOUNCY_CASTLE_PROVIDER_CLASS);
        final Field provider_name = clazz.getField("PROVIDER_NAME");
        if (provider.equals(provider_name.get(null))) {
          Security.addProvider((Provider) clazz.getConstructor().newInstance());
          LOG.debug("Successfully added security provider {}", provider);
        }
      } catch (ClassNotFoundException e) {
        LOG_FAILED_TO_LOAD_CLASS.warn("Failed to load " + BOUNCY_CASTLE_PROVIDER_CLASS, e);
      } catch (NoSuchFieldException e) {
        LOG_FAILED_TO_GET_FIELD.warn("Failed to get field " + PROVIDER_NAME_FIELD
            + " from class " + BOUNCY_CASTLE_PROVIDER_CLASS, e);
      } catch (Exception e) {
        LOG_FAILED_TO_ADD_PROVIDER.warn("Failed to add security provider for {}", provider, e);
      }
    }
    return provider;
  }

  private CryptoUtils() {}
}

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.security.Provider;
import java.security.Security;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_ADD_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_ADD_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY;

@InterfaceAudience.Private
public class CryptoUtils {
  static final Logger LOG = LoggerFactory.getLogger(CryptoUtils.class);

  private static final String BOUNCY_CASTLE_PROVIDER_CLASS
      = "org.bouncycastle.jce.provider.BouncyCastleProvider";
  private static final String PROVIDER_NAME_FIELD = "PROVIDER_NAME";

  private static void addProvider(String provider) {
    if (provider == null || provider.isEmpty()) {
      return;
    }
    try {
      // For backward compatible, try to auto-add BOUNCY_CASTLE_PROVIDER_CLASS.
      final Class<?> clazz = Class.forName(BOUNCY_CASTLE_PROVIDER_CLASS);
      final Field provider_name = clazz.getField("PROVIDER_NAME");
      if (provider.equals(provider_name.get(null))) {
        Security.addProvider((Provider) clazz.getConstructor().newInstance());
        LOG.debug("Successfully added security provider {}", provider);
      }
    } catch (ClassNotFoundException e) {
      LOG.warn("Failed to load " + BOUNCY_CASTLE_PROVIDER_CLASS, e);
    } catch (NoSuchFieldException e) {
      LOG.warn("Failed to get field " + PROVIDER_NAME_FIELD
          + " from class " + BOUNCY_CASTLE_PROVIDER_CLASS, e);
    } catch (Exception e) {
      LOG.warn("Failed to add security provider for {}", provider, e);
    }
  }

  public static String getJceProvider(Configuration conf) {
    final String jceProvider = conf.get(HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY);
    if (conf.getBoolean(HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_ADD_KEY,
        HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_ADD_DEFAULT)) {
      CryptoUtils.addProvider(jceProvider);
    }
    return jceProvider;
  }
}

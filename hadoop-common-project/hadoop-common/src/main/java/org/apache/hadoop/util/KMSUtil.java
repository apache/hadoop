/**
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
package org.apache.hadoop.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Utils for KMS.
 */
@InterfaceAudience.Private
public final class KMSUtil {
  public static final Logger LOG = LoggerFactory.getLogger(KMSUtil.class);

  private KMSUtil() { /* Hidden constructor */ }

  /**
   * Creates a new KeyProvider from the given Configuration
   * and configuration key name.
   *
   * @param conf Configuration
   * @param configKeyName The configuration key name
   * @return new KeyProvider, or null if no provider was found.
   * @throws IOException if the KeyProvider is improperly specified in
   *                             the Configuration
   */
  public static KeyProvider createKeyProvider(final Configuration conf,
      final String configKeyName) throws IOException {
    LOG.debug("Creating key provider with config key {}", configKeyName);
    final String providerUriStr = conf.getTrimmed(configKeyName);
    // No provider set in conf
    if (providerUriStr == null || providerUriStr.isEmpty()) {
      return null;
    }
    KeyProvider kp = KMSUtilFaultInjector.get().createKeyProviderForTests(
        providerUriStr, conf);
    if (kp != null) {
      LOG.info("KeyProvider is created with uri: {}. This should happen only " +
              "in tests.", providerUriStr);
      return kp;
    }
    return createKeyProviderFromUri(conf, URI.create(providerUriStr));
  }

  public static KeyProvider createKeyProviderFromUri(final Configuration conf,
      final URI providerUri) throws IOException {
    KeyProvider keyProvider = KeyProviderFactory.get(providerUri, conf);
    if (keyProvider == null) {
      throw new IOException("Could not instantiate KeyProvider for uri: " +
          providerUri);
    }
    if (keyProvider.isTransient()) {
      throw new IOException("KeyProvider " + keyProvider.toString()
          + " was found but it is a transient provider.");
    }
    return keyProvider;
  }

  /**
   * Creates a key provider from token service field, which must be URI format.
   *
   * @param conf
   * @param tokenServiceValue
   * @return new KeyProvider or null
   * @throws IOException
   */
  public static KeyProvider createKeyProviderFromTokenService(
      final Configuration conf, final String tokenServiceValue)
      throws IOException {
    LOG.debug("Creating key provider from token service value {}. ",
        tokenServiceValue);
    final KeyProvider kp = KMSUtilFaultInjector.get()
        .createKeyProviderForTests(tokenServiceValue, conf);
    if (kp != null) {
      LOG.info("KeyProvider is created with uri: {}. This should happen only "
          + "in tests.", tokenServiceValue);
      return kp;
    }
    if (!tokenServiceValue.contains("://")) {
      throw new IllegalArgumentException(
          "Invalid token service " + tokenServiceValue);
    }
    final URI tokenServiceUri;
    try {
      tokenServiceUri = new URI(tokenServiceValue);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          "Invalid token service " + tokenServiceValue, e);
    }
    return createKeyProviderFromUri(conf, tokenServiceUri);
  }
}

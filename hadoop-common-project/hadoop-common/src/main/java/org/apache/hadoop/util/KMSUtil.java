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

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.crypto.key.kms.KMSRESTConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

  @SuppressWarnings("unchecked")
  public static Map toJSON(KeyProvider.KeyVersion keyVersion) {
    Map json = new HashMap();
    if (keyVersion != null) {
      json.put(KMSRESTConstants.NAME_FIELD,
          keyVersion.getName());
      json.put(KMSRESTConstants.VERSION_NAME_FIELD,
          keyVersion.getVersionName());
      json.put(KMSRESTConstants.MATERIAL_FIELD,
          Base64.encodeBase64URLSafeString(
              keyVersion.getMaterial()));
    }
    return json;
  }

  @SuppressWarnings("unchecked")
  public static Map toJSON(EncryptedKeyVersion encryptedKeyVersion) {
    Map json = new HashMap();
    if (encryptedKeyVersion != null) {
      json.put(KMSRESTConstants.VERSION_NAME_FIELD,
          encryptedKeyVersion.getEncryptionKeyVersionName());
      json.put(KMSRESTConstants.IV_FIELD, Base64
          .encodeBase64URLSafeString(encryptedKeyVersion.getEncryptedKeyIv()));
      json.put(KMSRESTConstants.ENCRYPTED_KEY_VERSION_FIELD,
          toJSON(encryptedKeyVersion.getEncryptedKeyVersion()));
    }
    return json;
  }

  public static <T> T checkNotNull(T o, String name)
      throws IllegalArgumentException {
    if (o == null) {
      throw new IllegalArgumentException("Parameter '" + name +
          "' cannot be null");
    }
    return o;
  }

  public static String checkNotEmpty(String s, String name)
      throws IllegalArgumentException {
    checkNotNull(s, name);
    if (s.isEmpty()) {
      throw new IllegalArgumentException("Parameter '" + name +
          "' cannot be empty");
    }
    return s;
  }

  @SuppressWarnings("rawtypes")
  public static List<EncryptedKeyVersion>
      parseJSONEncKeyVersions(String keyName, List valueList) {
    checkNotNull(valueList, "valueList");
    List<EncryptedKeyVersion> ekvs = new ArrayList<>(valueList.size());
    if (!valueList.isEmpty()) {
      for (Object values : valueList) {
        Map valueMap = (Map) values;
        ekvs.add(parseJSONEncKeyVersion(keyName, valueMap));
      }
    }
    return ekvs;
  }

  @SuppressWarnings("unchecked")
  public static EncryptedKeyVersion parseJSONEncKeyVersion(String keyName,
      Map valueMap) {
    checkNotNull(valueMap, "valueMap");
    String versionName = checkNotNull(
        (String) valueMap.get(KMSRESTConstants.VERSION_NAME_FIELD),
        KMSRESTConstants.VERSION_NAME_FIELD);

    byte[] iv = Base64.decodeBase64(checkNotNull(
        (String) valueMap.get(KMSRESTConstants.IV_FIELD),
        KMSRESTConstants.IV_FIELD));

    Map encValueMap = checkNotNull((Map)
            valueMap.get(KMSRESTConstants.ENCRYPTED_KEY_VERSION_FIELD),
        KMSRESTConstants.ENCRYPTED_KEY_VERSION_FIELD);

    String encVersionName = checkNotNull((String)
            encValueMap.get(KMSRESTConstants.VERSION_NAME_FIELD),
        KMSRESTConstants.VERSION_NAME_FIELD);

    byte[] encKeyMaterial = Base64.decodeBase64(checkNotNull((String)
            encValueMap.get(KMSRESTConstants.MATERIAL_FIELD),
        KMSRESTConstants.MATERIAL_FIELD));

    return new KMSClientProvider.KMSEncryptedKeyVersion(keyName, versionName,
        iv, encVersionName, encKeyMaterial);
  }

  @SuppressWarnings("unchecked")
  public static KeyProvider.KeyVersion parseJSONKeyVersion(Map valueMap) {
    checkNotNull(valueMap, "valueMap");
    KeyProvider.KeyVersion keyVersion = null;
    if (!valueMap.isEmpty()) {
      byte[] material =
          (valueMap.containsKey(KMSRESTConstants.MATERIAL_FIELD)) ?
              Base64.decodeBase64(
                  (String) valueMap.get(KMSRESTConstants.MATERIAL_FIELD)) :
              null;
      String versionName =
          (String) valueMap.get(KMSRESTConstants.VERSION_NAME_FIELD);
      String keyName = (String) valueMap.get(KMSRESTConstants.NAME_FIELD);
      keyVersion =
          new KMSClientProvider.KMSKeyVersion(keyName, versionName, material);
    }
    return keyVersion;
  }

  @SuppressWarnings("unchecked")
  public static KeyProvider.Metadata parseJSONMetadata(Map valueMap) {
    checkNotNull(valueMap, "valueMap");
    KeyProvider.Metadata metadata = null;
    if (!valueMap.isEmpty()) {
      metadata = new KMSClientProvider.KMSMetadata(
          (String) valueMap.get(KMSRESTConstants.CIPHER_FIELD),
          (Integer) valueMap.get(KMSRESTConstants.LENGTH_FIELD),
          (String) valueMap.get(KMSRESTConstants.DESCRIPTION_FIELD),
          (Map<String, String>) valueMap.get(KMSRESTConstants.ATTRIBUTES_FIELD),
          new Date((Long) valueMap.get(KMSRESTConstants.CREATED_FIELD)),
          (Integer) valueMap.get(KMSRESTConstants.VERSIONS_FIELD));
    }
    return metadata;
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

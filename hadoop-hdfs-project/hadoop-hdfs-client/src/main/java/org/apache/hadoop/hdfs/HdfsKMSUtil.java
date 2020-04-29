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
package org.apache.hadoop.hdfs;

import static org.apache.hadoop.fs.CommonConfigurationKeys.DFS_CLIENT_IGNORE_NAMENODE_DEFAULT_KMS_URI;
import static org.apache.hadoop.fs.CommonConfigurationKeys.DFS_CLIENT_IGNORE_NAMENODE_DEFAULT_KMS_URI_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_KEY_PREFIX;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.GeneralSecurityException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.CryptoInputStream;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProvider.KeyVersion;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.crypto.key.KeyProviderTokenIssuer;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.KMSUtil;

/**
 * Utility class for key provider related methods in hdfs client package.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class HdfsKMSUtil {
  private static final String DFS_KMS_PREFIX = "dfs-kms-";
  private static String keyProviderUriKeyName =
      CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH;

  private HdfsKMSUtil() { /* Hidden constructor */ }

  /**
   * Creates a new KeyProvider from the given Configuration.
   *
   * @param conf Configuration
   * @return new KeyProvider, or null if no provider was found.
   * @throws IOException if the KeyProvider is improperly specified in
   *                             the Configuration
   */
  public static KeyProvider createKeyProvider(
      final Configuration conf) throws IOException {
    return KMSUtil.createKeyProvider(conf, keyProviderUriKeyName);
  }

  /**
   * Obtain the crypto protocol version from the provided FileEncryptionInfo,
   * checking to see if this version is supported by.
   *
   * @param feInfo FileEncryptionInfo
   * @return CryptoProtocolVersion from the feInfo
   * @throws IOException if the protocol version is unsupported.
   */
  public static CryptoProtocolVersion getCryptoProtocolVersion(
      FileEncryptionInfo feInfo) throws IOException {
    final CryptoProtocolVersion version = feInfo.getCryptoProtocolVersion();
    if (!CryptoProtocolVersion.supports(version)) {
      throw new IOException("Client does not support specified " +
          "CryptoProtocolVersion " + version.getDescription() + " version " +
          "number" + version.getVersion());
    }
    return version;
  }

  /**
   * Obtain a CryptoCodec based on the CipherSuite set in a FileEncryptionInfo
   * and the available CryptoCodecs configured in the Configuration.
   *
   * @param conf   Configuration
   * @param feInfo FileEncryptionInfo
   * @return CryptoCodec
   * @throws IOException if no suitable CryptoCodec for the CipherSuite is
   *                     available.
   */
  public static CryptoCodec getCryptoCodec(Configuration conf,
      FileEncryptionInfo feInfo) throws IOException {
    final CipherSuite suite = feInfo.getCipherSuite();
    if (suite.equals(CipherSuite.UNKNOWN)) {
      throw new IOException("NameNode specified unknown CipherSuite with ID "
          + suite.getUnknownValue() + ", cannot instantiate CryptoCodec.");
    }
    final CryptoCodec codec = CryptoCodec.getInstance(conf, suite);
    if (codec == null) {
      throw new UnknownCipherSuiteException(
          "No configuration found for the cipher suite "
              + suite.getConfigSuffix() + " prefixed with "
              + HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_KEY_PREFIX
              + ". Please see the example configuration "
              + "hadoop.security.crypto.codec.classes.EXAMPLECIPHERSUITE "
              + "at core-default.xml for details.");
    }
    return codec;
  }

  /**
   * The key provider uri is searched in the following order.
   * 1. If there is a mapping in Credential's secrets map for namenode uri.
   * 2. From namenode getServerDefaults call.
   * 3. Finally fallback to local conf.
   * @return keyProviderUri if found from either of above 3 cases,
   * null otherwise
   * @throws IOException
   */
  public static URI getKeyProviderUri(UserGroupInformation ugi,
      URI namenodeUri, String keyProviderUriStr, Configuration conf)
          throws IOException {
    URI keyProviderUri = null;
    // Lookup the secret in credentials object for namenodeuri.
    Credentials credentials = ugi.getCredentials();
    Text credsKey = getKeyProviderMapKey(namenodeUri);
    byte[] keyProviderUriBytes =
        credentials.getSecretKey(credsKey);
    if(keyProviderUriBytes != null) {
      keyProviderUri =
          URI.create(DFSUtilClient.bytes2String(keyProviderUriBytes));
    }
    if (keyProviderUri == null) {
      // Check if NN provided uri is not null and ignore property is false.
      if (keyProviderUriStr != null && !conf.getBoolean(
          DFS_CLIENT_IGNORE_NAMENODE_DEFAULT_KMS_URI,
          DFS_CLIENT_IGNORE_NAMENODE_DEFAULT_KMS_URI_DEFAULT)) {
        if (!keyProviderUriStr.isEmpty()) {
          keyProviderUri = URI.create(keyProviderUriStr);
        }
      }
      // Fallback to configuration.
      if (keyProviderUri == null) {
        // Either NN is old and doesn't report provider or ignore NN KMS
        // provider property is set to true, so use conf.
        keyProviderUri = KMSUtil.getKeyProviderUri(conf, keyProviderUriKeyName);
      }
      if (keyProviderUri != null) {
        credentials.addSecretKey(
            credsKey, DFSUtilClient.string2Bytes(keyProviderUri.toString()));
      }
    }
    return keyProviderUri;
  }

  public static KeyProvider getKeyProvider(KeyProviderTokenIssuer issuer,
                                           Configuration conf)
      throws IOException {
    URI keyProviderUri = issuer.getKeyProviderUri();
    if (keyProviderUri != null) {
      return KMSUtil.createKeyProviderFromUri(conf, keyProviderUri);
    }
    return null;
  }

  /**
   * Returns a key to map namenode uri to key provider uri.
   * Tasks will lookup this key to find key Provider.
   */
  public static Text getKeyProviderMapKey(URI namenodeUri) {
    return new Text(DFS_KMS_PREFIX + namenodeUri.getScheme()
        +"://" + namenodeUri.getAuthority());
  }

  public static CryptoInputStream createWrappedInputStream(InputStream is,
      KeyProvider keyProvider, FileEncryptionInfo fileEncryptionInfo,
      Configuration conf) throws IOException {
    // File is encrypted, wrap the stream in a crypto stream.
    // Currently only one version, so no special logic based on the version#
    HdfsKMSUtil.getCryptoProtocolVersion(fileEncryptionInfo);
    final CryptoCodec codec = HdfsKMSUtil.getCryptoCodec(
        conf, fileEncryptionInfo);
    final KeyVersion decrypted =
        decryptEncryptedDataEncryptionKey(fileEncryptionInfo, keyProvider);
    return new CryptoInputStream(is, codec, decrypted.getMaterial(),
        fileEncryptionInfo.getIV());
  }

  /**
   * Decrypts a EDEK by consulting the KeyProvider.
   */
  static KeyVersion decryptEncryptedDataEncryptionKey(FileEncryptionInfo
      feInfo, KeyProvider keyProvider) throws IOException {
    if (keyProvider == null) {
      throw new IOException("No KeyProvider is configured, cannot access" +
          " an encrypted file");
    }
    EncryptedKeyVersion ekv = EncryptedKeyVersion.createForDecryption(
        feInfo.getKeyName(), feInfo.getEzKeyVersionName(), feInfo.getIV(),
        feInfo.getEncryptedDataEncryptionKey());
    try {
      KeyProviderCryptoExtension cryptoProvider = KeyProviderCryptoExtension
          .createKeyProviderCryptoExtension(keyProvider);
      return cryptoProvider.decryptEncryptedKey(ekv);
    } catch (GeneralSecurityException e) {
      throw new IOException(e);
    }
  }
}

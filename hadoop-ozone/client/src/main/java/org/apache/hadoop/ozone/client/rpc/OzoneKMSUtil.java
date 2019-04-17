/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.KMSUtil;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;

/**
 * KMS utility class for Ozone Data Encryption At-Rest.
 */
public final class OzoneKMSUtil {

  private static final String UTF8_CSN = StandardCharsets.UTF_8.name();
  private static final String O3_KMS_PREFIX = "ozone-kms-";
  private static String keyProviderUriKeyName =
      "hadoop.security.key.provider.path";

  private OzoneKMSUtil() {
  }

  public static KeyProvider.KeyVersion decryptEncryptedDataEncryptionKey(
      FileEncryptionInfo feInfo, KeyProvider keyProvider) throws IOException {
    if (keyProvider == null) {
      throw new IOException("No KeyProvider is configured, " +
          "cannot access an encrypted file");
    } else {
      EncryptedKeyVersion ekv = EncryptedKeyVersion.createForDecryption(
          feInfo.getKeyName(), feInfo.getEzKeyVersionName(), feInfo.getIV(),
          feInfo.getEncryptedDataEncryptionKey());

      try {
        KeyProviderCryptoExtension cryptoProvider = KeyProviderCryptoExtension
            .createKeyProviderCryptoExtension(keyProvider);
        return cryptoProvider.decryptEncryptedKey(ekv);
      } catch (GeneralSecurityException gse) {
        throw new IOException(gse);
      }
    }
  }

  /**
   * Returns a key to map ozone uri to key provider uri.
   * Tasks will lookup this key to find key Provider.
   */
  public static Text getKeyProviderMapKey(URI namespaceUri) {
    return new Text(O3_KMS_PREFIX + namespaceUri.getScheme()
        +"://" + namespaceUri.getAuthority());
  }

  public static String bytes2String(byte[] bytes) {
    return bytes2String(bytes, 0, bytes.length);
  }

  private static String bytes2String(byte[] bytes, int offset, int length) {
    try {
      return new String(bytes, offset, length, UTF8_CSN);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException("UTF8 encoding is not supported", e);
    }
  }

  public static URI getKeyProviderUri(UserGroupInformation ugi,
      URI namespaceUri, String kmsUriSrv, Configuration conf)
      throws IOException {
    URI keyProviderUri = null;
    Credentials credentials = ugi.getCredentials();
    Text credsKey = null;
    if (namespaceUri != null) {
      // from ugi
      credsKey = getKeyProviderMapKey(namespaceUri);
      byte[] keyProviderUriBytes = credentials.getSecretKey(credsKey);
      if (keyProviderUriBytes != null) {
        keyProviderUri = URI.create(bytes2String(keyProviderUriBytes));
      }
    }
    if (keyProviderUri == null) {
      // from client conf
      if (kmsUriSrv == null) {
        keyProviderUri = KMSUtil.getKeyProviderUri(
            conf, keyProviderUriKeyName);
      } else if (!kmsUriSrv.isEmpty()) {
        // from om server
        keyProviderUri = URI.create(kmsUriSrv);
      }
    }
    // put back into UGI
    if (keyProviderUri != null && credsKey != null) {
      credentials.addSecretKey(
          credsKey, DFSUtil.string2Bytes(keyProviderUri.toString()));
    }

    return keyProviderUri;
  }

  public static KeyProvider getKeyProvider(final Configuration conf,
      final URI serverProviderUri) throws IOException{
    return KMSUtil.createKeyProviderFromUri(conf, serverProviderUri);
  }

  public static CryptoProtocolVersion getCryptoProtocolVersion(
      FileEncryptionInfo feInfo) throws IOException {
    CryptoProtocolVersion version = feInfo.getCryptoProtocolVersion();
    if (!CryptoProtocolVersion.supports(version)) {
      throw new IOException("Client does not support specified " +
              "CryptoProtocolVersion " + version.getDescription() +
              " version number" + version.getVersion());
    } else {
      return version;
    }
  }

  public static void checkCryptoProtocolVersion(
          FileEncryptionInfo feInfo) throws IOException {
    CryptoProtocolVersion version = feInfo.getCryptoProtocolVersion();
    if (!CryptoProtocolVersion.supports(version)) {
      throw new IOException("Client does not support specified " +
              "CryptoProtocolVersion " + version.getDescription() +
              " version number" + version.getVersion());
    }
  }

  public static CryptoCodec getCryptoCodec(Configuration conf,
      FileEncryptionInfo feInfo) throws IOException {
    CipherSuite suite = feInfo.getCipherSuite();
    if (suite.equals(CipherSuite.UNKNOWN)) {
      throw new IOException("NameNode specified unknown CipherSuite with ID " +
              suite.getUnknownValue() + ", cannot instantiate CryptoCodec.");
    } else {
      CryptoCodec codec = CryptoCodec.getInstance(conf, suite);
      if (codec == null) {
        throw new OMException("No configuration found for the cipher suite " +
                suite.getConfigSuffix() + " prefixed with " +
                "hadoop.security.crypto.codec.classes. Please see the" +
                " example configuration hadoop.security.crypto.codec.classes." +
                "EXAMPLE CIPHER SUITE at core-default.xml for details.",
                OMException.ResultCodes.UNKNOWN_CIPHER_SUITE);
      } else {
        return codec;
      }
    }
  }
}

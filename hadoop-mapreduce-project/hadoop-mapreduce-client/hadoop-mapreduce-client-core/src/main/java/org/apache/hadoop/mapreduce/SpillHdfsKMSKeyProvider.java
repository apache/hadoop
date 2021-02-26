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
package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.crypto.key.kms.KMSRESTConstants;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.JsonSerialization;
import org.apache.hadoop.util.KMSUtil;

/**
 * Implementation that allows Allow KMS generated spill encryption keys.
 */
public class SpillHdfsKMSKeyProvider extends SpillNullKeyProvider {
  private Text spillEdek;
  private String kmsSpillKeyName;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf == null) {
      return;
    }
    setEncryptionEnabled(conf.getBoolean(
        MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA,
        MRJobConfig.DEFAULT_MR_ENCRYPTED_INTERMEDIATE_DATA));
    spillEdek = new Text(getConf().get(
        MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA_KEYPROVIDER_EDEK,
        MRJobConfig.DEFAULT_MR_ENCRYPTED_INTERMEDIATE_DATA_KEYPROVIDER_EDEK));
    kmsSpillKeyName = getConf().get(
        MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA_KEYPROVIDER_KEY_NAME,
        MRJobConfig
            .DEFAULT_MR_ENCRYPTED_INTERMEDIATE_DATA_KEYPROVIDER_KEY_NAME);
  }

  @Override
  public void addEncryptedSpillKey(Credentials credentials) throws IOException {
    // assumes key provider is from DFS
    KeyProvider keyProvider =
        ((DistributedFileSystem) FileSystem.get(getConf())).getKeyProvider();
    try (KeyProviderCryptoExtension keyProviderCryptoExtension =
             KeyProviderCryptoExtension
                 .createKeyProviderCryptoExtension(keyProvider)) {
      KeyProviderCryptoExtension.EncryptedKeyVersion version =
          keyProviderCryptoExtension.generateEncryptedKey(kmsSpillKeyName);
      Map jsonLoad = KMSUtil.toJSON(version);
      jsonLoad.put(KMSRESTConstants.NAME_FIELD,
          version.getEncryptionKeyName());
      byte[] encKeyVersionJSON =
          JsonSerialization.writer().writeValueAsBytes(jsonLoad);
      credentials.addSecretKey(spillEdek, encKeyVersionJSON);
    } catch (GeneralSecurityException e) {
      throw new IOException(e);
    }
  }

  @Override
  public byte[] getEncryptionSpillKey(
      Credentials credentials) throws IOException {
    byte[] versionJson = credentials.getSecretKey(spillEdek);
    // assumes key provider is from DFS
    KeyProvider keyProvider =
        ((DistributedFileSystem) FileSystem.get(getConf())).getKeyProvider();
    Map jsonMap = JsonSerialization.mapReader().readValue(versionJson);
    String keyName = (String) jsonMap.get(KMSRESTConstants.NAME_FIELD);
    EncryptedKeyVersion kmsEDEK =
        KMSUtil.parseJSONEncKeyVersion(keyName, jsonMap);
    try (KeyProviderCryptoExtension keyProviderCryptoExtension =
             KeyProviderCryptoExtension
                 .createKeyProviderCryptoExtension(keyProvider)) {
      KeyProvider.KeyVersion ek =
          keyProviderCryptoExtension.decryptEncryptedKey(kmsEDEK);
      return ek.getMaterial();
    } catch (GeneralSecurityException e) {
      throw new IOException(e);
    }
  }
}

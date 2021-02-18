/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.EncryptionMaterials;
import com.amazonaws.services.s3.model.EncryptionMaterialsProvider;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.StaticEncryptionMaterialsProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfEncryptionTestsDisabled;

/**
 * Testing the S3 CSE - CUSTOM method by generating and using an Asymmetric
 * key.
 */
public class ITestS3AEncryptionCSEAsymmetric extends ITestS3AEncryptionCSE {

  private static final String KEY_WRAP_ALGO = "RSA-OAEP-SHA1";
  private static final String CONTENT_ENCRYPTION_ALGO = "AES/GCM/NoPadding";

  /**
   * Custom implementation of the S3ACSEMaterialsProviderConfig class to
   * create a custom EncryptionMaterialsProvider by generating and using an
   * asymmetric key.
   */
  protected static class AsymmetricKeyConfig
      implements S3ACSEMaterialProviderConfig {
    @Override
    public EncryptionMaterialsProvider buildEncryptionProvider() {
      SecureRandom srand = new SecureRandom();
      KeyPairGenerator keyGenerator;
      try {
        keyGenerator = KeyPairGenerator.getInstance("RSA");
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException("No such Algo error: ", e);
      }
      keyGenerator.initialize(1024, srand);
      KeyPair keyPair = keyGenerator.generateKeyPair();

      EncryptionMaterials encryptionMaterials = new EncryptionMaterials(
          keyPair);

      return new StaticEncryptionMaterialsProvider(encryptionMaterials);
    }
  }

  /**
   * Creating custom configs to use S3-CSE - CUSTOM method.
   *
   * @return Configuration.
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.disableFilesystemCaching(conf);
    conf.set(Constants.CLIENT_SIDE_ENCRYPTION_METHOD,
        S3AEncryptionMethods.CSE_CUSTOM.getMethod());
    conf.setClass(Constants.CLIENT_SIDE_ENCRYPTION_MATERIALS_PROVIDER,
        AsymmetricKeyConfig.class, S3ACSEMaterialProviderConfig.class);
    return conf;
  }

  @Override
  protected void skipTest() {
    skipIfEncryptionTestsDisabled(getConfiguration());
  }

  @Override
  protected void assertEncrypted(Path path) throws IOException {
    ObjectMetadata md = getFileSystem().getObjectMetadata(path);

    //Assert key-wrap algo
    assertEquals("Key wrap algo isn't same as expected", KEY_WRAP_ALGO,
        md.getUserMetaDataOf(Headers.CRYPTO_KEYWRAP_ALGORITHM));

    // Assert content encryption algo
    assertEquals("Key wrap algo isn't same as expected",
        CONTENT_ENCRYPTION_ALGO,
        md.getUserMetaDataOf(Headers.CRYPTO_CEK_ALGORITHM));

  }
}

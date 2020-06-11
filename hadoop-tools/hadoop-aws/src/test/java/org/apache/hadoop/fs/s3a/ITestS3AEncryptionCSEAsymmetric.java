/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import com.amazonaws.services.s3.model.EncryptionMaterials;
import com.amazonaws.services.s3.model.EncryptionMaterialsProvider;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.StaticEncryptionMaterialsProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;

/**
 * Testing S3A client-side encryption/decryption with ASYMMETRIC RSA.
 */
public class ITestS3AEncryptionCSEAsymmetric extends ITestS3AEncryptionCSE {

  protected static class AsymmetricKeyConfig
          implements S3ACSEMaterialProviderConfig {
    @Override
    public EncryptionMaterialsProvider buildMaterialsProvider()
            throws Exception {
      SecureRandom srand = new SecureRandom();
      KeyPairGenerator keyGenerator = KeyPairGenerator.getInstance("RSA");
      keyGenerator.initialize(1024, srand);
      KeyPair keyPair = keyGenerator.generateKeyPair();

      EncryptionMaterials encryptionMaterials = new EncryptionMaterials(
              keyPair);

      return new StaticEncryptionMaterialsProvider(encryptionMaterials);
    }
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.disableFilesystemCaching(conf);
    conf.set(Constants.CLIENT_SIDE_ENCRYPTION_METHOD,
            S3AClientEncryptionMethods.CUSTOM.getMethod());
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
    assertEquals("RSA/ECB/OAEPWithSHA-256AndMGF1Padding",
            md.getUserMetaDataOf("x-amz-wrap-alg"));
  }
}

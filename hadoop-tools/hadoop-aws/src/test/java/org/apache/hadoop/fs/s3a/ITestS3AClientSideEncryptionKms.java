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

import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.assertj.core.api.Assertions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfEncryptionTestsDisabled;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfKmsKeyIdIsNotSet;

/**
 * Testing the S3 CSE - KME method.
 */
public class ITestS3AClientSideEncryptionKms
    extends ITestS3AClientSideEncryption {

  private static final String KMS_KEY_WRAP_ALGO = "kms+context";
  private static final String KMS_CONTENT_ENCRYPTION_ALGO = "AES/GCM/NoPadding";

  /**
   * Creating custom configs for KMS testing.
   *
   * @return Configuration.
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.disableFilesystemCaching(conf);
    return conf;
  }

  @Override
  protected void skipTest() {
    skipIfEncryptionTestsDisabled(getConfiguration());
    skipIfKmsKeyIdIsNotSet(getConfiguration());
  }

  @Override
  protected void assertEncrypted(Path path) throws IOException {
    ObjectMetadata md = getFileSystem().getObjectMetadata(path);

    // Assert KeyWrap Algo
    assertEquals("Key wrap algo isn't same as expected", KMS_KEY_WRAP_ALGO,
        md.getUserMetaDataOf(
            Headers.CRYPTO_KEYWRAP_ALGORITHM));

    // Assert content encryption algo for KMS, is present in the
    // materials description and KMS key ID isn't.
    String keyId =
        getConfiguration().get(Constants.CLIENT_SIDE_ENCRYPTION_KMS_KEY_ID);
    Assertions.assertThat(md.getUserMetaDataOf(Headers.MATERIALS_DESCRIPTION))
        .describedAs("Materials Description should contain the content "
            + "encryption algo and should not contain the KMS keyID.")
        .contains(KMS_CONTENT_ENCRYPTION_ALGO)
        .doesNotContain(keyId);
  }
}

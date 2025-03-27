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
import java.util.Map;

import org.assertj.core.api.Assertions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.impl.AWSHeaders;
import org.apache.hadoop.fs.s3a.impl.HeaderProcessing;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfEncryptionNotSet;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfEncryptionTestsDisabled;
import static org.apache.hadoop.fs.s3a.S3AUtils.getS3EncryptionKey;

/**
 * Testing the S3 CSE - KMS method.
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
  protected void maybeSkipTest() throws IOException {
    skipIfEncryptionTestsDisabled(getConfiguration());
    // skip the test if CSE-KMS or KMS key is not set.
    skipIfEncryptionNotSet(getConfiguration(), S3AEncryptionMethods.CSE_KMS);
  }

  @Override
  protected void assertEncrypted(Path path) throws IOException {
    Map<String, byte[]> fsXAttrs = getFileSystem().getXAttrs(path);
    String xAttrPrefix = "header.";

    // Assert KeyWrap Algo
    assertEquals("Key wrap algo isn't same as expected", KMS_KEY_WRAP_ALGO,
        processHeader(fsXAttrs,
            xAttrPrefix + AWSHeaders.CRYPTO_KEYWRAP_ALGORITHM));

    // Assert content encryption algo for KMS, is present in the
    // materials description and KMS key ID isn't.
    String keyId = getS3EncryptionKey(getTestBucketName(getConfiguration()),
        getConfiguration());
    Assertions.assertThat(processHeader(fsXAttrs,
        xAttrPrefix + AWSHeaders.MATERIALS_DESCRIPTION))
        .describedAs("Materials Description should contain the content "
            + "encryption algo and should not contain the KMS keyID.")
        .contains(KMS_CONTENT_ENCRYPTION_ALGO)
        .doesNotContain(keyId);
  }

  /**
   * A method to process a FS xAttribute Header key by decoding it.
   *
   * @param fsXAttrs  Map of String(Key) and bytes[](Value) to represent fs
   *                  xAttr.
   * @param headerKey Key value of the header we are trying to process.
   * @return String representing the value of key header provided.
   */
  private String processHeader(Map<String, byte[]> fsXAttrs,
      String headerKey) {
    return HeaderProcessing.decodeBytes(fsXAttrs.get(headerKey));
  }
}

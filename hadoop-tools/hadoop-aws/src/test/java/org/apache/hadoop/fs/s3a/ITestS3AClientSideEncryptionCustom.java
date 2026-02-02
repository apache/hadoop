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

import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_CSE_CUSTOM_KEYRING_CLASS_NAME;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfEncryptionNotSet;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfEncryptionTestsDisabled;

/**
 * Tests to verify Custom S3 client side encryption CSE-CUSTOM.
 */
public class ITestS3AClientSideEncryptionCustom extends ITestS3AClientSideEncryption {

  private static final String KMS_KEY_WRAP_ALGO = "kms+context";
  /**
   * Creating custom configs for CSE-CUSTOM testing.
   *
   * @return Configuration.
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.disableFilesystemCaching(conf);
    conf.set(S3_ENCRYPTION_CSE_CUSTOM_KEYRING_CLASS_NAME,
        CustomKeyring.class.getCanonicalName());
    return conf;
  }

  @Override
  protected void maybeSkipTest() throws IOException {
    skipIfEncryptionTestsDisabled(getConfiguration());
    // skip the test if CSE-CUSTOM is not set.
    skipIfEncryptionNotSet(getConfiguration(), S3AEncryptionMethods.CSE_CUSTOM);
  }


  @Override
  protected void assertEncrypted(Path path) throws IOException {
    Map<String, byte[]> fsXAttrs = getFileSystem().getXAttrs(path);
    String xAttrPrefix = "header.";

    // Assert KeyWrap Algo
    Assertions.assertThat(processHeader(fsXAttrs,
            xAttrPrefix + AWSHeaders.CRYPTO_KEYWRAP_ALGORITHM))
        .describedAs("Key wrap algo")
        .isEqualTo(KMS_KEY_WRAP_ALGO);
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

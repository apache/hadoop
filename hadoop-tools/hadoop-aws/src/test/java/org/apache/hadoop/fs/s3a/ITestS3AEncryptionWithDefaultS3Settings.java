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

import com.amazonaws.services.s3.model.ObjectMetadata;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.s3a.S3AEncryptionMethods.SSE_KMS;

/**
 * Concrete class that extends {@link AbstractTestS3AEncryption}
 * and tests already configured bucket level encryption using s3 console.
 * This requires the SERVER_SIDE_ENCRYPTION_KEY
 * to be set in auth-keys.xml for it to run. The value should match with the
 * kms key set for the bucket.
 */
public class ITestS3AEncryptionWithDefaultS3Settings extends
        AbstractTestS3AEncryption {

  @Override
  protected Configuration getConfiguration() {
    // get the KMS key for this test.
    Configuration c = new Configuration();
    String kmsKey = c.get(SERVER_SIDE_ENCRYPTION_KEY);
    if (StringUtils.isBlank(kmsKey)) {
      skip(SERVER_SIDE_ENCRYPTION_KEY + " is not set for " +
              SSE_KMS.getMethod());
    }
    return super.createConfiguration();
  }

  /**
   * Setting this to NONE as we don't want to overwrite
   * already configured encryption settings.
   * @return
   */
  @Override
  protected S3AEncryptionMethods getSSEAlgorithm() {
    return S3AEncryptionMethods.NONE;
  }

  @Override
  protected void assertEncrypted(Path path) throws IOException {
    Configuration c = new Configuration();
    String kmsKey = c.get(SERVER_SIDE_ENCRYPTION_KEY);
    ObjectMetadata objectMetadata = getFileSystem().getObjectMetadata(path);
    assertEquals("SSE KMS key id should match", kmsKey,
            objectMetadata.getSSEAwsKmsKeyId());
  }

  @Override
  @Ignore
  @Test
  public void testEncryptionSettingPropagation() throws Throwable {
  }

  @Override
  @Ignore
  @Test
  public void testEncryption() throws Throwable {
  }
}

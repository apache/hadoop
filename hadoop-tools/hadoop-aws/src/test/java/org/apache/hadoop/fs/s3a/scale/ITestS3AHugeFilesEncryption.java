/*
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

package org.apache.hadoop.fs.s3a.scale;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.EncryptionTestUtils;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.s3a.S3AEncryptionMethods.DSSE_KMS;
import static org.apache.hadoop.fs.s3a.S3AEncryptionMethods.SSE_KMS;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfEncryptionNotSet;
import static org.apache.hadoop.fs.s3a.S3AUtils.getEncryptionAlgorithm;
import static org.apache.hadoop.fs.s3a.S3AUtils.getS3EncryptionKey;

/**
 * Class to test SSE_KMS encryption settings for huge files.
 * Tests will only run if value of {@link Constants#S3_ENCRYPTION_KEY}
 * is set in the configuration. The testing bucket must be configured with this
 * same key else test might fail.
 */
public class ITestS3AHugeFilesEncryption extends AbstractSTestS3AHugeFiles {

  @Override
  public void setup() throws Exception {
    Configuration c = new Configuration();
    skipIfEncryptionNotSet(c, SSE_KMS, DSSE_KMS);
    super.setup();
  }

  @Override
  protected String getBlockOutputBufferName() {
    return Constants.FAST_UPLOAD_BUFFER_ARRAY;
  }

  /**
   * @param fileSystem
   * @return true if {@link Constants#S3_ENCRYPTION_KEY} is set
   * in the config.
   */
  @Override
  protected boolean isEncrypted(S3AFileSystem fileSystem) {
    Configuration conf = new Configuration();
    return StringUtils.isNotBlank(getS3EncryptionKey(getTestBucketName(conf), conf));
  }

  /**
   * This test suite will run if the algorithm is set to SSE_KMS or DSSE_KMS;
   * the assertions validate this.
   * @param hugeFile file to validate.
   * @throws IOException problems with encryption lookup.
   * @throws AssertionError if the encryption is not as expected.
   */
  @Override
  protected void assertEncrypted(Path hugeFile) throws IOException {
    Configuration conf = new Configuration();

    final String bucket = getTestBucketName(conf);
    String kmsKey = getS3EncryptionKey(bucket, conf);
    final S3AEncryptionMethods algorithm = getEncryptionAlgorithm(bucket, conf);
    if (SSE_KMS.equals(algorithm)) {
      EncryptionTestUtils.assertEncrypted(getFileSystem(), hugeFile, SSE_KMS, kmsKey);
    } else if (DSSE_KMS.equals(algorithm)) {
      EncryptionTestUtils.assertEncrypted(getFileSystem(), hugeFile, DSSE_KMS, kmsKey);
    } else {
      throw new AssertionError("Invalid encryption configured: " + algorithm);
    }
  }
}

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

import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.s3a.S3AEncryptionMethods.DSSE_KMS;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfEncryptionNotSet;

/**
 * Concrete class that extends {@link AbstractTestS3AEncryption}
 * and tests DSSE-KMS encryption.
 */
public class ITestS3AEncryptionDSSEKMSUserDefinedKey
    extends AbstractTestS3AEncryption {

  @Override
  protected Configuration createConfiguration() {
    // get the KMS key for this test.
    Configuration c = new Configuration();
    String kmsKey = S3AUtils.getS3EncryptionKey(getTestBucketName(c), c);
    // skip the test if DSSE-KMS or KMS key not set.
    try {
      skipIfEncryptionNotSet(c, DSSE_KMS);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    assume("KMS key is expected to be present", StringUtils.isNotBlank(kmsKey));
    Configuration conf = super.createConfiguration();
    conf.set(S3_ENCRYPTION_KEY, kmsKey);
    return conf;
  }

  @Override
  protected S3AEncryptionMethods getSSEAlgorithm() {
    return DSSE_KMS;
  }
}

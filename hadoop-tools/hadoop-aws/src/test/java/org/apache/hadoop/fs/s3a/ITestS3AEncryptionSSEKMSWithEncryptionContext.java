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
import java.util.Set;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.impl.S3AEncryption;

import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_CONTEXT;
import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.s3a.S3AEncryptionMethods.DSSE_KMS;
import static org.apache.hadoop.fs.s3a.S3AEncryptionMethods.SSE_KMS;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;

/**
 * Concrete class that extends {@link AbstractTestS3AEncryption}
 * and tests KMS encryption with encryption context.
 * S3's HeadObject doesn't return the object's encryption context.
 * Therefore, we don't have a way to assert its value in code.
 * In order to properly test if the encryption context is being set,
 * the KMS key or the IAM User need to have a deny statements like the one below in the policy:
 * <pre>
 * {
 *     "Effect": "Deny",
 *     "Principal": {
 *         "AWS": "*"
 *     },
 *     "Action": "kms:Decrypt",
 *     "Resource": "*",
 *     "Condition": {
 *         "StringNotEquals": {
 *             "kms:EncryptionContext:project": "hadoop"
 *         }
 *     }
 * }
 * </pre>
 * With the statement above, S3A will fail to read the object from S3 if it was encrypted
 * without the key-pair <code>"project": "hadoop"</code> in the encryption context.
 */
public class ITestS3AEncryptionSSEKMSWithEncryptionContext
    extends AbstractTestS3AEncryption {

  private static final Set<S3AEncryptionMethods> KMS_ENCRYPTION_ALGORITHMS = ImmutableSet.of(
      SSE_KMS, DSSE_KMS);

  private S3AEncryptionMethods encryptionAlgorithm;

  @Override
  protected Configuration createConfiguration() {
    try {
      // get the KMS key and context for this test.
      Configuration c = new Configuration();
      final String bucketName = getTestBucketName(c);
      String kmsKey = S3AUtils.getS3EncryptionKey(bucketName, c);
      String encryptionContext = S3AEncryption.getS3EncryptionContext(bucketName, c);
      encryptionAlgorithm = S3AUtils.getEncryptionAlgorithm(bucketName, c);
      assume("Expected a KMS encryption algorithm",
          KMS_ENCRYPTION_ALGORITHMS.contains(encryptionAlgorithm));
      if (StringUtils.isBlank(encryptionContext)) {
        skip(S3_ENCRYPTION_CONTEXT + " is not set.");
      }
      Configuration conf = super.createConfiguration();
      S3ATestUtils.removeBaseAndBucketOverrides(conf, S3_ENCRYPTION_KEY, S3_ENCRYPTION_CONTEXT);
      conf.set(S3_ENCRYPTION_KEY, kmsKey);
      conf.set(S3_ENCRYPTION_CONTEXT, encryptionContext);
      return conf;

    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  protected S3AEncryptionMethods getSSEAlgorithm() {
    return encryptionAlgorithm;
  }
}

/*
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

package org.apache.hadoop.fs.s3a.scale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AWSUnsupportedFeatureException;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;

import java.nio.file.AccessDeniedException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfAnalyticsAcceleratorEnabled;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfEncryptionTestsDisabled;

/**
 * Concrete class that extends {@link ITestS3AHugeFilesDiskBlocks}
 * and tests huge files operations with SSE-C encryption enabled.
 * Skipped if the SSE tests are disabled.
 */
public class ITestS3AHugeFilesSSECDiskBlocks
    extends ITestS3AHugeFilesDiskBlocks {

  private static final String KEY_1
      = "4niV/jPK5VFRHY+KNb6wtqYd4xXyMgdJ9XQJpcQUVbs=";

  /**
   * Skipping tests when running against mandatory encryption bucket
   * which allows only certain encryption method.
   * S3 throw AmazonS3Exception with status 403 AccessDenied
   * then it is translated into AccessDeniedException by S3AUtils.translateException(...)
   */
  @Override
  public void setup() throws Exception {
    try {
      super.setup();
      skipIfAnalyticsAcceleratorEnabled(getConfiguration(),
          "Analytics Accelerator currently does not support SSE-C");
    } catch (AccessDeniedException | AWSUnsupportedFeatureException e) {
      skip("Bucket does not allow " + S3AEncryptionMethods.SSE_C + " encryption method");
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  protected Configuration createScaleConfiguration() {
    Configuration conf = super.createScaleConfiguration();
    removeBaseAndBucketOverrides(conf, S3_ENCRYPTION_KEY,
        S3_ENCRYPTION_ALGORITHM, SERVER_SIDE_ENCRYPTION_ALGORITHM,
        SERVER_SIDE_ENCRYPTION_KEY);
    skipIfEncryptionTestsDisabled(conf);
    conf.set(Constants.S3_ENCRYPTION_ALGORITHM,
        getSSEAlgorithm().getMethod());
    conf.set(Constants.S3_ENCRYPTION_KEY, KEY_1);
    return conf;
  }

  private S3AEncryptionMethods getSSEAlgorithm() {
    return S3AEncryptionMethods.SSE_C;
  }
}

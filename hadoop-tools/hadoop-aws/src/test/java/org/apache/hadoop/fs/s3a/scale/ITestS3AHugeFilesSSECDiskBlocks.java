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
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.S3ATestUtils;

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

  @Override
  public void setup() throws Exception {
    super.setup();
    skipIfEncryptionTestsDisabled(getConfiguration());
  }

  @Override
  protected Configuration createScaleConfiguration() {
    Configuration conf = super.createScaleConfiguration();
    S3ATestUtils.disableFilesystemCaching(conf);
    conf.set(Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM,
        getSSEAlgorithm().getMethod());
    conf.set(Constants.SERVER_SIDE_ENCRYPTION_KEY, KEY_1);
    return conf;
  }

  private S3AEncryptionMethods getSSEAlgorithm() {
    return S3AEncryptionMethods.SSE_C;
  }
}

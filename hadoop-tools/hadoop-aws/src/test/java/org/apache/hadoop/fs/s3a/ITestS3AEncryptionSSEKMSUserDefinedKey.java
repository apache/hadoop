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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.s3a.S3AEncryptionMethods.SSE_KMS;

/**
 * Concrete class that extends {@link AbstractTestS3AEncryption}
 * and tests SSE-KMS encryption.  This requires the SERVER_SIDE_ENCRYPTION_KEY
 * to be set in auth-keys.xml for it to run.
 */
public class ITestS3AEncryptionSSEKMSUserDefinedKey
    extends AbstractTestS3AEncryption {

  @Override
  protected Configuration createConfiguration() {
    // get the KMS key for this test.
    Configuration c = new Configuration();
    String kmsKey = c.get(SERVER_SIDE_ENCRYPTION_KEY);
    if (StringUtils.isBlank(kmsKey)){
      skip(SERVER_SIDE_ENCRYPTION_KEY+ " is not set for " +
          SSE_KMS.getMethod());
    }
    Configuration conf = super.createConfiguration();
    conf.set(SERVER_SIDE_ENCRYPTION_KEY, kmsKey);
    return conf;
  }

  @Override
  protected S3AEncryptionMethods getSSEAlgorithm() {
    return SSE_KMS;
  }
}

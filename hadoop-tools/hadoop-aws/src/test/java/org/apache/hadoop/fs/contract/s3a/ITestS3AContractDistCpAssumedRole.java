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

package org.apache.hadoop.fs.contract.s3a;

import org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider;

import static org.apache.hadoop.fs.s3a.Constants.ASSUMED_ROLE_ARN;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.authenticationContains;

/**
 * Run DistCP under an assumed role.
 * This is skipped if the FS is already set to run under an assumed role,
 * because it would duplicate that of the superclass.
 */
public class ITestS3AContractDistCpAssumedRole extends ITestS3AContractDistCp {

  @Override
  public void setup() throws Exception {

    super.setup();
    // check for the fs having assumed roles
    assume("No ARN for role tests", !getAssumedRoleARN().isEmpty());
    assume("Already running as an assumed role",
        !authenticationContains(getFileSystem().getConf(),
            AssumedRoleCredentialProvider.NAME));
  }

  /**
   * Probe for an ARN for the test FS.
   * @return any ARN for the (previous created) filesystem.
   */
  private String getAssumedRoleARN() {
    return getFileSystem().getConf().getTrimmed(ASSUMED_ROLE_ARN, "");
  }
}

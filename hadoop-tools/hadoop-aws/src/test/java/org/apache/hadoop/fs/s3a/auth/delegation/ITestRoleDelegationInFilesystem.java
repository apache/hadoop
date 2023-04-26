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

package org.apache.hadoop.fs.s3a.auth.delegation;

import java.nio.file.AccessDeniedException;

import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.io.Text;

import static org.apache.hadoop.fs.s3a.auth.RoleTestUtils.probeForAssumedRoleARN;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.DELEGATION_TOKEN_ROLE_BINDING;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.ROLE_TOKEN_KIND;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Subclass of the session test which checks roles; only works if
 * a role ARN has been declared.
 */
public class ITestRoleDelegationInFilesystem extends
    ITestSessionDelegationInFilesystem {

  @Override
  public void setup() throws Exception {
    super.setup();
    probeForAssumedRoleARN(getConfiguration());
  }

  @Override
  protected String getDelegationBinding() {
    return DELEGATION_TOKEN_ROLE_BINDING;
  }

  @Override
  public Text getTokenKind() {
    return ROLE_TOKEN_KIND;
  }

  /**
   * This verifies that the granted credentials only access the target bucket
   * by using the credentials in a new S3 client to query the AWS-owned landsat
   * bucket.
   * @param delegatedFS delegated FS with role-restricted access.
   * @throws Exception failure
   */
  @Override
  protected void verifyRestrictedPermissions(final S3AFileSystem delegatedFS)
      throws Exception {
    intercept(AccessDeniedException.class,
        () -> readLandsatMetadata(delegatedFS));
  }

}

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

import java.util.EnumSet;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.auth.MarshalledCredentials;
import org.apache.hadoop.fs.s3a.auth.RoleModel;
import org.apache.hadoop.io.Text;

import static org.apache.hadoop.fs.s3a.auth.RoleTestUtils.probeForAssumedRoleARN;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.DELEGATION_TOKEN_ROLE_BINDING;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.E_NO_SESSION_TOKENS_FOR_ROLE_BINDING;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.ROLE_TOKEN_KIND;
import static org.apache.hadoop.fs.s3a.auth.delegation.RoleTokenBinding.E_NO_ARN;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Rerun the session token tests with a role binding.
 * Some tests will fail as role bindings prevent certain operations.
 */
public class ITestRoleDelegationTokens extends ITestSessionDelegationTokens {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestRoleDelegationTokens.class);
  @Override
  protected String getDelegationBinding() {
    return DELEGATION_TOKEN_ROLE_BINDING;
  }

  @Override
  public Text getTokenKind() {
    return ROLE_TOKEN_KIND;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    probeForAssumedRoleARN(getConfiguration());
  }

  /**
   * Session credentials will not propagate with role tokens,
   * so the superclass's method will fail.
   * This subclass intercepts the exception which is expected.
   * @param fs base FS to bond to.
   * @param marshalledCredentials session credentials from first DT.
   * @param conf config to use
   * @return null
   * @throws Exception failure
   */
  @Override
  protected AbstractS3ATokenIdentifier verifyCredentialPropagation(
      final S3AFileSystem fs,
      final MarshalledCredentials marshalledCredentials,
      final Configuration conf) throws Exception {
    intercept(DelegationTokenIOException.class,
        E_NO_SESSION_TOKENS_FOR_ROLE_BINDING,
        () -> super.verifyCredentialPropagation(fs,
            marshalledCredentials, conf));
    return null;
  }

  @Test
  public void testBindingWithoutARN() throws Throwable {
    describe("verify that a role binding only needs a role ARN when creating"
        + " a new token");

    Configuration conf = new Configuration(getConfiguration());
    conf.unset(DelegationConstants.DELEGATION_TOKEN_ROLE_ARN);
    try (S3ADelegationTokens delegationTokens2 = new S3ADelegationTokens()) {
      final S3AFileSystem fs = getFileSystem();
      delegationTokens2.bindToFileSystem(fs.getUri(), fs);
      delegationTokens2.init(conf);
      delegationTokens2.start();

      // cannot create a DT at this point
      intercept(IllegalStateException.class,
          E_NO_ARN,
          () -> delegationTokens2.createDelegationToken(
              new EncryptionSecrets()));
    }
  }

  @Test
  public void testCreateRoleModel() throws Throwable {
    describe("self contained role model retrieval");
    EnumSet<AWSPolicyProvider.AccessLevel> access
        = EnumSet.of(
        AWSPolicyProvider.AccessLevel.READ,
        AWSPolicyProvider.AccessLevel.WRITE);
    S3AFileSystem fs = getFileSystem();
    List<RoleModel.Statement> rules = fs.listAWSPolicyRules(
        access);
    assertTrue("No AWS policy rules from FS", !rules.isEmpty());
    String ruleset = new RoleModel().toJson(new RoleModel.Policy(rules));
    LOG.info("Access policy for {}\n{}", fs.getUri(), ruleset);
  }
}

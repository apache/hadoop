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

package org.apache.hadoop.fs.s3a.auth;

import java.nio.file.AccessDeniedException;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Assume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.auth.RoleModel.*;
import static org.apache.hadoop.fs.s3a.auth.RolePolicies.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Helper class for testing roles.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class RoleTestUtils {

  private static final Logger LOG =
      LoggerFactory.getLogger(RoleTestUtils.class);

  private static final RoleModel MODEL = new RoleModel();


  /** Example ARN of a role. */
  public static final String ROLE_ARN_EXAMPLE
      = "arn:aws:iam::9878543210123:role/role-s3-restricted";


  /** Deny GET requests to all buckets. */
  public static final Statement DENY_S3_GET_OBJECT =
      statement(false, S3_ALL_BUCKETS, S3_GET_OBJECT);

  public static final Statement ALLOW_S3_GET_BUCKET_LOCATION
      =  statement(true, S3_ALL_BUCKETS, S3_GET_BUCKET_LOCATION);

  /**
   * This is AWS policy removes read access from S3, leaves S3Guard access up.
   * This will allow clients to use S3Guard list/HEAD operations, even
   * the ability to write records, but not actually access the underlying
   * data.
   * The client does need {@link RolePolicies#S3_GET_BUCKET_LOCATION} to
   * get the bucket location.
   */
  public static final Policy RESTRICTED_POLICY = policy(
      DENY_S3_GET_OBJECT, STATEMENT_ALL_DDB, ALLOW_S3_GET_BUCKET_LOCATION
      );

  private RoleTestUtils() {
  }

  /**
   * Bind the configuration's {@code ASSUMED_ROLE_POLICY} option to
   * the given policy.
   * @param conf configuration to patch
   * @param policy policy to apply
   * @return the modified configuration
   * @throws JsonProcessingException JSON marshalling error
   */
  public static Configuration bindRolePolicy(final Configuration conf,
      final Policy policy) throws JsonProcessingException {
    String p = MODEL.toJson(policy);
    LOG.info("Setting role policy to policy of size {}:\n{}", p.length(), p);
    conf.set(ASSUMED_ROLE_POLICY, p);
    return conf;
  }

  /**
   * Wrap a set of statements with a policy and bind the configuration's
   * {@code ASSUMED_ROLE_POLICY} option to it.
   * @param conf configuration to patch
   * @param statements statements to aggregate
   * @return the modified configuration
   * @throws JsonProcessingException JSON marshalling error
   */
  public static Configuration bindRolePolicyStatements(
      final Configuration conf,
      final Statement... statements) throws JsonProcessingException {
    return bindRolePolicy(conf, policy(statements));
  }


  /**
   * Try to delete a file, verify that it is not allowed.
   * @param fs filesystem
   * @param path path
   */
  public static void assertDeleteForbidden(final FileSystem fs, final Path path)
      throws Exception {
    intercept(AccessDeniedException.class, "",
        () -> fs.delete(path, true));
  }

  /**
   * Try to touch a file, verify that it is not allowed.
   * @param fs filesystem
   * @param path path
   */
  public static void assertTouchForbidden(final FileSystem fs, final Path path)
      throws Exception {
    intercept(AccessDeniedException.class, "",
        "Caller could create file at " + path,
        () -> {
          touch(fs, path);
          return fs.getFileStatus(path);
        });
  }

  /**
   * Create a config for an assumed role; it also disables FS caching.
   * @param srcConf source config: this is not modified
   * @param roleARN ARN of role
   * @return the new configuration
   */
  public static Configuration newAssumedRoleConfig(
      final Configuration srcConf,
      final String roleARN) {
    Configuration conf = new Configuration(srcConf);
    conf.set(AWS_CREDENTIALS_PROVIDER, AssumedRoleCredentialProvider.NAME);
    conf.set(ASSUMED_ROLE_ARN, roleARN);
    conf.set(ASSUMED_ROLE_SESSION_NAME, "test");
    conf.set(ASSUMED_ROLE_SESSION_DURATION, "15m");
    conf.unset(DelegationConstants.DELEGATION_TOKEN_BINDING);
    disableFilesystemCaching(conf);
    return conf;
  }

  /**
   * Assert that an operation is forbidden.
   * @param contained contained text, may be null
   * @param eval closure to evaluate
   * @param <T> type of closure
   * @return the access denied exception
   * @throws Exception any other exception
   */
  public static <T> AccessDeniedException forbidden(
      String contained,
      Callable<T> eval)
      throws Exception {
    return intercept(AccessDeniedException.class,
        contained, eval);
  }

  /**
   * Get the Assumed role referenced by ASSUMED_ROLE_ARN;
   * skip the test if it is unset.
   * @param conf config
   * @return the string
   */
  public static String probeForAssumedRoleARN(Configuration conf) {
    String arn = conf.getTrimmed(ASSUMED_ROLE_ARN, "");
    Assume.assumeTrue("No ARN defined in " + ASSUMED_ROLE_ARN,
        !arn.isEmpty());
    return arn;
  }

  /**
   * Assert that credentials are equal without printing secrets.
   * Different assertions will have different message details.
   * @param message message to use as base of error.
   * @param expected expected credentials
   * @param actual actual credentials.
   */
  public static void assertCredentialsEqual(final String message,
      final MarshalledCredentials expected,
      final MarshalledCredentials actual) {
    // DO NOT use assertEquals() here, as that could print a secret to
    // the test report.
    assertEquals(message + ": access key",
        expected.getAccessKey(),
        actual.getAccessKey());
    assertTrue(message + ": secret key",
        expected.getSecretKey().equals(actual.getSecretKey()));
    assertEquals(message + ": session token",
        expected.getSessionToken(),
        actual.getSessionToken());

  }
}

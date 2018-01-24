/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.util.concurrent.Callable;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Tests use of assumed roles.
 * Only run if an assumed role is provided.
 */
public class ITestAssumeRole extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestAssumeRole.class);

  private static final String ARN_EXAMPLE
      = "arn:aws:kms:eu-west-1:00000000000:key/" +
      "0000000-16c9-4832-a1a9-c8bbef25ec8b";

  private static final String E_BAD_ROLE
      = "Not authorized to perform sts:AssumeRole";

  /**
   * This is AWS policy removes read access.
   */
  public static final String RESTRICTED_POLICY = "{\n"
      + "   \"Version\": \"2012-10-17\",\n"
      + "   \"Statement\": [{\n"
      + "      \"Effect\": \"Deny\",\n"
      + "      \"Action\": \"s3:ListObjects\",\n"
      + "      \"Resource\": \"*\"\n"
      + "    }\n"
      + "   ]\n"
      + "}";

  private void assumeRoleTests() {
    assume("No ARN for role tests", !getAssumedRoleARN().isEmpty());
  }

  private String getAssumedRoleARN() {
    return getContract().getConf().getTrimmed(ASSUMED_ROLE_ARN, "");
  }

  /**
   * Expect a filesystem to fail to instantiate.
   * @param conf config to use
   * @param clazz class of exception to expect
   * @param text text in exception
   * @param <E> type of exception as inferred from clazz
   * @throws Exception if the exception was the wrong class
   */
  private <E extends Throwable> void expectFileSystemFailure(
      Configuration conf,
      Class<E> clazz,
      String text) throws Exception {
    interceptC(clazz,
        text,
        () -> new Path(getFileSystem().getUri()).getFileSystem(conf));
  }

  /**
   * Experimental variant of intercept() which closes any Closeable
   * returned.
   */
  private static <E extends Throwable> E interceptC(
      Class<E> clazz, String text,
      Callable<Closeable> eval)
      throws Exception {

    return intercept(clazz, text,
        () -> {
          try (Closeable c = eval.call()) {
            return c.toString();
          }
        });
  }

  @Test
  public void testCreateCredentialProvider() throws IOException {
    assumeRoleTests();
    describe("Create the credential provider");

    String roleARN = getAssumedRoleARN();

    Configuration conf = new Configuration(getContract().getConf());
    conf.set(AWS_CREDENTIALS_PROVIDER, AssumedRoleCredentialProvider.NAME);
    conf.set(ASSUMED_ROLE_ARN, roleARN);
    conf.set(ASSUMED_ROLE_SESSION_NAME, "valid");
    conf.set(ASSUMED_ROLE_SESSION_DURATION, "45m");
    conf.set(ASSUMED_ROLE_POLICY, RESTRICTED_POLICY);
    try (AssumedRoleCredentialProvider provider
             = new AssumedRoleCredentialProvider(conf)) {
      LOG.info("Provider is {}", provider);
      AWSCredentials credentials = provider.getCredentials();
      assertNotNull("Null credentials from " + provider, credentials);
    }
  }

  @Test
  public void testAssumeRoleCreateFS() throws IOException {
    assumeRoleTests();
    describe("Create an FS client with the role and do some basic IO");

    String roleARN = getAssumedRoleARN();
    Configuration conf = createAssumedRoleConfig(roleARN);
    conf.set(ASSUMED_ROLE_SESSION_NAME, "valid");
    conf.set(ASSUMED_ROLE_SESSION_DURATION, "45m");
    Path path = new Path(getFileSystem().getUri());
    LOG.info("Creating test FS and user {} with assumed role {}",
        conf.get(ACCESS_KEY), roleARN);

    try (FileSystem fs = path.getFileSystem(conf)) {
      fs.getFileStatus(new Path("/"));
      fs.mkdirs(path("testAssumeRoleFS"));
    }
  }

  @Test
  public void testAssumeRoleRestrictedPolicyFS() throws Exception {
    assumeRoleTests();
    describe("Restrict the policy for this session; verify that reads fail");

    String roleARN = getAssumedRoleARN();
    Configuration conf = createAssumedRoleConfig(roleARN);
    conf.set(ASSUMED_ROLE_POLICY, RESTRICTED_POLICY);
    Path path = new Path(getFileSystem().getUri());
    try (FileSystem fs = path.getFileSystem(conf)) {
      intercept(AccessDeniedException.class, "getFileStatus",
          () -> fs.getFileStatus(new Path("/")));
      intercept(AccessDeniedException.class, "getFileStatus",
          () -> fs.listStatus(new Path("/")));
      intercept(AccessDeniedException.class, "getFileStatus",
          () -> fs.mkdirs(path("testAssumeRoleFS")));
    }
  }

  @Test
  public void testAssumeRoleFSBadARN() throws Exception {
    assumeRoleTests();
    describe("Attemnpt to create the FS with an invalid ARN");
    Configuration conf = createAssumedRoleConfig(getAssumedRoleARN());
    conf.set(ASSUMED_ROLE_ARN, ARN_EXAMPLE);
    expectFileSystemFailure(conf, AccessDeniedException.class, E_BAD_ROLE);
  }

  @Test
  public void testAssumeRoleNoARN() throws Exception {
    assumeRoleTests();
    describe("Attemnpt to create the FS with no ARN");
    Configuration conf = createAssumedRoleConfig(getAssumedRoleARN());
    conf.unset(ASSUMED_ROLE_ARN);
    expectFileSystemFailure(conf,
        IOException.class,
        AssumedRoleCredentialProvider.E_NO_ROLE);
  }

  @Test
  public void testAssumeRoleFSBadPolicy() throws Exception {
    assumeRoleTests();
    describe("Attemnpt to create the FS with malformed JSON");
    Configuration conf = createAssumedRoleConfig(getAssumedRoleARN());
    // add some malformed JSON
    conf.set(ASSUMED_ROLE_POLICY, "}");
    expectFileSystemFailure(conf,
        AWSBadRequestException.class,
        "JSON");
  }

  @Test
  public void testAssumeRoleFSBadPolicy2() throws Exception {
    assumeRoleTests();
    describe("Attemnpt to create the FS with valid but non-compliant JSON");
    Configuration conf = createAssumedRoleConfig(getAssumedRoleARN());
    // add some invalid JSON
    conf.set(ASSUMED_ROLE_POLICY, "{'json':'but not what AWS wants}");
    expectFileSystemFailure(conf,
        AWSBadRequestException.class,
        "Syntax errors in policy");
  }

  @Test
  public void testAssumeRoleCannotAuthAssumedRole() throws Exception {
    assumeRoleTests();
    describe("Assert that you can't use assumed roles to auth assumed roles");

    Configuration conf = createAssumedRoleConfig(getAssumedRoleARN());
    conf.set(ASSUMED_ROLE_CREDENTIALS_PROVIDER,
        AssumedRoleCredentialProvider.NAME);
    expectFileSystemFailure(conf,
        IOException.class,
        AssumedRoleCredentialProvider.E_FORBIDDEN_PROVIDER);
  }

  @Test
  public void testAssumeRoleBadInnerAuth() throws Exception {
    assumeRoleTests();
    describe("Try to authenticate with a keypair with spaces");

    Configuration conf = createAssumedRoleConfig(getAssumedRoleARN());
    conf.set(ASSUMED_ROLE_CREDENTIALS_PROVIDER,
        SimpleAWSCredentialsProvider.NAME);
    conf.set(ACCESS_KEY, "not valid");
    conf.set(SECRET_KEY, "not secret");
    expectFileSystemFailure(conf, AWSBadRequestException.class, "not a valid " +
        "key=value pair (missing equal-sign) in Authorization header");
  }

  @Test
  public void testAssumeRoleBadInnerAuth2() throws Exception {
    assumeRoleTests();
    describe("Try to authenticate with an invalid keypair");

    Configuration conf = createAssumedRoleConfig(getAssumedRoleARN());
    conf.set(ASSUMED_ROLE_CREDENTIALS_PROVIDER,
        SimpleAWSCredentialsProvider.NAME);
    conf.set(ACCESS_KEY, "notvalid");
    conf.set(SECRET_KEY, "notsecret");
    expectFileSystemFailure(conf, AccessDeniedException.class,
        "The security token included in the request is invalid");
  }

  @Test
  public void testAssumeRoleBadSession() throws Exception {
    assumeRoleTests();
    describe("Try to authenticate with an invalid session");

    Configuration conf = createAssumedRoleConfig(getAssumedRoleARN());
    conf.set(ASSUMED_ROLE_SESSION_NAME,
        "Session Names cannot Hava Spaces!");
    expectFileSystemFailure(conf, AWSBadRequestException.class,
        "Member must satisfy regular expression pattern");
  }

  /**
   * Create a config for an assumed role; it also disables FS caching.
   * @param roleARN ARN of role
   * @return the configuration
   */
  private Configuration createAssumedRoleConfig(String roleARN) {
    Configuration conf = new Configuration(getContract().getConf());
    conf.set(AWS_CREDENTIALS_PROVIDER, AssumedRoleCredentialProvider.NAME);
    conf.set(ASSUMED_ROLE_ARN, roleARN);
    disableFilesystemCaching(conf);
    return conf;
  }

  @Test
  public void testAssumedRoleCredentialProviderValidation() throws Throwable {
    Configuration conf = new Configuration();
    conf.set(ASSUMED_ROLE_ARN, "");
    interceptC(IOException.class,
        AssumedRoleCredentialProvider.E_NO_ROLE,
        () -> new AssumedRoleCredentialProvider(conf));
  }

  @Test
  public void testAssumedDuration() throws Throwable {
    assumeRoleTests();
    describe("Expect the constructor to fail if the session is to short");
    Configuration conf = new Configuration();
    conf.set(ASSUMED_ROLE_SESSION_DURATION, "30s");
    interceptC(IllegalArgumentException.class, "",
        () -> new AssumedRoleCredentialProvider(conf));
  }

  @Test
  public void testAssumedInvalidRole() throws Throwable {
    assumeRoleTests();
    describe("Expect the constructor to fail if the role is invalid");
    Configuration conf = new Configuration();
    conf.set(ASSUMED_ROLE_ARN, ARN_EXAMPLE);
    interceptC(AWSSecurityTokenServiceException.class,
        E_BAD_ROLE,
        () -> new AssumedRoleCredentialProvider(conf));
  }

  /**
   * This is here to check up on the S3ATestUtils probes themselves.
   * @see S3ATestUtils#authenticationContains(Configuration, String).
   */
  @Test
  public void testauthenticationContainsProbes() {
    Configuration conf = new Configuration(false);
    assertFalse("found AssumedRoleCredentialProvider",
        authenticationContains(conf, AssumedRoleCredentialProvider.NAME));

    conf.set(AWS_CREDENTIALS_PROVIDER, AssumedRoleCredentialProvider.NAME);
    assertTrue("didn't find AssumedRoleCredentialProvider",
        authenticationContains(conf, AssumedRoleCredentialProvider.NAME));
  }
}

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

package org.apache.hadoop.fs.s3a.auth.delegation;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.auth.MarshalledCredentials;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assumeSessionTestsEnabled;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.roundTrip;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.unsetHadoopCredentialProviders;
import static org.apache.hadoop.fs.s3a.auth.MarshalledCredentialBinding.fromAWSCredentials;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.DELEGATION_TOKEN_CREDENTIALS_PROVIDER;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.DELEGATION_TOKEN_SESSION_BINDING;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.SESSION_TOKEN_KIND;
import static org.apache.hadoop.fs.s3a.auth.delegation.SessionTokenBinding.CREDENTIALS_CONVERTED_TO_DELEGATION_TOKEN;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests use of Hadoop delegation tokens to marshall S3 credentials.
 */
public class ITestSessionDelegationTokens extends AbstractDelegationIT {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestSessionDelegationTokens.class);

  public static final String KMS_KEY = "arn:kms:key";

  private S3ADelegationTokens delegationTokens;

  /**
   * Get the delegation token binding for this test suite.
   * @return which DT binding to use.
   */
  protected String getDelegationBinding() {
    return DELEGATION_TOKEN_SESSION_BINDING;
  }

  public Text getTokenKind() {
    return SESSION_TOKEN_KIND;
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    enableDelegationTokens(conf, getDelegationBinding());
    return conf;
  }

  @BeforeEach
  @Override
  public void setup() throws Exception {
    super.setup();
    assumeSessionTestsEnabled(getConfiguration());
    resetUGI();
    delegationTokens = instantiateDTSupport(getConfiguration());
    delegationTokens.start();
  }

  @AfterEach
  @Override
  public void teardown() throws Exception {
    IOUtils.cleanupWithLogger(LOG, delegationTokens);
    resetUGI();
    super.teardown();
  }

  /**
   * Checks here to catch any regressions in canonicalization
   * logic.
   */
  @Test
  public void testCanonicalization() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    assertEquals(0, fs.getDefaultPort(),
        "Default port has changed");
    URI uri = fs.getCanonicalUri();
    String service = fs.getCanonicalServiceName();
    assertEquals(uri, new URI(service),
        "canonical URI and service name mismatch");
  }

  @Test
  public void testSaveLoadTokens() throws Throwable {
    File tokenFile = File.createTempFile("token", "bin");
    EncryptionSecrets encryptionSecrets = new EncryptionSecrets(
        S3AEncryptionMethods.SSE_KMS, KMS_KEY, "");
    Token<AbstractS3ATokenIdentifier> dt
        = delegationTokens.createDelegationToken(encryptionSecrets, null);
    final SessionTokenIdentifier origIdentifier
        = (SessionTokenIdentifier) dt.decodeIdentifier();
    assertEquals(getTokenKind(), dt.getKind(), "kind in " + dt);
    Configuration conf = getConfiguration();
    saveDT(tokenFile, dt);
    assertTrue(tokenFile.length() > 0, "Empty token file");
    Credentials creds = Credentials.readTokenStorageFile(tokenFile, conf);
    Text serviceId = delegationTokens.getService();
    Token<? extends TokenIdentifier> token = requireNonNull(
        creds.getToken(serviceId),
        () -> "No token for \"" + serviceId + "\" in: " + creds.getAllTokens());
    SessionTokenIdentifier decoded =
        (SessionTokenIdentifier) token.decodeIdentifier();
    decoded.validate();
    assertEquals(origIdentifier, decoded, "token identifier ");
    assertEquals(origIdentifier.getOrigin(), decoded.getOrigin(),
        "Origin in " + decoded);
    assertEquals(origIdentifier.getExpiryTime(), decoded.getExpiryTime(),
        "Expiry time");
    assertEquals(encryptionSecrets, decoded.getEncryptionSecrets(),
        "Encryption Secrets");
  }

  /**
   * This creates a DT from a set of credentials, then verifies
   * that you can use the round-tripped credentials as a source of
   * authentication for another DT binding, and when
   * that is asked for a DT token, the secrets it returns are
   * the same as the original.
   *
   * That is different from DT propagation, as here the propagation
   * is by setting the fs.s3a session/secret/id keys from the marshalled
   * values, and using session token auth.
   * This verifies that session token authentication can be used
   * for DT credential auth, and that new tokens aren't created.
   *
   * From a testing perspective, this is not as "good" as having
   * separate tests, but given the effort to create session tokens
   * is all hidden in the first FS, it is actually easier to write
   * and now forms an extra test on those generated tokens as well
   * as the marshalling.
   */
  @Test
  public void testCreateAndUseDT() throws Throwable {
    describe("Create a Delegation Token, round trip then reuse");

    final S3AFileSystem fs = getFileSystem();
    final Configuration conf = fs.getConf();

    assertNull(delegationTokens.selectTokenFromFSOwner(),
        "Current User has delegation token");
    EncryptionSecrets secrets = new EncryptionSecrets(
        S3AEncryptionMethods.SSE_KMS, KMS_KEY, "");
    Token<AbstractS3ATokenIdentifier> originalDT
        = delegationTokens.createDelegationToken(secrets, null);
    assertEquals(getTokenKind(), originalDT.getKind(), "Token kind mismatch");

    // decode to get the binding info
    SessionTokenIdentifier issued =
        requireNonNull(
            (SessionTokenIdentifier) originalDT.decodeIdentifier(),
            () -> "no identifier in " + originalDT);
    issued.validate();

    final MarshalledCredentials creds;
    try(S3ADelegationTokens dt2 = instantiateDTSupport(getConfiguration())) {
      dt2.start();
      // first creds are good
      dt2.getCredentialProviders().resolveCredentials();

      // reset to the original dt

      dt2.resetTokenBindingToDT(originalDT);
      final AwsSessionCredentials awsSessionCreds
          = verifySessionCredentials(
              dt2.getCredentialProviders().resolveCredentials());
      final MarshalledCredentials origCreds = fromAWSCredentials(
          awsSessionCreds);

      Token<AbstractS3ATokenIdentifier> boundDT =
          dt2.getBoundOrNewDT(secrets, null);
      assertEquals(originalDT, boundDT, "Delegation Tokens");
      // simulate marshall and transmission
      creds = roundTrip(origCreds, conf);
      SessionTokenIdentifier reissued
          = (SessionTokenIdentifier) dt2.createDelegationToken(secrets, null)
          .decodeIdentifier();
      reissued.validate();
      String userAgentField = dt2.getUserAgentField();
      assertThat(userAgentField).contains(issued.getUuid()).
          as("UA field does not contain UUID");
    }

    // now use those chained credentials to create a new FS instance
    // and then get a session DT from it and expect equality
    verifyCredentialPropagation(fs, creds, new Configuration(conf));
  }

  @Test
  public void testCreateWithRenewer() throws Throwable {
    describe("Create a Delegation Token, round trip then reuse");

    final S3AFileSystem fs = getFileSystem();
    final Configuration conf = fs.getConf();
    final Text renewer = new Text("yarn");

    assertNull(delegationTokens.selectTokenFromFSOwner(),
        "Current User has delegation token");
    EncryptionSecrets secrets = new EncryptionSecrets(
        S3AEncryptionMethods.SSE_KMS, KMS_KEY, "");
    Token<AbstractS3ATokenIdentifier> dt
        = delegationTokens.createDelegationToken(secrets, renewer);
    assertEquals(getTokenKind(), dt.getKind(), "Token kind mismatch");

    // decode to get the binding info
    SessionTokenIdentifier issued =
        requireNonNull(
            (SessionTokenIdentifier) dt.decodeIdentifier(),
            () -> "no identifier in " + dt);
    issued.validate();
    assertEquals(renewer, issued.getRenewer(), "Token renewer mismatch");
  }

  /**
   * This verifies that AWS Session credentials can be picked up and
   * returned in a DT.
   * With a session binding, this holds; for role binding it will fail.
   * @param fs base FS to bond to.
   * @param session session credentials from first DT.
   * @param conf config to use
   * @return the retrieved DT. This is only for error reporting.
   * @throws IOException failure.
   */
  @SuppressWarnings({"OptionalGetWithoutIsPresent"})
  protected AbstractS3ATokenIdentifier verifyCredentialPropagation(
      final S3AFileSystem fs,
      final MarshalledCredentials session,
      final Configuration conf)
      throws Exception {
    describe("Verify Token Propagation");
    // clear any credential paths to ensure they don't get picked up and used
    // for authentication.
    unsetHadoopCredentialProviders(conf);
    conf.set(DELEGATION_TOKEN_CREDENTIALS_PROVIDER, TemporaryAWSCredentialsProvider.NAME);
    session.setSecretsInConfiguration(conf);
    try(S3ADelegationTokens delegationTokens2 = new S3ADelegationTokens()) {
      delegationTokens2.bindToFileSystem(
          fs.getCanonicalUri(),
          fs.createStoreContext(),
          fs.createDelegationOperations());
      delegationTokens2.init(conf);
      delegationTokens2.start();

      final Token<AbstractS3ATokenIdentifier> newDT
          = delegationTokens2.getBoundOrNewDT(new EncryptionSecrets(), null);
      delegationTokens2.resetTokenBindingToDT(newDT);
      final AbstractS3ATokenIdentifier boundId
          = delegationTokens2.getDecodedIdentifier().get();

      LOG.info("Regenerated DT is {}", newDT);
      final MarshalledCredentials creds2 = fromAWSCredentials(
          verifySessionCredentials(
              delegationTokens2.getCredentialProviders().resolveCredentials()));
      assertEquals(session, creds2, "Credentials");
      assertTrue(boundId.getOrigin()
          .contains(CREDENTIALS_CONVERTED_TO_DELEGATION_TOKEN),
          "Origin in " + boundId);
      return boundId;
    }
  }

  private AwsSessionCredentials verifySessionCredentials(
      final AwsCredentials creds) {
    AwsSessionCredentials session = (AwsSessionCredentials) creds;
    assertNotNull(session.accessKeyId(), "access key");
    assertNotNull(session.secretAccessKey(), "secret key");
    assertNotNull(session.sessionToken(), "session token");
    return session;
  }

  @Test
  public void testDBindingReentrancyLock() throws Throwable {
    describe("Verify that S3ADelegationTokens cannot be bound twice when there"
        + " is no token");
    S3ADelegationTokens delegation = instantiateDTSupport(getConfiguration());
    delegation.start();
    assertFalse(delegation.isBoundToDT(),
        "Delegation is bound to a DT: " + delegation);
  }

}

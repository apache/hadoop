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

import java.net.URI;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.S3ATestConstants;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.auth.MarshalledCredentialBinding;
import org.apache.hadoop.fs.s3a.auth.MarshalledCredentials;
import org.apache.hadoop.fs.s3a.auth.delegation.providers.InjectingTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.HadoopTestBase;

import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.FULL_TOKEN_KIND;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.SESSION_TOKEN_KIND;
import static org.apache.hadoop.fs.s3a.auth.delegation.S3ADtFetcher.FETCH_FAILED;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Unit tests related to S3A DT support.
 */
public class TestS3ADelegationTokenSupport extends HadoopTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestS3ADelegationTokenSupport.class);

  private static URI landsatUri;

  @BeforeClass
  public static void classSetup() throws Exception {
    landsatUri = new URI(S3ATestConstants.DEFAULT_CSVTEST_FILE);
  }

  @Test
  public void testToStringWhenNotInited() throws Throwable {
    LOG.info("tokens " +
        new S3ADelegationTokens().toString());
  }
  @Test
  public void testSessionTokenKind() throws Throwable {
    AbstractS3ATokenIdentifier identifier
        = new SessionTokenIdentifier();
    assertEquals(SESSION_TOKEN_KIND, identifier.getKind());
  }

  @Test
  public void testSessionTokenDecode() throws Throwable {
    Text alice = new Text("alice");
    Text renewer = new Text("yarn");
    AbstractS3ATokenIdentifier identifier
        = new SessionTokenIdentifier(SESSION_TOKEN_KIND,
        alice,
        renewer,
        new URI("s3a://landsat-pds/"),
        new MarshalledCredentials("a", "b", ""),
        new EncryptionSecrets(S3AEncryptionMethods.SSE_S3, ""),
        "origin");
    Token<AbstractS3ATokenIdentifier> t1 =
        new Token<>(identifier,
            new SessionSecretManager());
    AbstractS3ATokenIdentifier decoded = t1.decodeIdentifier();
    decoded.validate();
    MarshalledCredentials creds
        = ((SessionTokenIdentifier) decoded).getMarshalledCredentials();
    assertNotNull("credentials",
        MarshalledCredentialBinding.toAWSCredentials(creds,
        MarshalledCredentials.CredentialTypeRequired.AnyNonEmpty, ""));
    assertEquals(alice, decoded.getOwner());
    UserGroupInformation decodedUser = decoded.getUser();
    assertEquals("name of " + decodedUser,
        "alice",
        decodedUser.getUserName());
    assertEquals("renewer", renewer, decoded.getRenewer());
    assertEquals("Authentication method of " + decodedUser,
        UserGroupInformation.AuthenticationMethod.TOKEN,
        decodedUser.getAuthenticationMethod());
    assertEquals("origin", decoded.getOrigin());
  }

  @Test
  public void testFullTokenKind() throws Throwable {
    AbstractS3ATokenIdentifier identifier
        = new FullCredentialsTokenIdentifier();
    assertEquals(FULL_TOKEN_KIND, identifier.getKind());
  }

  @Test
  public void testSessionTokenIdentifierRoundTrip() throws Throwable {
    Text renewer = new Text("yarn");
    SessionTokenIdentifier id = new SessionTokenIdentifier(
        SESSION_TOKEN_KIND,
        new Text(),
        renewer,
        landsatUri,
        new MarshalledCredentials("a", "b", "c"),
        new EncryptionSecrets(), "");

    SessionTokenIdentifier result = S3ATestUtils.roundTrip(id, null);
    String ids = id.toString();
    assertEquals("URI in " + ids, id.getUri(), result.getUri());
    assertEquals("credentials in " + ids,
        id.getMarshalledCredentials(),
        result.getMarshalledCredentials());
    assertEquals("renewer in " + ids, renewer, id.getRenewer());
  }

  @Test
  public void testSessionTokenIdentifierRoundTripNoRenewer() throws Throwable {
    SessionTokenIdentifier id = new SessionTokenIdentifier(
        SESSION_TOKEN_KIND,
        new Text(),
        null,
        landsatUri,
        new MarshalledCredentials("a", "b", "c"),
        new EncryptionSecrets(), "");

    SessionTokenIdentifier result = S3ATestUtils.roundTrip(id, null);
    String ids = id.toString();
    assertEquals("URI in " + ids, id.getUri(), result.getUri());
    assertEquals("credentials in " + ids,
        id.getMarshalledCredentials(),
        result.getMarshalledCredentials());
    assertEquals("renewer in " + ids, new Text(), id.getRenewer());
  }

  @Test
  public void testRoleTokenIdentifierRoundTrip() throws Throwable {
    RoleTokenIdentifier id = new RoleTokenIdentifier(
        landsatUri,
        new Text(),
        new Text(),
        new MarshalledCredentials("a", "b", "c"),
        new EncryptionSecrets(), "");

    RoleTokenIdentifier result = S3ATestUtils.roundTrip(id, null);
    String ids = id.toString();
    assertEquals("URI in " + ids, id.getUri(), result.getUri());
    assertEquals("credentials in " + ids,
        id.getMarshalledCredentials(),
        result.getMarshalledCredentials());
    assertEquals("renewer in " + ids, new Text(), id.getRenewer());
  }

  @Test
  public void testFullTokenIdentifierRoundTrip() throws Throwable {
    Text renewer = new Text("renewerName");
    FullCredentialsTokenIdentifier id = new FullCredentialsTokenIdentifier(
        landsatUri,
        new Text(),
        renewer,
        new MarshalledCredentials("a", "b", ""),
        new EncryptionSecrets(), "");

    FullCredentialsTokenIdentifier result = S3ATestUtils.roundTrip(id, null);
    String ids = id.toString();
    assertEquals("URI in " + ids, id.getUri(), result.getUri());
    assertEquals("credentials in " + ids,
        id.getMarshalledCredentials(),
        result.getMarshalledCredentials());
    assertEquals("renewer in " + ids, renewer, result.getRenewer());
  }
  @Test
  public void testInjectingTokenIdentifierRoundTrip() throws Throwable {
    Text renewer = new Text("bob");
    int issueNumber = 6502;
    InjectingTokenIdentifier id = new InjectingTokenIdentifier(
        landsatUri,
        new Text(),
        renewer,
        new EncryptionSecrets(),
        issueNumber);

    InjectingTokenIdentifier result = S3ATestUtils.roundTrip(id, null);
    String ids = id.toString();
    assertEquals("URI in " + ids, id.getUri(), result.getUri());
    assertEquals("issue number in " + ids,
        issueNumber,
        result.getIssueNumber());
    assertEquals("renewer in " + ids, renewer, result.getRenewer());
  }

  @Test
  public void testS3ADtFetcher() throws Throwable {
    LocalFileSystem lfs = FileSystem.getLocal(new Configuration());
    intercept(DelegationTokenIOException.class, FETCH_FAILED, () ->
        S3ADtFetcher.addFSDelegationTokens(lfs, null, new Credentials()));
  }

  /**
   * The secret manager always uses the same secret; the
   * factory for new identifiers is that of the token manager.
   */
  private static final class SessionSecretManager
      extends SecretManager<AbstractS3ATokenIdentifier> {

    @Override
    protected byte[] createPassword(AbstractS3ATokenIdentifier identifier) {
      return "PASSWORD".getBytes();
    }

    @Override
    public byte[] retrievePassword(AbstractS3ATokenIdentifier identifier)
        throws InvalidToken {
      return "PASSWORD".getBytes();
    }

    @Override
    public AbstractS3ATokenIdentifier createIdentifier() {
      return new SessionTokenIdentifier();
    }
  }

}

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
import java.nio.charset.StandardCharsets;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.auth.MarshalledCredentialBinding;
import org.apache.hadoop.fs.s3a.auth.MarshalledCredentials;
import org.apache.hadoop.fs.s3a.test.PublicDatasetTestUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;

import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.FULL_TOKEN_KIND;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.SESSION_TOKEN_KIND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests related to S3A DT support.
 */
public class TestS3ADelegationTokenSupport {

  private static URI externalUri;

  @BeforeClass
  public static void classSetup() throws Exception {
    externalUri = new URI(PublicDatasetTestUtils.DEFAULT_EXTERNAL_FILE);
  }

  @Test
  public void testSessionTokenKind() throws Throwable {
    AbstractS3ATokenIdentifier identifier
        = new SessionTokenIdentifier();
    assertEquals(SESSION_TOKEN_KIND, identifier.getKind());
  }

  @Test
  public void testSessionTokenIssueDate() throws Throwable {
    AbstractS3ATokenIdentifier identifier
        = new SessionTokenIdentifier();
    assertEquals(SESSION_TOKEN_KIND, identifier.getKind());
    assertTrue("issue date is not set", identifier.getIssueDate() > 0L);
  }

  @Test
  public void testSessionTokenDecode() throws Throwable {
    Text alice = new Text("alice");
    Text renewer = new Text("yarn");
    String encryptionKey = "encryptionKey";
    String encryptionContextJson = "{\"key\":\"value\", \"key2\": \"value3\"}";
    String encryptionContextEncoded = Base64.encodeBase64String(encryptionContextJson.getBytes(
        StandardCharsets.UTF_8));
    AbstractS3ATokenIdentifier identifier
        = new SessionTokenIdentifier(SESSION_TOKEN_KIND,
        alice,
        renewer,
        new URI("s3a://anything/"),
        new MarshalledCredentials("a", "b", ""),
        new EncryptionSecrets(S3AEncryptionMethods.SSE_S3, encryptionKey, encryptionContextEncoded),
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
    assertEquals("issue date", identifier.getIssueDate(),
        decoded.getIssueDate());
    EncryptionSecrets encryptionSecrets = decoded.getEncryptionSecrets();
    assertEquals(S3AEncryptionMethods.SSE_S3, encryptionSecrets.getEncryptionMethod());
    assertEquals(encryptionKey, encryptionSecrets.getEncryptionKey());
    assertEquals(encryptionContextEncoded, encryptionSecrets.getEncryptionContext());
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
    String encryptionKey = "encryptionKey";
    String encryptionContextJson = "{\"key\":\"value\", \"key2\": \"value3\"}";
    String encryptionContextEncoded = Base64.encodeBase64String(encryptionContextJson.getBytes(
        StandardCharsets.UTF_8));
    SessionTokenIdentifier id = new SessionTokenIdentifier(
        SESSION_TOKEN_KIND,
        new Text(),
        renewer,
        externalUri,
        new MarshalledCredentials("a", "b", "c"),
        new EncryptionSecrets(S3AEncryptionMethods.DSSE_KMS, encryptionKey,
            encryptionContextEncoded),
        "");

    SessionTokenIdentifier result = S3ATestUtils.roundTrip(id, null);
    String ids = id.toString();
    assertEquals("URI in " + ids, id.getUri(), result.getUri());
    assertEquals("credentials in " + ids,
        id.getMarshalledCredentials(),
        result.getMarshalledCredentials());
    assertEquals("renewer in " + ids, renewer, id.getRenewer());
    EncryptionSecrets encryptionSecrets = result.getEncryptionSecrets();
    assertEquals(S3AEncryptionMethods.DSSE_KMS, encryptionSecrets.getEncryptionMethod());
    assertEquals(encryptionKey, encryptionSecrets.getEncryptionKey());
    assertEquals(encryptionContextEncoded, encryptionSecrets.getEncryptionContext());
  }

  @Test
  public void testSessionTokenIdentifierRoundTripNoRenewer() throws Throwable {
    SessionTokenIdentifier id = new SessionTokenIdentifier(
        SESSION_TOKEN_KIND,
        new Text(),
        null,
        externalUri,
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
        externalUri,
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
        externalUri,
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

  /**
   * The secret manager always uses the same secret; the
   * factory for new identifiers is that of the token manager.
   */
  private  class SessionSecretManager
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

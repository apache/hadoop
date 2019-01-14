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

import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.S3ATestConstants;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.auth.MarshalledCredentialBinding;
import org.apache.hadoop.fs.s3a.auth.MarshalledCredentials;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;

import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.FULL_TOKEN_KIND;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.SESSION_TOKEN_KIND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Unit tests related to S3A DT support.
 */
public class TestS3ADelegationTokenSupport {

  private static URI landsatUri;

  @BeforeClass
  public static void classSetup() throws Exception {
    landsatUri = new URI(S3ATestConstants.DEFAULT_CSVTEST_FILE);
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
    AbstractS3ATokenIdentifier identifier
        = new SessionTokenIdentifier(SESSION_TOKEN_KIND,
        alice,
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
    SessionTokenIdentifier id = new SessionTokenIdentifier(
        SESSION_TOKEN_KIND,
        new Text(),
        landsatUri,
        new MarshalledCredentials("a", "b", "c"),
        new EncryptionSecrets(), "");

    SessionTokenIdentifier result = S3ATestUtils.roundTrip(id, null);
    String ids = id.toString();
    assertEquals("URI in " + ids, id.getUri(), result.getUri());
    assertEquals("credentials in " + ids,
        id.getMarshalledCredentials(),
        result.getMarshalledCredentials());
  }

  @Test
  public void testRoleTokenIdentifierRoundTrip() throws Throwable {
    RoleTokenIdentifier id = new RoleTokenIdentifier(
        landsatUri,
        new Text(),
        new MarshalledCredentials("a", "b", "c"),
        new EncryptionSecrets(), "");

    RoleTokenIdentifier result = S3ATestUtils.roundTrip(id, null);
    String ids = id.toString();
    assertEquals("URI in " + ids, id.getUri(), result.getUri());
    assertEquals("credentials in " + ids,
        id.getMarshalledCredentials(),
        result.getMarshalledCredentials());
  }

  @Test
  public void testFullTokenIdentifierRoundTrip() throws Throwable {
    FullCredentialsTokenIdentifier id = new FullCredentialsTokenIdentifier(
        landsatUri,
        new Text(),
        new MarshalledCredentials("a", "b", ""),
        new EncryptionSecrets(), "");

    FullCredentialsTokenIdentifier result = S3ATestUtils.roundTrip(id, null);
    String ids = id.toString();
    assertEquals("URI in " + ids, id.getUri(), result.getUri());
    assertEquals("credentials in " + ids,
        id.getMarshalledCredentials(),
        result.getMarshalledCredentials());
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

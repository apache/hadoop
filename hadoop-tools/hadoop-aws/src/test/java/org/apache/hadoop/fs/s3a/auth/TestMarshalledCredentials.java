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

import java.net.URI;
import java.net.URISyntaxException;

import com.amazonaws.auth.AWSCredentials;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecrets;
import org.apache.hadoop.test.HadoopTestBase;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Unit test of marshalled credential support.
 */
public class TestMarshalledCredentials extends HadoopTestBase {

  private MarshalledCredentials credentials;

  private int expiration;

  private URI bucketURI;

  @Before
  public void createSessionToken() throws URISyntaxException {
    bucketURI = new URI("s3a://bucket1");
    credentials = new MarshalledCredentials("accessKey",
        "secretKey", "sessionToken");
    credentials.setRoleARN("roleARN");
    expiration = 1970;
    credentials.setExpiration(expiration);
  }

  @Test
  public void testRoundTrip() throws Throwable {
    MarshalledCredentials c2 = S3ATestUtils.roundTrip(this.credentials,
        new Configuration());
    assertEquals(credentials, c2);
    assertEquals("accessKey", c2.getAccessKey());
    assertEquals("secretKey", c2.getSecretKey());
    assertEquals("sessionToken", c2.getSessionToken());
    assertEquals(expiration, c2.getExpiration());
    assertEquals(credentials, c2);
  }

  @Test
  public void testRoundTripNoSessionData() throws Throwable {
    MarshalledCredentials c = new MarshalledCredentials();
    c.setAccessKey("A");
    c.setSecretKey("K");
    MarshalledCredentials c2 = S3ATestUtils.roundTrip(c,
        new Configuration());
    assertEquals(c, c2);
  }

  @Test
  public void testRoundTripEncryptionData() throws Throwable {
    EncryptionSecrets secrets = new EncryptionSecrets(
        S3AEncryptionMethods.SSE_KMS,
        "key");
    EncryptionSecrets result = S3ATestUtils.roundTrip(secrets,
        new Configuration());
    assertEquals("round trip", secrets, result);
  }

  @Test
  public void testMarshalledCredentialProviderSession() throws Throwable {
    MarshalledCredentialProvider provider
        = new MarshalledCredentialProvider("test",
        bucketURI,
        new Configuration(false),
        credentials,
        MarshalledCredentials.CredentialTypeRequired.SessionOnly);
    AWSCredentials aws = provider.getCredentials();
    assertEquals(credentials.toString(),
        credentials.getAccessKey(),
        aws.getAWSAccessKeyId());
    assertEquals(credentials.toString(),
        credentials.getSecretKey(),
        aws.getAWSSecretKey());
    // because the credentials are set to full only, creation will fail
  }

  /**
   * Create with a mismatch of type and supplied credentials.
   * Verify that the operation fails, but only when credentials
   * are actually requested.
   */
  @Test
  public void testCredentialTypeMismatch() throws Throwable {
    MarshalledCredentialProvider provider
        = new MarshalledCredentialProvider("test",
        bucketURI,
        new Configuration(false),
        credentials,
        MarshalledCredentials.CredentialTypeRequired.FullOnly);
    // because the credentials are set to full only, creation will fail
    intercept(NoAuthWithAWSException.class, "test",
        () ->  provider.getCredentials());
  }

  /**
   * This provider fails fast if there's no URL.
   */
  @Test
  public void testCredentialProviderNullURI() throws Throwable {
    intercept(NullPointerException.class, "",
        () ->
            new MarshalledCredentialProvider("test",
            null,
            new Configuration(false),
            credentials,
            MarshalledCredentials.CredentialTypeRequired.FullOnly));
  }
}

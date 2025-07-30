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

import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

import org.assertj.core.api.Assertions;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.auth.delegation.DelegationTokenIOException;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecrets;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.test.HadoopTestBase;

import static org.apache.hadoop.fs.s3a.S3AEncryptionMethods.SSE_S3;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Unit test of marshalled credential support.
 */
public class TestMarshalledCredentials extends HadoopTestBase {

  private MarshalledCredentials credentials;

  private int expiration;

  private URI bucketURI;

  @BeforeEach
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
    final String context = "encryptionContext";
    EncryptionSecrets secrets = new EncryptionSecrets(
        S3AEncryptionMethods.SSE_KMS,
        KEY,
        context);
    EncryptionSecrets result = S3ATestUtils.roundTrip(secrets,
        new Configuration());
    assertEquals(secrets, result, "round trip");
    Assertions.assertThat(result .getEncryptionContext())
        .describedAs("encryptionContext")
        .isEqualTo(context);
  }

  @Test
  public void testRoundTripEncryptionSecretsNoContext() throws Throwable {
    EncryptionSecrets secrets = new EncryptionSecrets(
        S3AEncryptionMethods.SSE_KMS,
        KEY);
    EncryptionSecrets result = S3ATestUtils.roundTrip(secrets,
        new Configuration());
    assertEquals(secrets, result, "round trip");
    // not equal to secrets with a context
    Assertions.assertThat(result)
        .isNotEqualTo(new EncryptionSecrets(
            S3AEncryptionMethods.SSE_KMS,
            KEY,
            "encryptionContext"));
  }

  @Test
  public void testMarshalledCredentialProviderSession() throws Throwable {
    MarshalledCredentialProvider provider
        = new MarshalledCredentialProvider("test",
        bucketURI,
        new Configuration(false),
        credentials,
        MarshalledCredentials.CredentialTypeRequired.SessionOnly);
    AwsCredentials aws = provider.resolveCredentials();
    assertEquals(credentials.getAccessKey(),
        aws.accessKeyId(), credentials.toString());
    assertEquals(credentials.getSecretKey(),
        aws.secretAccessKey(), credentials.toString());
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
        () ->  provider.resolveCredentials());
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

  @org.junit.Test
  public void testUnmarshallOldEncryptionSecrets() throws Throwable {

  }

  /**
   * Generate the equivalent to a marshalled EncryptionSecrets value.
   * @param id serialization ID.
   * @param encryptionAlgorithm algorithm.
   * @param encryptionKey key
   * @param encryptionContext optional context
   * @return the input
   * @throws IOException write failure.
   */
  private DataInputBuffer writeEncryptionSecrets(long id,
      final String encryptionAlgorithm,
      final String encryptionKey,
      final Optional<String> encryptionContext) throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    new LongWritable(id).write(out);
    Text.writeString(out, encryptionAlgorithm);
    Text.writeString(out, encryptionKey);
    if (encryptionContext.isPresent()) {
      Text.writeString(out, encryptionContext.get());
    }

    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(out.getData(), out.getLength());
    return dib;
  }

  private EncryptionSecrets readEncryptionSecrets(DataInputBuffer dib) throws IOException {
    final EncryptionSecrets secrets = new EncryptionSecrets();
    secrets.readFields(dib);
    return secrets;
  }

  private static final String ENCRYPTION_ALGORITHM = SSE_S3.getMethod();

  private static final String KEY = "key";

  private static final String CONTEXT = "context";

  /**
   * Verify that the low level marshalling code works.
   */
  @Test
  public void testMarshallCurrentSecrets() throws Throwable {
    EncryptionSecrets src = new EncryptionSecrets(ENCRYPTION_ALGORITHM,
        KEY,
        CONTEXT);
    final DataInputBuffer in =
        writeEncryptionSecrets(EncryptionSecrets.SERIAL_VERSION_UID_CURRENT,
            ENCRYPTION_ALGORITHM, KEY, Optional.of(CONTEXT));
    final EncryptionSecrets read = readEncryptionSecrets(in);
    Assertions.assertThat(read)
        .isEqualTo(src);
  }

  /**
   * Generate the layout of an old secret entry, unmarshall it to the new one.
   */
  @Test
  public void testUnmarshallOldSecrets() throws Throwable {
    final DataInputBuffer dib = writeEncryptionSecrets(EncryptionSecrets.SERIAL_VERSION_UID_1,
        ENCRYPTION_ALGORITHM, KEY, Optional.empty());
    final EncryptionSecrets read = readEncryptionSecrets(dib);

    // all the data has been read in
    Assertions.assertThat(dib.read())
        .describedAs("Input stream read() at end of unmarshalling")
        .isEqualTo(-1);
    Assertions.assertThat(read)
        .matches(s -> !s.hasEncryptionContext())
        .hasFieldOrPropertyWithValue("encryptionAlgorithm", ENCRYPTION_ALGORITHM)
        .hasFieldOrPropertyWithValue("getEncryptionKey", KEY);
  }

  /**
   * Generate the layout of an old secret entry, unmarshall it to the new one.
   */
  @Test
  public void testCurrentSecretsRequireContext() throws Throwable {
    final DataInputBuffer in = writeEncryptionSecrets(
        EncryptionSecrets.SERIAL_VERSION_UID_CURRENT,
        ENCRYPTION_ALGORITHM, KEY, Optional.empty());
    intercept(EOFException.class, "", () ->
      readEncryptionSecrets(in));
  }

  /**
   * Usea unknown version ID; expect an exception with the version ID in the message.
   */
  @Test
  public void testUnmarshallUnknownSecretVersion() throws Throwable {
    EncryptionSecrets src = new EncryptionSecrets(ENCRYPTION_ALGORITHM, KEY, CONTEXT);
    final DataInputBuffer in =
        writeEncryptionSecrets(12345L,
            src.getEncryptionAlgorithm(), src.getEncryptionKey(),
            Optional.of("context1"));
    intercept(DelegationTokenIOException.class, "12345", () -> {
      readEncryptionSecrets(in);
    });
  }

}

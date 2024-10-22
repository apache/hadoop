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

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.concurrent.TimeUnit;

import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.Credentials;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.auth.MarshalledCredentialBinding;
import org.apache.hadoop.fs.s3a.auth.MarshalledCredentials;
import org.apache.hadoop.fs.s3a.auth.STSClientFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.auth.delegation.SessionTokenIdentifier;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assumeSessionTestsEnabled;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.requestSessionCredentials;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.unsetHadoopCredentialProviders;
import static org.apache.hadoop.fs.s3a.auth.MarshalledCredentialBinding.fromSTSCredentials;
import static org.apache.hadoop.fs.s3a.auth.MarshalledCredentialBinding.toAWSCredentials;
import static org.apache.hadoop.fs.s3a.auth.RoleTestUtils.assertCredentialsEqual;
import static org.apache.hadoop.fs.s3a.auth.STSClientFactory.E_INVALID_STS_ENDPOINT_ERROR_MSG;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.*;
import static org.apache.hadoop.fs.s3a.auth.delegation.SessionTokenBinding.CREDENTIALS_CONVERTED_TO_DELEGATION_TOKEN;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests use of temporary credentials (for example, AWS STS & S3).
 *
 * The property {@link Constants#ASSUMED_ROLE_STS_ENDPOINT} can be set to
 * point this at different STS endpoints.
 * This test will use the AWS credentials (if provided) for
 * S3A tests to request temporary credentials, then attempt to use those
 * credentials instead.
 */
public class ITestS3ATemporaryCredentials extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ATemporaryCredentials.class);

  private static final String TEMPORARY_AWS_CREDENTIALS
      = TemporaryAWSCredentialsProvider.NAME;

  private static final long TEST_FILE_SIZE = 1024;

  public static final String STS_LONDON = "sts.eu-west-2.amazonaws.com";

  public static final String EU_IRELAND = "eu-west-1";

  private AWSCredentialProviderList credentials;

  @Override
  public void setup() throws Exception {
    super.setup();
    assumeSessionTestsEnabled(getConfiguration());
  }

  @Override
  public void teardown() throws Exception {
    S3AUtils.closeAutocloseables(LOG, credentials);
    super.teardown();
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.set(DELEGATION_TOKEN_BINDING,
        DELEGATION_TOKEN_SESSION_BINDING);
    return conf;
  }

  /**
   * Test use of STS for requesting temporary credentials.
   *
   * The property test.sts.endpoint can be set to point this at different
   * STS endpoints. This test will use the AWS credentials (if provided) for
   * S3A tests to request temporary credentials, then attempt to use those
   * credentials instead.
   *
   * @throws IOException failure
   */
  @Test
  public void testSTS() throws IOException {
    Configuration conf = getContract().getConf();
    S3AFileSystem testFS = getFileSystem();
    credentials = getS3AInternals().shareCredentials("testSTS");

    String bucket = testFS.getBucket();
    StsClientBuilder builder = STSClientFactory.builder(
        conf,
        bucket,
        credentials,
        getStsEndpoint(conf),
        getStsRegion(conf));
    Credentials sessionCreds;
    try (STSClientFactory.STSClient clientConnection =
        STSClientFactory.createClientConnection(builder.build(),
            new Invoker(new S3ARetryPolicy(conf), Invoker.LOG_EVENT))) {
      sessionCreds = clientConnection
          .requestSessionCredentials(
              TEST_SESSION_TOKEN_DURATION_SECONDS, TimeUnit.SECONDS);
    }

    // clone configuration so changes here do not affect the base FS.
    Configuration conf2 = new Configuration(conf);
    S3AUtils.clearBucketOption(conf2, bucket, AWS_CREDENTIALS_PROVIDER);
    S3AUtils.clearBucketOption(conf2, bucket, ACCESS_KEY);
    S3AUtils.clearBucketOption(conf2, bucket, SECRET_KEY);
    S3AUtils.clearBucketOption(conf2, bucket, SESSION_TOKEN);

    MarshalledCredentials mc = fromSTSCredentials(sessionCreds);
    updateConfigWithSessionCreds(conf2, mc);

    conf2.set(AWS_CREDENTIALS_PROVIDER, TEMPORARY_AWS_CREDENTIALS);

    // with valid credentials, we can set properties.
    try(S3AFileSystem fs = S3ATestUtils.createTestFileSystem(conf2)) {
      createAndVerifyFile(fs, path("testSTS"), TEST_FILE_SIZE);
    }

    // now create an invalid set of credentials by changing the session
    // token
    conf2.set(SESSION_TOKEN, "invalid-" + sessionCreds.sessionToken());
    try (S3AFileSystem fs = S3ATestUtils.createTestFileSystem(conf2)) {
      createAndVerifyFile(fs, path("testSTSInvalidToken"), TEST_FILE_SIZE);
      fail("Expected an access exception, but file access to "
          + fs.getUri() + " was allowed: " + fs);
    } catch (AWSS3IOException | AWSBadRequestException ex) {
      LOG.info("Expected Exception: {}", ex.toString());
      LOG.debug("Expected Exception: {}", ex, ex);
    }
  }

  /**
   * Create a StsClientBuilder instance with a given STS URL.
   *
   * @param stsURL The STS URL to be used to create an StsClientBuilder instance.
   * @return StsClientBuilder.
   *
   * @throws IOException If there is an error while creating the StsClientBuilder instance.
   */
  private StsClientBuilder createSTS(String stsURL, String region) throws IOException {
    Configuration conf = getContract().getConf();
    S3AFileSystem testFS = getFileSystem();
    credentials = getS3AInternals().shareCredentials("testSTS");

    String bucket = testFS.getBucket();
    return STSClientFactory.builder(conf, bucket, credentials, stsURL, region);
  }

  /**
   * Interface for creating a lambda function that verifies the exception that is thrown when creating
   * an StsClientBuilder.
   */
  @FunctionalInterface
  interface VerifyCreateSTSException {
    void verifyException(String stsURL) throws IOException;
  }

  /**
   * Test different STS URLs. We will test a combination of valid and invalid URLs.
   *
   * The property test.sts.endpoint can be set to point this at different
   * STS endpoints. This test will use the AWS credentials (if provided) for
   * S3A tests to request temporary credentials, then attempt to use those
   * credentials instead.
   *
   * @throws IOException failure
   */

  @Test
  public void testSTSURLs() throws IOException {
    // List of STS URLs to check for validity.
    String [] validStsURLs = {
      "sts.amazonaws.com",
      "sts.us-east-2.amazonaws.com",
      "sts.us-east-1.amazonaws.com",
      "sts.us-west-1.amazonaws.com",
      "sts.us-west-2.amazonaws.com",
      "sts.af-south-1.amazonaws.com",
      "sts.ap-east-1.amazonaws.com",
      "sts.ap-south-2.amazonaws.com",
      "sts.ap-southeast-3.amazonaws.com",
      "sts.ap-southeast-4.amazonaws.com",
      "sts.ap-south-1.amazonaws.com",
      "sts.ap-northeast-3.amazonaws.com",
      "sts.ap-northeast-2.amazonaws.com",
      "sts.ap-southeast-1.amazonaws.com",
      "sts.ap-southeast-2.amazonaws.com",
      "sts.ap-northeast-1.amazonaws.com",
      "sts.ca-central-1.amazonaws.com",
      "sts.ca-west-1.amazonaws.com",
      "sts.cn-north-1.amazonaws.com.cn",
      "sts.cn-northwest-1.amazonaws.com.cn",
      "sts.eu-central-1.amazonaws.com",
      "sts.eu-west-1.amazonaws.com",
      "sts.eu-west-2.amazonaws.com",
      "sts.eu-south-1.amazonaws.com",
      "sts.eu-west-3.amazonaws.com",
      "sts.eu-south-2.amazonaws.com",
      "sts.eu-north-1.amazonaws.com",
      "sts.eu-central-2.amazonaws.com",
      "sts.il-central-1.amazonaws.com",
      "sts.me-south-1.amazonaws.com",
      "sts.me-central-1.amazonaws.com",
      "sts.sa-east-1.amazonaws.com"
    };

    // The lambda function can run an invalid STS URL and verify
    // that the correct exception is thrown.
    VerifyCreateSTSException vcse = (stsURL) -> {
      try {
        createSTS(stsURL, "us-west-2");
      } catch (IllegalArgumentException iae) {
        LOG.info("Expected Exception: {}", iae.toString());
      }
    };

    // INVALID STS URL VERIFICATIONS

    // STS URLs start with sts.
    vcse.verifyException("invalid-sts.eu-west-2.com");

    // Should have been sts.cn-northwest-1.amazonaws.com.cn.
    vcse.verifyException("sts.cn-north-1.amazonaws.cn");

    // Not a valid STS URL.
    vcse.verifyException("www.google.com");

    // Random string is not a valid STS URL.
    vcse.verifyException("invalid-sts");

    // VALID STS URL VERIFICATIONS

    // Verify that there are no exceptions when testing for the different STS URLs.
    for (String validStsURL : validStsURLs) {
      String [] splits = validStsURL.split("\\.");
      if (validStsURL == "sts.amazonaws.com") {
        createSTS(validStsURL, "");
      }
      else {
        createSTS(validStsURL, splits[1]);
      }
    }
  }

  protected String getStsEndpoint(final Configuration conf) {
    return conf.getTrimmed(ASSUMED_ROLE_STS_ENDPOINT,
            DEFAULT_ASSUMED_ROLE_STS_ENDPOINT);
  }

  protected String getStsRegion(final Configuration conf) {
    return conf.getTrimmed(ASSUMED_ROLE_STS_ENDPOINT_REGION,
        ASSUMED_ROLE_STS_ENDPOINT_REGION_DEFAULT);
  }

  @Test
  public void testTemporaryCredentialValidation() throws Throwable {
    Configuration conf = new Configuration();
    conf.set(ACCESS_KEY, "accesskey");
    conf.set(SECRET_KEY, "secretkey");
    conf.set(SESSION_TOKEN, "");
    LambdaTestUtils.intercept(CredentialInitializationException.class,
        () -> new TemporaryAWSCredentialsProvider(conf).resolveCredentials());
  }

  /**
   * Test that session tokens are propagated, with the origin string
   * declaring this.
   */
  @Test
  public void testSessionTokenPropagation() throws Exception {
    Configuration conf = new Configuration(getContract().getConf());
    MarshalledCredentials sc = requestSessionCredentials(conf,
        getFileSystem().getBucket());
    updateConfigWithSessionCreds(conf, sc);
    conf.set(AWS_CREDENTIALS_PROVIDER, TEMPORARY_AWS_CREDENTIALS);

    try (S3AFileSystem fs = S3ATestUtils.createTestFileSystem(conf)) {
      createAndVerifyFile(fs, path("testSTS"), TEST_FILE_SIZE);
      SessionTokenIdentifier identifier
          = (SessionTokenIdentifier) fs.getDelegationToken("")
          .decodeIdentifier();
      String ids = identifier.toString();
      assertThat("origin in " + ids,
          identifier.getOrigin(),
          containsString(CREDENTIALS_CONVERTED_TO_DELEGATION_TOKEN));

      // and validate the AWS bits to make sure everything has come across.
      assertCredentialsEqual("Reissued credentials in " + ids,
          sc,
          identifier.getMarshalledCredentials());
    }
  }

  /**
   * Examine the returned expiry time and validate it against expectations.
   * Allows for some flexibility in local clock, but not much.
   */
  @Test
  public void testSessionTokenExpiry() throws Exception {
    Configuration conf = new Configuration(getContract().getConf());
    MarshalledCredentials sc = requestSessionCredentials(conf,
        getFileSystem().getBucket());
    long permittedExpiryOffset = 60;
    OffsetDateTime expirationTimestamp = sc.getExpirationDateTime().get();
    OffsetDateTime localTimestamp = OffsetDateTime.now();
    assertTrue("local time of " + localTimestamp
            + " is after expiry time of " + expirationTimestamp,
        localTimestamp.isBefore(expirationTimestamp));

    // what is the interval
    Duration actualDuration = Duration.between(localTimestamp,
        expirationTimestamp);
    Duration offset = actualDuration.minus(TEST_SESSION_TOKEN_DURATION);

    assertThat(
        "Duration of session " + actualDuration
            + " out of expected range of with " + offset
            + " this host's clock may be wrong.",
        offset.getSeconds(),
        Matchers.lessThanOrEqualTo(permittedExpiryOffset));
  }

  protected void updateConfigWithSessionCreds(final Configuration conf,
      final MarshalledCredentials sc) {
    unsetHadoopCredentialProviders(conf);
    sc.setSecretsInConfiguration(conf);
  }

  /**
   * Create an invalid session token and verify that it is rejected.
   */
  @Test
  public void testInvalidSTSBinding() throws Exception {
    Configuration conf = new Configuration(getContract().getConf());

    MarshalledCredentials sc = requestSessionCredentials(conf,
        getFileSystem().getBucket());
    toAWSCredentials(sc,
        MarshalledCredentials.CredentialTypeRequired.AnyNonEmpty, "");
    updateConfigWithSessionCreds(conf, sc);

    conf.set(AWS_CREDENTIALS_PROVIDER, TEMPORARY_AWS_CREDENTIALS);
    conf.set(SESSION_TOKEN, "invalid-" + sc.getSessionToken());
    S3AFileSystem fs = null;

    try {
      // this may throw an exception, which is an acceptable outcome.
      // it must be in the try/catch clause.
      fs = S3ATestUtils.createTestFileSystem(conf);
      Path path = path("testSTSInvalidToken");
      createAndVerifyFile(fs,
          path,
            TEST_FILE_SIZE);
      // this is a failure path, so fail with a meaningful error
      fail("request to create a file should have failed");
    } catch (AWSBadRequestException expected){
      // could fail in fs creation or file IO
    } finally {
      IOUtils.closeStream(fs);
    }
  }


  @Test
  public void testSessionCredentialsBadRegion() throws Throwable {
    describe("Create a session with a bad region and expect failure");
    expectedSessionRequestFailure(
        IllegalArgumentException.class,
        DEFAULT_DELEGATION_TOKEN_ENDPOINT,
        "us-west-12",
        "");
  }

  @Test
  public void testSessionCredentialsWrongRegion() throws Throwable {
    describe("Create a session with the wrong region and expect failure");
    expectedSessionRequestFailure(
        AccessDeniedException.class,
        STS_LONDON,
        EU_IRELAND,
        "");
  }

  @Test
  public void testSessionCredentialsWrongCentralRegion() throws Throwable {
    describe("Create a session sts.amazonaws.com; region='us-west-1'");
    expectedSessionRequestFailure(
        IllegalArgumentException.class,
        "sts.amazonaws.com",
        "us-west-1",
        "");
  }

  @Test
  public void testSessionCredentialsRegionNoEndpoint() throws Throwable {
    describe("Create a session with a bad region and expect fast failure");
    expectedSessionRequestFailure(
        IllegalArgumentException.class,
        "",
        EU_IRELAND,
        EU_IRELAND);
  }

  @Test
  public void testSessionCredentialsRegionBadEndpoint() throws Throwable {
    describe("Create a session with a bad region and expect fast failure");
    IllegalArgumentException ex
        = expectedSessionRequestFailure(
            IllegalArgumentException.class,
        " ",
        EU_IRELAND,
        "");
    LOG.info("Outcome: ", ex);
  }

  @Test
  public void testSessionCredentialsEndpointNoRegion() throws Throwable {
    expectedSessionRequestFailure(
        IllegalArgumentException.class,
        STS_LONDON,
        "",
        STS_LONDON);
  }

  /**
   * Expect an attempt to create a session or request credentials to fail
   * with a specific exception class, optionally text.
   * @param clazz exact class of exception.
   * @param endpoint value for the sts endpoint option.
   * @param region signing region.
   * @param exceptionText text or "" in the exception.
   * @param <E> type of exception.
   * @return the caught exception.
   * @throws Exception any unexpected exception.
   */
  public <E extends Exception> E expectedSessionRequestFailure(
      final Class<E> clazz,
      final String endpoint,
      final String region,
      final String exceptionText) throws Exception {
    try(AWSCredentialProviderList parentCreds =
            getS3AInternals().shareCredentials("test");
        DurationInfo ignored = new DurationInfo(LOG, "requesting credentials")) {
      Configuration conf = new Configuration(getContract().getConf());

      return intercept(clazz, exceptionText,
          () -> {
            StsClient tokenService =
                STSClientFactory.builder(parentCreds,
                    conf,
                    endpoint,
                    region,
                    getFileSystem().getBucket())
                    .build();
            Invoker invoker = new Invoker(new S3ARetryPolicy(conf),
                LOG_AT_ERROR);

            try (STSClientFactory.STSClient stsClient =
                STSClientFactory.createClientConnection(
                    tokenService, invoker)) {
              return stsClient.requestSessionCredentials(
                  30, TimeUnit.MINUTES);
            }
          });
    }
  }

  /**
   * Log retries at debug.
   */
  public static final Invoker.Retried LOG_AT_ERROR =
      (text, exception, retries, idempotent) -> {
        LOG.error("{}", text, exception);
      };

  @Test
  public void testTemporaryCredentialValidationOnLoad() throws Throwable {
    Configuration conf = new Configuration();
    unsetHadoopCredentialProviders(conf);
    conf.set(ACCESS_KEY, "aaa");
    conf.set(SECRET_KEY, "bbb");
    conf.set(SESSION_TOKEN, "");
    final MarshalledCredentials sc = MarshalledCredentialBinding.fromFileSystem(
        null, conf);
    intercept(IOException.class,
        MarshalledCredentials.INVALID_CREDENTIALS,
        () -> {
          sc.validate("",
              MarshalledCredentials.CredentialTypeRequired.SessionOnly);
          return sc.toString();
        });
  }

  @Test
  public void testEmptyTemporaryCredentialValidation() throws Throwable {
    Configuration conf = new Configuration();
    unsetHadoopCredentialProviders(conf);
    conf.set(ACCESS_KEY, "");
    conf.set(SECRET_KEY, "");
    conf.set(SESSION_TOKEN, "");
    final MarshalledCredentials sc = MarshalledCredentialBinding.fromFileSystem(
        null, conf);
    intercept(IOException.class,
        MarshalledCredentialBinding.NO_AWS_CREDENTIALS,
        () -> {
          sc.validate("",
              MarshalledCredentials.CredentialTypeRequired.SessionOnly);
          return sc.toString();
        });
  }

  /**
   * Verify that the request mechanism is translating exceptions.
   * @throws Exception on a failure
   */
  @Test
  public void testSessionRequestExceptionTranslation() throws Exception {
    intercept(IOException.class,
        () -> requestSessionCredentials(getConfiguration(),
            getFileSystem().getBucket(), 10));
  }

}

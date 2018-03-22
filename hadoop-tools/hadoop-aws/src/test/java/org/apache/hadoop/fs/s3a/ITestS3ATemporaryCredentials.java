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
import java.net.URI;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.GetSessionTokenRequest;
import com.amazonaws.services.securitytoken.model.GetSessionTokenResult;
import com.amazonaws.services.securitytoken.model.Credentials;

import org.apache.hadoop.fs.s3native.S3xLoginHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.LambdaTestUtils;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.Constants.*;

/**
 * Tests use of temporary credentials (for example, AWS STS & S3).
 * This test extends a class that "does things to the root directory", and
 * should only be used against transient filesystems where you don't care about
 * the data.
 */
public class ITestS3ATemporaryCredentials extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ATemporaryCredentials.class);

  private static final String PROVIDER_CLASS
      = TemporaryAWSCredentialsProvider.NAME;

  private static final long TEST_FILE_SIZE = 1024;

  /**
   * Test use of STS for requesting temporary credentials.
   *
   * The property test.sts.endpoint can be set to point this at different
   * STS endpoints. This test will use the AWS credentials (if provided) for
   * S3A tests to request temporary credentials, then attempt to use those
   * credentials instead.
   *
   * @throws IOException
   */
  @Test
  public void testSTS() throws IOException {
    Configuration conf = getContract().getConf();
    if (!conf.getBoolean(TEST_STS_ENABLED, true)) {
      skip("STS functional tests disabled");
    }

    S3xLoginHelper.Login login = S3AUtils.getAWSAccessKeys(
        URI.create("s3a://foobar"), conf);
    if (!login.hasLogin()) {
      skip("testSTS disabled because AWS credentials not configured");
    }
    AWSCredentialsProvider parentCredentials = new BasicAWSCredentialsProvider(
        login.getUser(), login.getPassword());

    String stsEndpoint = conf.getTrimmed(TEST_STS_ENDPOINT, "");
    AWSSecurityTokenServiceClient stsClient;
    stsClient = new AWSSecurityTokenServiceClient(parentCredentials);
    if (!stsEndpoint.isEmpty()) {
      LOG.debug("STS Endpoint ={}", stsEndpoint);
      stsClient.setEndpoint(stsEndpoint);
    }
    GetSessionTokenRequest sessionTokenRequest = new GetSessionTokenRequest();
    sessionTokenRequest.setDurationSeconds(900);
    GetSessionTokenResult sessionTokenResult;
    sessionTokenResult = stsClient.getSessionToken(sessionTokenRequest);
    Credentials sessionCreds = sessionTokenResult.getCredentials();

    String childAccessKey = sessionCreds.getAccessKeyId();
    conf.set(ACCESS_KEY, childAccessKey);
    String childSecretKey = sessionCreds.getSecretAccessKey();
    conf.set(SECRET_KEY, childSecretKey);
    String sessionToken = sessionCreds.getSessionToken();
    conf.set(SESSION_TOKEN, sessionToken);

    conf.set(AWS_CREDENTIALS_PROVIDER, PROVIDER_CLASS);

    try(S3AFileSystem fs = S3ATestUtils.createTestFileSystem(conf)) {
      createAndVerifyFile(fs, path("testSTS"), TEST_FILE_SIZE);
    }

    // now create an invalid set of credentials by changing the session
    // token
    conf.set(SESSION_TOKEN, "invalid-" + sessionToken);
    try (S3AFileSystem fs = S3ATestUtils.createTestFileSystem(conf)) {
      createAndVerifyFile(fs, path("testSTSInvalidToken"), TEST_FILE_SIZE);
      fail("Expected an access exception, but file access to "
          + fs.getUri() + " was allowed: " + fs);
    } catch (AWSS3IOException | AWSBadRequestException ex) {
      LOG.info("Expected Exception: {}", ex.toString());
      LOG.debug("Expected Exception: {}", ex, ex);
    }
  }

  @Test
  public void testTemporaryCredentialValidation() throws Throwable {
    Configuration conf = new Configuration();
    conf.set(ACCESS_KEY, "accesskey");
    conf.set(SECRET_KEY, "secretkey");
    conf.set(SESSION_TOKEN, "");
    LambdaTestUtils.intercept(CredentialInitializationException.class,
        () -> new TemporaryAWSCredentialsProvider(conf).getCredentials());
  }
}

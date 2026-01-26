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

package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.assertj.core.api.AbstractStringAssert;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.AwsClient;
import software.amazon.awssdk.s3accessgrants.plugin.S3AccessGrantsIdentityProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_ACCESS_GRANTS_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;


/**
 * Test S3 Access Grants configurations.
 */
public class TestS3AccessGrantConfiguration extends AbstractHadoopTestBase {
  /**
   * This credential provider will be attached to any client
   * that has been configured with the S3 Access Grants plugin.
   * {@code software.amazon.awssdk.s3accessgrants.plugin.S3AccessGrantsPlugin}.
   */
  public static final String S3_ACCESS_GRANTS_EXPECTED_CREDENTIAL_PROVIDER_CLASS =
      S3AccessGrantsIdentityProvider.class.getName();

  @Test
  public void testS3AccessGrantsEnabled() throws IOException, URISyntaxException {
    assertCredentialProviderClass(
        createConfig(true),
        true,
        "S3 Access Grants is explicitly enabled on an S3 Async Client",
        true);

    assertCredentialProviderClass(
        createConfig(true),
        false,
        "S3 Access Grants is explicitly enabled on an S3 Non-Async Client",
        true);
  }

  @Test
  public void testS3AccessGrantsDisabled() throws IOException, URISyntaxException {
    assertCredentialProviderClass(
        new Configuration(),
        true,
        "S3 Access Grants is implicitly disabled (default behavior) on an S3 Async Client",
        false);

    assertCredentialProviderClass(
        new Configuration(),
        false,
        "S3 Access Grants is implicitly disabled (default behavior) on an S3 Non-Async Client",
        false);

    assertCredentialProviderClass(
        createConfig(false),
        true,
        "S3 Access Grants is explicitly disabled on an S3 Async Client",
        false);

    assertCredentialProviderClass(
        createConfig(false),
        false,
        "S3 Access Grants is explicitly disabled on an S3 Non-Async Client",
        false);
  }

  private Configuration createConfig(boolean s3agEnabled) {
    Configuration conf = new Configuration();
    conf.setBoolean(AWS_S3_ACCESS_GRANTS_ENABLED, s3agEnabled);
    return conf;
  }

  private String getCredentialProviderName(AwsClient awsClient) {
    return awsClient.serviceClientConfiguration().credentialsProvider().getClass().getName();
  }

  private AwsClient getAwsClient(Configuration conf, boolean asyncClient)
      throws IOException, URISyntaxException {
    DefaultS3ClientFactory factory = new DefaultS3ClientFactory();
    factory.setConf(conf);
    S3ClientFactory.S3ClientCreationParameters parameters =
        new S3ClientFactory.S3ClientCreationParameters();
    URI uri = new URI("any-uri");
    return asyncClient ?
        factory.createS3AsyncClient(uri, parameters): factory.createS3Client(uri, parameters);
  }

  private void assertCredentialProviderClass(
      Configuration configuration, boolean asyncClient, String message, boolean shouldMatch)
      throws IOException, URISyntaxException {
    AwsClient awsClient = getAwsClient(configuration, asyncClient);
    AbstractStringAssert<?> assertion =
        assertThat(S3_ACCESS_GRANTS_EXPECTED_CREDENTIAL_PROVIDER_CLASS)
        .describedAs(message);
    if (shouldMatch) {
      assertion.isEqualTo(getCredentialProviderName(awsClient));
    } else {
      assertion.isNotEqualTo(getCredentialProviderName(awsClient));
    }
  }
}

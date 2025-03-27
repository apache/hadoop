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

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.services.sts.model.GetSessionTokenRequest;
import software.amazon.awssdk.thirdparty.org.apache.http.client.utils.URIBuilder;
import org.apache.hadoop.fs.s3a.impl.AWSClientConfig;
import org.apache.hadoop.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.Retries;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.fs.s3a.Constants.AWS_SERVICE_IDENTIFIER_STS;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.*;

/**
 * Factory for creating STS Clients.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class STSClientFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(STSClientFactory.class);

  /**
   * Create the builder ready for any final configuration options.
   * Picks up connection settings from the Hadoop configuration, including
   * proxy secrets.
   * The endpoint comes from the configuration options
   * {@link org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants#DELEGATION_TOKEN_ENDPOINT}
   * and
   * {@link org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants#DELEGATION_TOKEN_REGION}
   * @param conf Configuration to act as source of options.
   * @param bucket Optional bucket to use to look up per-bucket proxy secrets
   * @param credentials AWS credential chain to use
   * @return the builder to call {@code build()}
   * @throws IOException problem reading proxy secrets
   */
  public static StsClientBuilder builder(
      final Configuration conf,
      final String bucket,
      final AwsCredentialsProvider credentials) throws IOException {
    String endpoint = conf.getTrimmed(DELEGATION_TOKEN_ENDPOINT,
        DEFAULT_DELEGATION_TOKEN_ENDPOINT);
    String region = conf.getTrimmed(DELEGATION_TOKEN_REGION,
        DEFAULT_DELEGATION_TOKEN_REGION);
    return builder(credentials, conf, endpoint, region, bucket);
  }

  /**
   * Create the builder ready for any final configuration options.
   * Picks up connection settings from the Hadoop configuration, including
   * proxy secrets.
   * @param conf Configuration to act as source of options.
   * @param bucket Optional bucket to use to look up per-bucket proxy secrets
   * @param credentials AWS credential chain to use
   * @param stsEndpoint optional endpoint "https://sns.us-west-1.amazonaws.com"
   * @param stsRegion AWS recommend setting the endpoint instead.
   * @return the builder to call {@code build()}
   * @throws IOException problem reading proxy secrets
   */
  public static StsClientBuilder builder(
      final Configuration conf,
      final String bucket,
      final AwsCredentialsProvider credentials,
      final String stsEndpoint,
      final String stsRegion) throws IOException {
    return builder(credentials, conf, stsEndpoint, stsRegion, bucket);
  }

  /**
   * Create the builder ready for any final configuration options.
   * Picks up connection settings from the Hadoop configuration, including
   * proxy secrets.
   * @param conf AWS configuration.
   * @param credentials AWS credential chain to use
   * @param stsEndpoint optional endpoint "https://sns.us-west-1.amazonaws.com"
   * @param stsRegion the region, e.g "us-west-1". Must be set if endpoint is.
   * @param bucket bucket name
   * @return the builder to call {@code build()}
   * @throws IOException problem reading proxy secrets
   */
  public static StsClientBuilder builder(final AwsCredentialsProvider credentials,
      final Configuration conf, final String stsEndpoint, final String stsRegion,
      final String bucket) throws IOException {
    final StsClientBuilder stsClientBuilder = StsClient.builder();

    Preconditions.checkArgument(credentials != null, "No credentials");

    final ClientOverrideConfiguration.Builder clientOverrideConfigBuilder =
        AWSClientConfig.createClientConfigBuilder(conf, AWS_SERVICE_IDENTIFIER_STS);

    final ApacheHttpClient.Builder httpClientBuilder =
        AWSClientConfig.createHttpClientBuilder(conf);

    final RetryPolicy.Builder retryPolicyBuilder = AWSClientConfig.createRetryPolicyBuilder(conf);

    final ProxyConfiguration proxyConfig = AWSClientConfig.createProxyConfiguration(conf, bucket);

    clientOverrideConfigBuilder.retryPolicy(retryPolicyBuilder.build());
    httpClientBuilder.proxyConfiguration(proxyConfig);

    stsClientBuilder.httpClientBuilder(httpClientBuilder)
        .overrideConfiguration(clientOverrideConfigBuilder.build())
        .credentialsProvider(credentials);

    boolean destIsStandardEndpoint = STS_STANDARD.equals(stsEndpoint);
    if (isNotEmpty(stsEndpoint) && !destIsStandardEndpoint) {
      Preconditions.checkArgument(isNotEmpty(stsRegion),
          "STS endpoint is set to %s but no signing region was provided", stsEndpoint);
      LOG.debug("STS Endpoint={}; region='{}'", stsEndpoint, stsRegion);
      stsClientBuilder.endpointOverride(getSTSEndpoint(stsEndpoint)).region(Region.of(stsRegion));
    } else {
      Preconditions.checkArgument(isEmpty(stsRegion),
          "STS signing region set set to %s but no STS endpoint specified", stsRegion);
    }
    return stsClientBuilder;
  }

  /**
   * Given a endpoint string, create the endpoint URI.
   *
   * @param endpoint possibly null endpoint.
   * @return an endpoint uri
   */
  private static URI getSTSEndpoint(String endpoint) {
    try {
      return new URIBuilder().setScheme("https").setHost(endpoint).build();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }


  /**
   * Create an STS Client instance.
   * @param stsClient STS instance
   * @param invoker invoker to use
   * @return an STS client bonded to that interface.
   */
  public static STSClient createClientConnection(
      final StsClient stsClient,
      final Invoker invoker) {
    return new STSClient(stsClient, invoker);
  }

  /**
   * STS client connection with retries.
   */
  public static final class STSClient implements Closeable {

    private final StsClient stsClient;

    private final Invoker invoker;

    private STSClient(final StsClient stsClient,
        final Invoker invoker) {
      this.stsClient = stsClient;
      this.invoker = invoker;
    }

    @Override
    public void close() throws IOException {
      stsClient.close();
    }

    /**
     * Request a set of session credentials.
     *
     * @param duration duration of the credentials
     * @param timeUnit time unit of duration
     * @return the role result
     * @throws IOException on a failure of the request
     */
    @Retries.RetryTranslated
    public Credentials requestSessionCredentials(
        final long duration,
        final TimeUnit timeUnit) throws IOException {
      int durationSeconds = (int) timeUnit.toSeconds(duration);
      LOG.debug("Requesting session token of duration {}", duration);
      final GetSessionTokenRequest request =
          GetSessionTokenRequest.builder().durationSeconds(durationSeconds).build();
      return invoker.retry("request session credentials", "",
          true,
          () ->{
            LOG.info("Requesting Amazon STS Session credentials");
            return stsClient.getSessionToken(request).credentials();
          });
    }

    /**
     * Request a set of role credentials.
     *
     * @param roleARN ARN to request
     * @param sessionName name of the session
     * @param policy optional policy; "" is treated as "none"
     * @param duration duration of the credentials
     * @param timeUnit time unit of duration
     * @return the role result
     * @throws IOException on a failure of the request
     */
    @Retries.RetryTranslated
    public Credentials requestRole(
        final String roleARN,
        final String sessionName,
        final String policy,
        final long duration,
        final TimeUnit timeUnit) throws IOException {
      LOG.debug("Requesting role {} with duration {}; policy = {}",
          roleARN, duration, policy);
      AssumeRoleRequest.Builder requestBuilder =
          AssumeRoleRequest.builder().durationSeconds((int) timeUnit.toSeconds(duration))
              .roleArn(roleARN).roleSessionName(sessionName);
      if (isNotEmpty(policy)) {
        requestBuilder.policy(policy);
      }
      return invoker.retry("request role credentials", "", true,
          () -> stsClient.assumeRole(requestBuilder.build()).credentials());
    }
  }
}

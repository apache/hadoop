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
import java.util.concurrent.TimeUnit;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetSessionTokenRequest;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AUtils;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
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
  public static AWSSecurityTokenServiceClientBuilder builder(
      final Configuration conf,
      final String bucket,
      final AWSCredentialsProvider credentials) throws IOException {
    final ClientConfiguration awsConf = S3AUtils.createAwsConf(conf, bucket);
    String endpoint = conf.getTrimmed(DELEGATION_TOKEN_ENDPOINT,
        DEFAULT_DELEGATION_TOKEN_ENDPOINT);
    String region = conf.getTrimmed(DELEGATION_TOKEN_REGION,
        DEFAULT_DELEGATION_TOKEN_REGION);
    return builder(credentials, awsConf, endpoint, region);
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
  public static AWSSecurityTokenServiceClientBuilder builder(
      final Configuration conf,
      final String bucket,
      final AWSCredentialsProvider credentials,
      final String stsEndpoint,
      final String stsRegion) throws IOException {
    final ClientConfiguration awsConf = S3AUtils.createAwsConf(conf, bucket);
    return builder(credentials, awsConf, stsEndpoint, stsRegion);
  }

  /**
   * Create the builder ready for any final configuration options.
   * Picks up connection settings from the Hadoop configuration, including
   * proxy secrets.
   * @param awsConf AWS configuration.
   * @param credentials AWS credential chain to use
   * @param stsEndpoint optional endpoint "https://sns.us-west-1.amazonaws.com"
   * @param stsRegion the region, e.g "us-west-1". Must be set if endpoint is.
   * @return the builder to call {@code build()}
   */
  public static AWSSecurityTokenServiceClientBuilder builder(
      final AWSCredentialsProvider credentials,
      final ClientConfiguration awsConf,
      final String stsEndpoint,
      final String stsRegion) {
    final AWSSecurityTokenServiceClientBuilder builder
        = AWSSecurityTokenServiceClientBuilder.standard();
    Preconditions.checkArgument(credentials != null, "No credentials");
    builder.withClientConfiguration(awsConf);
    builder.withCredentials(credentials);
    boolean destIsStandardEndpoint = STS_STANDARD.equals(stsEndpoint);
    if (isNotEmpty(stsEndpoint) && !destIsStandardEndpoint) {
      Preconditions.checkArgument(
          isNotEmpty(stsRegion),
          "STS endpoint is set to %s but no signing region was provided",
          stsEndpoint);
      LOG.debug("STS Endpoint={}; region='{}'", stsEndpoint, stsRegion);
      builder.withEndpointConfiguration(
          new AwsClientBuilder.EndpointConfiguration(stsEndpoint, stsRegion));
    } else {
      Preconditions.checkArgument(isEmpty(stsRegion),
          "STS signing region set set to %s but no STS endpoint specified",
          stsRegion);
    }
    return builder;
  }

  /**
   * Create an STS Client instance.
   * @param tokenService STS instance
   * @param invoker invoker to use
   * @return an STS client bonded to that interface.
   * @throws IOException on any failure
   */
  public static STSClient createClientConnection(
      final AWSSecurityTokenService tokenService,
      final Invoker invoker)
      throws IOException {
    return new STSClient(tokenService, invoker);
  }

  /**
   * STS client connection with retries.
   */
  public static final class STSClient implements Closeable {

    private final AWSSecurityTokenService tokenService;

    private final Invoker invoker;

    private STSClient(final AWSSecurityTokenService tokenService,
        final Invoker invoker) {
      this.tokenService = tokenService;
      this.invoker = invoker;
    }

    @Override
    public void close() throws IOException {
      try {
        tokenService.shutdown();
      } catch (UnsupportedOperationException ignored) {
        // ignore this, as it is what the STS client currently
        // does.
      }
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
      final GetSessionTokenRequest request = new GetSessionTokenRequest();
      request.setDurationSeconds(durationSeconds);
      return invoker.retry("request session credentials", "",
          true,
          () ->{
            LOG.info("Requesting Amazon STS Session credentials");
            return tokenService.getSessionToken(request).getCredentials();
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
      AssumeRoleRequest request = new AssumeRoleRequest();
      request.setDurationSeconds((int) timeUnit.toSeconds(duration));
      request.setRoleArn(roleARN);
      request.setRoleSessionName(sessionName);
      if (isNotEmpty(policy)) {
        request.setPolicy(policy);
      }
      return invoker.retry("request role credentials", "", true,
          () -> tokenService.assumeRole(request).getCredentials());
    }
  }
}

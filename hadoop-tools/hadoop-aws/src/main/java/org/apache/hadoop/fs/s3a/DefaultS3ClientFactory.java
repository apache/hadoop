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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.internal.ServiceUtils;
import com.amazonaws.util.AwsHostNameUtils;
import com.amazonaws.util.RuntimeHttpUtils;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.s3a.statistics.StatisticsFromAwsSdk;
import org.apache.hadoop.fs.s3a.statistics.impl.AwsStatisticsCollector;

import static org.apache.hadoop.fs.s3a.Constants.EXPERIMENTAL_AWS_INTERNAL_THROTTLING;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.EXPERIMENTAL_AWS_INTERNAL_THROTTLING_DEFAULT;
import static org.apache.hadoop.fs.s3a.Constants.PATH_STYLE_ACCESS;

/**
 * The default {@link S3ClientFactory} implementation.
 * This calls the AWS SDK to configure and create an
 * {@link AmazonS3Client} that communicates with the S3 service.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DefaultS3ClientFactory extends Configured
    implements S3ClientFactory {

  private static final String S3_SERVICE_NAME = "s3";
  private static final String S3_SIGNER = "S3SignerType";
  private static final String S3_V4_SIGNER = "AWSS3V4SignerType";

  /**
   * Subclasses refer to this.
   */
  protected static final Logger LOG =
      LoggerFactory.getLogger(DefaultS3ClientFactory.class);

  /**
   * Create the client.
   * <p>
   * If the AWS stats are not null then a {@link AwsStatisticsCollector}.
   * is created to bind to the two.
   * <i>Important: until this binding works properly across regions,
   * this should be null.</i>
   */
  @Override
  public AmazonS3 createS3Client(URI name,
      final String bucket,
      final AWSCredentialsProvider credentials,
      final String userAgentSuffix,
      final StatisticsFromAwsSdk statisticsFromAwsSdk) throws IOException {
    Configuration conf = getConf();
    final ClientConfiguration awsConf = S3AUtils
        .createAwsConf(conf, bucket, Constants.AWS_SERVICE_IDENTIFIER_S3);

    // When EXPERIMENTAL_AWS_INTERNAL_THROTTLING is false
    // throttling is explicitly disabled on the S3 client so that
    // all failures are collected in S3A instrumentation, and its
    // retry policy is the only one used.
    // This may cause problems in copy/rename.
    awsConf.setUseThrottleRetries(
        conf.getBoolean(EXPERIMENTAL_AWS_INTERNAL_THROTTLING,
            EXPERIMENTAL_AWS_INTERNAL_THROTTLING_DEFAULT));

    if (!StringUtils.isEmpty(userAgentSuffix)) {
      awsConf.setUserAgentSuffix(userAgentSuffix);
    }
    // optional metrics
    RequestMetricCollector metrics = statisticsFromAwsSdk != null
        ? new AwsStatisticsCollector(statisticsFromAwsSdk)
        : null;

    return newAmazonS3Client(
        credentials,
        awsConf,
        metrics,
        conf.getTrimmed(ENDPOINT, ""),
        conf.getBoolean(PATH_STYLE_ACCESS, false));
  }

  /**
   * Create an {@link AmazonS3} client.
   * Override this to provide an extended version of the client
   * @param credentials credentials to use
   * @param awsConf  AWS configuration
   * @param metrics metrics collector or null
   * @param endpoint endpoint string; may be ""
   * @param pathStyleAccess enable path style access?
   * @return new AmazonS3 client
   */
  protected AmazonS3 newAmazonS3Client(
      final AWSCredentialsProvider credentials,
      final ClientConfiguration awsConf,
      final RequestMetricCollector metrics,
      final String endpoint,
      final boolean pathStyleAccess) {
    if (metrics != null) {
      LOG.debug("Building S3 client using the SDK builder API");
      return buildAmazonS3Client(credentials, awsConf, metrics, endpoint,
          pathStyleAccess);
    } else {
      LOG.debug("Building S3 client using the SDK builder API");
      return classicAmazonS3Client(credentials, awsConf, endpoint,
          pathStyleAccess);
    }
  }

  /**
   * Use the (newer) Builder SDK to create a an AWS S3 client.
   * <p>
   * This has a more complex endpoint configuration in a
   * way which does not yet work in this code in a way
   * which doesn't trigger regressions. So it is only used
   * when SDK metrics are supplied.
   * @param credentials credentials to use
   * @param awsConf  AWS configuration
   * @param metrics metrics collector or null
   * @param endpoint endpoint string; may be ""
   * @param pathStyleAccess enable path style access?
   * @return new AmazonS3 client
   */
  private AmazonS3 buildAmazonS3Client(
      final AWSCredentialsProvider credentials,
      final ClientConfiguration awsConf,
      final RequestMetricCollector metrics,
      final String endpoint,
      final boolean pathStyleAccess) {
    AmazonS3ClientBuilder b = AmazonS3Client.builder();
    b.withCredentials(credentials);
    b.withClientConfiguration(awsConf);
    b.withPathStyleAccessEnabled(pathStyleAccess);
    if (metrics != null) {
      b.withMetricsCollector(metrics);
    }

    // endpoint set up is a PITA
    //  client.setEndpoint("") is no longer available
    AwsClientBuilder.EndpointConfiguration epr
        = createEndpointConfiguration(endpoint, awsConf);
    if (epr != null) {
      // an endpoint binding was constructed: use it.
      b.withEndpointConfiguration(epr);
    }
    final AmazonS3 client = b.build();
    return client;
  }

  /**
   * Wrapper around constructor for {@link AmazonS3} client.
   * Override this to provide an extended version of the client.
   * <p>
   * This uses a deprecated constructor -it is currently
   * the only one which works for us.
   * @param credentials credentials to use
   * @param awsConf  AWS configuration
   * @param endpoint endpoint string; may be ""
   * @param pathStyleAccess enable path style access?
   * @return new AmazonS3 client
   */
  @SuppressWarnings("deprecation")
  private AmazonS3 classicAmazonS3Client(
      AWSCredentialsProvider credentials,
      ClientConfiguration awsConf,
      final String endpoint,
      final boolean pathStyleAccess) {
    final AmazonS3 client = new AmazonS3Client(credentials, awsConf);
    return configureAmazonS3Client(client, endpoint, pathStyleAccess);
  }

  /**
   * Configure classic S3 client.
   * <p>
   * This includes: endpoint, Path Access and possibly other
   * options.
   *
   * @param s3 S3 Client.
   * @param endPoint s3 endpoint, may be empty
   * @param pathStyleAccess enable path style access?
   * @return S3 client
   * @throws IllegalArgumentException if misconfigured
   */
  protected static AmazonS3 configureAmazonS3Client(AmazonS3 s3,
      final String endPoint,
      final boolean pathStyleAccess)
      throws IllegalArgumentException {
    if (!endPoint.isEmpty()) {
      try {
        s3.setEndpoint(endPoint);
      } catch (IllegalArgumentException e) {
        String msg = "Incorrect endpoint: "  + e.getMessage();
        LOG.error(msg);
        throw new IllegalArgumentException(msg, e);
      }
    }
    return applyS3ClientOptions(s3, pathStyleAccess);
  }

  /**
   * Perform any tuning of the {@code S3ClientOptions} settings based on
   * the Hadoop configuration.
   * This is different from the general AWS configuration creation as
   * it is unique to S3 connections.
   * <p>
   * The {@link Constants#PATH_STYLE_ACCESS} option enables path-style access
   * to S3 buckets if configured.  By default, the
   * behavior is to use virtual hosted-style access with URIs of the form
   * {@code http://bucketname.s3.amazonaws.com}
   * <p>
   * Enabling path-style access and a
   * region-specific endpoint switches the behavior to use URIs of the form
   * {@code http://s3-eu-west-1.amazonaws.com/bucketname}.
   * It is common to use this when connecting to private S3 servers, as it
   * avoids the need to play with DNS entries.
   * @param s3 S3 client
   * @param pathStyleAccess enable path style access?
   * @return the S3 client
   */
  protected static AmazonS3 applyS3ClientOptions(AmazonS3 s3,
      final boolean pathStyleAccess) {
    if (pathStyleAccess) {
      LOG.debug("Enabling path style access!");
      s3.setS3ClientOptions(S3ClientOptions.builder()
          .setPathStyleAccess(true)
          .build());
    }
    return s3;
  }

  /**
   * Given an endpoint string, return an endpoint config, or null, if none
   * is needed.
   * <p>
   * This is a pretty painful piece of code. It is trying to replicate
   * what AwsClient.setEndpoint() does, because you can't
   * call that setter on an AwsClient constructed via
   * the builder, and you can't pass a metrics collector
   * down except through the builder.
   * <p>
   * Note also that AWS signing is a mystery which nobody fully
   * understands, especially given all problems surface in a
   * "400 bad request" response, which, like all security systems,
   * provides minimal diagnostics out of fear of leaking
   * secrets.
   *
   * @param endpoint possibly null endpoint.
   * @param awsConf config to build the URI from.
   * @return a configuration for the S3 client builder.
   */
  @VisibleForTesting
  public static AwsClientBuilder.EndpointConfiguration
      createEndpointConfiguration(
          final String endpoint, final ClientConfiguration awsConf) {
    LOG.debug("Creating endpoint configuration for {}", endpoint);
    if (endpoint == null || endpoint.isEmpty()) {
      // the default endpoint...we should be using null at this point.
      LOG.debug("Using default endpoint -no need to generate a configuration");
      return null;
    }

    final URI epr = RuntimeHttpUtils.toUri(endpoint, awsConf);
    LOG.debug("Endpoint URI = {}", epr);

    String region;
    if (!ServiceUtils.isS3USStandardEndpoint(endpoint)) {
      LOG.debug("Endpoint {} is not the default; parsing", epr);
      region = AwsHostNameUtils.parseRegion(
          epr.getHost(),
          S3_SERVICE_NAME);
    } else {
      // US-east, set region == null.
      LOG.debug("Endpoint {} is the standard one; declare region as null", epr);
      region = null;
    }
    LOG.debug("Region for endpoint {}, URI {} is determined as {}",
        endpoint, epr, region);
    return new AwsClientBuilder.EndpointConfiguration(endpoint, region);
  }
}

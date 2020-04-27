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
import com.amazonaws.services.s3.internal.ServiceUtils;
import com.amazonaws.util.AwsHostNameUtils;
import com.amazonaws.util.RuntimeHttpUtils;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.s3a.impl.statistics.AwsStatisticsCollector;
import org.apache.hadoop.fs.s3a.impl.statistics.StatisticsFromAwsSdk;

import static org.apache.hadoop.fs.s3a.Constants.EXPERIMENTAL_AWS_INTERNAL_THROTTLING;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.EXPERIMENTAL_AWS_INTERNAL_THROTTLING_DEFAULT;
import static org.apache.hadoop.fs.s3a.Constants.PATH_STYLE_ACCESS;

/**
 * The default {@link S3ClientFactory} implementation.
 * This which calls the AWS SDK to configure and create an
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
   * Wrapper around constructor for {@link AmazonS3} client.
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
    // if this worked life would be so much simpler
    // client.setEndpoint(endpoint);
    return client;
  }

  /**
   * Patch a classically-constructed s3 instance's endpoint.
   * @param s3 S3 client
   * @param endpoint possibly empty endpoint.
   *
   * @throws IllegalArgumentException if misconfigured
   */
  protected static AmazonS3 setEndpoint(AmazonS3 s3,
      String endpoint)
      throws IllegalArgumentException {
    if (!endpoint.isEmpty()) {
     try {
        s3.setEndpoint(endpoint);
      } catch (IllegalArgumentException e) {
        String msg = "Incorrect endpoint: " + e.getMessage();
        LOG.error(msg);
        throw new IllegalArgumentException(msg, e);
      }
    }
    return s3;
  }

  /**
   * Given an endpoint string, return an endpoint config, or null, if none
   * is needed.
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

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
import com.amazonaws.util.AwsHostNameUtils;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;

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

  protected static final Logger LOG = S3AFileSystem.LOG;

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
      AWSCredentialsProvider credentials,
      ClientConfiguration awsConf,
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
    AwsClientBuilder.EndpointConfiguration epr
        = createEndpointConfiguration(endpoint);
    if (epr != null) {
      b.withEndpointConfiguration(epr);
    }
    return b.build();
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
   * @param endpoint possibly null endpoint.
   * @return a configuration for the S3 client builder.
   */
  @VisibleForTesting
  public static AwsClientBuilder.EndpointConfiguration createEndpointConfiguration(
      final String endpoint) {
    if (endpoint.isEmpty()) {
      return null;
    }

    String region = AwsHostNameUtils.parseRegionName(endpoint, "s3");;
    LOG.debug("Region for endpoint {} is determined as {}", endpoint, region);
    return new AwsClientBuilder.EndpointConfiguration(endpoint, region);
  }
}

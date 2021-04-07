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
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.handlers.RequestHandler2;
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
import org.apache.hadoop.fs.s3a.statistics.impl.AwsStatisticsCollector;

import static org.apache.hadoop.fs.s3a.Constants.EXPERIMENTAL_AWS_INTERNAL_THROTTLING;
import static org.apache.hadoop.fs.s3a.Constants.EXPERIMENTAL_AWS_INTERNAL_THROTTLING_DEFAULT;

/**
 * The default {@link S3ClientFactory} implementation.
 * This calls the AWS SDK to configure and create an
 * {@code AmazonS3Client} that communicates with the S3 service.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DefaultS3ClientFactory extends Configured
    implements S3ClientFactory {

  private static final String S3_SERVICE_NAME = "s3";

  /**
   * Subclasses refer to this.
   */
  protected static final Logger LOG =
      LoggerFactory.getLogger(DefaultS3ClientFactory.class);

  /**
   * Create the client by preparing the AwsConf configuration
   * and then invoking {@code buildAmazonS3Client()}.
   */
  @Override
  public AmazonS3 createS3Client(
      final URI uri,
      final S3ClientCreationParameters parameters) throws IOException {
    Configuration conf = getConf();
    final ClientConfiguration awsConf = S3AUtils
        .createAwsConf(conf,
            uri.getHost(),
            Constants.AWS_SERVICE_IDENTIFIER_S3);
    // add any headers
    parameters.getHeaders().forEach((h, v) ->
        awsConf.addHeader(h, v));

    // When EXPERIMENTAL_AWS_INTERNAL_THROTTLING is false
    // throttling is explicitly disabled on the S3 client so that
    // all failures are collected in S3A instrumentation, and its
    // retry policy is the only one used.
    // This may cause problems in copy/rename.
    awsConf.setUseThrottleRetries(
        conf.getBoolean(EXPERIMENTAL_AWS_INTERNAL_THROTTLING,
            EXPERIMENTAL_AWS_INTERNAL_THROTTLING_DEFAULT));

    if (!StringUtils.isEmpty(parameters.getUserAgentSuffix())) {
      awsConf.setUserAgentSuffix(parameters.getUserAgentSuffix());
    }

    return buildAmazonS3Client(
        awsConf,
        parameters);
  }

  /**
   * Use the Builder API to create an AWS S3 client.
   * <p>
   * This has a more complex endpoint configuration mechanism
   * which initially caused problems; the
   * {@code withForceGlobalBucketAccessEnabled(true)}
   * command is critical here.
   * @param awsConf  AWS configuration
   * @param parameters parameters
   * @return new AmazonS3 client
   */
  protected AmazonS3 buildAmazonS3Client(
      final ClientConfiguration awsConf,
      final S3ClientCreationParameters parameters) {
    AmazonS3ClientBuilder b = AmazonS3Client.builder();
    b.withCredentials(parameters.getCredentialSet());
    b.withClientConfiguration(awsConf);
    b.withPathStyleAccessEnabled(parameters.isPathStyleAccess());

    if (parameters.getMetrics() != null) {
      b.withMetricsCollector(
          new AwsStatisticsCollector(parameters.getMetrics()));
    }
    if (parameters.getRequestHandlers() != null) {
      b.withRequestHandlers(
          parameters.getRequestHandlers().toArray(new RequestHandler2[0]));
    }
    if (parameters.getMonitoringListener() != null) {
      b.withMonitoringListener(parameters.getMonitoringListener());
    }

    // endpoint set up is a PITA
    AwsClientBuilder.EndpointConfiguration epr
        = createEndpointConfiguration(parameters.getEndpoint(),
        awsConf);
    if (epr != null) {
      // an endpoint binding was constructed: use it.
      b.withEndpointConfiguration(epr);
    } else {
      // no idea what the endpoint is, so tell the SDK
      // to work it out at the cost of an extra HEAD request
      b.withForceGlobalBucketAccessEnabled(true);
    }
    final AmazonS3 client = b.build();
    return client;
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

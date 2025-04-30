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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.s3a.impl.AWSClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.auth.spi.scheme.AuthScheme;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.metrics.LoggingMetricPublisher;
import software.amazon.awssdk.s3accessgrants.plugin.S3AccessGrantsPlugin;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.s3.internal.crt.S3CrtAsyncClient;
import software.amazon.awssdk.services.s3.multipart.MultipartConfiguration;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.s3a.impl.AWSRegionEndpointInformation;
import org.apache.hadoop.fs.s3a.impl.AWSRegionEndpointResolver;
import org.apache.hadoop.fs.s3a.statistics.impl.AwsStatisticsCollector;
import org.apache.hadoop.fs.store.LogExactlyOnce;

import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_ACCESS_GRANTS_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_ACCESS_GRANTS_FALLBACK_TO_IAM_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.FIPS_ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.HTTP_SIGNER_CLASS_NAME;
import static org.apache.hadoop.fs.s3a.Constants.HTTP_SIGNER_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.HTTP_SIGNER_ENABLED_DEFAULT;
import static org.apache.hadoop.fs.s3a.Constants.AWS_SERVICE_IDENTIFIER_S3;
import static org.apache.hadoop.fs.s3a.Constants.REQUESTER_PAYS_HEADER_VALUE;
import static org.apache.hadoop.fs.s3a.auth.SignerFactory.createHttpSigner;
import static org.apache.hadoop.fs.s3a.impl.AWSHeaders.REQUESTER_PAYS_HEADER;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.AUTH_SCHEME_AWS_SIGV_4;


/**
 * The default {@link S3ClientFactory} implementation.
 * This calls the AWS SDK to configure and create an
 * {@code AmazonS3Client} that communicates with the S3 service.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DefaultS3ClientFactory extends Configured
    implements S3ClientFactory {

  /**
   * Subclasses refer to this.
   */
  protected static final Logger LOG =
      LoggerFactory.getLogger(DefaultS3ClientFactory.class);

  /**
   * A one-off log stating whether S3 CRT client is enabled.
   */
  private static final LogExactlyOnce LOG_S3_CRT_ENABLED = new LogExactlyOnce(LOG);


  /** Exactly once log to inform about ignoring the AWS-SDK Warnings for CSE. */
  private static final LogExactlyOnce IGNORE_CSE_WARN = new LogExactlyOnce(LOG);

  /**
   * Error message when an endpoint is set with FIPS enabled: {@value}.
   */
  @VisibleForTesting
  public static final String ERROR_ENDPOINT_WITH_FIPS =
      "Non central endpoint cannot be set when " + FIPS_ENDPOINT + " is true";

  /**
   * A one-off log stating whether S3 Access Grants are enabled.
   */
  private static final LogExactlyOnce LOG_S3AG_ENABLED = new LogExactlyOnce(LOG);

  @Override
  public S3Client createS3Client(
      final URI uri,
      final S3ClientCreationParameters parameters) throws IOException {

    Configuration conf = getConf();
    String bucket = uri.getHost();

    ApacheHttpClient.Builder httpClientBuilder = AWSClientConfig
        .createHttpClientBuilder(conf)
        .proxyConfiguration(AWSClientConfig.createProxyConfiguration(conf, bucket));
    return configureClientBuilder(S3Client.builder(), parameters, conf, bucket)
        .httpClientBuilder(httpClientBuilder)
        .build();
  }

  @Override
  public S3AsyncClient createS3AsyncClient(final URI uri,
      final S3ClientCreationParameters parameters) throws IOException {
    if (parameters.isCrtEnabled()) {
      LOG_S3_CRT_ENABLED.debug("The S3 CRT client is enabled");
      return createS3CrtAsyncClient(uri, parameters);
    } else {
      return createJavaAsyncClient(uri, parameters);
    }
  }

  public S3AsyncClient createJavaAsyncClient(
      final URI uri,
      final S3ClientCreationParameters parameters) throws IOException {

    Configuration conf = getConf();
    String bucket = uri.getHost();

    NettyNioAsyncHttpClient.Builder httpClientBuilder = AWSClientConfig
        .createAsyncHttpClientBuilder(conf)
        .proxyConfiguration(AWSClientConfig.createAsyncProxyConfiguration(conf, bucket));

    MultipartConfiguration multipartConfiguration = MultipartConfiguration.builder()
        .minimumPartSizeInBytes(parameters.getMinimumPartSize())
        .thresholdInBytes(parameters.getMultiPartThreshold())
        .build();

    S3AsyncClientBuilder s3AsyncClientBuilder =
            configureClientBuilder(S3AsyncClient.builder(), parameters, conf, bucket)
                .httpClientBuilder(httpClientBuilder);

    s3AsyncClientBuilder.multipartConfiguration(multipartConfiguration)
              .multipartEnabled(parameters.isMultipartCopy());

    return s3AsyncClientBuilder.build();
  }

  private S3AsyncClient createS3CrtAsyncClient(URI uri, S3ClientCreationParameters parameters)
      throws IOException {
    Configuration conf = getConf();
    String bucket = uri.getHost();

    S3CrtAsyncClientBuilder s3CrtAsyncClientBuilder = S3CrtAsyncClient.builder();

    AWSRegionEndpointInformation regionEndpointInformation =
        AWSRegionEndpointResolver.getEndpointRegionResolution(parameters, conf);

    if (regionEndpointInformation.getRegion() != null) {
      s3CrtAsyncClientBuilder.region(regionEndpointInformation.getRegion());
    }

    if (regionEndpointInformation.getEndpoint() != null) {
      s3CrtAsyncClientBuilder.endpointOverride(regionEndpointInformation.getEndpoint());
    }

    s3CrtAsyncClientBuilder
        .crossRegionAccessEnabled(regionEndpointInformation.isCrossRegionAccessEnabled());

    AWSClientConfig.configureConnectionSettings(s3CrtAsyncClientBuilder, conf, bucket);

    s3CrtAsyncClientBuilder
        .credentialsProvider(parameters.getCredentialSet())
        .forcePathStyle(parameters.isPathStyleAccess())
        .checksumValidationEnabled(parameters.isChecksumValidationEnabled());

    return s3CrtAsyncClientBuilder.build();
  }

  @Override
  public S3TransferManager createS3TransferManager(final S3AsyncClient s3AsyncClient) {
    return S3TransferManager.builder()
        .s3Client(s3AsyncClient)
        .build();
  }

  /**
   * Configure a sync or async S3 client builder.
   * This method handles all shared configuration, including
   * path style access, credentials and whether or not to use S3Express
   * CreateSession.
   * @param builder S3 client builder
   * @param parameters parameter object
   * @param conf configuration object
   * @param bucket bucket name
   * @return the builder object
   * @param <BuilderT> S3 client builder type
   * @param <ClientT> S3 client type
   */
  private <BuilderT extends S3BaseClientBuilder<BuilderT, ClientT>, ClientT> BuilderT configureClientBuilder(
      BuilderT builder, S3ClientCreationParameters parameters, Configuration conf, String bucket)
      throws IOException {

    AWSRegionEndpointInformation regionEndpointInformation =
        AWSRegionEndpointResolver.getEndpointRegionResolution(parameters, conf);

    if(regionEndpointInformation.getRegion() != null) {
      builder.region(regionEndpointInformation.getRegion());
    }

    if (regionEndpointInformation.getEndpoint() != null) {
      builder.endpointOverride(regionEndpointInformation.getEndpoint());
    }

    builder.crossRegionAccessEnabled(regionEndpointInformation.isCrossRegionAccessEnabled());
    builder.fipsEnabled(regionEndpointInformation.isFipsEnabled());

    maybeApplyS3AccessGrantsConfigurations(builder, conf);

    S3Configuration serviceConfiguration = S3Configuration.builder()
        .pathStyleAccessEnabled(parameters.isPathStyleAccess())
        .checksumValidationEnabled(parameters.isChecksumValidationEnabled())
        .build();

    final ClientOverrideConfiguration.Builder override =
        createClientOverrideConfiguration(parameters, conf);

    S3BaseClientBuilder<BuilderT, ClientT> s3BaseClientBuilder = builder
        .overrideConfiguration(override.build())
        .credentialsProvider(parameters.getCredentialSet())
        .disableS3ExpressSessionAuth(!parameters.isExpressCreateSession())
        .serviceConfiguration(serviceConfiguration);

    if (LOG.isTraceEnabled()) {
      // if this log is set to "trace" then we turn on logging of SDK metrics.
      // The metrics itself will log at info; it is just that reflection work
      // would be needed to change that setting safely for shaded and unshaded aws artifacts.
      s3BaseClientBuilder.overrideConfiguration(o ->
          o.addMetricPublisher(LoggingMetricPublisher.create()));
    }

    if (conf.getBoolean(HTTP_SIGNER_ENABLED, HTTP_SIGNER_ENABLED_DEFAULT)) {
      // use an http signer through an AuthScheme
      final AuthScheme<AwsCredentialsIdentity> signer =
          createHttpSigner(conf, AUTH_SCHEME_AWS_SIGV_4, HTTP_SIGNER_CLASS_NAME);
      builder.putAuthScheme(signer);
    }
    return (BuilderT) s3BaseClientBuilder;
  }

  /**
   * Create an override configuration for an S3 client.
   * @param parameters parameter object
   * @param conf configuration object
   * @throws IOException any IOE raised, or translated exception
   * @throws RuntimeException some failures creating an http signer
   * @return the override configuration
   * @throws IOException any IOE raised, or translated exception
   */
  protected ClientOverrideConfiguration.Builder createClientOverrideConfiguration(
      S3ClientCreationParameters parameters, Configuration conf) throws IOException {
    final ClientOverrideConfiguration.Builder clientOverrideConfigBuilder =
        AWSClientConfig.createClientConfigBuilder(conf, AWS_SERVICE_IDENTIFIER_S3);

    // add any headers
    parameters.getHeaders().forEach((h, v) -> clientOverrideConfigBuilder.putHeader(h, v));

    if (parameters.isRequesterPays()) {
      // All calls must acknowledge requester will pay via header.
      clientOverrideConfigBuilder.putHeader(REQUESTER_PAYS_HEADER, REQUESTER_PAYS_HEADER_VALUE);
    }

    if (!StringUtils.isEmpty(parameters.getUserAgentSuffix())) {
      clientOverrideConfigBuilder.putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_SUFFIX,
          parameters.getUserAgentSuffix());
    }

    if (parameters.getExecutionInterceptors() != null) {
      for (ExecutionInterceptor interceptor : parameters.getExecutionInterceptors()) {
        clientOverrideConfigBuilder.addExecutionInterceptor(interceptor);
      }
    }

    if (parameters.getMetrics() != null) {
      clientOverrideConfigBuilder.addMetricPublisher(
          new AwsStatisticsCollector(parameters.getMetrics()));
    }

    final RetryPolicy.Builder retryPolicyBuilder = AWSClientConfig.createRetryPolicyBuilder(conf);
    clientOverrideConfigBuilder.retryPolicy(retryPolicyBuilder.build());

    return clientOverrideConfigBuilder;
  }

  private static <BuilderT extends S3BaseClientBuilder<BuilderT, ClientT>, ClientT> void
  maybeApplyS3AccessGrantsConfigurations(BuilderT builder, Configuration conf) {
    boolean isS3AccessGrantsEnabled = conf.getBoolean(AWS_S3_ACCESS_GRANTS_ENABLED, false);
    if (!isS3AccessGrantsEnabled){
      LOG.debug("S3 Access Grants plugin is not enabled.");
      return;
    }

    boolean isFallbackEnabled =
        conf.getBoolean(AWS_S3_ACCESS_GRANTS_FALLBACK_TO_IAM_ENABLED, false);
    S3AccessGrantsPlugin accessGrantsPlugin =
        S3AccessGrantsPlugin.builder()
            .enableFallback(isFallbackEnabled)
            .build();
    builder.addPlugin(accessGrantsPlugin);
    LOG_S3AG_ENABLED.info(
        "S3 Access Grants plugin is enabled with IAM fallback set to {}", isFallbackEnabled);
  }

}

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.auth.spi.scheme.AuthScheme;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.metrics.LoggingMetricPublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.s3accessgrants.plugin.S3AccessGrantsPlugin;
import software.amazon.awssdk.services.s3.LegacyMd5Plugin;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.multipart.MultipartConfiguration;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.s3a.impl.AWSClientConfig;
import org.apache.hadoop.fs.s3a.impl.RegionResolution;
import org.apache.hadoop.fs.s3a.statistics.impl.AwsStatisticsCollector;
import org.apache.hadoop.fs.store.LogExactlyOnce;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_ACCESS_GRANTS_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_ACCESS_GRANTS_FALLBACK_TO_IAM_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.HTTP_SIGNER_CLASS_NAME;
import static org.apache.hadoop.fs.s3a.Constants.HTTP_SIGNER_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.HTTP_SIGNER_ENABLED_DEFAULT;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_SECURE_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.SECURE_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.AWS_SERVICE_IDENTIFIER_S3;
import static org.apache.hadoop.fs.s3a.auth.SignerFactory.createHttpSigner;
import static org.apache.hadoop.fs.s3a.impl.AWSHeaders.REQUESTER_PAYS_HEADER;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.AUTH_SCHEME_AWS_SIGV_4;
import static org.apache.hadoop.fs.s3a.impl.RegionResolution.RegionResolutionMechanism.Sdk;
import static org.apache.hadoop.fs.s3a.impl.RegionResolution.calculateRegion;


/**
 * The default {@link S3ClientFactory} implementation.
 * This calls the AWS SDK to configure and create an
 * {@code AmazonS3Client} that communicates with the S3 service.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DefaultS3ClientFactory extends Configured
    implements S3ClientFactory {

  private static final String REQUESTER_PAYS_HEADER_VALUE = "requester";

  /**
   * Subclasses refer to this.
   */
  protected static final Logger LOG =
      LoggerFactory.getLogger(DefaultS3ClientFactory.class);

  /**
   * Message printed when the SDK Region chain is in use.
   */
  private static final String SDK_REGION_CHAIN_IN_USE =
      "S3A filesystem client is using the SDK region resolution chain.";

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
  public S3AsyncClient createS3AsyncClient(
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

    // multipart upload pending with HADOOP-19326.
    if (!parameters.isClientSideEncryptionEnabled() &&
        !parameters.isAnalyticsAcceleratorEnabled()) {
      s3AsyncClientBuilder.multipartConfiguration(multipartConfiguration)
              .multipartEnabled(parameters.isMultipartCopy());
    }

    return s3AsyncClientBuilder.build();
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

    configureEndpointAndRegion(builder, parameters, conf);

    // add a plugin to add a Content-MD5 header.
    // this is required when performing some operations with third party stores
    // (for example: bulk delete), and is somewhat harmless when working with AWS S3.
    if (parameters.isMd5HeaderEnabled()) {
      LOG.debug("MD5 header enabled");
      builder.addPlugin(LegacyMd5Plugin.create());
    }

    //when to calculate request checksums.
    final RequestChecksumCalculation checksumCalculation =
        parameters.isChecksumCalculationEnabled()
            ? RequestChecksumCalculation.WHEN_SUPPORTED
            : RequestChecksumCalculation.WHEN_REQUIRED;
    LOG.debug("Using checksum calculation policy: {}", checksumCalculation);
    builder.requestChecksumCalculation(checksumCalculation);

    // response checksum validation. Slow, even with CRC32 checksums.
    final ResponseChecksumValidation checksumValidation;
    checksumValidation = parameters.isChecksumValidationEnabled()
        ? ResponseChecksumValidation.WHEN_SUPPORTED
        : ResponseChecksumValidation.WHEN_REQUIRED;
    LOG.debug("Using checksum validation policy: {}", checksumValidation);
    builder.responseChecksumValidation(checksumValidation);

    maybeApplyS3AccessGrantsConfigurations(builder, conf);

    S3Configuration serviceConfiguration = S3Configuration.builder()
        .pathStyleAccessEnabled(parameters.isPathStyleAccess())
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

  /**
   * This method configures the endpoint and region for a S3 client.
   * See {@link RegionResolution} for the details.
   * @param builder S3 client builder.
   * @param parameters parameter object
   * @param conf conf configuration object
   * @param <BuilderT> S3 client builder type
   * @param <ClientT> S3 client type
   * @return how the region was resolved.
   * @throws IllegalArgumentException if endpoint is set when FIPS is enabled.
   */
  private <BuilderT extends S3BaseClientBuilder<BuilderT, ClientT>, ClientT> RegionResolution.Resolution
  configureEndpointAndRegion(BuilderT builder,
      S3ClientCreationParameters parameters,
      Configuration conf) throws IOException {

    final RegionResolution.Resolution resolution =
        calculateRegion(parameters, conf);
    LOG.debug("Region Resolution: {}", resolution);

    // always setting to true or false guarantees the value is non-null,
    // which tests expect.
    builder.fipsEnabled(resolution.isUseFips());

    if (Sdk != resolution.getMechanism()) {

      // a region has been determined from configuration,
      // or it is falling back to central region.

      final Region region = resolution.getRegion();
      builder.region(requireNonNull(region));
      // s3 cross region access
      if (resolution.isCrossRegionAccessEnabled()) {
        builder.crossRegionAccessEnabled(true);
      }
      final URI endpointUri = resolution.getEndpointUri();
      if (endpointUri != null && !resolution.isUseCentralEndpoint()) {
        LOG.debug("Setting endpoint to {}", endpointUri);
        builder.endpointOverride(endpointUri);
      }
    }
    return resolution;
  }

  /**
   * Given a endpoint string, create the endpoint URI.
   * <p>Kept in as subclasses use it.
   * @param endpoint possibly null endpoint.
   * @param conf config to build the URI from.
   * @return an endpoint uri
   */
  protected static URI getS3Endpoint(String endpoint, final Configuration conf) {
    boolean secureConnections = conf.getBoolean(SECURE_CONNECTIONS, DEFAULT_SECURE_CONNECTIONS);
    return RegionResolution.buildEndpointUri(endpoint, secureConnections);
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

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

package org.apache.hadoop.fs.s3a.impl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.auth.SignerFactory;
import org.apache.hadoop.util.VersionInfo;

import static org.apache.hadoop.fs.s3a.Constants.CONNECTION_ACQUISITION_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.AWS_SERVICE_IDENTIFIER_S3;
import static org.apache.hadoop.fs.s3a.Constants.AWS_SERVICE_IDENTIFIER_STS;
import static org.apache.hadoop.fs.s3a.Constants.CONNECTION_IDLE_TIME;
import static org.apache.hadoop.fs.s3a.Constants.CONNECTION_KEEPALIVE;
import static org.apache.hadoop.fs.s3a.Constants.CONNECTION_TTL;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_CONNECTION_ACQUISITION_TIMEOUT_DURATION;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_CONNECTION_IDLE_TIME_DURATION;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_CONNECTION_KEEPALIVE;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_CONNECTION_TTL_DURATION;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_ESTABLISH_TIMEOUT_DURATION;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_MAXIMUM_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_MAX_ERROR_RETRIES;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_REQUEST_TIMEOUT_DURATION;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_SOCKET_TIMEOUT_DURATION;
import static org.apache.hadoop.fs.s3a.Constants.ESTABLISH_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.MAXIMUM_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.MAX_ERROR_RETRIES;
import static org.apache.hadoop.fs.s3a.Constants.MINIMUM_NETWORK_OPERATION_DURATION;
import static org.apache.hadoop.fs.s3a.Constants.PROXY_DOMAIN;
import static org.apache.hadoop.fs.s3a.Constants.PROXY_HOST;
import static org.apache.hadoop.fs.s3a.Constants.PROXY_PASSWORD;
import static org.apache.hadoop.fs.s3a.Constants.PROXY_PORT;
import static org.apache.hadoop.fs.s3a.Constants.PROXY_SECURED;
import static org.apache.hadoop.fs.s3a.Constants.PROXY_USERNAME;
import static org.apache.hadoop.fs.s3a.Constants.PROXY_WORKSTATION;
import static org.apache.hadoop.fs.s3a.Constants.REQUEST_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.SIGNING_ALGORITHM;
import static org.apache.hadoop.fs.s3a.Constants.SIGNING_ALGORITHM_S3;
import static org.apache.hadoop.fs.s3a.Constants.SIGNING_ALGORITHM_STS;
import static org.apache.hadoop.fs.s3a.Constants.SOCKET_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.USER_AGENT_PREFIX;
import static org.apache.hadoop.fs.s3a.impl.ConfigurationHelper.enforceMinimumDuration;
import static org.apache.hadoop.fs.s3a.impl.ConfigurationHelper.getDuration;
import static org.apache.hadoop.util.Preconditions.checkArgument;


/**
 * Methods for configuring the S3 client.
 * These methods are used when creating and configuring
 * the HTTP clients which communicate with the S3 service.
 * <p>
 * See {@code software.amazon.awssdk.http.SdkHttpConfigurationOption}
 * for the default values.
 */
public final class AWSClientConfig {

  private static final Logger LOG = LoggerFactory.getLogger(AWSClientConfig.class);

  /**
   * The minimum operation duration.
   */
  private static Duration minimumOperationDuration = MINIMUM_NETWORK_OPERATION_DURATION;


  private AWSClientConfig() {
  }

  /**
   * Create the config for a given service...the service identifier is used
   * to determine signature implementation.
   * @param conf configuration
   * @param awsServiceIdentifier service
   * @return the builder inited with signer, timeouts and UA.
   * @throws IOException failure.
   * @throws RuntimeException some failures creating an http signer
   */
  public static ClientOverrideConfiguration.Builder createClientConfigBuilder(Configuration conf,
      String awsServiceIdentifier) throws IOException {
    ClientOverrideConfiguration.Builder overrideConfigBuilder =
        ClientOverrideConfiguration.builder();

    initRequestTimeout(conf, overrideConfigBuilder);

    initUserAgent(conf, overrideConfigBuilder);

    String signer = conf.getTrimmed(SIGNING_ALGORITHM, "");
    if (!signer.isEmpty()) {
      LOG.debug("Signer override = {}", signer);
      overrideConfigBuilder.putAdvancedOption(SdkAdvancedClientOption.SIGNER,
          SignerFactory.createSigner(signer, SIGNING_ALGORITHM));
    }

    initSigner(conf, overrideConfigBuilder, awsServiceIdentifier);

    return overrideConfigBuilder;
  }

  /**
   * Create and configure the http client-based connector with timeouts for:
   * connection acquisition, max idle, timeout, TTL, socket and keepalive.
   * SSL channel mode is set up via
   * {@link NetworkBinding#bindSSLChannelMode(Configuration, ApacheHttpClient.Builder)}.
   *
   * @param conf The Hadoop configuration
   * @return Http client builder
   * @throws IOException on any problem
   */
  public static ApacheHttpClient.Builder createHttpClientBuilder(Configuration conf)
      throws IOException {
    final ConnectionSettings conn = createConnectionSettings(conf);
    ApacheHttpClient.Builder httpClientBuilder =
        ApacheHttpClient.builder()
            .connectionAcquisitionTimeout(conn.getAcquisitionTimeout())
            .connectionMaxIdleTime(conn.getMaxIdleTime())
            .connectionTimeout(conn.getEstablishTimeout())
            .connectionTimeToLive(conn.getConnectionTTL())
            .maxConnections(conn.getMaxConnections())
            .socketTimeout(conn.getSocketTimeout())
            .tcpKeepAlive(conn.isKeepAlive())
            .useIdleConnectionReaper(true);  // true by default in the SDK

    NetworkBinding.bindSSLChannelMode(conf, httpClientBuilder);

    return httpClientBuilder;
  }

  /**
   * Create and configure the async http client with timeouts for:
   * connection acquisition, max idle, timeout, TTL, socket and keepalive.
   * This is netty based and does not allow for the SSL channel mode to be set.
   * @param conf The Hadoop configuration
   * @return Async Http client builder
   */
  public static NettyNioAsyncHttpClient.Builder createAsyncHttpClientBuilder(Configuration conf) {
    final ConnectionSettings conn = createConnectionSettings(conf);

    NettyNioAsyncHttpClient.Builder httpClientBuilder =
        NettyNioAsyncHttpClient.builder()
            .connectionAcquisitionTimeout(conn.getAcquisitionTimeout())
            .connectionMaxIdleTime(conn.getMaxIdleTime())
            .connectionTimeout(conn.getEstablishTimeout())
            .connectionTimeToLive(conn.getConnectionTTL())
            .maxConcurrency(conn.getMaxConnections())
            .readTimeout(conn.getSocketTimeout())
            .tcpKeepAlive(conn.isKeepAlive())
            .useIdleConnectionReaper(true)  // true by default in the SDK
            .writeTimeout(conn.getSocketTimeout());

    // TODO: Don't think you can set a socket factory for the netty client.
    //  NetworkBinding.bindSSLChannelMode(conf, awsConf);

    return httpClientBuilder;
  }

  /**
   * Configures the retry policy.
   * Retry policy is {@code RetryMode.ADAPTIVE}, which
   * "dynamically limits the rate of AWS requests to maximize success rate",
   * possibly at the expense of latency.
   * Based on the ABFS experience, it is better to limit the rate requests are
   * made rather than have to resort to exponential backoff after failures come
   * in -especially as that backoff is per http connection.
   *
   * @param conf The Hadoop configuration
   * @return Retry policy builder
   */
  public static RetryPolicy.Builder createRetryPolicyBuilder(Configuration conf) {

    RetryPolicy.Builder retryPolicyBuilder = RetryPolicy.builder(RetryMode.ADAPTIVE);

    retryPolicyBuilder.numRetries(S3AUtils.intOption(conf, MAX_ERROR_RETRIES,
        DEFAULT_MAX_ERROR_RETRIES, 0));

    return retryPolicyBuilder;
  }

  /**
   * Configures the proxy.
   *
   * @param conf The Hadoop configuration
   * @param bucket Optional bucket to use to look up per-bucket proxy secrets
   * @return Proxy configuration
   * @throws IOException on any IO problem
   */
  public static ProxyConfiguration createProxyConfiguration(Configuration conf,
      String bucket) throws IOException {

    ProxyConfiguration.Builder proxyConfigBuilder = ProxyConfiguration.builder();

    String proxyHost = conf.getTrimmed(PROXY_HOST, "");
    int proxyPort = conf.getInt(PROXY_PORT, -1);

    if (!proxyHost.isEmpty()) {
      if (proxyPort >= 0) {
        String scheme = conf.getBoolean(PROXY_SECURED, false) ? "https" : "http";
        proxyConfigBuilder.endpoint(buildURI(scheme, proxyHost, proxyPort));
      } else {
        if (conf.getBoolean(PROXY_SECURED, false)) {
          LOG.warn("Proxy host set without port. Using HTTPS default 443");
          proxyConfigBuilder.endpoint(buildURI("https", proxyHost, 443));
        } else {
          LOG.warn("Proxy host set without port. Using HTTP default 80");
          proxyConfigBuilder.endpoint(buildURI("http", proxyHost, 80));
        }
      }
      final String proxyUsername = S3AUtils.lookupPassword(bucket, conf, PROXY_USERNAME,
          null, null);
      final String proxyPassword = S3AUtils.lookupPassword(bucket, conf, PROXY_PASSWORD,
          null, null);
      if ((proxyUsername == null) != (proxyPassword == null)) {
        String msg = "Proxy error: " + PROXY_USERNAME + " or " +
            PROXY_PASSWORD + " set without the other.";
        LOG.error(msg);
        throw new IllegalArgumentException(msg);
      }
      proxyConfigBuilder.username(proxyUsername);
      proxyConfigBuilder.password(proxyPassword);
      proxyConfigBuilder.ntlmDomain(conf.getTrimmed(PROXY_DOMAIN));
      proxyConfigBuilder.ntlmWorkstation(conf.getTrimmed(PROXY_WORKSTATION));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using proxy server {}:{} as user {} with password {} on "
                + "domain {} as workstation {}", proxyHost, proxyPort, proxyUsername, proxyPassword,
            PROXY_DOMAIN, PROXY_WORKSTATION);
      }
    } else if (proxyPort >= 0) {
      String msg =
          "Proxy error: " + PROXY_PORT + " set without " + PROXY_HOST;
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    return proxyConfigBuilder.build();

  }

  /**
   * Configures the proxy for the async http client.
   * <p>
   * Although this is netty specific, it is part of the AWS SDK public API
   * and not any shaded netty internal class.
   * @param conf The Hadoop configuration
   * @param bucket Optional bucket to use to look up per-bucket proxy secrets
   * @return Proxy configuration
   * @throws IOException on any IO problem
   */
  public static software.amazon.awssdk.http.nio.netty.ProxyConfiguration
      createAsyncProxyConfiguration(Configuration conf,
          String bucket) throws IOException {

    software.amazon.awssdk.http.nio.netty.ProxyConfiguration.Builder proxyConfigBuilder =
        software.amazon.awssdk.http.nio.netty.ProxyConfiguration.builder();

    String proxyHost = conf.getTrimmed(PROXY_HOST, "");
    int proxyPort = conf.getInt(PROXY_PORT, -1);

    if (!proxyHost.isEmpty()) {
      if (proxyPort >= 0) {
        String scheme = conf.getBoolean(PROXY_SECURED, false) ? "https" : "http";
        proxyConfigBuilder.host(proxyHost);
        proxyConfigBuilder.port(proxyPort);
        proxyConfigBuilder.scheme(scheme);
      } else {
        if (conf.getBoolean(PROXY_SECURED, false)) {
          LOG.warn("Proxy host set without port. Using HTTPS default 443");
          proxyConfigBuilder.host(proxyHost);
          proxyConfigBuilder.port(443);
          proxyConfigBuilder.scheme("https");
        } else {
          LOG.warn("Proxy host set without port. Using HTTP default 80");
          proxyConfigBuilder.host(proxyHost);
          proxyConfigBuilder.port(80);
          proxyConfigBuilder.scheme("http");
        }
      }
      final String proxyUsername = S3AUtils.lookupPassword(bucket, conf, PROXY_USERNAME,
          null, null);
      final String proxyPassword = S3AUtils.lookupPassword(bucket, conf, PROXY_PASSWORD,
          null, null);
      if ((proxyUsername == null) != (proxyPassword == null)) {
        String msg = "Proxy error: " + PROXY_USERNAME + " or " +
            PROXY_PASSWORD + " set without the other.";
        LOG.error(msg);
        throw new IllegalArgumentException(msg);
      }
      proxyConfigBuilder.username(proxyUsername);
      proxyConfigBuilder.password(proxyPassword);
      // TODO: check NTLM support
      // proxyConfigBuilder.ntlmDomain(conf.getTrimmed(PROXY_DOMAIN));
      // proxyConfigBuilder.ntlmWorkstation(conf.getTrimmed(PROXY_WORKSTATION));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using proxy server {}:{} as user {} with password {} on "
                + "domain {} as workstation {}", proxyHost, proxyPort, proxyUsername, proxyPassword,
            PROXY_DOMAIN, PROXY_WORKSTATION);
      }
    } else if (proxyPort >= 0) {
      String msg =
          "Proxy error: " + PROXY_PORT + " set without " + PROXY_HOST;
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    } else {
      return null;
    }

    return proxyConfigBuilder.build();
  }

  /**
   * Builds a URI, throws an IllegalArgumentException in case of errors.
   *
   * @param host proxy host
   * @param port proxy port
   * @return uri with host and port
   */
  private static URI buildURI(String scheme, String host, int port) {
    try {
      return new URI(scheme, null, host, port, null, null, null);
    } catch (URISyntaxException e) {
      String msg =
          "Proxy error: incorrect " + PROXY_HOST + " or " + PROXY_PORT;
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
  }

  /**
   * Initializes the User-Agent header to send in HTTP requests to AWS
   * services.  We always include the Hadoop version number.  The user also
   * may set an optional custom prefix to put in front of the Hadoop version
   * number.  The AWS SDK internally appends its own information, which seems
   * to include the AWS SDK version, OS and JVM version.
   *
   * @param conf Hadoop configuration
   * @param clientConfig AWS SDK configuration to update
   */
  private static void initUserAgent(Configuration conf,
      ClientOverrideConfiguration.Builder clientConfig) {
    String userAgent = "Hadoop " + VersionInfo.getVersion();
    String userAgentPrefix = conf.getTrimmed(USER_AGENT_PREFIX, "");
    if (!userAgentPrefix.isEmpty()) {
      userAgent = userAgentPrefix + ", " + userAgent;
    }
    LOG.debug("Using User-Agent: {}", userAgent);
    clientConfig.putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, userAgent);
  }

  /**
   * Initializes the signer override for the given service.
   * @param conf hadoop configuration
   * @param clientConfig client configuration to update
   * @param awsServiceIdentifier service name
   * @throws IOException failure.
   */
  private static void initSigner(Configuration conf,
      ClientOverrideConfiguration.Builder clientConfig, String awsServiceIdentifier)
      throws IOException {
    String configKey = null;
    switch (awsServiceIdentifier) {
    case AWS_SERVICE_IDENTIFIER_S3:
      configKey = SIGNING_ALGORITHM_S3;
      break;
    case AWS_SERVICE_IDENTIFIER_STS:
      configKey = SIGNING_ALGORITHM_STS;
      break;
    default:
      // Nothing to do. The original signer override is already setup
    }
    if (configKey != null) {
      String signerOverride = conf.getTrimmed(configKey, "");
      if (!signerOverride.isEmpty()) {
        LOG.debug("Signer override for {} = {}", awsServiceIdentifier, signerOverride);
        clientConfig.putAdvancedOption(SdkAdvancedClientOption.SIGNER,
            SignerFactory.createSigner(signerOverride, configKey));
      }
    }
  }

  /**
   * Configures request timeout in the client configuration.
   * This is independent of the timeouts set in the sync and async HTTP clients;
   * the same method
   *
   * @param conf Hadoop configuration
   * @param clientConfig AWS SDK configuration to update
   */
  private static void initRequestTimeout(Configuration conf,
      ClientOverrideConfiguration.Builder clientConfig) {
    // Get the connection settings
    final Duration callTimeout = createApiConnectionSettings(conf).getApiCallTimeout();

    if (callTimeout.toMillis() > 0) {
      clientConfig.apiCallAttemptTimeout(callTimeout);
      clientConfig.apiCallTimeout(callTimeout);
    }
  }

  /**
   * Reset the minimum operation duration to the default.
   * For test use only; Logs at INFO.
   * <p>
   * This MUST be called in test teardown in any test suite which
   * called {@link #setMinimumOperationDuration(Duration)}.
   */
  @VisibleForTesting
  public static void resetMinimumOperationDuration() {
    setMinimumOperationDuration(MINIMUM_NETWORK_OPERATION_DURATION);
  }

  /**
   * Set the minimum operation duration.
   * This is for testing and will log at info; does require a non-negative duration.
   * <p>
   * Test suites must call {@link #resetMinimumOperationDuration()} in their teardown
   * to avoid breaking subsequent tests in the same process.
   * @param duration non-negative duration
   * @throws IllegalArgumentException if the duration is negative.
   */
  @VisibleForTesting
  public static void setMinimumOperationDuration(Duration duration) {
    LOG.info("Setting minimum operation duration to {}ms", duration.toMillis());
    checkArgument(duration.compareTo(Duration.ZERO) >= 0,
        "Duration must be positive: %sms", duration.toMillis());
    minimumOperationDuration = duration;
  }

  /**
   * Get the current minimum operation duration.
   * @return current duration.
   */
  public static Duration getMinimumOperationDuration() {
    return minimumOperationDuration;
  }

  /**
   * Settings for the AWS client, rather than the http client.
   */
  static class ClientSettings {
    private final Duration apiCallTimeout;

    private ClientSettings(final Duration apiCallTimeout) {
      this.apiCallTimeout = apiCallTimeout;
    }

    Duration getApiCallTimeout() {
      return apiCallTimeout;
    }

    @Override
    public String toString() {
      return "ClientSettings{" +
          "apiCallTimeout=" + apiCallTimeout +
          '}';
    }
  }

  /**
   * All the connection settings, wrapped as a class for use by
   * both the sync and async client.
   */
  static class ConnectionSettings {
    private final int maxConnections;
    private final boolean keepAlive;
    private final Duration acquisitionTimeout;
    private final Duration connectionTTL;
    private final Duration establishTimeout;
    private final Duration maxIdleTime;
    private final Duration socketTimeout;

    private ConnectionSettings(
        final int maxConnections,
        final boolean keepAlive,
        final Duration acquisitionTimeout,
        final Duration connectionTTL,
        final Duration establishTimeout,
        final Duration maxIdleTime,
        final Duration socketTimeout) {
      this.maxConnections = maxConnections;
      this.keepAlive = keepAlive;
      this.acquisitionTimeout = acquisitionTimeout;
      this.connectionTTL = connectionTTL;
      this.establishTimeout = establishTimeout;
      this.maxIdleTime = maxIdleTime;
      this.socketTimeout = socketTimeout;
    }

    int getMaxConnections() {
      return maxConnections;
    }

    boolean isKeepAlive() {
      return keepAlive;
    }

    Duration getAcquisitionTimeout() {
      return acquisitionTimeout;
    }

    Duration getConnectionTTL() {
      return connectionTTL;
    }

    Duration getEstablishTimeout() {
      return establishTimeout;
    }

    Duration getMaxIdleTime() {
      return maxIdleTime;
    }

    Duration getSocketTimeout() {
      return socketTimeout;
    }

    @Override
    public String toString() {
      return "ConnectionSettings{" +
          "maxConnections=" + maxConnections +
          ", keepAlive=" + keepAlive +
          ", acquisitionTimeout=" + acquisitionTimeout +
          ", connectionTTL=" + connectionTTL +
          ", establishTimeout=" + establishTimeout +
          ", maxIdleTime=" + maxIdleTime +
          ", socketTimeout=" + socketTimeout +
          '}';
    }
  }

  /**
   * Build a client settings object.
   * @param conf configuration to evaluate
   * @return connection settings.
   */
  static ClientSettings createApiConnectionSettings(Configuration conf) {

    Duration apiCallTimeout = getDuration(conf, REQUEST_TIMEOUT,
        DEFAULT_REQUEST_TIMEOUT_DURATION, TimeUnit.MILLISECONDS, Duration.ZERO);

    // if the API call timeout is set, it must be at least the minimum duration
    if (apiCallTimeout.compareTo(Duration.ZERO) > 0) {
      apiCallTimeout = enforceMinimumDuration(REQUEST_TIMEOUT,
          apiCallTimeout, minimumOperationDuration);
    }
    return new ClientSettings(apiCallTimeout);
  }

  /**
   * Build the HTTP connection settings object from the configuration.
   * All settings are calculated.
   * @param conf configuration to evaluate
   * @return connection settings.
   */
  static ConnectionSettings createConnectionSettings(Configuration conf) {

    int maxConnections = S3AUtils.intOption(conf, MAXIMUM_CONNECTIONS,
        DEFAULT_MAXIMUM_CONNECTIONS, 1);

    final boolean keepAlive = conf.getBoolean(CONNECTION_KEEPALIVE,
        DEFAULT_CONNECTION_KEEPALIVE);

    // time to acquire a connection from the pool
    Duration acquisitionTimeout = getDuration(conf, CONNECTION_ACQUISITION_TIMEOUT,
        DEFAULT_CONNECTION_ACQUISITION_TIMEOUT_DURATION, TimeUnit.MILLISECONDS,
        minimumOperationDuration);

    // set the connection TTL irrespective of whether the connection is in use or not.
    // this can balance requests over different S3 servers, and avoid failed
    // connections. See HADOOP-18845.
    Duration connectionTTL = getDuration(conf, CONNECTION_TTL,
        DEFAULT_CONNECTION_TTL_DURATION, TimeUnit.MILLISECONDS,
        null);

    Duration establishTimeout = getDuration(conf, ESTABLISH_TIMEOUT,
        DEFAULT_ESTABLISH_TIMEOUT_DURATION, TimeUnit.MILLISECONDS,
        minimumOperationDuration);

    // limit on the time a connection can be idle in the pool
    Duration maxIdleTime = getDuration(conf, CONNECTION_IDLE_TIME,
        DEFAULT_CONNECTION_IDLE_TIME_DURATION, TimeUnit.MILLISECONDS, Duration.ZERO);

    Duration socketTimeout = getDuration(conf, SOCKET_TIMEOUT,
        DEFAULT_SOCKET_TIMEOUT_DURATION, TimeUnit.MILLISECONDS,
        minimumOperationDuration);

    return new ConnectionSettings(
        maxConnections,
        keepAlive,
        acquisitionTimeout,
        connectionTTL,
        establishTimeout,
        maxIdleTime,
        socketTimeout);
  }

}

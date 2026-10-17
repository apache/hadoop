/**
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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ProtocolException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbfsCountersImpl;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.TestAbfsConfigurationFieldsValidation;
import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.constants.HttpOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsApacheHttpExpect100Exception;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TokenAccessProviderException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.utils.MetricFormat;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.apache.hadoop.test.ReflectionUtils;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.utils.URIBuilder;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.ITestAzureBlobFileSystemListStatus.TEST_CONTINUATION_TOKEN;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APN_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPEND_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CLIENT_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DOT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EXPECT_100_JDK_ERROR;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FORWARD_SLASH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_HEAD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PATCH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HUNDRED_CONTINUE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_IS_HNS_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ALWAYS_USE_HTTPS;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_METRICS_COLLECTION_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_METRICS_EMIT_THRESHOLD_INTERVAL_SECS;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_METRICS_ACCOUNT_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_METRICS_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_METRICS_FORMAT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_METRICS_EMIT_THRESHOLD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.JAVA_VENDOR;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.JAVA_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.OS_ARCH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.OS_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.OS_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SEMICOLON;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SINGLE_WHITE_SPACE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_CLUSTER_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_CLUSTER_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_VALUE_UNKNOWN;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.EXPECT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_HTTP_METHOD_OVERRIDE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpOperationType.APACHE_HTTP_CLIENT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpOperationType.JDK_HTTP_URL_CONNECTION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_POSITION;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.TEST_CONFIGURATION_FILE_NAME;
import static org.apache.hadoop.fs.azurebfs.services.AuthType.SharedKey;
import static org.apache.hadoop.fs.azurebfs.services.RetryPolicyConstants.EXPONENTIAL_RETRY_POLICY_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.utils.MetricFormat.INTERNAL_BACKOFF_METRIC_FORMAT;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * Test useragent of abfs client.
 *
 */
@ParameterizedClass(name="{0}")
@MethodSource("params")
public final class ITestAbfsClient extends AbstractAbfsIntegrationTest {

  private static final String ACCOUNT_NAME = "bogusAccountName.dfs.core.windows.net";
  private static final String FS_AZURE_USER_AGENT_PREFIX = "Partner Service";
  private static final String FNS_BLOB_USER_AGENT_IDENTIFIER = "FNS";
  private static final String HUNDRED_CONTINUE_USER_AGENT = SINGLE_WHITE_SPACE + HUNDRED_CONTINUE + SEMICOLON;
  private static final String TEST_PATH = "/testfile";
  public static final int REDUCED_RETRY_COUNT = 2;
  public static final int REDUCED_BACKOFF_INTERVAL = 100;
  public static final int BUFFER_LENGTH = 5;
  public static final int BUFFER_OFFSET = 0;
  private static final String RANDOM_URI = "abcd";
  private static final String RANDOM_FILESYSTEM_ID = "abcde";

  private final Pattern userAgentStringPattern;

  public HttpOperationType httpOperationType;

  public static Iterable<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {HttpOperationType.JDK_HTTP_URL_CONNECTION},
        {APACHE_HTTP_CLIENT}
    });
  }

  public ITestAbfsClient(HttpOperationType pHttpOperationType) throws Exception {
    this.httpOperationType = pHttpOperationType;
    StringBuilder regEx = new StringBuilder();
    regEx.append("^");
    regEx.append(APN_VERSION);
    regEx.append(SINGLE_WHITE_SPACE);
    regEx.append(CLIENT_VERSION);
    regEx.append(SINGLE_WHITE_SPACE);
    regEx.append("\\(");
    regEx.append(System.getProperty(JAVA_VENDOR)
        .replaceAll(SINGLE_WHITE_SPACE, EMPTY_STRING));
    regEx.append(SINGLE_WHITE_SPACE);
    regEx.append("JavaJRE");
    regEx.append(SINGLE_WHITE_SPACE);
    regEx.append(System.getProperty(JAVA_VERSION));
    regEx.append(SEMICOLON);
    regEx.append(SINGLE_WHITE_SPACE);
    regEx.append(System.getProperty(OS_NAME)
        .replaceAll(SINGLE_WHITE_SPACE, EMPTY_STRING));
    regEx.append(SINGLE_WHITE_SPACE);
    regEx.append(System.getProperty(OS_VERSION));
    regEx.append(FORWARD_SLASH);
    regEx.append(System.getProperty(OS_ARCH));
    regEx.append(SEMICOLON);
    regEx.append("([a-zA-Z].*; )?");      // Regex for sslProviderName
    regEx.append("([a-zA-Z].*; )?");      // Regex for tokenProvider
    regEx.append(" ?");
    regEx.append(".+");                   // cluster name
    regEx.append(FORWARD_SLASH);
    regEx.append(".+");            // cluster type
    regEx.append("\\)");
    regEx.append("( .*)?");        //  Regex for user agent prefix
    regEx.append("$");
    this.userAgentStringPattern = Pattern.compile(regEx.toString());
  }

  private String getUserAgentString(AbfsConfiguration config,
      boolean includeSSLProvider) throws IOException, URISyntaxException {
    AbfsCounters abfsCounters = Mockito.spy(new AbfsCountersImpl(new URI(RANDOM_URI)));
    AbfsClientContext abfsClientContext = new AbfsClientContextBuilder().withAbfsCounters(abfsCounters).build();
    AbfsClient client;
    if (AbfsServiceType.DFS.equals(config.getFsConfiguredServiceType())) {
      client = new AbfsDfsClient(new URL("https://azure.com"), null,
          config, (AccessTokenProvider) null, null, null, abfsClientContext);
    } else {
      client = new AbfsBlobClient(new URL("https://azure.com"), null,
          config, (AccessTokenProvider) null, null, null, abfsClientContext);
    }
    String sslProviderName = null;
    if (includeSSLProvider) {
      sslProviderName = DelegatingSSLSocketFactory.getDefaultFactory()
          .getProviderName();
    }
    return client.initializeUserAgent(config, sslProviderName);
  }

  @Test
  public void verifyBasicInfo() throws Exception {
    assumeThat(JDK_HTTP_URL_CONNECTION).isEqualTo(httpOperationType);
    final Configuration configuration = new Configuration();
    configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    verifyBasicInfo(getUserAgentString(abfsConfiguration, false));
  }

  private void verifyBasicInfo(String userAgentStr) {
    Assertions.assertThat(userAgentStr)
        .describedAs("User-Agent string [" + userAgentStr
            + "] should be of the pattern: " + this.userAgentStringPattern.pattern())
        .matches(this.userAgentStringPattern)
        .describedAs("User-Agent string should contain java vendor")
        .contains(System.getProperty(JAVA_VENDOR)
            .replaceAll(SINGLE_WHITE_SPACE, EMPTY_STRING))
        .describedAs("User-Agent string should contain java version")
        .contains(System.getProperty(JAVA_VERSION))
        .describedAs("User-Agent string should contain  OS name")
        .contains(System.getProperty(OS_NAME)
            .replaceAll(SINGLE_WHITE_SPACE, EMPTY_STRING))
        .describedAs("User-Agent string should contain OS version")
        .contains(System.getProperty(OS_VERSION))
        .describedAs("User-Agent string should contain OS arch")
        .contains(System.getProperty(OS_ARCH));
  }

  @Test
  public void verifyUserAgentPrefix()
      throws IOException, IllegalAccessException, URISyntaxException {
    assumeThat(JDK_HTTP_URL_CONNECTION).isEqualTo(httpOperationType);
    final Configuration configuration = new Configuration();
    configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
    configuration.set(ConfigurationKeys.FS_AZURE_USER_AGENT_PREFIX_KEY, FS_AZURE_USER_AGENT_PREFIX);
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    String userAgentStr = getUserAgentString(abfsConfiguration, false);

    verifyBasicInfo(userAgentStr);
    Assertions.assertThat(userAgentStr)
      .describedAs("User-Agent string should contain " + FS_AZURE_USER_AGENT_PREFIX)
      .contains(FS_AZURE_USER_AGENT_PREFIX);

    configuration.unset(ConfigurationKeys.FS_AZURE_USER_AGENT_PREFIX_KEY);
    abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    userAgentStr = getUserAgentString(abfsConfiguration, false);

    verifyBasicInfo(userAgentStr);
    Assertions.assertThat(userAgentStr)
      .describedAs("User-Agent string should not contain " + FS_AZURE_USER_AGENT_PREFIX)
      .doesNotContain(FS_AZURE_USER_AGENT_PREFIX);
  }

  /**
   * This method represents a unit test for verifying the behavior of the User-Agent header
   * with respect to the "Expect: 100-continue" header setting in the Azure Blob File System (ABFS) configuration.
   *
   * The test ensures that the User-Agent string includes or excludes specific information based on whether the
   * "Expect: 100-continue" header is enabled or disabled in the configuration.
   *
   */
  @Test
  public void verifyUserAgentExpectHeader()
          throws IOException, IllegalAccessException, URISyntaxException {
    assumeThat(JDK_HTTP_URL_CONNECTION).isEqualTo(httpOperationType);
    final Configuration configuration = new Configuration();
    configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
    configuration.set(ConfigurationKeys.FS_AZURE_USER_AGENT_PREFIX_KEY, FS_AZURE_USER_AGENT_PREFIX);
    configuration.setBoolean(ConfigurationKeys.FS_AZURE_ACCOUNT_IS_EXPECT_HEADER_ENABLED, true);
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
            ACCOUNT_NAME);
    String userAgentStr = getUserAgentString(abfsConfiguration, false);

    verifyBasicInfo(userAgentStr);
    Assertions.assertThat(userAgentStr)
            .describedAs("User-Agent string should contain " + HUNDRED_CONTINUE_USER_AGENT)
            .contains(HUNDRED_CONTINUE_USER_AGENT);

    configuration.setBoolean(ConfigurationKeys.FS_AZURE_ACCOUNT_IS_EXPECT_HEADER_ENABLED, false);
    abfsConfiguration = new AbfsConfiguration(configuration,
            ACCOUNT_NAME);
    userAgentStr = getUserAgentString(abfsConfiguration, false);

    verifyBasicInfo(userAgentStr);
    Assertions.assertThat(userAgentStr)
            .describedAs("User-Agent string should not contain " + HUNDRED_CONTINUE_USER_AGENT)
            .doesNotContain(HUNDRED_CONTINUE_USER_AGENT);
  }

  @Test
  public void verifyUserAgentWithoutSSLProvider() throws Exception {
    assumeThat(JDK_HTTP_URL_CONNECTION).isEqualTo(httpOperationType);
    final Configuration configuration = new Configuration();
    configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
    configuration.set(ConfigurationKeys.FS_AZURE_SSL_CHANNEL_MODE_KEY,
        DelegatingSSLSocketFactory.SSLChannelMode.Default_JSSE.name());
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    String userAgentStr = getUserAgentString(abfsConfiguration, true);

    verifyBasicInfo(userAgentStr);
    Assertions.assertThat(userAgentStr)
      .describedAs("User-Agent string should contain sslProvider")
      .contains(DelegatingSSLSocketFactory.getDefaultFactory().getProviderName());

    userAgentStr = getUserAgentString(abfsConfiguration, false);

    verifyBasicInfo(userAgentStr);
    Assertions.assertThat(userAgentStr)
      .describedAs("User-Agent string should not contain sslProvider")
      .doesNotContain(DelegatingSSLSocketFactory.getDefaultFactory().getProviderName());
  }

  @Test
  public void verifyUserAgentClusterName() throws Exception {
    assumeThat(JDK_HTTP_URL_CONNECTION).isEqualTo(httpOperationType);
    final String clusterName = "testClusterName";
    final Configuration configuration = new Configuration();
    configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
    configuration.set(FS_AZURE_CLUSTER_NAME, clusterName);
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    String userAgentStr = getUserAgentString(abfsConfiguration, false);

    verifyBasicInfo(userAgentStr);
    Assertions.assertThat(userAgentStr)
      .describedAs("User-Agent string should contain cluster name")
      .contains(clusterName);

    configuration.unset(FS_AZURE_CLUSTER_NAME);
    abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    userAgentStr = getUserAgentString(abfsConfiguration, false);

    verifyBasicInfo(userAgentStr);
    Assertions.assertThat(userAgentStr)
      .describedAs("User-Agent string should not contain cluster name")
      .doesNotContain(clusterName)
      .describedAs("User-Agent string should contain UNKNOWN as cluster name config is absent")
      .contains(DEFAULT_VALUE_UNKNOWN);
  }

  @Test
  public void verifyUserAgentClusterType() throws Exception {
    assumeThat(JDK_HTTP_URL_CONNECTION).isEqualTo(httpOperationType);
    final String clusterType = "testClusterType";
    final Configuration configuration = new Configuration();
    configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
    configuration.set(FS_AZURE_CLUSTER_TYPE, clusterType);
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    String userAgentStr = getUserAgentString(abfsConfiguration, false);

    verifyBasicInfo(userAgentStr);
    Assertions.assertThat(userAgentStr)
      .describedAs("User-Agent string should contain cluster type")
      .contains(clusterType);

    configuration.unset(FS_AZURE_CLUSTER_TYPE);
    abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    userAgentStr = getUserAgentString(abfsConfiguration, false);

    verifyBasicInfo(userAgentStr);
    Assertions.assertThat(userAgentStr)
      .describedAs("User-Agent string should not contain cluster type")
      .doesNotContain(clusterType)
      .describedAs("User-Agent string should contain UNKNOWN as cluster type config is absent")
      .contains(DEFAULT_VALUE_UNKNOWN);
  }

  @Test
  // Test to verify the unique identifier in user agent string for FNS-Blob accounts
  public void verifyUserAgentForFNS() throws Exception {
    assumeHnsDisabled();
    final AzureBlobFileSystem fs = getFileSystem();
    final AbfsConfiguration configuration = fs.getAbfsStore()
        .getAbfsConfiguration();

    String userAgentStr = getUserAgentString(configuration, false);
    verifyBasicInfo(userAgentStr);
    Assertions.assertThat(userAgentStr)
        .describedAs(
            "User-Agent string for FNS accounts on Blob endpoint should contain "
                + FNS_BLOB_USER_AGENT_IDENTIFIER)
        .contains(FNS_BLOB_USER_AGENT_IDENTIFIER);
  }

  public static AbfsClient createTestClientFromCurrentContext(
      AbfsClient baseAbfsClientInstance,
      AbfsConfiguration abfsConfig) throws IOException, URISyntaxException {
    AuthType currentAuthType = abfsConfig.getAuthType(
        abfsConfig.getAccountName());

    AbfsPerfTracker tracker = new AbfsPerfTracker("test",
        abfsConfig.getAccountName(),
        abfsConfig);
    AbfsCounters abfsCounters = Mockito.spy(new AbfsCountersImpl(new URI(RANDOM_URI)));

    AbfsClientContext abfsClientContext =
        new AbfsClientContextBuilder().withAbfsPerfTracker(tracker)
                                .withExponentialRetryPolicy(
                                    new ExponentialRetryPolicy(abfsConfig.getMaxIoRetries()))
                                .withAbfsCounters(abfsCounters)
                                .build();

    // Create test AbfsClient
    AbfsClient testClient;
    if (AbfsServiceType.DFS.equals(abfsConfig.getFsConfiguredServiceType())) {
      testClient = new AbfsDfsClient(
          baseAbfsClientInstance.getBaseUrl(),
          (currentAuthType == SharedKey
              ? new SharedKeyCredentials(
              abfsConfig.getAccountName().substring(0,
                  abfsConfig.getAccountName().indexOf(DOT)),
              abfsConfig.getStorageAccountKey())
              : null),
          abfsConfig,
          (currentAuthType == AuthType.OAuth
              ? abfsConfig.getTokenProvider()
              : null),
          null,
          null,
          abfsClientContext);
    } else {
      testClient = new AbfsBlobClient(
          baseAbfsClientInstance.getBaseUrl(),
          (currentAuthType == SharedKey
              ? new SharedKeyCredentials(
              abfsConfig.getAccountName().substring(0,
                  abfsConfig.getAccountName().indexOf(DOT)),
              abfsConfig.getStorageAccountKey())
              : null),
          abfsConfig,
          (currentAuthType == AuthType.OAuth
              ? abfsConfig.getTokenProvider()
              : null),
          null,
          null,
          abfsClientContext);
    }

    return testClient;
  }

  public static AbfsClient createBlobClientFromCurrentContext(
      AbfsClient baseAbfsClientInstance,
      AbfsConfiguration abfsConfig) throws IOException, URISyntaxException {
    AuthType currentAuthType = abfsConfig.getAuthType(
        abfsConfig.getAccountName());

    AbfsPerfTracker tracker = new AbfsPerfTracker("test",
        abfsConfig.getAccountName(),
        abfsConfig);
    AbfsCounters abfsCounters = Mockito.spy(new AbfsCountersImpl(new URI(RANDOM_URI)));

    AbfsClientContext abfsClientContext =
        new AbfsClientContextBuilder().withAbfsPerfTracker(tracker)
            .withExponentialRetryPolicy(
                new ExponentialRetryPolicy(abfsConfig.getMaxIoRetries()))
            .withAbfsCounters(abfsCounters)
            .build();

    AbfsClient testClient = new AbfsBlobClient(
        baseAbfsClientInstance.getBaseUrl(),
        (currentAuthType == SharedKey
            ? new SharedKeyCredentials(
            abfsConfig.getAccountName().substring(0,
                abfsConfig.getAccountName().indexOf(DOT)),
            abfsConfig.getStorageAccountKey())
            : null),
        abfsConfig,
        (currentAuthType == AuthType.OAuth
            ? abfsConfig.getTokenProvider()
            : null),
        null,
        null,
        abfsClientContext);

    return testClient;
  }

  public static AbfsClient getMockAbfsClient(AbfsClient baseAbfsClientInstance,
      AbfsConfiguration abfsConfig) throws Exception {
    AuthType currentAuthType = abfsConfig.getAuthType(
        abfsConfig.getAccountName());
    AbfsCounters abfsCounters = Mockito.spy(new AbfsCountersImpl(new URI(RANDOM_URI)));

    assumeThat(currentAuthType)
        .as("Auth type must be SharedKey or OAuth for this test")
        .isIn(SharedKey, AuthType.OAuth);

    AbfsClient client;
    if (AbfsServiceType.DFS.equals(abfsConfig.getFsConfiguredServiceType())) {
      client = mock(AbfsDfsClient.class);
    } else {
      client = mock(AbfsBlobClient.class);
    }
    AbfsPerfTracker tracker = new AbfsPerfTracker(
        "test",
        abfsConfig.getAccountName(),
        abfsConfig);

    when(client.getAbfsPerfTracker()).thenReturn(tracker);
    when(client.getAuthType()).thenReturn(currentAuthType);
    when(client.getExponentialRetryPolicy()).thenReturn(
        new ExponentialRetryPolicy(1));
    when(client.getRetryPolicy(any())).thenReturn(
        new ExponentialRetryPolicy(1));

    when(client.createDefaultUriQueryBuilder()).thenCallRealMethod();
    when(client.createRequestUrl(any(), any())).thenCallRealMethod();
    when(client.createRequestUrl(any(), any(), any())).thenCallRealMethod();
    when(client.getAccessToken()).thenCallRealMethod();
    when(client.getSharedKeyCredentials()).thenCallRealMethod();
    when(client.createDefaultHeaders()).thenCallRealMethod();
    when(client.getAbfsConfiguration()).thenReturn(abfsConfig);

    when(client.getIntercept()).thenReturn(
        AbfsThrottlingInterceptFactory.getInstance(
            abfsConfig.getAccountName().substring(0,
                abfsConfig.getAccountName().indexOf(DOT)), abfsConfig));
    when(client.getAbfsCounters()).thenReturn(abfsCounters);
    Mockito.doReturn(baseAbfsClientInstance.getAbfsApacheHttpClient()).when(client).getAbfsApacheHttpClient();

    // override baseurl
    ReflectionUtils.setFinalField(AbfsClient.class, client, "abfsConfiguration", abfsConfig);

    // override baseurl
    ReflectionUtils.setFinalField(AbfsClient.class, client, "baseUrl", baseAbfsClientInstance.getBaseUrl());

    // override xMsVersion
    ReflectionUtils.setFinalField(AbfsClient.class, client, "xMsVersion", baseAbfsClientInstance.getxMsVersion());

    // override auth provider
    if (currentAuthType == SharedKey) {
      ReflectionUtils.setFinalField(AbfsClient.class, client, "sharedKeyCredentials", new SharedKeyCredentials(
              abfsConfig.getAccountName().substring(0,
                  abfsConfig.getAccountName().indexOf(DOT)),
              abfsConfig.getStorageAccountKey()));
    } else {
      ReflectionUtils.setFinalField(AbfsClient.class, client, "tokenProvider", abfsConfig.getTokenProvider());
    }

    // override user agent
    String userAgent = "APN/1.0 Azure Blob FS/3.5.0-SNAPSHOT (PrivateBuild "
        + "JavaJRE 1.8.0_252; Linux 5.3.0-59-generic/amd64; openssl-1.0; "
        + "UNKNOWN/UNKNOWN) MSFT";
    ReflectionUtils.setFinalField(AbfsClient.class, client, "userAgent", userAgent);

    return client;
  }

  /**
   * Test helper method to access private createRequestUrl method.
   * @param client test AbfsClient instace
   * @param path path to generate Url
   * @return return store path url
   * @throws AzureBlobFileSystemException
   */
  public static URL getTestUrl(AbfsClient client, String path) throws
      AzureBlobFileSystemException {
    final AbfsUriQueryBuilder abfsUriQueryBuilder
        = client.createDefaultUriQueryBuilder();
    return client.createRequestUrl(path, abfsUriQueryBuilder.toString());
  }

  /**
   * Test helper method to access private createDefaultHeaders method.
   * @param client test AbfsClient instance
   * @return List of AbfsHttpHeaders
   */
  public static List<AbfsHttpHeader> getTestRequestHeaders(AbfsClient client) {
    return client.createDefaultHeaders();
  }

  /**
   * Test helper method to create an AbfsRestOperation instance.
   * @param type RestOpType
   * @param client AbfsClient
   * @param method HttpMethod
   * @param url Test path url
   * @param requestHeaders request headers
   * @return instance of AbfsRestOperation
   */
  public static AbfsRestOperation getRestOp(AbfsRestOperationType type,
      AbfsClient client,
      String method,
      URL url,
      List<AbfsHttpHeader> requestHeaders, AbfsConfiguration abfsConfiguration) {
    return new AbfsRestOperation(
        type,
        client,
        method,
        url,
        requestHeaders,
        abfsConfiguration);
  }

  public static AccessTokenProvider getAccessTokenProvider(AbfsClient client) {
    return client.getTokenProvider();
  }

  @Override
  public AzureBlobFileSystem getFileSystem(final Configuration configuration)
      throws Exception {
    Configuration conf = new Configuration(configuration);
    conf.set(ConfigurationKeys.FS_AZURE_NETWORKING_LIBRARY, httpOperationType.toString());
    return (AzureBlobFileSystem) FileSystem.newInstance(conf);
  }

  /**
   * Test to verify that client retries append request without
   * expect header enabled if append with expect header enabled fails
   * with 4xx kind of error.
   * @throws Exception
   */
  @Test
  public void testExpectHundredContinue() throws Exception {
    // Get the filesystem.
    final AzureBlobFileSystem fs = getFileSystem(getRawConfiguration());

    final Configuration configuration = fs.getAbfsStore().getAbfsConfiguration()
        .getRawConfiguration();
    configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
    AbfsClient abfsClient = fs.getAbfsStore().getClient();

    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        configuration.get(FS_AZURE_ABFS_ACCOUNT_NAME));

    // Update the configuration with reduced retry count and reduced backoff interval.
    AbfsConfiguration abfsConfig
        = TestAbfsConfigurationFieldsValidation.updateRetryConfigs(
        abfsConfiguration,
        REDUCED_RETRY_COUNT, REDUCED_BACKOFF_INTERVAL);

    // Gets the client.
    AbfsClient testClient = Mockito.spy(
        ITestAbfsClient.createTestClientFromCurrentContext(
            abfsClient,
            abfsConfig));

    // Create the append request params with expect header enabled initially.
    AppendRequestParameters appendRequestParameters
        = new AppendRequestParameters(
        BUFFER_OFFSET, BUFFER_OFFSET, BUFFER_LENGTH,
        AppendRequestParameters.Mode.APPEND_MODE, false, null, true, null);

    byte[] buffer = getRandomBytesArray(BUFFER_LENGTH);

    // Create a test container to upload the data.
    Path testPath = path(TEST_PATH);
    fs.create(testPath);
    String finalTestPath = testPath.toString()
        .substring(testPath.toString().lastIndexOf("/"));

    // Creates a list of request headers.
    final List<AbfsHttpHeader> requestHeaders
        = ITestAbfsClient.getTestRequestHeaders(testClient);
    requestHeaders.add(
        new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE, HTTP_METHOD_PATCH));
    if (appendRequestParameters.isExpectHeaderEnabled()) {
      requestHeaders.add(new AbfsHttpHeader(EXPECT, HUNDRED_CONTINUE));
    }

    // Updates the query parameters.
    final AbfsUriQueryBuilder abfsUriQueryBuilder
        = testClient.createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, APPEND_ACTION);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_POSITION,
        Long.toString(appendRequestParameters.getPosition()));

    // Creates the url for the specified path.
    URL url = testClient.createRequestUrl(finalTestPath, abfsUriQueryBuilder.toString());

    // Create a mock of the AbfsRestOperation to set the urlConnection in the corresponding httpOperation.
    AbfsRestOperation op = Mockito.spy(new AbfsRestOperation(
        AbfsRestOperationType.Append,
        testClient,
        HTTP_METHOD_PUT,
        url,
        requestHeaders, buffer,
        appendRequestParameters.getoffset(),
        appendRequestParameters.getLength(), null, abfsConfig));

    Mockito.doAnswer(answer -> {
      AbfsHttpOperation httpOperation = Mockito.spy((AbfsHttpOperation) answer.callRealMethod());
      // Sets the expect request property if expect header is enabled.
      if (appendRequestParameters.isExpectHeaderEnabled()) {
        Mockito.doReturn(HUNDRED_CONTINUE).when(httpOperation)
            .getConnProperty(EXPECT);
      }
      Mockito.doNothing().when(httpOperation).setRequestProperty(Mockito
          .any(), Mockito.any());
      Mockito.doReturn(url).when(httpOperation).getConnUrl();

      // Give user error code 404 when processResponse is called.
      Mockito.doReturn(HTTP_METHOD_PUT).when(httpOperation).getMethod();
      Mockito.doReturn(HTTP_NOT_FOUND).when(httpOperation).getStatusCode();
      Mockito.doReturn(HTTP_NOT_FOUND).when(httpOperation).getConnResponseCode();
      Mockito.doReturn("Resource Not Found")
          .when(httpOperation)
          .getConnResponseMessage();

      if (httpOperation instanceof AbfsJdkHttpOperation) {
        // Make the getOutputStream throw IOException to see it returns from the sendRequest correctly.
        Mockito.doThrow(new ProtocolException(EXPECT_100_JDK_ERROR))
            .when((AbfsJdkHttpOperation) httpOperation)
            .getConnOutputStream();
      }

      if (httpOperation instanceof AbfsAHCHttpOperation) {
        Mockito.doNothing()
            .when((AbfsAHCHttpOperation) httpOperation)
            .parseResponseHeaderAndBody(Mockito.any(byte[].class),
                Mockito.anyInt(), Mockito.anyInt());
        Mockito.doReturn(HTTP_NOT_FOUND)
            .when((AbfsAHCHttpOperation) httpOperation)
            .parseStatusCode(Mockito.nullable(
                HttpResponse.class));
        Mockito.doThrow(
                new AbfsApacheHttpExpect100Exception(Mockito.mock(HttpResponse.class)))
            .when((AbfsAHCHttpOperation) httpOperation)
            .executeRequest();
      }
      return httpOperation;
    }).when(op).createHttpOperation();

    // Mock the restOperation for the client.
    Mockito.doReturn(op)
        .when(testClient)
        .getAbfsRestOperation(eq(AbfsRestOperationType.Append),
            Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
            Mockito.nullable(int.class), Mockito.nullable(int.class),
            Mockito.any());

    TracingContext tracingContext = Mockito.spy(new TracingContext(RANDOM_URI,
        RANDOM_FILESYSTEM_ID, FSOperationType.APPEND,
        TracingHeaderFormat.ALL_ID_FORMAT, null));

    // Check that expect header is enabled before the append call.
    Assertions.assertThat(appendRequestParameters.isExpectHeaderEnabled())
            .describedAs("The expect header is not true before the append call")
            .isTrue();

    intercept(AzureBlobFileSystemException.class,
        () -> testClient.append(finalTestPath, buffer, appendRequestParameters, null, null, tracingContext));

    // Verify that the request was not exponentially retried because of user error.
    Assertions.assertThat(tracingContext.getRetryCount())
        .describedAs("The retry count is incorrect")
        .isEqualTo(0);

    // Verify that the same request was retried with expect header disabled.
    Assertions.assertThat(appendRequestParameters.isExpectHeaderEnabled())
            .describedAs("The expect header is not false")
            .isFalse();
  }

  /**
   * Parameterized test to verify the correct setup of authentication providers
   * for each supported AuthType in the Azure Blob FileSystem configuration.
   * For each AuthType, this test checks that the expected provider(s) are present
   * and that unsupported providers throw the correct exceptions.
   *
   * OAuth: Token provider must be present, SAS provider must throw exception.
   * SharedKey: Token provider must throw exception, SAS provider must throw exception.
   * SAS: SAS provider must be present, token provider must throw exception.
   * UserboundSASWithOAuth: Both AccessTokenProvider and SASTokenProvider must be present.
   * Custom: Test is skipped.
   *
   * @param authType the authentication type to test
   * @throws Exception if any error occurs during test execution
   */
  @ParameterizedTest
  @EnumSource(AuthType.class)
  public void testAuthTypeProviderSetup(AuthType authType) throws Exception {
    if (authType.name().equals("Custom")) {
      return;
    }

    this.getConfiguration().set(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, SharedKey.name());
    AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(
        getRawConfiguration());
    this.getConfiguration().set(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, authType.name());

    AbfsConfiguration abfsConfig = fs.getAbfsStore().getAbfsConfiguration();

    switch (authType) {
    case OAuth:
      assertNotNull(abfsConfig.getTokenProvider(),
          "OAuth should have token provider");
      assertThrows(AzureBlobFileSystemException.class,
          () -> abfsConfig.getSASTokenProvider(),
          "SharedKey should not have SAS provider");
      break;

    case SharedKey:
      assertThrows(TokenAccessProviderException.class,
          () -> abfsConfig.getTokenProvider(),
          "SharedKey should not have token provider");
      assertThrows(AzureBlobFileSystemException.class,
          () -> abfsConfig.getSASTokenProvider(),
          "SharedKey should not have SAS provider");
      break;

    case SAS:
      if (!abfsConfig.getIsNamespaceEnabledAccount().toBoolean()) {
        assumeBlobServiceType();
      }
      assertThrows(TokenAccessProviderException.class,
          () -> abfsConfig.getTokenProvider(),
          "SharedKey should not have token provider");
      assertNotNull(abfsConfig.getSASTokenProvider(),
          "SAS should have SAS provider");
      break;

    case UserboundSASWithOAuth:
      assumeHnsEnabled();
      Object[] providers = abfsConfig.getUserBoundSASBothTokenProviders();
      assertNotNull(providers, "Providers array must not be null");
      assertTrue(providers[0] instanceof AccessTokenProvider,
          "First should be AccessTokenProvider");
      assertTrue(providers[1] instanceof SASTokenProvider,
          "Second should be SASTokenProvider");
      break;

    default:
      fail("Unexpected AuthType: " + authType);
    }

    fs.close();
  }

  /**
   * Test to verify that when initializing a filesystem with a DFS endpoint for a FNS account,
   * we force to Blob endpoint internally.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testFNSDfsUsesBlobInstance() throws Exception {
    assumeHnsDisabled();
    String scheme = "abfs";
    String dfsDomain = "dfs.core.windows.net";
    String blobDomain = "blob.core.windows.net";
    Configuration conf = new Configuration(getRawConfiguration());
    conf.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, true);

    String dfsUri = String.format("%s://%s@%s.%s/",
            scheme, getFileSystemName(),
            getAccountName().substring(0, getAccountName().indexOf(DOT)),
            dfsDomain);

    // Initialize filesystem with DFS endpoint
    AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(
        new URI(dfsUri), conf);

    // Filesystem initialization should have forced to use Blob instance for FNS-DFS
    AbfsClient abfsClient = fs.getAbfsStore().getClient();
    Assertions.assertThat(abfsClient)
            .as("abfsClient should be instance of AbfsBlobClient")
            .isInstanceOf(AbfsBlobClient.class);
    Assertions.assertThat(abfsClient.getBaseUrl().toString())
            .contains(blobDomain);
  }

  @Test
  public void testIsNonEmptyDirectory() throws IOException {
    testIsNonEmptyDirectoryInternal(EMPTY_STRING, true, EMPTY_STRING,
        true, 1, false);
    testIsNonEmptyDirectoryInternal(EMPTY_STRING, false, EMPTY_STRING,
        false, 1, true);

    testIsNonEmptyDirectoryInternal(TEST_CONTINUATION_TOKEN, true, EMPTY_STRING,
        true, 2, false);
    testIsNonEmptyDirectoryInternal(TEST_CONTINUATION_TOKEN, true, EMPTY_STRING,
        false, 2, true);
    testIsNonEmptyDirectoryInternal(TEST_CONTINUATION_TOKEN + 1, true, TEST_CONTINUATION_TOKEN + 2,
        true, 3, false);
    testIsNonEmptyDirectoryInternal(TEST_CONTINUATION_TOKEN + 1, true, TEST_CONTINUATION_TOKEN + 2,
        false, 2, true);

    testIsNonEmptyDirectoryInternal(TEST_CONTINUATION_TOKEN, false, EMPTY_STRING,
        true, 1, true);
    testIsNonEmptyDirectoryInternal(TEST_CONTINUATION_TOKEN, false, EMPTY_STRING,
        false, 1, true);
    testIsNonEmptyDirectoryInternal(TEST_CONTINUATION_TOKEN + 1, false, TEST_CONTINUATION_TOKEN + 2,
        true, 1, true);
    testIsNonEmptyDirectoryInternal(TEST_CONTINUATION_TOKEN + 1, false, TEST_CONTINUATION_TOKEN + 2,
        false, 1, true);
  }

  /**
   * Test to verify that in case metric account is not set,
   * metric collection is enabled with default metric format
   * and account url.
   *
   * @throws Exception in case of any failure
   */
  @Test
  public void testMetricAccountFallback() throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.setBoolean(
        AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, false);
    configuration.setBoolean(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, false);
    configuration.unset(FS_AZURE_METRICS_ACCOUNT_NAME);
    configuration.unset(FS_AZURE_METRICS_ACCOUNT_KEY);
    configuration.unset(FS_AZURE_METRICS_FORMAT);
    configuration.setBoolean(FS_AZURE_ALWAYS_USE_HTTPS, true);
    final AzureBlobFileSystem fs = getFileSystem(configuration);
    Assertions.assertThat(
            fs.getAbfsStore().getAbfsConfiguration().getMetricFormat())
        .describedAs(
            "In case metric format is not set, metric format should "
                + "be defaulted to internal metric format")
        .isEqualTo(MetricFormat.INTERNAL_METRIC_FORMAT);

    Assertions.assertThat(
            fs.getAbfsStore().getClient().getAbfsMetricsManager().isMetricCollectionEnabled())
        .describedAs(
            "Metric collection should be enabled even if metric account is not set")
        .isTrue();

    Assertions.assertThat(
            fs.getAbfsStore().getClient().getAbfsCounters().toString())
        .describedAs(
            "AbfsCounters should not contain backoff related metrics "
                + "as no metric is collected for backoff")
        .doesNotContain("#BO:");

    Assertions.assertThat(
            fs.getAbfsStore().getClient().getAbfsCounters().toString())
        .describedAs(
            "AbfsCounters should not contain read footer related metrics "
                + "as no metric is collected for read footer")
        .doesNotContain("#FO:");

    final URIBuilder uriBuilder = new URIBuilder();
    uriBuilder.setScheme(FileSystemUriSchemes.HTTPS_SCHEME);
    uriBuilder.setHost(fs.getUri().getHost());
    uriBuilder.setPath(FORWARD_SLASH);
    Assertions.assertThat(fs.getAbfsStore().getClient().getAbfsMetricsManager().getMetricsUrl())
        .describedAs(
            "In case metric account is not set, account url should be used")
        .isEqualTo(
            UriUtils.changeUrlFromBlobToDfs(uriBuilder.build().toURL()));
  }

  @Test
  public void testInvalidMetricAccount() throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.setBoolean(
        AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, false);
    configuration.setBoolean(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, false);
    configuration.set(FS_AZURE_METRICS_ACCOUNT_NAME, "invalidAccountName!");
    configuration.set(FS_AZURE_METRICS_ACCOUNT_KEY, "invalidAccountKey!");
    configuration.unset(FS_AZURE_METRICS_FORMAT); // Use default metric format
    // Filesystem initialization should not fail if metric account is invalid
    try (AzureBlobFileSystem fs = getFileSystem(configuration)) {
      // Incase metric account is invalid, metric collection should be disabled
      Assertions.assertThat(
              fs.getAbfsStore()
                  .getClient()
                  .getAbfsMetricsManager()
                  .isMetricCollectionEnabled())
          .describedAs(
              "In case metric account is invalid, metric collection should be disabled")
          .isFalse();
      fs.create(new Path("/testPath"));
      FileStatus fileStatus = fs.getFileStatus(new Path("/testPath"));
      Assertions.assertThat(fileStatus)
          .describedAs("FileStatus should be returned for the created path")
          .isNotNull();
      // Get metrics and it should be null
      Assertions.assertThat(
              fs.getAbfsStore().getClient().getAbfsCounters().toString())
          .describedAs(
              "In case metric account is invalid, no metrics should be collected")
          .doesNotContain("#BO:")
          .doesNotContain("#FO:");
    }
  }

  /**
   * Test to verify that in case metric format is set to empty,
   * metric collection is disabled.
   *
   * @throws Exception in case of any failure
   */
  @Test
  public void testMetricCollectionWithDifferentMetricFormat() throws Exception {
    Configuration configuration = getRawConfiguration();
    // Setting this configuration just to ensure there is only one call during filesystem initialization
    configuration.setBoolean(
        AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, true);
    configuration.setBoolean(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, false);
    configuration.unset(FS_AZURE_METRICS_FORMAT);
    configuration.setEnum(FS_AZURE_METRICS_FORMAT,
        INTERNAL_BACKOFF_METRIC_FORMAT);
    final AzureBlobFileSystem fs = getFileSystem(configuration);
    int totalCalls = 1; // Filesystem initialization call
    Assertions.assertThat(
            fs.getAbfsStore().getClient().getAbfsMetricsManager().isMetricCollectionEnabled())
        .describedAs("Metric collection should be enabled by default")
        .isTrue();

    Assertions.assertThat(
            fs.getAbfsStore().getAbfsConfiguration().getMetricFormat())
        .describedAs("Metric format should be as set in configuration")
        .isEqualTo(INTERNAL_BACKOFF_METRIC_FORMAT);

    Assertions.assertThat(
            fs.getAbfsStore().getClient().getAbfsCounters().toString())
        .describedAs(
            "AbfsCounters should only contains backoff related metrics when "
                + "metric format is internal backoff metric format")
        .contains("#BO:");

    Assertions.assertThat(
            fs.getAbfsStore().getClient().getAbfsCounters().toString())
        .describedAs(
            "AbfsCounters should not contains read footer related metrics when "
                + "metric format is internal backoff metric format")
        .doesNotContain("#FO:");

    Assertions.assertThat(fs.getAbfsStore()
            .getClient()
            .getAbfsCounters()
            .getAbfsBackoffMetrics()
            .getMetricValue(
                AbfsBackoffMetricsEnum.TOTAL_NUMBER_OF_REQUESTS))
        .describedAs(
            "Total number of requests should be 1 for filesystem initialization")
        .isEqualTo(totalCalls);


    if (fs.getAbfsStore().getClient() instanceof AbfsDfsClient) {
      intercept(FileNotFoundException.class,
          "The specified path does not exist.",
          () -> fs.listStatus(path("/testPath")));
      totalCalls += 1; // listStatus call
    } else {
      intercept(FileNotFoundException.class,
          "The specified blob does not exist.",
          () -> fs.listStatus(path("/testPath")));
      totalCalls += 2; // listStatus call makes 2 calls to the service
    }

    Assertions.assertThat(fs.getAbfsStore()
            .getClient()
            .getAbfsCounters()
            .getAbfsBackoffMetrics()
            .getMetricValue(
                AbfsBackoffMetricsEnum.TOTAL_NUMBER_OF_REQUESTS))
        .describedAs(
            "Total number of requests should be 2 after listStatus")
        .isEqualTo(totalCalls);
  }

  /**
   * Test to verify that clientRequestId contains backoff metrics
   * when metric format is set to internal backoff metric format.
   *
   * @throws Exception in case of any failure
   */
  @Test
  public void testGetMetricsCallMethod() throws Exception {
    // File system init will make few calls to the service.
    // Backoff metrics will be collected for those calls.
    AzureBlobFileSystem fs = getFileSystem();
    TracingContext tracingContext = new TracingContext(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        "test-filesystem-id", FSOperationType.TEST_OP, true,
        TracingHeaderFormat.AGGREGATED_METRICS_FORMAT, null,
        fs.getAbfsStore().getClient().getAbfsCounters().toString());

    AbfsHttpOperation abfsHttpOperation = getAbfsClient(
        fs.getAbfsStore()).getAbfsRestOperation(
            AbfsRestOperationType.GetFileSystemProperties,
            HTTP_METHOD_HEAD,
            fs.getAbfsStore().getClient().getAbfsMetricsManager().getMetricsUrl(),
            getTestRequestHeaders(fs.getAbfsStore().getClient()))
        .createHttpOperation();
    tracingContext.constructHeader(abfsHttpOperation, null,
        EXPONENTIAL_RETRY_POLICY_ABBREVIATION);
    assertThat(abfsHttpOperation.getClientRequestId())
        .describedAs("ClientRequestId should be containing Backoff metrics")
        .contains("#BO:");
  }

  /**
   * Verifies that metrics are emitted when the threshold is low.
   */
  @Test
  public void testMetricsEmitBasedOnCount() throws Exception {
    final long threshold = 10L;
    runMetricsEmitTest(threshold, true);
  }

  /**
   * Verifies that metrics are not emitted when the threshold is high.
   */
  @Test
  public void testMetricsEmitWithHighThreshold() throws Exception {
    final long threshold = 100L;
    runMetricsEmitTest(threshold, false);
  }

  /**
   * Runs a metrics emit test for a given threshold and expected behavior.
   * Uses the same write/flush pattern and asserts based on emit expectation.
   */
  private void runMetricsEmitTest(long threshold, boolean expectEmit)
      throws Exception {
    final int totalWaitTime = 30;
    AzureBlobFileSystem fs = getFileSystem();
    Configuration configuration = fs.getAbfsStore()
        .getAbfsConfiguration()
        .getRawConfiguration();
    configuration.setLong(FS_AZURE_METRICS_EMIT_THRESHOLD, threshold);
    configuration.setLong(FS_AZURE_METRICS_EMIT_THRESHOLD_INTERVAL_SECS, totalWaitTime);
    fs = (AzureBlobFileSystem) FileSystem.newInstance(configuration);

    // Initial total metrics
    long totalMetrics = fs.getAbfsStore().getClient().getAbfsCounters()
        .getAbfsBackoffMetrics()
        .getMetricValue(AbfsBackoffMetricsEnum.TOTAL_NUMBER_OF_REQUESTS);

    // Create file
    Path testPath = path(TEST_PATH);
    FSDataOutputStream stream = fs.create(testPath);
    if (fs.getAbfsStore()
        .getClientHandler()
        .getIngressClient() instanceof AbfsDfsClient) {
      // create file + set properties requests
      totalMetrics += 1;
    } else if (fs.getAbfsStore()
        .getClient() instanceof AbfsDfsClient
        && fs.getAbfsStore()
        .getClientHandler()
        .getIngressClient() instanceof AbfsBlobClient
        && getIsNamespaceEnabled(fs)) {
      totalMetrics += 2;
    } else {
      // create file + set properties + get properties requests
      totalMetrics += 4;
    }

    Assertions.assertThat(currentTotal(fs))
        .describedAs("Total number of requests should increase after create")
        .isEqualTo(totalMetrics);

    // Append data
    final int writeSize = 1024 * 1024;
    final int numWrites = 10;
    final byte dataByte = 5;
    byte[] data = new byte[writeSize];
    Arrays.fill(data, dataByte);

    for (int i = 0; i < numWrites; i++) {
      stream.write(data);  // +1 request
      stream.hflush();
      if (fs.getAbfsStore()
          .isAppendBlobKey(fs.makeQualified(testPath).toString())) {
        totalMetrics += 1; // +1 request
      } else {
        totalMetrics += 2; // +2 requests
      }
    }

    if (fs.getAbfsStore()
        .getClientHandler()
        .getIngressClient() instanceof AbfsDfsClient) {
      totalMetrics += 1; // One extra call for flush in case of DFS client
    }

    // Close stream
    stream.close();

    // Before waiting for emit scheduler to run, total metrics should match
    Assertions.assertThat(currentTotal(fs))
        .describedAs(
            "Total requests should match counted requests when threshold is high")
        .isEqualTo(totalMetrics);
    // Wait for emit scheduler to run
    Thread.sleep(totalWaitTime * 1000); // 30 seconds

    if (expectEmit) {
      Assertions.assertThat(currentTotal(fs))
          .describedAs(
              "Once the emit scheduler has run, total requests should be reset to 0")
          .isEqualTo(0);
    } else {
      Assertions.assertThat(currentTotal(fs))
          .describedAs(
              "In case threshold is high, total requests should remain the same after emit scheduler run")
          .isEqualTo(totalMetrics);
    }
  }

  @Test
  public void testAggregateMetricsConfigs() throws Exception {
    Configuration configuration = getRawConfiguration();
    // Disabling the aggregate metrics collection
    configuration.setBoolean(FS_AZURE_METRICS_COLLECTION_ENABLED, false);
    AzureBlobFileSystem fs = this.getFileSystem(configuration);
    Assertions.assertThat(fs.getAbfsStore().getClient().getAbfsMetricsManager().getMetricsEmitScheduler())
        .describedAs("Since metrics collection is not enabled, "
            + "scheduler should not be initialised")
        .isNull();

    // Disabling the aggregate metrics collection
    configuration.setBoolean(FS_AZURE_METRICS_COLLECTION_ENABLED, true);
    fs = this.getFileSystem(configuration);
    Assertions.assertThat(fs.getAbfsStore().getClient().getAbfsMetricsManager().getMetricsEmitScheduler())
        .describedAs("Since metrics collection is not enabled, "
            + "scheduler should initialised")
        .isNotNull();
  }

  /**
   * Returns the current total number of requests from AbfsBackoffMetrics.
   */
  private long currentTotal(AzureBlobFileSystem fs) {
    return fs.getAbfsStore().getClient().getAbfsCounters()
        .getAbfsBackoffMetrics()
        .getMetricValue(AbfsBackoffMetricsEnum.TOTAL_NUMBER_OF_REQUESTS);
  }

  private void testIsNonEmptyDirectoryInternal(String firstCT,
      boolean isfirstEmpty, String secondCT, boolean isSecondEmpty,
      int expectedInvocations, boolean isNonEmpty) throws IOException {

    assumeBlobServiceType();
    AzureBlobFileSystem spiedFs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore spiedStore = Mockito.spy(spiedFs.getAbfsStore());
    AbfsBlobClient spiedClient = Mockito.spy(spiedStore.getClientHandler().getBlobClient());
    Mockito.doReturn(spiedStore).when(spiedFs).getAbfsStore();
    Mockito.doReturn(spiedClient).when(spiedStore).getClient();
    VersionedFileStatus status1 = new VersionedFileStatus(
        "owner", "group", null, false, 0, false, 0, 0, 0,
        new Path("/testPath/file1"), "version", "encryptionContext");
    VersionedFileStatus status2 = new VersionedFileStatus(
        "owner", "group", null, false, 0, false, 0, 0, 0,
        new Path("/testPath/file2"), "version", "encryptionContext");

    List<VersionedFileStatus> mockedList1 = new ArrayList<>();
    mockedList1.add(status1);
    List<VersionedFileStatus> mockedList2 = new ArrayList<>();
    mockedList2.add(status2);

    ListResponseData listResponseData1 = new ListResponseData();
    listResponseData1.setContinuationToken(firstCT);
    listResponseData1.setFileStatusList(isfirstEmpty ? new ArrayList<>() : mockedList1);
    listResponseData1.setOp(Mockito.mock(AbfsRestOperation.class));

    ListResponseData listResponseData2 = new ListResponseData();
    listResponseData2.setContinuationToken(secondCT);
    listResponseData2.setFileStatusList(isSecondEmpty ? new ArrayList<>() : mockedList2);
    listResponseData2.setOp(Mockito.mock(AbfsRestOperation.class));

    ListResponseData listResponseData3 = new ListResponseData();
    listResponseData3.setContinuationToken(EMPTY_STRING);
    listResponseData3.setFileStatusList(new ArrayList<>());
    listResponseData3.setOp(Mockito.mock(AbfsRestOperation.class));

    Mockito.doReturn(listResponseData1).doReturn(listResponseData2).doReturn(listResponseData3)
        .when(spiedClient).listPath(eq("/testPath"), eq(false), eq(1),
            any(), any(), any());

    final int[] itr = new int[1];
    final String[] continuationTokenUsed = new String[3];

    Mockito.doAnswer(invocationOnMock -> {
      if (itr[0] == 0) {
        itr[0]++;
        continuationTokenUsed[0] = invocationOnMock.getArgument(3);
        return listResponseData1;
      } else if (itr[0] == 1) {
        itr[0]++;
        continuationTokenUsed[1] = invocationOnMock.getArgument(3);
        return listResponseData2;
      }
      continuationTokenUsed[2] = invocationOnMock.getArgument(3);
      return listResponseData3;
    }).when(spiedClient).listPath(eq("/testPath"), eq(false), eq(1),
        any(), any(TracingContext.class), any());

    Assertions.assertThat(spiedClient.isNonEmptyDirectory("/testPath",
        Mockito.mock(TracingContext.class)))
        .describedAs("isNonEmptyDirectory in client giving unexpected results")
        .isEqualTo(isNonEmpty);

    Assertions.assertThat(continuationTokenUsed[0])
        .describedAs("First continuation token used is not as expected")
        .isNull();

    if (expectedInvocations > 1) {
      Assertions.assertThat(continuationTokenUsed[1])
          .describedAs("Second continuation token used is not as expected")
          .isEqualTo(firstCT);
    }

    if (expectedInvocations > 2) {
      Assertions.assertThat(continuationTokenUsed[2])
          .describedAs("Third continuation token used is not as expected")
          .isEqualTo(secondCT);
    }

    Mockito.verify(spiedClient, times(expectedInvocations))
        .listPath(eq("/testPath"), eq(false), eq(1),
            any(), any(TracingContext.class), any());
  }

  /**
   * Test to verify that the KeepAliveCache is initialized with the correct number of connections.
   */
  @Test
  public void testKeepAliveCacheInitializationWithApacheHttpClient()
      throws Exception {
    assumeThat(APACHE_HTTP_CLIENT).isEqualTo(httpOperationType);
    assumeThat(APACHE_HTTP_CLIENT).isEqualTo(
        this.getFileSystem().getAbfsStore()
            .getAbfsConfiguration().getPreferredHttpOperationType());
    final AzureBlobFileSystem fs = this.getFileSystem();
    AbfsClientHandler abfsClientHandler = fs.getAbfsStore().getClientHandler();

    AbfsClient dfsClient = abfsClientHandler.getDfsClient();
    AbfsClient blobClient = abfsClientHandler.getBlobClient();

    checkKacState(dfsClient, blobClient);
  }

  /**
   * Test to verify the behavior of stale connections in the KeepAliveCache.
   * This test is applicable only for ApacheHttpClient.
   */
  @Test
  public void testStaleConnectionBehavior() throws Exception {
    assumeThat(APACHE_HTTP_CLIENT).isEqualTo(httpOperationType);
    assumeThat(APACHE_HTTP_CLIENT).isEqualTo(
        this.getFileSystem().getAbfsStore()
            .getAbfsConfiguration().getPreferredHttpOperationType());
    final AzureBlobFileSystem fs = this.getFileSystem();
    Configuration conf = fs.getConf();

    conf.unset(FS_AZURE_METRICS_ACCOUNT_NAME);

    AzureBlobFileSystemStore store = this.getFileSystem(conf).getAbfsStore();
    AbfsClientHandler abfsClientHandler = store.getClientHandler();

    AbfsClient dfsClient = abfsClientHandler.getDfsClient();
    AbfsClient blobClient = abfsClientHandler.getBlobClient();

    checkKacState(dfsClient, blobClient);

    // Stall to trigger staling logic
    Thread.sleep(TimeUnit.MINUTES.toMillis(5));

    checkKacAfterMakingConnectionsStale(dfsClient);
    checkKacAfterMakingConnectionsStale(blobClient);
  }

  /**
   * Test to verify that the connection is not reused after an IOException occurs.
   */
  @Test
  public void testConnectionNotReusedOnIOException() throws Exception {
    assumeThat(APACHE_HTTP_CLIENT).isEqualTo(httpOperationType);

    AzureBlobFileSystem fs = this.getFileSystem();
    AbfsClient client = fs.getAbfsStore().getClientHandler().getClient();
    KeepAliveCache keepAliveCache = client.getKeepAliveCache();

    String host = getBaseUrl(client);
    // Ensure the spy is at the head of the ConcurrentLinkedDeque
    HttpClientConnection spiedConnection = bringSpiedConnectionToFront(
        keepAliveCache, host, true);

    try {
      client.listPath("/", false, 1, null, getTestTracingContext(fs, true),
          null);
    } catch (Exception e) {
      // Expected
    }

    KeepAliveCache.HostQueue hq = keepAliveCache.getHostQueue(host);

    if (hq != null) {
      // Stream is thread-safe on ConcurrentLinkedDeque
      boolean isSpiedConnInCache = hq.queue.stream().anyMatch(p -> {
        try {
          java.lang.reflect.Field connField = p.getClass()
              .getDeclaredField("conn");
          connField.setAccessible(true);
          return connField.get(p) == spiedConnection;
        } catch (Exception e) {
          return false;
        }
      });

      Assertions.assertThat(isSpiedConnInCache)
          .describedAs(
              "The failed spied connection should not have been returned to the cache")
          .isFalse();
    }
  }

  /**
   * Test to verify that connections for non-base hosts are reused correctly in the KeepAliveCache.
   *
   * @throws Exception if any error occurs during test execution
   */
  @Test
  public void testNonBaseHostConnectionReuse() throws Exception {
    assumeThat(APACHE_HTTP_CLIENT).isEqualTo(httpOperationType);
    AzureBlobFileSystem fs = this.getFileSystem();
    AbfsClient client = fs.getAbfsStore().getClientHandler().getClient();
    KeepAliveCache keepAliveCache = client.getKeepAliveCache();

    String nonBaseHost = "otheraccount.blob.core.windows.net";

    // Start with a clean cluster cache for this test
    while (keepAliveCache.get(nonBaseHost, false) != null) ;

    AbfsManagedApacheHttpConnection mockConn = createMockConnection(
        nonBaseHost);
    keepAliveCache.put(mockConn, false);

    Assertions.assertThat(keepAliveCache.getDefaultConnectionsSize())
        .describedAs("Default cache size should remain constant")
        .isEqualTo(this.getConfiguration().getApacheCacheWarmupCount());

    HttpClientConnection retrieved = keepAliveCache.get(nonBaseHost, false);
    Assertions.assertThat(retrieved)
        .describedAs(
            "Should retrieve the mock connection from the cluster-specific logic")
        .isEqualTo(mockConn);
  }

  /**
   * Test to verify that connections for non-base hosts are not reused in the KeepAliveCache after an IOException occurs.
   *
   * This test simulates a failure on a connection for a non-base host by throwing an IOException when
   * `receiveResponseEntity` is called. It then asserts that the failed connection is discarded and not found
   * in subsequent cache retrievals, ensuring that faulty connections are not reused.
   *
   * @throws Exception if any error occurs during test execution
   */
  @Test
  public void testNonBaseConnectionNotReusedOnIOException() throws Exception {
    assumeThat(APACHE_HTTP_CLIENT).isEqualTo(httpOperationType);
    KeepAliveCache keepAliveCache = this.getFileSystem().getAbfsStore()
        .getClientHandler().getClient().getKeepAliveCache();

    String host = "nonbase.blob.core.windows.net";
    // Using a fresh mock directly to avoid double-proxy issues with spies
    AbfsManagedApacheHttpConnection mockConn = createMockConnection(host);

    Mockito.doThrow(new IOException("Failure"))
        .when(mockConn).receiveResponseEntity(any());

    keepAliveCache.put(mockConn, false);

    HttpClientConnection conn = keepAliveCache.get(host, false);
    try {
      conn.receiveResponseEntity(null);
    } catch (IOException e) {
      // Simulated failure
    }

    Assertions.assertThat(keepAliveCache.get(host, false))
        .describedAs(
            "Connection should be discarded and not found in subsequent get")
        .isNull();
  }

  /**
   * Test to verify that the KeepAliveCache evicts the oldest connection from a cluster when the maximum cluster size is exceeded.
   *
   * This test:
   * 1. Drains the cluster cache.
   * 2. Fills the cluster cache with connections for Host A up to the maximum allowed.
   * 3. Adds a connection for Host B, triggering eviction.
   * 4. Verifies that the oldest connection for Host A is evicted.
   *
   * @throws Exception if any error occurs during test execution
   */
  @Test
  public void testClusterGlobalEviction() throws Exception {
    assumeThat(APACHE_HTTP_CLIENT).isEqualTo(httpOperationType);
    KeepAliveCache keepAliveCache = this.getFileSystem().getAbfsStore()
        .getClientHandler().getClient().getKeepAliveCache();

    int maxCluster = keepAliveCache.getMaxCacheConnectionsForHost();
    String hostA = "hostA.blob.core.windows.net";
    String hostB = "hostB.blob.core.windows.net";

    // 1. DRAIN the cluster cache
    while (keepAliveCache.get("any-host", false) != null) ;

    // 2. Fill cluster cache with Host A connections
    AbfsManagedApacheHttpConnection firstConnA = createMockConnection(hostA);
    keepAliveCache.put(firstConnA, false);

    for (int i = 1; i < maxCluster; i++) {
      keepAliveCache.put(createMockConnection(hostA), false);
    }

    // 3. Trigger Eviction by adding Host B
    AbfsManagedApacheHttpConnection hostBConn = createMockConnection(hostB);
    keepAliveCache.put(hostBConn, false);

    // 4. Verification
    KeepAliveCache.HostQueue hqA = keepAliveCache.getHostQueue(hostA);
    boolean firstConnAStillExists = hqA.queue.stream().anyMatch(p -> {
      try {
        java.lang.reflect.Field f = p.getClass().getDeclaredField("conn");
        f.setAccessible(true);
        return f.get(p) == firstConnA;
      } catch (Exception e) {
        return false;
      }
    });

    Assertions.assertThat(firstConnAStillExists)
        .describedAs(
            "The oldest cluster connection (firstConnA) should be evicted")
        .isFalse();
  }

  /**
   * Tests the thread safety and correctness of concurrent access to the KeepAliveCache.
   * This test launches multiple threads that simultaneously perform put and get operations
   * on the cache, ensuring that the internal state remains consistent and no race conditions
   * or data corruption occur. The test verifies that the cache size does not exceed the maximum
   * allowed connections and that concurrent operations do not break cache invariants.
   *
   * @throws Exception if any error occurs during concurrent execution
   */
  @Test
  public void testConcurrentAccessSafety() throws Exception {
    assumeThat(APACHE_HTTP_CLIENT).isEqualTo(httpOperationType);
    final KeepAliveCache cache = this.getFileSystem()
        .getAbfsStore()
        .getClientHandler()
        .getClient()
        .getKeepAliveCache();
    int threadCount = 20;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger successCount = new AtomicInteger(0);

    for (int i = 0; i < threadCount; i++) {
      executor.submit(() -> {
        try {
          latch.await(); // Start all at once
          for (int j = 0; j < 100; j++) {
            AbfsManagedApacheHttpConnection conn = createMockConnection("host");
            if (cache.put(conn, true)) {
              HttpClientConnection retrieved = cache.get("host", true);
              if (retrieved != null) {successCount.incrementAndGet();}
            }
          }
        } catch (Exception ignored) {}
      });
    }

    latch.countDown();
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);

    Assertions.assertThat(cache.getDefaultConnectionsSize())
        .isLessThanOrEqualTo(cache.getMaxCacheConnections());
  }

  /**
   * Tests the thread safety and atomicity of the KeepAliveCache under high contention parallel access
   * for the default host. This test launches multiple threads performing concurrent put and get operations
   * on the cache for the default host, and verifies:
   * <ul>
   *   <li>The atomic counter for default host connections matches the physical queue size after operations.</li>
   *   <li>After draining the cache, the counter returns exactly to zero.</li>
   * </ul>
   * This ensures that the cache maintains correct synchronization and state under stress conditions.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testHighContentionParallelAccessForDefaultHost()
      throws Exception {
    String host = getBaseUrl(this.getFileSystem().getAbfsStore().getClient());
    performHighContentionTest(host, true);
  }

  /**
   * Tests the thread safety and atomicity of the KeepAliveCache under high contention parallel access
   * for a non-default host. This test launches multiple threads performing concurrent put and get operations
   * on the cache for a non-default host, and verifies:
   * <ul>
   *   <li>The atomic counter for cluster (non-default) host connections matches the physical queue size after operations.</li>
   *   <li>After draining the cache, the counter returns exactly to zero.</li>
   * </ul>
   * This ensures that the cache maintains correct synchronization and state for non-default hosts under stress conditions.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testHighContentionParallelAccessForNonDefaultHost()
      throws Exception {
    String host = "test-non-default-host";
    performHighContentionTest(host, false);
  }

  /**
   * Tests the security and correctness of the KeepAliveCache after it has been closed.
   * <p>
   * This test verifies that:
   * <ul>
   *   <li>After closing the cache, put operations fail and return false.</li>
   *   <li>Get operations throw a {@link org.apache.hadoop.fs.ClosedIOException}.</li>
   * </ul>
   * This ensures that the cache does not allow further access or modification after closure,
   * maintaining proper resource safety and preventing unintended usage.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testCacheSecurityAfterClose() throws Exception {
    assumeThat(APACHE_HTTP_CLIENT).isEqualTo(httpOperationType);
    KeepAliveCache cache = this.getFileSystem()
        .getAbfsStore()
        .getClientHandler()
        .getClient()
        .getKeepAliveCache();

    cache.close();

    // 1. Put should fail
    boolean putResult = cache.put(createMockConnection("any"), true);
    Assertions.assertThat(putResult).isFalse();

    // 2. Get should throw ClosedIOException
    Assertions.assertThatThrownBy(() -> cache.get("any", true))
        .isInstanceOf(org.apache.hadoop.fs.ClosedIOException.class);
  }

  /**
   * Tests that the KeepAliveCache get operation correctly filters out stale connections.
   * <p>
   * This test puts two connections (one stale, one healthy) into the cache for a host,
   * then verifies that:
   * <ul>
   *   <li>The get operation skips the stale connection and returns the healthy one.</li>
   *   <li>The stale connection is properly closed.</li>
   * </ul>
   * This ensures that the cache maintains connection health and resource safety by not returning stale connections.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testGetFiltersStaleConnections() throws Exception {
    assumeThat(APACHE_HTTP_CLIENT).isEqualTo(httpOperationType);
    KeepAliveCache cache = this.getFileSystem()
        .getAbfsStore()
        .getClientHandler()
        .getClient()
        .getKeepAliveCache();
    String host = "stale-host.blob.core.windows.net";

    // 1. Put two connections, first one is stale
    AbfsManagedApacheHttpConnection staleConn = createMockConnection(host);
    AbfsManagedApacheHttpConnection healthyConn = createMockConnection(host);

    Mockito.when(staleConn.isStale()).thenReturn(true);

    cache.put(staleConn, true);
    cache.put(healthyConn, true);

    // 2. Action: Call get()
    HttpClientConnection result = cache.get(host, true);

    // 3. Verification
    Assertions.assertThat(result)
        .describedAs(
            "Should skip the stale connection and return the healthy one")
        .isEqualTo(healthyConn);

    Mockito.verify(staleConn, Mockito.atLeastOnce()).close();
  }

  /**
   * Tests the global eviction consistency of the KeepAliveCache when the cluster cache reaches its maximum size.
   * <p>
   * This test fills the cache for one host to its limit, then adds a connection for a different host,
   * triggering eviction. It verifies that:
   * <ul>
   *   <li>The oldest connection from the original host is evicted from its queue.</li>
   *   <li>The host queue size is reduced by one after eviction.</li>
   * </ul>
   * This ensures that eviction logic is consistent and synchronized across hosts in the cluster cache.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testGlobalEvictionConsistency() throws Exception {
    assumeThat(APACHE_HTTP_CLIENT).isEqualTo(httpOperationType);
    KeepAliveCache cache = this.getFileSystem()
        .getAbfsStore()
        .getClientHandler()
        .getClient()
        .getKeepAliveCache();
    int maxCluster = cache.getMaxCacheConnectionsForHost();

    // 1. Setup: Clean cluster and fill to limit
    while (cache.get("any", false) != null) ;

    String hostA = "hostA.blob.core.windows.net";
    AbfsManagedApacheHttpConnection oldestConn = createMockConnection(hostA);
    cache.put(oldestConn, false);

    for (int i = 1; i < maxCluster; i++) {
      cache.put(createMockConnection(hostA), false);
    }

    // 2. Trigger Eviction
    String hostB = "hostB.blob.core.windows.net";
    cache.put(createMockConnection(hostB), false);

    // 3. Verify oldest is gone from HostA's queue via stream
    boolean exists = cache.getHostQueue(hostA).queue.stream()
        .anyMatch(p -> {
          try {
            java.lang.reflect.Field f = p.getClass().getDeclaredField("conn");
            f.setAccessible(true);
            return f.get(p) == oldestConn;
          } catch (Exception e) {return false;}
        });

    Assertions.assertThat(exists)
        .describedAs("Oldest connection should be evicted from host queue")
        .isFalse();
    Assertions.assertThat(cache.getHostQueue(hostA).queue.size())
        .isEqualTo(maxCluster - 1);
  }

  /**
   * Tests atomicity of concurrent get operations for the default host in KeepAliveCache.
   * <p>
   * This test launches multiple threads competing for a single connection in the cache for the default host,
   * and verifies that:
   * <ul>
   *   <li>Exactly one thread successfully claims the connection.</li>
   *   <li>All other threads receive null, confirming atomic access.</li>
   * </ul>
   * This ensures thread safety and atomicity under race conditions for the default host.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testConcurrentGetRaceConditionOnDefaultHost() throws Exception {
    String host = getBaseUrl(this.getFileSystem().getAbfsStore().getClient());
    performConcurrentGetRaceTest(host, true);
  }

  /**
   * Tests atomicity of concurrent get operations for a non-default host in KeepAliveCache.
   * <p>
   * This test launches multiple threads competing for a single connection in the cache for a non-default host,
   * and verifies that:
   * <ul>
   *   <li>Exactly one thread successfully claims the connection.</li>
   *   <li>All other threads receive null, confirming atomic access.</li>
   * </ul>
   * This ensures thread safety and atomicity under race conditions for non-default hosts.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testConcurrentGetRaceConditionOnNonDefaultHost()
      throws Exception {
    String host = "non-default-race-host.blob.core.windows.net";
    performConcurrentGetRaceTest(host, false);
  }

  /**
   * Helper to verify that multiple threads competing for the same single connection
   * result in exactly one "winner" and N-1 "losers" (null returns).
   */
  private void performConcurrentGetRaceTest(String host, boolean isDefault)
      throws Exception {
    assumeThat(APACHE_HTTP_CLIENT).isEqualTo(httpOperationType);
    final KeepAliveCache cache = this.getFileSystem()
        .getAbfsStore()
        .getClient()
        .getKeepAliveCache();

    // 1. Setup: Ensure the host queue has exactly ONE connection
    while (cache.get(host, isDefault) != null) ;
    AbfsManagedApacheHttpConnection singletonConn = createMockConnection(host);
    cache.put(singletonConn, isDefault);

    int threadCount = 20;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startGun = new CountDownLatch(1);
    CountDownLatch finishLine = new CountDownLatch(threadCount);

    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger nullCount = new AtomicInteger(0);

    // 2. Action: 20 threads compete for 1 connection
    for (int i = 0; i < threadCount; i++) {
      executor.submit(() -> {
        try {
          startGun.await(); // Synchronize all threads to start at once
          HttpClientConnection conn = cache.get(host, isDefault);
          if (conn != null) {
            successCount.incrementAndGet();
          } else {
            nullCount.incrementAndGet();
          }
        } catch (Exception ignored) {
          // In a test race, we expect clean execution
        } finally {
          finishLine.countDown();
        }
      });
    }

    startGun.countDown(); // FIRE! All threads hit cache.get() simultaneously
    boolean finished = finishLine.await(10, TimeUnit.SECONDS);
    executor.shutdown();

    Assertions.assertThat(finished)
        .describedAs("Test timed out waiting for threads to complete")
        .isTrue();

    // 3. Verification of Atomicity
    Assertions.assertThat(successCount.get())
        .describedAs(
            "Atomicity Failure: Exactly one thread should claim the connection for %s host",
            isDefault ? "default" : "non-default")
        .isEqualTo(1);

    Assertions.assertThat(nullCount.get())
        .describedAs(
            "Atomicity Failure: Remaining %d threads should have received null",
            threadCount - 1)
        .isEqualTo(threadCount - 1);

    // 4. Final state check: The counter should now be zero
    int finalCounter = isDefault
        ? cache.getDefaultConnectionsSize()
        : cache.getClusterConnectionsSize();
    Assertions.assertThat(finalCounter)
        .describedAs(
            "Counter should be 0 after the single connection was claimed")
        .isEqualTo(0);
  }

  /**
   * Helper method to simulate high concurrent pressure on the KeepAliveCache.
   * Verifies that the internal Atomic counters stay in sync with the physical queue sizes.
   */
  private void performHighContentionTest(String host, boolean isDefaultHost)
      throws Exception {
    assumeThat(APACHE_HTTP_CLIENT).isEqualTo(httpOperationType);
    final KeepAliveCache cache = this.getFileSystem()
        .getAbfsStore()
        .getClient()
        .getKeepAliveCache();

    int numThreads = 10;
    int iterations = 100;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    // 1. Initial Cleanup: Ensure a fresh state for this host
    while (cache.get(host, isDefaultHost) != null) ;

    // 2. Execution: Concurrent Put and Get operations
    for (int i = 0; i < numThreads; i++) {
      executor.submit(() -> {
        for (int j = 0; j < iterations; j++) {
          try {
            AbfsManagedApacheHttpConnection mock = createMockConnection(host);
            cache.put(mock, isDefaultHost);
            // We don't assert 'retrieved != null' because of the race condition with other threads
            cache.get(host, isDefaultHost);
          } catch (IOException ignored) {
            // Context-specific failure in a stress test
          }
        }
      });
    }

    executor.shutdown();
    Assertions.assertThat(executor.awaitTermination(30, TimeUnit.SECONDS))
        .describedAs("Threads should finish within the timeout")
        .isTrue();

    // 3. Counter Sync Verification
    // Verify that the AtomicInteger exactly matches the number of items in the Deque
    int counterSize = isDefaultHost
        ? cache.getDefaultConnectionsSize()
        : cache.getClusterConnectionsSize();
    KeepAliveCache.HostQueue hq = cache.getHostQueue(host);
    int physicalQueueSize = (hq != null) ? hq.queue.size() : 0;

    Assertions.assertThat(counterSize)
        .describedAs(
            "The Atomic counter (%d) must match physical queue size (%d) for %s host",
            counterSize, physicalQueueSize,
            isDefaultHost ? "default" : "non-default")
        .isEqualTo(physicalQueueSize);

    // 4. Final Drain Verification
    // Ensure that after a full drain, the counter returns exactly to zero
    while (cache.get(host, isDefaultHost) != null) ;

    int finalCounterSize = isDefaultHost
        ? cache.getDefaultConnectionsSize()
        : cache.getClusterConnectionsSize();
    Assertions.assertThat(finalCounterSize)
        .describedAs("Counter should be exactly 0 after full drain")
        .isEqualTo(0);
  }

  /**
   * Brings the specified spied connection to the front of the KeepAliveCache for the given host.
   * Ensures the connection is at the head of the queue for testing purposes.
   *
   * @param keepAliveCache the cache instance
   * @param host the host name
   * @param isDefaultHost whether the host is the default
   * @return the spied HttpClientConnection
   */
  private HttpClientConnection bringSpiedConnectionToFront(KeepAliveCache keepAliveCache,
      String host, boolean isDefaultHost) throws HttpException, IOException {
    // Drain to ensure we control exactly what goes back in
    while (keepAliveCache.get(host, isDefaultHost) != null) ;
    AbfsManagedApacheHttpConnection realConn = createMockConnection(host);
    HttpClientConnection spiedConnection = Mockito.spy(realConn);
    Mockito.doThrow(new IOException("Incomplete input stream"))
        .when(spiedConnection).receiveResponseEntity(any());
    // In lock-free FIFO, the first one put into an empty queue is at the front
    keepAliveCache.put(spiedConnection, isDefaultHost);
    return spiedConnection;
  }

  /**
   * Helper method to create a mock instance of AbfsManagedApacheHttpConnection for the specified host.
   * This is used in tests to simulate HTTP connections for the KeepAliveCache.
   *
   * @param host the host name for which the mock connection is created
   * @return a mocked AbfsManagedApacheHttpConnection instance
   */
  private AbfsManagedApacheHttpConnection createMockConnection(String host) {
    AbfsManagedApacheHttpConnection mockConn = Mockito.mock(
        AbfsManagedApacheHttpConnection.class);
    Mockito.when(mockConn.isOpen()).thenReturn(true);
    Mockito.when(mockConn.isStale()).thenReturn(false);
    HttpHost httpHost = new HttpHost(host);
    Mockito.when(mockConn.getTargetHost()).thenReturn(httpHost);
    return mockConn;
  }

  /**
   * Helper method to check the KeepAliveCache on both clients based on the
   * configured service type.
   * @param dfsClient AbfsClient instance for DFS endpoint
   * @param blobClient AbfsClient instance for Blob endpoint
   * @throws IOException if an error occurs while checking the cache
   */
  private void checkKacState(AbfsClient dfsClient, AbfsClient blobClient)
      throws IOException {
    if (getAbfsServiceType() == AbfsServiceType.DFS) {
      checkKacOnDefaultClientsAfterFSInit(dfsClient);
      checkKacOnNonDefaultClientsAfterFSInit(blobClient);
    } else {
      checkKacOnDefaultClientsAfterFSInit(blobClient);
      checkKacOnNonDefaultClientsAfterFSInit(dfsClient);
    }
  }

  /**
   * Helper method to get Base Url based on client
   * @param client Abfs client
   * @return String base url
   */
  private String getBaseUrl(AbfsClient client) {
    URL baseUrl = client.getBaseUrl();
    HttpHost baseHost = new HttpHost(baseUrl.getHost(),
        baseUrl.getDefaultPort(), baseUrl.getProtocol());
    return baseHost.toHostString();
  }

  /**
   * Helper method to check the KeepAliveCache on both clients.
   * @param abfsClient AbfsClient instance to check
   * @throws IOException if an error occurs while checking the cache
   */
  private void checkKacOnDefaultClientsAfterFSInit(AbfsClient abfsClient)
      throws IOException {
    AbfsApacheHttpClient abfsApacheHttpClient
        = abfsClient.getAbfsApacheHttpClient();
    Assertions.assertThat(abfsApacheHttpClient)
        .describedAs("AbfsApacheHttpClient should not be null")
        .isNotNull();

    KeepAliveCache keepAliveCache = abfsClient.getKeepAliveCache();

    Assertions.assertThat(keepAliveCache.getDefaultConnectionsSize())
        .describedAs(
            "KeepAliveCache should be warm with default connection count")
        .isEqualTo(this.getConfiguration().getApacheCacheWarmupCount());
    Assertions.assertThat(keepAliveCache.get(getBaseUrl(abfsClient), true))
        .describedAs("KeepAliveCache should not be null")
        .isNotNull();

    // 1 connection is taken in above get call, so size should be
    // DEFAULT_APACHE_CACHE_WARMUP_CONNECTION_COUNT - 1
    // after the get call.
    Assertions.assertThat(keepAliveCache.getDefaultConnectionsSize())
        .describedAs(
            "KeepAliveCache size should be one less than the warmup count")
        .isEqualTo(this.getConfiguration().getApacheCacheWarmupCount() - 1);
  }

  /**
   * Helper method to check the KeepAliveCache on both clients.
   * @param abfsClient AbfsClient instance to check
   * @throws IOException if an error occurs while checking the cache
   */
  private void checkKacOnNonDefaultClientsAfterFSInit(AbfsClient abfsClient)
      throws IOException {
    AbfsApacheHttpClient abfsApacheHttpClient
        = abfsClient.getAbfsApacheHttpClient();

    Assertions.assertThat(abfsApacheHttpClient)
        .describedAs("AbfsApacheHttpClient should not be null")
        .isNotNull();

    KeepAliveCache keepAliveCache = abfsClient.getKeepAliveCache();

    Assertions.assertThat(keepAliveCache.getDefaultConnectionsSize())
        .describedAs(
            "KeepAliveCache size should be 0 as non-default clients do not warmup")
        .isEqualTo(0);

    Assertions.assertThat(keepAliveCache.get(getBaseUrl(abfsClient), true))
        .describedAs("KeepAliveCache should be null")
        .isNull();

    // 1 connection is taken in above get call, so size should be
    // DEFAULT_APACHE_CACHE_WARMUP_CONNECTION_COUNT - 1
    // after the get call.
    Assertions.assertThat(keepAliveCache.getDefaultConnectionsSize())
        .describedAs(
            "KeepAliveCache size should be 0 as no new connection is added")
        .isEqualTo(0);
  }

  /**
   * Helper method to check the KeepAliveCache after making connections stale.
   * @param abfsClient AbfsClient instance to check
   * @throws IOException if an error occurs while checking the cache
   */
  private void checkKacAfterMakingConnectionsStale(AbfsClient abfsClient)
      throws IOException {
    KeepAliveCache keepAliveCache = abfsClient.getKeepAliveCache();
    Assertions.assertThat(keepAliveCache.get(getBaseUrl(abfsClient), true))
        .describedAs("KeepAliveCache should return null")
        .isNull();
    // Verify that the cache is empty after making connections stale
    Assertions.assertThat(keepAliveCache.getDefaultConnectionsSize())
        .describedAs(
            "KeepAliveCache should be empty after making connections stale")
        .isEqualTo(0);
  }
}

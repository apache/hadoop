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

import java.io.IOException;
import java.net.ProtocolException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbfsCountersImpl;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.TestAbfsConfigurationFieldsValidation;
import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.constants.HttpOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsApacheHttpExpect100Exception;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.apache.hadoop.test.ReflectionUtils;
import org.apache.http.HttpResponse;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPEND_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EXPECT_100_JDK_ERROR;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PATCH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HUNDRED_CONTINUE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.*;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.EXPECT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_HTTP_METHOD_OVERRIDE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_POSITION;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.HttpOperationType.APACHE_HTTP_CLIENT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpOperationType.JDK_HTTP_URL_CONNECTION;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APN_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CLIENT_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DOT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FORWARD_SLASH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.JAVA_VENDOR;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.JAVA_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.OS_ARCH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.OS_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.OS_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SEMICOLON;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SINGLE_WHITE_SPACE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_CLUSTER_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_CLUSTER_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.TEST_CONFIGURATION_FILE_NAME;

/**
 * Test useragent of abfs client.
 *
 */
@RunWith(Parameterized.class)
public final class ITestAbfsClient extends AbstractAbfsIntegrationTest {

  private static final String ACCOUNT_NAME = "bogusAccountName.dfs.core.windows.net";
  private static final String FS_AZURE_USER_AGENT_PREFIX = "Partner Service";
  private static final String HUNDRED_CONTINUE_USER_AGENT = SINGLE_WHITE_SPACE + HUNDRED_CONTINUE + SEMICOLON;
  private static final String TEST_PATH = "/testfile";
  public static final int REDUCED_RETRY_COUNT = 2;
  public static final int REDUCED_BACKOFF_INTERVAL = 100;
  public static final int BUFFER_LENGTH = 5;
  public static final int BUFFER_OFFSET = 0;

  private final Pattern userAgentStringPattern;

  @Parameterized.Parameter
  public HttpOperationType httpOperationType;

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {HttpOperationType.JDK_HTTP_URL_CONNECTION},
        {APACHE_HTTP_CLIENT}
    });
  }

  public ITestAbfsClient() throws Exception {
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
    AbfsCounters abfsCounters = Mockito.spy(new AbfsCountersImpl(new URI("abcd")));
    AbfsClientContext abfsClientContext = new AbfsClientContextBuilder().withAbfsCounters(abfsCounters).build();
    AbfsClient client;
    if (AbfsServiceType.DFS.equals(config.getFsConfiguredServiceType())) {
      client = new AbfsDfsClient(new URL("https://azure.com"), null,
          config, (AccessTokenProvider) null, null, abfsClientContext);
    } else {
      client = new AbfsBlobClient(new URL("https://azure.com"), null,
          config, (AccessTokenProvider) null, null, abfsClientContext);
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
    Assume.assumeTrue(JDK_HTTP_URL_CONNECTION == httpOperationType);
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
    Assume.assumeTrue(JDK_HTTP_URL_CONNECTION == httpOperationType);
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
    Assume.assumeTrue(JDK_HTTP_URL_CONNECTION == httpOperationType);
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
    Assume.assumeTrue(JDK_HTTP_URL_CONNECTION == httpOperationType);
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
    Assume.assumeTrue(JDK_HTTP_URL_CONNECTION == httpOperationType);
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
    Assume.assumeTrue(JDK_HTTP_URL_CONNECTION == httpOperationType);
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

  public static AbfsClient createTestClientFromCurrentContext(
      AbfsClient baseAbfsClientInstance,
      AbfsConfiguration abfsConfig) throws IOException, URISyntaxException {
    AuthType currentAuthType = abfsConfig.getAuthType(
        abfsConfig.getAccountName());

    AbfsPerfTracker tracker = new AbfsPerfTracker("test",
        abfsConfig.getAccountName(),
        abfsConfig);
    AbfsCounters abfsCounters = Mockito.spy(new AbfsCountersImpl(new URI("abcd")));

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
          (currentAuthType == AuthType.SharedKey
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
          abfsClientContext);
    } else {
      testClient = new AbfsBlobClient(
          baseAbfsClientInstance.getBaseUrl(),
          (currentAuthType == AuthType.SharedKey
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
    AbfsCounters abfsCounters = Mockito.spy(new AbfsCountersImpl(new URI("abcd")));

    AbfsClientContext abfsClientContext =
        new AbfsClientContextBuilder().withAbfsPerfTracker(tracker)
            .withExponentialRetryPolicy(
                new ExponentialRetryPolicy(abfsConfig.getMaxIoRetries()))
            .withAbfsCounters(abfsCounters)
            .build();

    AbfsClient testClient = new AbfsBlobClient(
        baseAbfsClientInstance.getBaseUrl(),
        (currentAuthType == AuthType.SharedKey
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
        abfsClientContext);

    return testClient;
  }

  public static AbfsClient getMockAbfsClient(AbfsClient baseAbfsClientInstance,
      AbfsConfiguration abfsConfig) throws Exception {
    AuthType currentAuthType = abfsConfig.getAuthType(
        abfsConfig.getAccountName());
    AbfsCounters abfsCounters = Mockito.spy(new AbfsCountersImpl(new URI("abcd")));

    org.junit.Assume.assumeTrue(
        (currentAuthType == AuthType.SharedKey)
        || (currentAuthType == AuthType.OAuth));

    // TODO : [FnsOverBlob][HADOOP-19234] Update to work with Blob Endpoint as well when Fns Over Blob is ready.
    AbfsClient client = mock(AbfsDfsClient.class);
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
    if (currentAuthType == AuthType.SharedKey) {
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

  /**
   * Test helper method to get random bytes array.
   * @param length The length of byte buffer.
   * @return byte buffer.
   */
  private byte[] getRandomBytesArray(int length) {
    final byte[] b = new byte[length];
    new Random().nextBytes(b);
    return b;
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
        AppendRequestParameters.Mode.APPEND_MODE, false, null, true);

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

    TracingContext tracingContext = Mockito.spy(new TracingContext("abcd",
        "abcde", FSOperationType.APPEND,
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
}

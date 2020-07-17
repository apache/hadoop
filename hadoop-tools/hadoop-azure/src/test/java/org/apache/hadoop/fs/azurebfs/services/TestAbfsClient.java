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
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Pattern;

import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
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
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_VALUE_UNKNOWN;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.TEST_CONFIGURATION_FILE_NAME;

/**
 * Test useragent of abfs client.
 *
 */
public final class TestAbfsClient {

  private static final String ACCOUNT_NAME = "bogusAccountName.dfs.core.windows.net";
  private static final String FS_AZURE_USER_AGENT_PREFIX = "Partner Service";

  private final Pattern userAgentStringPattern;

  public TestAbfsClient(){
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
      boolean includeSSLProvider) throws MalformedURLException {
    AbfsClient client = new AbfsClient(new URL("https://azure.com"), null,
        config, null, (AccessTokenProvider) null, null, null);
    String sslProviderName = null;
    if (includeSSLProvider) {
      sslProviderName = DelegatingSSLSocketFactory.getDefaultFactory()
          .getProviderName();
    }
    return client.initializeUserAgent(config, sslProviderName);
  }

  @Test
  public void verifybBasicInfo() throws Exception {
    final Configuration configuration = new Configuration();
    configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    verifybBasicInfo(getUserAgentString(abfsConfiguration, false));
  }

  private void verifybBasicInfo(String userAgentStr) {
    assertThat(userAgentStr)
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
      throws IOException, IllegalAccessException {
    final Configuration configuration = new Configuration();
    configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
    configuration.set(ConfigurationKeys.FS_AZURE_USER_AGENT_PREFIX_KEY, FS_AZURE_USER_AGENT_PREFIX);
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    String userAgentStr = getUserAgentString(abfsConfiguration, false);

    verifybBasicInfo(userAgentStr);
    assertThat(userAgentStr)
      .describedAs("User-Agent string should contain " + FS_AZURE_USER_AGENT_PREFIX)
      .contains(FS_AZURE_USER_AGENT_PREFIX);

    configuration.unset(ConfigurationKeys.FS_AZURE_USER_AGENT_PREFIX_KEY);
    abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    userAgentStr = getUserAgentString(abfsConfiguration, false);

    verifybBasicInfo(userAgentStr);
    assertThat(userAgentStr)
      .describedAs("User-Agent string should not contain " + FS_AZURE_USER_AGENT_PREFIX)
      .doesNotContain(FS_AZURE_USER_AGENT_PREFIX);
  }

  @Test
  public void verifyUserAgentWithoutSSLProvider() throws Exception {
    final Configuration configuration = new Configuration();
    configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
    configuration.set(ConfigurationKeys.FS_AZURE_SSL_CHANNEL_MODE_KEY,
        DelegatingSSLSocketFactory.SSLChannelMode.Default_JSSE.name());
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    String userAgentStr = getUserAgentString(abfsConfiguration, true);

    verifybBasicInfo(userAgentStr);
    assertThat(userAgentStr)
      .describedAs("User-Agent string should contain sslProvider")
      .contains(DelegatingSSLSocketFactory.getDefaultFactory().getProviderName());

    userAgentStr = getUserAgentString(abfsConfiguration, false);

    verifybBasicInfo(userAgentStr);
    assertThat(userAgentStr)
      .describedAs("User-Agent string should not contain sslProvider")
      .doesNotContain(DelegatingSSLSocketFactory.getDefaultFactory().getProviderName());
  }

  @Test
  public void verifyUserAgentClusterName() throws Exception {
    final String clusterName = "testClusterName";
    final Configuration configuration = new Configuration();
    configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
    configuration.set(FS_AZURE_CLUSTER_NAME, clusterName);
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    String userAgentStr = getUserAgentString(abfsConfiguration, false);

    verifybBasicInfo(userAgentStr);
    assertThat(userAgentStr)
      .describedAs("User-Agent string should contain cluster name")
      .contains(clusterName);

    configuration.unset(FS_AZURE_CLUSTER_NAME);
    abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    userAgentStr = getUserAgentString(abfsConfiguration, false);

    verifybBasicInfo(userAgentStr);
    assertThat(userAgentStr)
      .describedAs("User-Agent string should not contain cluster name")
      .doesNotContain(clusterName)
      .describedAs("User-Agent string should contain UNKNOWN as cluster name config is absent")
      .contains(DEFAULT_VALUE_UNKNOWN);
  }

  @Test
  public void verifyUserAgentClusterType() throws Exception {
    final String clusterType = "testClusterType";
    final Configuration configuration = new Configuration();
    configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
    configuration.set(FS_AZURE_CLUSTER_TYPE, clusterType);
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    String userAgentStr = getUserAgentString(abfsConfiguration, false);

    verifybBasicInfo(userAgentStr);
    assertThat(userAgentStr)
      .describedAs("User-Agent string should contain cluster type")
      .contains(clusterType);

    configuration.unset(FS_AZURE_CLUSTER_TYPE);
    abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    userAgentStr = getUserAgentString(abfsConfiguration, false);

    verifybBasicInfo(userAgentStr);
    assertThat(userAgentStr)
      .describedAs("User-Agent string should not contain cluster type")
      .doesNotContain(clusterType)
      .describedAs("User-Agent string should contain UNKNOWN as cluster type config is absent")
      .contains(DEFAULT_VALUE_UNKNOWN);
  }

  public static AbfsClient createTestClientFromCurrentContext(
      AbfsClient baseAbfsClientInstance,
      AbfsConfiguration abfsConfig)
      throws AzureBlobFileSystemException {
    AuthType currentAuthType = abfsConfig.getAuthType(
        abfsConfig.getAccountName());

    AbfsPerfTracker tracker = new AbfsPerfTracker("test",
        abfsConfig.getAccountName(),
        abfsConfig);

    // Create test AbfsClient
    AbfsClient testClient = new AbfsClient(
        baseAbfsClientInstance.getBaseUrl(),
        (currentAuthType == AuthType.SharedKey
            ? new SharedKeyCredentials(
            abfsConfig.getAccountName().substring(0,
                abfsConfig.getAccountName().indexOf(DOT)),
            abfsConfig.getStorageAccountKey())
            : null),
        abfsConfig,
        new ExponentialRetryPolicy(abfsConfig.getMaxIoRetries()),
        (currentAuthType == AuthType.OAuth
            ? abfsConfig.getTokenProvider()
            : null),
        tracker, null);

    return testClient;
  }

  public static AbfsClient getMockAbfsClient(AbfsClient baseAbfsClientInstance,
      AbfsConfiguration abfsConfig)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    AuthType currentAuthType = abfsConfig.getAuthType(
        abfsConfig.getAccountName());

    org.junit.Assume.assumeTrue(
        (currentAuthType == AuthType.SharedKey)
        || (currentAuthType == AuthType.OAuth));

    AbfsClient client = mock(AbfsClient.class);
    AbfsPerfTracker tracker = new AbfsPerfTracker(
        "test",
        abfsConfig.getAccountName(),
        abfsConfig);

    when(client.getAbfsPerfTracker()).thenReturn(tracker);
    when(client.getAuthType()).thenReturn(currentAuthType);
    when(client.getRetryPolicy()).thenReturn(
        new ExponentialRetryPolicy(1));

    when(client.createDefaultUriQueryBuilder()).thenCallRealMethod();
    when(client.createRequestUrl(any(), any())).thenCallRealMethod();
    when(client.getAccessToken()).thenCallRealMethod();
    when(client.getSharedKeyCredentials()).thenCallRealMethod();
    when(client.createDefaultHeaders()).thenCallRealMethod();

    // override baseurl
    Field baseUrlField = AbfsClient.class.getDeclaredField("baseUrl");
    baseUrlField.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(baseUrlField, baseUrlField.getModifiers() & ~java.lang.reflect.Modifier.FINAL);
    baseUrlField.set(client, baseAbfsClientInstance.getBaseUrl());

    // override auth provider
    if (currentAuthType == AuthType.SharedKey) {
      Field sharedKeyCredsField = AbfsClient.class.getDeclaredField(
          "sharedKeyCredentials");
      sharedKeyCredsField.setAccessible(true);
      modifiersField.setInt(sharedKeyCredsField,
          sharedKeyCredsField.getModifiers()
              & ~java.lang.reflect.Modifier.FINAL);
      sharedKeyCredsField.set(client, new SharedKeyCredentials(
          abfsConfig.getAccountName().substring(0,
              abfsConfig.getAccountName().indexOf(DOT)),
          abfsConfig.getStorageAccountKey()));
    } else {
      Field tokenProviderField = AbfsClient.class.getDeclaredField(
          "tokenProvider");
      tokenProviderField.setAccessible(true);
      modifiersField.setInt(tokenProviderField,
          tokenProviderField.getModifiers()
              & ~java.lang.reflect.Modifier.FINAL);
      tokenProviderField.set(client, abfsConfig.getTokenProvider());
    }

    // override user agent
    String userAgent = "APN/1.0 Azure Blob FS/3.4.0-SNAPSHOT (PrivateBuild "
        + "JavaJRE 1.8.0_252; Linux 5.3.0-59-generic/amd64; openssl-1.0; "
        + "UNKNOWN/UNKNOWN) MSFT";
    Field userAgentField = AbfsClient.class.getDeclaredField(
        "userAgent");
    userAgentField.setAccessible(true);
    modifiersField.setInt(userAgentField,
        userAgentField.getModifiers()
            & ~java.lang.reflect.Modifier.FINAL);
    userAgentField.set(client, userAgent);

    return client;
  }
}

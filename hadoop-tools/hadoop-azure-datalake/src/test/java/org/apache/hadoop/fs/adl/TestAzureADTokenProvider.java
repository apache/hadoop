/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.adl;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.adl.common.CustomMockTokenProvider;
import org.apache.hadoop.fs.adl.oauth2.AzureADTokenProvider;

import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.RefreshTokenBasedTokenProvider;

import static org.apache.hadoop.fs.adl.AdlConfKeys.AZURE_AD_CLIENT_ID_KEY;
import static org.apache.hadoop.fs.adl.AdlConfKeys.AZURE_AD_CLIENT_SECRET_KEY;
import static org.apache.hadoop.fs.adl.AdlConfKeys.AZURE_AD_REFRESH_TOKEN_KEY;
import static org.apache.hadoop.fs.adl.AdlConfKeys.AZURE_AD_REFRESH_URL_KEY;
import static org.apache.hadoop.fs.adl.AdlConfKeys
    .AZURE_AD_TOKEN_PROVIDER_CLASS_KEY;
import static org.apache.hadoop.fs.adl.AdlConfKeys
    .AZURE_AD_TOKEN_PROVIDER_TYPE_KEY;
import static org.apache.hadoop.fs.adl.TokenProviderType.*;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test appropriate token provider is loaded as per configuration.
 */
public class TestAzureADTokenProvider {

  private static final String CLIENT_ID = "MY_CLIENT_ID";
  private static final String REFRESH_TOKEN = "MY_REFRESH_TOKEN";
  private static final String CLIENT_SECRET = "MY_CLIENT_SECRET";
  private static final String REFRESH_URL = "http://localhost:8080/refresh";

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void testRefreshTokenProvider()
      throws URISyntaxException, IOException {
    Configuration conf = new Configuration();
    conf.set(AZURE_AD_CLIENT_ID_KEY, "MY_CLIENTID");
    conf.set(AZURE_AD_REFRESH_TOKEN_KEY, "XYZ");
    conf.setEnum(AZURE_AD_TOKEN_PROVIDER_TYPE_KEY, RefreshToken);
    conf.set(AZURE_AD_REFRESH_URL_KEY, "http://localhost:8080/refresh");

    URI uri = new URI("adl://localhost:8080");
    AdlFileSystem fileSystem = new AdlFileSystem();
    fileSystem.initialize(uri, conf);
    AccessTokenProvider tokenProvider = fileSystem.getTokenProvider();
    Assert.assertTrue(tokenProvider instanceof RefreshTokenBasedTokenProvider);
  }

  @Test
  public void testClientCredTokenProvider()
      throws IOException, URISyntaxException {
    Configuration conf = new Configuration();
    conf.set(AZURE_AD_CLIENT_ID_KEY, "MY_CLIENTID");
    conf.set(AZURE_AD_CLIENT_SECRET_KEY, "XYZ");
    conf.setEnum(AZURE_AD_TOKEN_PROVIDER_TYPE_KEY, ClientCredential);
    conf.set(AZURE_AD_REFRESH_URL_KEY, "http://localhost:8080/refresh");

    URI uri = new URI("adl://localhost:8080");
    AdlFileSystem fileSystem = new AdlFileSystem();
    fileSystem.initialize(uri, conf);
    AccessTokenProvider tokenProvider = fileSystem.getTokenProvider();
    Assert.assertTrue(tokenProvider instanceof ClientCredsTokenProvider);
  }

  @Test
  public void testCustomCredTokenProvider()
      throws URISyntaxException, IOException {
    Configuration conf = new Configuration();
    conf.setEnum(AZURE_AD_TOKEN_PROVIDER_TYPE_KEY, TokenProviderType.Custom);
    conf.setClass(AZURE_AD_TOKEN_PROVIDER_CLASS_KEY,
        CustomMockTokenProvider.class, AzureADTokenProvider.class);

    URI uri = new URI("adl://localhost:8080");
    AdlFileSystem fileSystem = new AdlFileSystem();
    fileSystem.initialize(uri, conf);
    AccessTokenProvider tokenProvider = fileSystem.getTokenProvider();
    Assert.assertTrue(tokenProvider instanceof SdkTokenProviderAdapter);
  }

  @Test
  public void testInvalidProviderConfigurationForType()
      throws URISyntaxException, IOException {
    Configuration conf = new Configuration();
    conf.setEnum(AZURE_AD_TOKEN_PROVIDER_TYPE_KEY, TokenProviderType.Custom);
    URI uri = new URI("adl://localhost:8080");
    AdlFileSystem fileSystem = new AdlFileSystem();
    try {
      fileSystem.initialize(uri, conf);
      Assert.fail("Initialization should have failed due no token provider "
          + "configuration");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains(
          AZURE_AD_TOKEN_PROVIDER_CLASS_KEY, e);
    }
    conf.setClass(AZURE_AD_TOKEN_PROVIDER_CLASS_KEY,
        CustomMockTokenProvider.class, AzureADTokenProvider.class);
    fileSystem.initialize(uri, conf);
  }

  @Test
  public void testInvalidProviderConfigurationForClassPath()
      throws URISyntaxException, IOException {
    Configuration conf = new Configuration();
    URI uri = new URI("adl://localhost:8080");
    AdlFileSystem fileSystem = new AdlFileSystem();
    conf.setEnum(AZURE_AD_TOKEN_PROVIDER_TYPE_KEY, TokenProviderType.Custom);
    conf.set(AZURE_AD_TOKEN_PROVIDER_CLASS_KEY,
        "wrong.classpath.CustomMockTokenProvider");
    try {
      fileSystem.initialize(uri, conf);
      Assert.fail("Initialization should have failed due invalid provider "
          + "configuration");
    } catch (RuntimeException e) {
      Assert.assertTrue(
          e.getMessage().contains("wrong.classpath.CustomMockTokenProvider"));
    }
  }

  private CredentialProvider createTempCredProvider(Configuration conf)
      throws URISyntaxException, IOException {
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());
    return CredentialProviderFactory.getProviders(conf).get(0);
  }

  @Test
  public void testRefreshTokenWithCredentialProvider()
      throws IOException, URISyntaxException {
    Configuration conf = new Configuration();
    conf.set(AZURE_AD_CLIENT_ID_KEY, "DUMMY");
    conf.set(AZURE_AD_REFRESH_TOKEN_KEY, "DUMMY");
    conf.setEnum(AZURE_AD_TOKEN_PROVIDER_TYPE_KEY, RefreshToken);

    CredentialProvider provider = createTempCredProvider(conf);
    provider.createCredentialEntry(AZURE_AD_CLIENT_ID_KEY,
        CLIENT_ID.toCharArray());
    provider.createCredentialEntry(AZURE_AD_REFRESH_TOKEN_KEY,
        REFRESH_TOKEN.toCharArray());
    provider.flush();

    URI uri = new URI("adl://localhost:8080");
    AdlFileSystem fileSystem = new AdlFileSystem();
    fileSystem.initialize(uri, conf);
    RefreshTokenBasedTokenProvider expected =
        new RefreshTokenBasedTokenProvider(CLIENT_ID, REFRESH_TOKEN);
    Assert.assertTrue(EqualsBuilder.reflectionEquals(expected,
        fileSystem.getTokenProvider()));
  }

  @Test
  public void testRefreshTokenWithCredentialProviderFallback()
      throws IOException, URISyntaxException {
    Configuration conf = new Configuration();
    conf.set(AZURE_AD_CLIENT_ID_KEY, CLIENT_ID);
    conf.set(AZURE_AD_REFRESH_TOKEN_KEY, REFRESH_TOKEN);
    conf.setEnum(AZURE_AD_TOKEN_PROVIDER_TYPE_KEY, RefreshToken);

    createTempCredProvider(conf);

    URI uri = new URI("adl://localhost:8080");
    AdlFileSystem fileSystem = new AdlFileSystem();
    fileSystem.initialize(uri, conf);
    RefreshTokenBasedTokenProvider expected =
        new RefreshTokenBasedTokenProvider(CLIENT_ID, REFRESH_TOKEN);
    Assert.assertTrue(EqualsBuilder.reflectionEquals(expected,
        fileSystem.getTokenProvider()));
  }

  @Test
  public void testClientCredWithCredentialProvider()
      throws IOException, URISyntaxException {
    Configuration conf = new Configuration();
    conf.set(AZURE_AD_CLIENT_ID_KEY, "DUMMY");
    conf.set(AZURE_AD_CLIENT_SECRET_KEY, "DUMMY");
    conf.set(AZURE_AD_REFRESH_URL_KEY, "DUMMY");
    conf.setEnum(AZURE_AD_TOKEN_PROVIDER_TYPE_KEY, ClientCredential);

    CredentialProvider provider = createTempCredProvider(conf);
    provider.createCredentialEntry(AZURE_AD_CLIENT_ID_KEY,
        CLIENT_ID.toCharArray());
    provider.createCredentialEntry(AZURE_AD_CLIENT_SECRET_KEY,
        CLIENT_SECRET.toCharArray());
    provider.createCredentialEntry(AZURE_AD_REFRESH_URL_KEY,
        REFRESH_URL.toCharArray());
    provider.flush();

    URI uri = new URI("adl://localhost:8080");
    AdlFileSystem fileSystem = new AdlFileSystem();
    fileSystem.initialize(uri, conf);
    ClientCredsTokenProvider expected = new ClientCredsTokenProvider(
        REFRESH_URL, CLIENT_ID, CLIENT_SECRET);
    Assert.assertTrue(EqualsBuilder.reflectionEquals(expected,
        fileSystem.getTokenProvider()));
  }

  @Test
  public void testClientCredWithCredentialProviderFallback()
      throws IOException, URISyntaxException {
    Configuration conf = new Configuration();
    conf.set(AZURE_AD_CLIENT_ID_KEY, CLIENT_ID);
    conf.set(AZURE_AD_CLIENT_SECRET_KEY, CLIENT_SECRET);
    conf.set(AZURE_AD_REFRESH_URL_KEY, REFRESH_URL);
    conf.setEnum(AZURE_AD_TOKEN_PROVIDER_TYPE_KEY, ClientCredential);

    createTempCredProvider(conf);

    URI uri = new URI("adl://localhost:8080");
    AdlFileSystem fileSystem = new AdlFileSystem();
    fileSystem.initialize(uri, conf);
    ClientCredsTokenProvider expected = new ClientCredsTokenProvider(
        REFRESH_URL, CLIENT_ID, CLIENT_SECRET);
    Assert.assertTrue(EqualsBuilder.reflectionEquals(expected,
        fileSystem.getTokenProvider()));
  }

  @Test
  public void testCredentialProviderPathExclusions() throws Exception {
    String providerPath =
        "user:///,jceks://adl/user/hrt_qa/sqoopdbpasswd.jceks," +
            "jceks://hdfs@nn1.example.com/my/path/test.jceks";
    Configuration config = new Configuration();
    config.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        providerPath);
    String newPath =
        "user:///,jceks://hdfs@nn1.example.com/my/path/test.jceks";

    excludeAndTestExpectations(config, newPath);
  }

  @Test
  public void testExcludeAllProviderTypesFromConfig() throws Exception {
    String providerPath =
        "jceks://adl/tmp/test.jceks," +
            "jceks://adl@/my/path/test.jceks";
    Configuration config = new Configuration();
    config.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        providerPath);
    String newPath = null;

    excludeAndTestExpectations(config, newPath);
  }

  void excludeAndTestExpectations(Configuration config, String newPath)
      throws Exception {
    Configuration conf = ProviderUtils.excludeIncompatibleCredentialProviders(
        config, AdlFileSystem.class);
    String effectivePath = conf.get(
        CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, null);
    assertEquals(newPath, effectivePath);
  }
}

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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

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

import org.junit.Assert;
import org.junit.Test;

/**
 * Test appropriate token provider is loaded as per configuration.
 */
public class TestAzureADTokenProvider {

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
    URI uri = new URI("adl://localhost:8080");
    AdlFileSystem fileSystem = new AdlFileSystem();
    try {
      fileSystem.initialize(uri, conf);
      Assert.fail("Initialization should have failed due no token provider "
          + "configuration");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(
          e.getMessage().contains("dfs.adls.oauth2.access.token.provider"));
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
}

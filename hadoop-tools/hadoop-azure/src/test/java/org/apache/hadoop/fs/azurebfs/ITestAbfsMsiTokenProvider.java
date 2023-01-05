/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Date;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADAuthenticator;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADToken;
import org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider;
import org.apache.hadoop.fs.azurebfs.services.ExponentialRetryPolicy;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_MAX_ATTEMPTS;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assume.assumeThat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.isEmptyString;

import static org.apache.hadoop.fs.azurebfs.constants.AuthConfigurations.DEFAULT_FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY;
import static org.apache.hadoop.fs.azurebfs.constants.AuthConfigurations.DEFAULT_FS_AZURE_ACCOUNT_OAUTH_MSI_ENDPOINT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_MSI_ENDPOINT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_MSI_TENANT;
import static org.mockito.Mockito.times;

/**
 * Test MsiTokenProvider.
 */
public final class ITestAbfsMsiTokenProvider
    extends AbstractAbfsIntegrationTest {

  public ITestAbfsMsiTokenProvider() throws Exception {
    super();
  }

  @Test
  public void test() throws IOException {
    AbfsConfiguration conf = getConfiguration();
    assumeThat(conf.get(FS_AZURE_ACCOUNT_OAUTH_MSI_ENDPOINT),
        not(isEmptyOrNullString()));
    assumeThat(conf.get(FS_AZURE_ACCOUNT_OAUTH_MSI_TENANT),
        not(isEmptyOrNullString()));
    assumeThat(conf.get(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID),
        not(isEmptyOrNullString()));
    assumeThat(conf.get(FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY),
        not(isEmptyOrNullString()));

    String tenantGuid = conf
        .getPasswordString(FS_AZURE_ACCOUNT_OAUTH_MSI_TENANT);
    String clientId = conf.getPasswordString(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID);
    String authEndpoint = getTrimmedPasswordString(conf,
        FS_AZURE_ACCOUNT_OAUTH_MSI_ENDPOINT,
        DEFAULT_FS_AZURE_ACCOUNT_OAUTH_MSI_ENDPOINT);
    String authority = getTrimmedPasswordString(conf,
        FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY,
        DEFAULT_FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY);
    AccessTokenProvider tokenProvider = new MsiTokenProvider(authEndpoint,
        tenantGuid, clientId, authority);

    AzureADToken token = null;
    token = tokenProvider.getToken();
    assertThat(token.getAccessToken(), not(isEmptyString()));
    assertThat(token.getExpiry().after(new Date()), is(true));
  }

  private String getTrimmedPasswordString(AbfsConfiguration conf, String key,
      String defaultValue) throws IOException {
    String value = conf.getPasswordString(key);
    if (StringUtils.isBlank(value)) {
      value = defaultValue;
    }
    return value.trim();
  }

  /**
   * Test to verify that token fetch is retried for throttling errors (too many requests 429).
   * @throws Exception
   */
  @Test
  public void testRetryForThrottling() throws Exception {
    final int HTTP_TOO_MANY_REQUESTS = 429;
    AbfsConfiguration conf = getConfiguration();

    // Exception to be thrown with throttling error code 429.
    AzureADAuthenticator.HttpException httpException
        = new AzureADAuthenticator.HttpException(HTTP_TOO_MANY_REQUESTS,
        "abc", "abc", "abc", "abc", "abc");

    String tenantGuid = "abcd";
    String clientId = "abcd";
    String authEndpoint = getTrimmedPasswordString(conf,
        FS_AZURE_ACCOUNT_OAUTH_MSI_ENDPOINT,
        DEFAULT_FS_AZURE_ACCOUNT_OAUTH_MSI_ENDPOINT);
    String authority = getTrimmedPasswordString(conf,
        FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY,
        DEFAULT_FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY);

    // Mock the getTokenSingleCall to throw exception so the retry logic comes into place.
    try (MockedStatic<AzureADAuthenticator> adAuthenticator = Mockito.mockStatic(
        AzureADAuthenticator.class, Mockito.CALLS_REAL_METHODS)) {
      adAuthenticator.when(
          () -> AzureADAuthenticator.getTokenSingleCall(Mockito.anyString(),
              Mockito.anyString(), Mockito.any(), Mockito.anyString(),
              Mockito.anyBoolean())).thenThrow(httpException);

      // Mock the tokenFetchRetryPolicy to verify retries.
      ExponentialRetryPolicy exponentialRetryPolicy = Mockito.spy(
          conf.getOauthTokenFetchRetryPolicy());
      Field tokenFetchRetryPolicy = AzureADAuthenticator.class.getDeclaredField(
          "tokenFetchRetryPolicy");
      tokenFetchRetryPolicy.setAccessible(true);
      tokenFetchRetryPolicy.set(ExponentialRetryPolicy.class,
          exponentialRetryPolicy);

      AccessTokenProvider tokenProvider = new MsiTokenProvider(authEndpoint,
          tenantGuid, clientId, authority);
      AzureADToken token = null;
      intercept(AzureADAuthenticator.HttpException.class,
          tokenProvider::getToken);

      // If the status code doesn't qualify for retry shouldRetry returns false and the loop ends.
      // It being called multiple times verifies that the retry was done for the throttling status code 429.
      Mockito.verify(exponentialRetryPolicy,
              times(DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_MAX_ATTEMPTS + 1))
          .shouldRetry(Mockito.anyInt(), Mockito.anyInt());
    }
  }
}

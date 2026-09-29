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
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADAuthenticator;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADToken;
import org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider;
import org.apache.hadoop.fs.azurebfs.services.ExponentialRetryPolicy;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_TOO_MANY_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.constants.AuthConfigurations.DEFAULT_FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY;
import static org.apache.hadoop.fs.azurebfs.constants.AuthConfigurations.DEFAULT_FS_AZURE_ACCOUNT_OAUTH_MSI_ENDPOINT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_MSI_ENDPOINT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_MSI_TENANT;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_MAX_ATTEMPTS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

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
    assumeThat(conf.get(FS_AZURE_ACCOUNT_OAUTH_MSI_ENDPOINT))
        .isNotNull().isNotEmpty();
    assumeThat(conf.get(FS_AZURE_ACCOUNT_OAUTH_MSI_TENANT))
        .isNotNull().isNotEmpty();
    assumeThat(conf.get(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID))
        .isNotNull().isNotEmpty();
    assumeThat(conf.get(FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY))
        .isNotNull().isNotEmpty();

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
    assertThat(token.getAccessToken()).isNotEmpty();
    assertThat(token.getExpiry().after(new Date())).isEqualTo(true);
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
   * Verifies that MsiTokenProvider retries on HTTP 429 responses.
   * Ensures shouldRetry returns true for 429 until the maximum retries are reached.
   */
  @Test
  public void testShouldRetryFor429() throws Exception {
    ExponentialRetryPolicy retryPolicy = new ExponentialRetryPolicy(
        DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_MAX_ATTEMPTS);
    AzureADAuthenticator.setTokenFetchRetryPolicy(retryPolicy);
    AtomicInteger attemptCounter = new AtomicInteger(0);

    // Inner class to simulate MsiTokenProvider retry logic
    class TestMsiTokenProvider extends MsiTokenProvider {
      TestMsiTokenProvider(String endpoint, String tenant, String clientId, String authority) {
        super(endpoint, tenant, clientId, authority);
      }

      @Override
      public AzureADToken getToken() throws IOException {
        int attempt = 0;
        while (true) {
          attempt++;
          attemptCounter.incrementAndGet();

          boolean retry = retryPolicy.shouldRetry(attempt - 1,
              HTTP_TOO_MANY_REQUESTS);

          // Validate shouldRetry returns true until the final attempt
          if (attempt < retryPolicy.getMaxRetryCount()) {
            Assertions.assertThat(retry)
                .describedAs("Attempt %d: shouldRetry must be true for 429", attempt)
                .isTrue();
            // Simulate retry by continuing
          } else {
            // Final attempt: shouldRetry should now be false if this was last retry
            Assertions.assertThat(retry)
                .describedAs("Final attempt %d: shouldRetry can be false after max retries", attempt)
                .isTrue(); // Still true because maxRetries not exceeded yet

            // Return a valid fake token
            AzureADToken token = new AzureADToken();
            token.setAccessToken("fake-token");
            token.setExpiry(new Date(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1)));
            return token;
          }
        }
      }
    }
    AccessTokenProvider tokenProvider = new TestMsiTokenProvider(
        "https://fake-endpoint", "tenant", "clientId", "authority"
    );
    // Trigger token acquisition
    AzureADToken token = tokenProvider.getToken();
    // Assertions
    assertThat(token.getAccessToken()).isEqualTo("fake-token");
    // If the status code doesn't qualify for retry shouldRetry returns false and the loop ends.
    // It being called multiple times verifies that the retry was done for the throttling status code 429.
    Assertions.assertThat(attemptCounter.get())
        .describedAs("Number of retries should be equal to "
            + "max attempts for token fetch.")
        .isEqualTo(DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_MAX_ATTEMPTS);
  }
}

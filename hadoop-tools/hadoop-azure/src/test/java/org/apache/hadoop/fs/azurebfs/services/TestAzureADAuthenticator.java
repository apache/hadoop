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
package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_OAUTH_TOKEN_FETCH_RETRY_COUNT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_OAUTH_TOKEN_FETCH_RETRY_DELTA_BACKOFF;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_OAUTH_TOKEN_FETCH_RETRY_MAX_BACKOFF;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_OAUTH_TOKEN_FETCH_RETRY_MIN_BACKOFF;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_DELTA_BACKOFF;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_MAX_ATTEMPTS;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_MAX_BACKOFF_INTERVAL;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_MIN_BACKOFF_INTERVAL;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ACCOUNT_NAME;

public class TestAzureADAuthenticator extends AbstractAbfsIntegrationTest {

  private static final int TEST_RETRY_COUNT = 10;
  private static final int TEST_MIN_BACKOFF = 20;
  private static final int TEST_MAX_BACKOFF = 30;
  private static final int TEST_DELTA_BACKOFF = 40;

  public TestAzureADAuthenticator() throws Exception {
    super();
  }

  @Test
  public void testDefaultOAuthTokenFetchRetryPolicy() throws Exception {
    getConfiguration().unset(AZURE_OAUTH_TOKEN_FETCH_RETRY_COUNT);
    getConfiguration().unset(AZURE_OAUTH_TOKEN_FETCH_RETRY_MIN_BACKOFF);
    getConfiguration().unset(AZURE_OAUTH_TOKEN_FETCH_RETRY_MAX_BACKOFF);
    getConfiguration().unset(AZURE_OAUTH_TOKEN_FETCH_RETRY_DELTA_BACKOFF);

    String accountName = getConfiguration().get(FS_AZURE_ACCOUNT_NAME);
    AbfsConfiguration abfsConfig = new AbfsConfiguration(getRawConfiguration(),
        accountName);

    ExponentialRetryPolicy retryPolicy = abfsConfig
        .getOauthTokenFetchRetryPolicy();

    Assertions.assertThat(retryPolicy.getRetryCount()).describedAs(
        "retryCount should be the default value {} as the same "
            + "is not configured",
        DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_MAX_ATTEMPTS)
        .isEqualTo(DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_MAX_ATTEMPTS);
    Assertions.assertThat(retryPolicy.getMinBackoff()).describedAs(
        "minBackOff should be the default value {} as the same is "
            + "not configured",
        DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_MIN_BACKOFF_INTERVAL)
        .isEqualTo(DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_MIN_BACKOFF_INTERVAL);
    Assertions.assertThat(retryPolicy.getMaxBackoff()).describedAs(
        "maxBackOff should be the default value {} as the same is "
            + "not configured",
        DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_MAX_BACKOFF_INTERVAL)
        .isEqualTo(DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_MAX_BACKOFF_INTERVAL);
    Assertions.assertThat(retryPolicy.getDeltaBackoff()).describedAs(
        "deltaBackOff should be the default value {} as the same " + "is "
            + "not configured",
        DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_DELTA_BACKOFF)
        .isEqualTo(DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_DELTA_BACKOFF);

  }

  @Test
  public void testOAuthTokenFetchRetryPolicy()
      throws IOException, IllegalAccessException {

    getConfiguration()
        .set(AZURE_OAUTH_TOKEN_FETCH_RETRY_COUNT, String.valueOf(TEST_RETRY_COUNT));
    getConfiguration().set(AZURE_OAUTH_TOKEN_FETCH_RETRY_MIN_BACKOFF,
        String.valueOf(TEST_MIN_BACKOFF));
    getConfiguration().set(AZURE_OAUTH_TOKEN_FETCH_RETRY_MAX_BACKOFF,
        String.valueOf(TEST_MAX_BACKOFF));
    getConfiguration().set(AZURE_OAUTH_TOKEN_FETCH_RETRY_DELTA_BACKOFF,
        String.valueOf(TEST_DELTA_BACKOFF));

    String accountName = getConfiguration().get(FS_AZURE_ACCOUNT_NAME);
    AbfsConfiguration abfsConfig = new AbfsConfiguration(getRawConfiguration(),
        accountName);

    ExponentialRetryPolicy retryPolicy = abfsConfig
        .getOauthTokenFetchRetryPolicy();

    Assertions.assertThat(retryPolicy.getRetryCount())
        .describedAs("retryCount should be {}", TEST_RETRY_COUNT)
        .isEqualTo(TEST_RETRY_COUNT);
    Assertions.assertThat(retryPolicy.getMinBackoff())
        .describedAs("minBackOff should be {}", TEST_MIN_BACKOFF)
        .isEqualTo(TEST_MIN_BACKOFF);
    Assertions.assertThat(retryPolicy.getMaxBackoff())
        .describedAs("maxBackOff should be {}", TEST_MAX_BACKOFF)
        .isEqualTo(TEST_MAX_BACKOFF);
    Assertions.assertThat(retryPolicy.getDeltaBackoff())
        .describedAs("deltaBackOff should be {}", TEST_DELTA_BACKOFF)
        .isEqualTo(TEST_DELTA_BACKOFF);
  }

}

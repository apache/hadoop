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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_MAX_IO_RETRIES;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_STATIC_RETRY_FOR_CONNECTION_TIMEOUT_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_STATIC_RETRY_INTERVAL;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_RESET_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_TIMEOUT_ABBREVIATION;

/**
 * Class to test the behavior of Static Retry policy as well the inheritance
 * between {@link AbfsRetryPolicy}, {@link ExponentialRetryPolicy}, {@link StaticRetryPolicy}
 */
public class ITestStaticRetryPolicy extends AbstractAbfsIntegrationTest {

  public ITestStaticRetryPolicy() throws Exception {
    super();
  }

  /**
   * Tests for retry policy related configurations.
   * Asserting that the correct retry policy is used for a given set of
   * configurations including default ones
   * @throws Exception
   */
  @Test
  public void testStaticRetryPolicyInitializationDefault() throws Exception {
    Configuration config = new Configuration(this.getRawConfiguration());
    assertInitialization(config, StaticRetryPolicy.class);
  }

  @Test
  public void testStaticRetryPolicyInitialization1() throws Exception {
    Configuration config = new Configuration(this.getRawConfiguration());
    config.set(AZURE_STATIC_RETRY_FOR_CONNECTION_TIMEOUT_ENABLED, "true");
    assertInitialization(config, StaticRetryPolicy.class);
  }

  @Test
  public void testStaticRetryPolicyInitialization2() throws Exception {
    Configuration config = new Configuration(this.getRawConfiguration());
    config.set(AZURE_STATIC_RETRY_FOR_CONNECTION_TIMEOUT_ENABLED, "false");
    assertInitialization(config, ExponentialRetryPolicy.class);
  }

  private void assertInitialization(Configuration config, Class retryPolicyClass) throws Exception{
    final AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem
        .newInstance(getFileSystem().getUri(), config);
    AbfsClient client = fs.getAbfsStore().getClient();

    // Assert that static retry policy will be used only for CT Failures
    AbfsRetryPolicy retryPolicy = client.getRetryPolicy(CONNECTION_TIMEOUT_ABBREVIATION);
    Assertions.assertThat(retryPolicy)
        .describedAs("RetryPolicy Type is Not As Expected")
        .isInstanceOf(retryPolicyClass);

    // For all other possible values of failureReason, Exponential retry is used
    retryPolicy = client.getRetryPolicy("");
    assertIsExponentialRetryPolicy(retryPolicy);
    retryPolicy = client.getRetryPolicy(null);
    assertIsExponentialRetryPolicy(retryPolicy);
    retryPolicy = client.getRetryPolicy(CONNECTION_RESET_ABBREVIATION);
    assertIsExponentialRetryPolicy(retryPolicy);
  }

  /**
   * Test to assert that static retry policy returns the same retry interval
   * independent of retry count
   * @throws Exception
   */
  @Test
  public void testStaticRetryInterval() throws Exception {
    Configuration config = new Configuration(this.getRawConfiguration());
    long retryInterval = 1000;
    int maxIoRetry = 5;
    config.set(AZURE_STATIC_RETRY_FOR_CONNECTION_TIMEOUT_ENABLED, "true");
    config.set(AZURE_STATIC_RETRY_INTERVAL, "1000");
    config.set(AZURE_MAX_IO_RETRIES, "5");
    final AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem
        .newInstance(getFileSystem().getUri(), config);
    AbfsClient client = fs.getAbfsStore().getClient();

    AbfsRetryPolicy retryPolicy = client.getRetryPolicy(CONNECTION_TIMEOUT_ABBREVIATION);
    assertIsStaticRetryPolicy(retryPolicy);

    Assertions.assertThat(retryPolicy.shouldRetry(0, -1))
        .describedAs("Should retry should be true")
        .isEqualTo(true);
    Assertions.assertThat(retryPolicy.getRetryInterval(0))
        .describedAs("Retry Interval Value Not as expected")
        .isEqualTo(retryInterval);
    Assertions.assertThat(retryPolicy.getRetryInterval(1))
        .describedAs("Retry Interval Value Not as expected")
        .isEqualTo(retryInterval);
    Assertions.assertThat(retryPolicy.getRetryInterval(2))
        .describedAs("Retry Interval Value Not as expected")
        .isEqualTo(retryInterval);
    Assertions.assertThat(retryPolicy.getRetryInterval(3))
        .describedAs("Retry Interval Value Not as expected")
        .isEqualTo(retryInterval);
    Assertions.assertThat(retryPolicy.shouldRetry(maxIoRetry, -1))
        .describedAs("Should retry for maxretrycount should be false")
        .isEqualTo(false);
  }

  private void assertIsExponentialRetryPolicy(AbfsRetryPolicy retryPolicy) {
    Assertions.assertThat(retryPolicy)
        .describedAs("Exponential Retry policy must be used")
        .isInstanceOf(ExponentialRetryPolicy.class);
  }

  private void assertIsStaticRetryPolicy(AbfsRetryPolicy retryPolicy) {
    Assertions.assertThat(retryPolicy)
        .describedAs("Static Retry policy must be used")
        .isInstanceOf(StaticRetryPolicy.class);
  }
}

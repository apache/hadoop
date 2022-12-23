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

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_BACKOFF_INTERVAL;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_MAX_BACKOFF_INTERVAL;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_MAX_IO_RETRIES;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_MIN_BACKOFF_INTERVAL;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;

/**
 * Unit test TestExponentialRetryPolicy.
 */
public class TestExponentialRetryPolicy extends AbstractAbfsIntegrationTest {
  private final int maxRetryCount = 30;
  private final int noRetryCount = 0;
  private final int retryCount = new Random().nextInt(maxRetryCount);
  private final int retryCountBeyondMax = maxRetryCount + 1;


  public TestExponentialRetryPolicy() throws Exception {
    super();
  }

  @Test
  public void testDifferentMaxIORetryCount() throws Exception {
    AbfsConfiguration abfsConfig = getAbfsConfig();
    abfsConfig.setMaxIoRetries(noRetryCount);
    testMaxIOConfig(abfsConfig);
    abfsConfig.setMaxIoRetries(retryCount);
    testMaxIOConfig(abfsConfig);
    abfsConfig.setMaxIoRetries(retryCountBeyondMax);
    testMaxIOConfig(abfsConfig);
  }

  @Test
  public void testDefaultMaxIORetryCount() throws Exception {
    AbfsConfiguration abfsConfig = getAbfsConfig();
    Assert.assertEquals(
        String.format("default maxIORetry count is %s.", maxRetryCount),
        maxRetryCount, abfsConfig.getMaxIoRetries());
    testMaxIOConfig(abfsConfig);
  }

  @Test
  public void testAbfsConfigConstructor() throws Exception {
    // Ensure we choose expected values that are not defaults
    ExponentialRetryPolicy template = new ExponentialRetryPolicy(
        getAbfsConfig().getMaxIoRetries());
    int testModifier = 1;
    int expectedMaxRetries = template.getRetryCount() + testModifier;
    int expectedMinBackoff = template.getMinBackoff() + testModifier;
    int expectedMaxBackoff = template.getMaxBackoff() + testModifier;
    int expectedDeltaBackoff = template.getDeltaBackoff() + testModifier;

    Configuration config = new Configuration(this.getRawConfiguration());
    config.setInt(AZURE_MAX_IO_RETRIES, expectedMaxRetries);
    config.setInt(AZURE_MIN_BACKOFF_INTERVAL, expectedMinBackoff);
    config.setInt(AZURE_MAX_BACKOFF_INTERVAL, expectedMaxBackoff);
    config.setInt(AZURE_BACKOFF_INTERVAL, expectedDeltaBackoff);

    ExponentialRetryPolicy policy = new ExponentialRetryPolicy(
        new AbfsConfiguration(config, "dummyAccountName"));

    Assert.assertEquals("Max retry count was not set as expected.", expectedMaxRetries, policy.getRetryCount());
    Assert.assertEquals("Min backoff interval was not set as expected.", expectedMinBackoff, policy.getMinBackoff());
    Assert.assertEquals("Max backoff interval was not set as expected.", expectedMaxBackoff, policy.getMaxBackoff());
    Assert.assertEquals("Delta backoff interval was not set as expected.", expectedDeltaBackoff, policy.getDeltaBackoff());
  }

  private AbfsConfiguration getAbfsConfig() throws Exception {
    Configuration
        config = new Configuration(this.getRawConfiguration());
    return new AbfsConfiguration(config, "dummyAccountName");
  }

  private void testMaxIOConfig(AbfsConfiguration abfsConfig) {
    ExponentialRetryPolicy retryPolicy = new ExponentialRetryPolicy(
        abfsConfig.getMaxIoRetries());
    int localRetryCount = 0;

    while (localRetryCount < abfsConfig.getMaxIoRetries()) {
      Assert.assertTrue(
          "Retry should be allowed when retryCount less than max count configured.",
          retryPolicy.shouldRetry(localRetryCount, -1));
      localRetryCount++;
    }

    Assert.assertEquals(
        "When all retries are exhausted, the retryCount will be same as max configured",
        abfsConfig.getMaxIoRetries(), localRetryCount);
  }
}

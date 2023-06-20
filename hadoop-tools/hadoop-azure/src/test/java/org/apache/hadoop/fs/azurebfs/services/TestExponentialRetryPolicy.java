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

import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_BACKOFF_INTERVAL;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_MAX_BACKOFF_INTERVAL;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_MAX_IO_RETRIES;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_MIN_BACKOFF_INTERVAL;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_LEVEL_THROTTLING_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_AUTOTHROTTLING;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MIN_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_ACCOUNT1_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.TEST_CONFIGURATION_FILE_NAME;

import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.fs.FSDataInputStream;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.mockito.Mockito;

import java.net.URI;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
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
  private static final String TEST_PATH = "/testfile";
  private static final double MULTIPLYING_FACTOR = 1.5;
  private static final int ANALYSIS_PERIOD = 10000;


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
  public void testThrottlingIntercept() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    final Configuration configuration = new Configuration();
    configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
    configuration.setBoolean(FS_AZURE_ENABLE_AUTOTHROTTLING, false);

    // On disabling throttling AbfsNoOpThrottlingIntercept object is returned
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        "dummy.dfs.core.windows.net");
    AbfsThrottlingIntercept intercept;
    AbfsClient abfsClient = ITestAbfsClient.createTestClientFromCurrentContext(fs.getAbfsStore().getClient(), abfsConfiguration);
    intercept = abfsClient.getIntercept();
    Assertions.assertThat(intercept)
        .describedAs("AbfsNoOpThrottlingIntercept instance expected")
        .isInstanceOf(AbfsNoOpThrottlingIntercept.class);

    configuration.setBoolean(FS_AZURE_ENABLE_AUTOTHROTTLING, true);
    configuration.setBoolean(FS_AZURE_ACCOUNT_LEVEL_THROTTLING_ENABLED, true);
    // On disabling throttling AbfsClientThrottlingIntercept object is returned
    AbfsConfiguration abfsConfiguration1 = new AbfsConfiguration(configuration,
        "dummy1.dfs.core.windows.net");
    AbfsClient abfsClient1 = ITestAbfsClient.createTestClientFromCurrentContext(fs.getAbfsStore().getClient(), abfsConfiguration1);
    intercept = abfsClient1.getIntercept();
    Assertions.assertThat(intercept)
        .describedAs("AbfsClientThrottlingIntercept instance expected")
        .isInstanceOf(AbfsClientThrottlingIntercept.class);
  }

  @Test
  public void testCreateMultipleAccountThrottling() throws Exception {
    Configuration config = new Configuration(getRawConfiguration());
    String accountName = config.get(FS_AZURE_ACCOUNT_NAME);
    if (accountName == null) {
      // check if accountName is set using different config key
      accountName = config.get(FS_AZURE_ABFS_ACCOUNT1_NAME);
    }
    assumeTrue("Not set: " + FS_AZURE_ABFS_ACCOUNT1_NAME,
        accountName != null && !accountName.isEmpty());

    Configuration rawConfig1 = new Configuration();
    rawConfig1.addResource(TEST_CONFIGURATION_FILE_NAME);

    AbfsRestOperation successOp = mock(AbfsRestOperation.class);
    AbfsHttpOperation http500Op = mock(AbfsHttpOperation.class);
    when(http500Op.getStatusCode()).thenReturn(HTTP_INTERNAL_ERROR);
    when(successOp.getResult()).thenReturn(http500Op);

    AbfsConfiguration configuration = Mockito.mock(AbfsConfiguration.class);
    when(configuration.getAnalysisPeriod()).thenReturn(ANALYSIS_PERIOD);
    when(configuration.isAutoThrottlingEnabled()).thenReturn(true);
    when(configuration.accountThrottlingEnabled()).thenReturn(false);

    AbfsThrottlingIntercept instance1 = AbfsThrottlingInterceptFactory.getInstance(accountName, configuration);
    String accountName1 = config.get(FS_AZURE_ABFS_ACCOUNT1_NAME);

    assumeTrue("Not set: " + FS_AZURE_ABFS_ACCOUNT1_NAME,
        accountName1 != null && !accountName1.isEmpty());

    AbfsThrottlingIntercept instance2 = AbfsThrottlingInterceptFactory.getInstance(accountName1, configuration);
    //if singleton is enabled, for different accounts both the instances should return same value
    Assertions.assertThat(instance1)
        .describedAs(
            "if singleton is enabled, for different accounts both the instances should return same value")
        .isEqualTo(instance2);

    when(configuration.accountThrottlingEnabled()).thenReturn(true);
    AbfsThrottlingIntercept instance3 = AbfsThrottlingInterceptFactory.getInstance(accountName, configuration);
    AbfsThrottlingIntercept instance4 = AbfsThrottlingInterceptFactory.getInstance(accountName1, configuration);
    AbfsThrottlingIntercept instance5 = AbfsThrottlingInterceptFactory.getInstance(accountName, configuration);
    //if singleton is not enabled, for different accounts instances should return different value
    Assertions.assertThat(instance3)
        .describedAs(
            "iff singleton is not enabled, for different accounts instances should return different value")
        .isNotEqualTo(instance4);

    //if singleton is not enabled, for same accounts instances should return same value
    Assertions.assertThat(instance3)
        .describedAs(
            "if singleton is not enabled, for same accounts instances should return same value")
        .isEqualTo(instance5);
  }

  @Test
  public void testOperationOnAccountIdle() throws Exception {
    //Get the filesystem.
    AzureBlobFileSystem fs = getFileSystem();
    AbfsClient client = fs.getAbfsStore().getClient();
    AbfsConfiguration configuration1 = client.getAbfsConfiguration();
    Assume.assumeTrue(configuration1.isAutoThrottlingEnabled());
    Assume.assumeTrue(configuration1.accountThrottlingEnabled());

    AbfsClientThrottlingIntercept accountIntercept
        = (AbfsClientThrottlingIntercept) client.getIntercept();
    final byte[] b = new byte[2 * MIN_BUFFER_SIZE];
    new Random().nextBytes(b);

    Path testPath = path(TEST_PATH);

    //Do an operation on the filesystem.
    try (FSDataOutputStream stream = fs.create(testPath)) {
      stream.write(b);
    }

    //Don't perform any operation on the account.
    int sleepTime = (int) ((getAbfsConfig().getAccountOperationIdleTimeout()) * MULTIPLYING_FACTOR);
    Thread.sleep(sleepTime);

    try (FSDataInputStream streamRead = fs.open(testPath)) {
      streamRead.read(b);
    }

    //Perform operations on another account.
    AzureBlobFileSystem fs1 = new AzureBlobFileSystem();
    Configuration config = new Configuration(getRawConfiguration());
    String accountName1 = config.get(FS_AZURE_ABFS_ACCOUNT1_NAME);
    assumeTrue("Not set: " + FS_AZURE_ABFS_ACCOUNT1_NAME,
        accountName1 != null && !accountName1.isEmpty());
    final String abfsUrl1 = this.getFileSystemName() + "12" + "@" + accountName1;
    URI defaultUri1 = null;
    defaultUri1 = new URI("abfss", abfsUrl1, null, null, null);
    fs1.initialize(defaultUri1, getRawConfiguration());
    AbfsClient client1 = fs1.getAbfsStore().getClient();
    AbfsClientThrottlingIntercept accountIntercept1
        = (AbfsClientThrottlingIntercept) client1.getIntercept();
    try (FSDataOutputStream stream1 = fs1.create(testPath)) {
      stream1.write(b);
    }

    //Verify the write analyzer for first account is idle but the read analyzer is not idle.
    Assertions.assertThat(accountIntercept.getWriteThrottler()
            .getIsOperationOnAccountIdle()
            .get())
        .describedAs("Write analyzer for first account should be idle the first time")
        .isTrue();

    Assertions.assertThat(
            accountIntercept.getReadThrottler()
                .getIsOperationOnAccountIdle()
                .get())
        .describedAs("Read analyzer for first account should not be idle")
        .isFalse();

    //Verify the write analyzer for second account is not idle.
    Assertions.assertThat(
            accountIntercept1.getWriteThrottler()
                .getIsOperationOnAccountIdle()
                .get())
        .describedAs("Write analyzer for second account should not be idle")
        .isFalse();

    //Again perform an operation on the first account.
    try (FSDataOutputStream stream2 = fs.create(testPath)) {
      stream2.write(b);
    }

    //Verify the write analyzer on first account is not idle.
    Assertions.assertThat(

            accountIntercept.getWriteThrottler()
                .getIsOperationOnAccountIdle()
                .get())
        .describedAs(
            "Write analyzer for first account should not be idle second time")
        .isFalse();
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

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
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MIN_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_ACCOUNT1_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.TEST_CONFIGURATION_FILE_NAME;

import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.fs.FSDataInputStream;
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
    when(configuration.getAnalysisPeriod()).thenReturn(10000);
    when(configuration.isAutoThrottlingEnabled()).thenReturn(true);
    when(configuration.isAccountThrottlingEnabled()).thenReturn(false);

    AbfsClientThrottlingIntercept instance1 = AbfsClientThrottlingInterceptFactory.getInstance(accountName, configuration);
    String accountName1 = config.get(FS_AZURE_ABFS_ACCOUNT1_NAME);

    AbfsClientThrottlingIntercept instance2 = AbfsClientThrottlingInterceptFactory.getInstance(accountName1, configuration);
    //if singleton is enabled, for different accounts both the instances should return same value
    assertEquals(instance1, instance2);

    when(configuration.isAccountThrottlingEnabled()).thenReturn(true);
    AbfsClientThrottlingIntercept instance3 = AbfsClientThrottlingInterceptFactory.getInstance(accountName, configuration);
    AbfsClientThrottlingIntercept instance4 = AbfsClientThrottlingInterceptFactory.getInstance(accountName1, configuration);
    AbfsClientThrottlingIntercept instance5 = AbfsClientThrottlingInterceptFactory.getInstance(accountName, configuration);
    //if singleton is not enabled, for different accounts instances should return different value
    assertNotEquals(instance3, instance4);

    //if singleton is not enabled, for same accounts instances should return same value
    assertEquals(instance3, instance5);
  }

  @Test
  public void testOperationOnAccountIdle() throws Exception {
    //Get the filesystem.
    AzureBlobFileSystem fs = getFileSystem();
    AbfsConfiguration configuration1 = fs.getAbfsStore().getClient().getAbfsConfiguration();
    Assume.assumeTrue(configuration1.isAccountThrottlingEnabled());

    AbfsClientThrottlingIntercept accountIntercept = AbfsClientThrottlingInterceptFactory.getInstance(this.getAccountName(),
            configuration1);
    int bufferSize = MIN_BUFFER_SIZE;
    final byte[] b = new byte[2 * bufferSize];
    new Random().nextBytes(b);

    Path testPath = path(TEST_PATH);

    //Do an operation on the filesystem.
    FSDataOutputStream stream = fs.create(testPath);

    try {
      stream.write(b);
    } finally{
      stream.close();
    }

    //Don't perform any operation on the account.
    int sleepTime = (int)((getAbfsConfig().getAccountOperationIdleTimeout()) * 1.5);
    Thread.sleep(sleepTime);

    FSDataInputStream streamRead = fs.open(testPath);
    try {
      streamRead.read(b);
    } finally{
      streamRead.close();
    }

    //Perform operations on another account.
    AzureBlobFileSystem fs1 = new AzureBlobFileSystem();
    Configuration config = new Configuration(getRawConfiguration());
    String accountName1 = config.get(FS_AZURE_ABFS_ACCOUNT1_NAME);
    final String abfsUrl1 = this.getFileSystemName() + "12" + "@" + accountName1;
    URI defaultUri1 = null;
    try {
      defaultUri1 = new URI("abfss", abfsUrl1, null, null, null);
    } catch (Exception ex) {
      throw new AssertionError(ex);
    }
    fs1.initialize(defaultUri1, getRawConfiguration());
    AbfsConfiguration configuration2 = fs1.getAbfsStore().getClient().getAbfsConfiguration();
    AbfsClientThrottlingIntercept accountIntercept1 = AbfsClientThrottlingInterceptFactory.getInstance(accountName1,
            configuration2);
    FSDataOutputStream stream1 = fs1.create(testPath);
    try {
      stream1.write(b);
    } finally{
      stream1.close();
    }

    //Verfiy the write analyzer for first account is idle but the read analyzer is not idle.
    if (accountIntercept != null){
      Assert.assertTrue(accountIntercept.getWriteThrottler().getIsOperationOnAccountIdle().get());
      Assert.assertFalse(accountIntercept.getReadThrottler().getIsOperationOnAccountIdle().get());
    }

    //Verify the write analyzer for second account is not idle.
    if (accountIntercept1 != null){
      Assert.assertFalse(accountIntercept1.getWriteThrottler().getIsOperationOnAccountIdle().get());
    }

    //Again perform an operation on the first account.
    FSDataOutputStream stream2 = fs.create(testPath);
    try {
      stream2.write(b);
    } finally{
      stream2.close();
    }

    //Verify the write analyzer on first account is not idle.
    if (accountIntercept != null) {
      Assert.assertFalse(accountIntercept.getWriteThrottler().getIsOperationOnAccountIdle().get());
    }
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

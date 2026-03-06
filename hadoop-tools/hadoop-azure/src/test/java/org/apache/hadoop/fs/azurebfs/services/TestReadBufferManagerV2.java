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

import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadBufferStatus;
import org.apache.hadoop.fs.azurebfs.utils.ResourceUtilizationUtils;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_READAHEAD_V2;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_READAHEAD_V2_DYNAMIC_SCALING;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READAHEAD_V2_CACHED_BUFFER_TTL_MILLIS;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READAHEAD_V2_CPU_MONITORING_INTERVAL_MILLIS;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READAHEAD_V2_CPU_USAGE_THRESHOLD_PERCENT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READAHEAD_V2_MAX_THREAD_POOL_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READAHEAD_V2_MEMORY_MONITORING_INTERVAL_MILLIS;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READAHEAD_V2_MEMORY_USAGE_THRESHOLD_PERCENT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READAHEAD_V2_MIN_THREAD_POOL_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HUNDRED;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit Tests around different components of Read Buffer Manager V2
 */
public class TestReadBufferManagerV2 extends AbstractAbfsIntegrationTest {
  private volatile boolean running = true;
  private final List<byte[]> allocations = new ArrayList<>();
  private static final double HIGH_MEMORY_USAGE_THRESHOLD_PERCENT = 0.8;

  public TestReadBufferManagerV2() throws Exception {
    super();
  }

  /**
   * Test to verify init of ReadBufferManagerV2
   * @throws Exception if test fails
   */
  @Test
  public void testReadBufferManagerV2Init() throws Exception {
    AbfsClient abfsClient = getFileSystem().getAbfsStore().getClient();
    ReadBufferManagerV2.setReadBufferManagerConfigs(getConfiguration().getReadAheadBlockSize(), getConfiguration());
    ReadBufferManagerV2.getBufferManager(abfsClient.getAbfsCounters()).testResetReadBufferManager();
    assertThat(ReadBufferManagerV2.getInstance())
        .as("ReadBufferManager should be uninitialized").isNull();
    intercept(IllegalStateException.class, "ReadBufferManagerV2 is not configured.", () -> {
      ReadBufferManagerV2.getBufferManager(abfsClient.getAbfsCounters());
    });
    // verify that multiple invocations of getBufferManager returns same instance.
    ReadBufferManagerV2.setReadBufferManagerConfigs(getConfiguration().getReadAheadBlockSize(), getConfiguration());
    ReadBufferManagerV2 bufferManager = ReadBufferManagerV2.getBufferManager(abfsClient.getAbfsCounters());
    ReadBufferManagerV2 bufferManager2 = ReadBufferManagerV2.getBufferManager(abfsClient.getAbfsCounters());
    ReadBufferManagerV2 bufferManager3 = ReadBufferManagerV2.getInstance();
    assertThat(bufferManager).isNotNull();
    assertThat(bufferManager2).isNotNull();
    assertThat(bufferManager).isSameAs(bufferManager2);
    assertThat(bufferManager3).isNotNull();
    assertThat(bufferManager3).isSameAs(bufferManager);

    // Verify default values are not invalid.
    assertThat(bufferManager.getMinBufferPoolSize()).isGreaterThan(0);
    assertThat(bufferManager.getMaxBufferPoolSize()).isGreaterThan(0);
  }

  /**
   * Test to verify that cpu monitor thread is not active if disabled.
   * @throws Exception if test fails
   */
  @Test
  public void testDynamicScalingSwitchingOnAndOff() throws Exception {
    Configuration conf = new Configuration(getRawConfiguration());
    conf.setBoolean(FS_AZURE_ENABLE_READAHEAD_V2, true);
    conf.setBoolean(FS_AZURE_ENABLE_READAHEAD_V2_DYNAMIC_SCALING, true);
    try(AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(getFileSystem().getUri(), conf)) {
      AbfsClient abfsClient = fs.getAbfsStore().getClient();
      AbfsConfiguration abfsConfiguration = fs.getAbfsStore().getAbfsConfiguration();
      ReadBufferManagerV2.setReadBufferManagerConfigs(abfsConfiguration.getReadAheadBlockSize(), abfsConfiguration);
      ReadBufferManagerV2.getBufferManager(abfsClient.getAbfsCounters()).testResetReadBufferManager();
      ReadBufferManagerV2.setReadBufferManagerConfigs(abfsConfiguration.getReadAheadBlockSize(), abfsConfiguration);
      ReadBufferManagerV2 bufferManagerV2 = ReadBufferManagerV2.getBufferManager(abfsClient.getAbfsCounters());
      assertThat(bufferManagerV2.getCpuMonitoringThread())
          .as("CPU Monitor thread should be initialized").isNotNull();
      bufferManagerV2.resetBufferManager();
    }

    conf.setBoolean(FS_AZURE_ENABLE_READAHEAD_V2_DYNAMIC_SCALING, false);
    try(AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(getFileSystem().getUri(), conf)) {
      AbfsClient abfsClient = fs.getAbfsStore().getClient();
      AbfsConfiguration abfsConfiguration = fs.getAbfsStore().getAbfsConfiguration();
      ReadBufferManagerV2.setReadBufferManagerConfigs(abfsConfiguration.getReadAheadBlockSize(), abfsConfiguration);
      ReadBufferManagerV2.getBufferManager(abfsClient.getAbfsCounters()).testResetReadBufferManager();
      ReadBufferManagerV2.setReadBufferManagerConfigs(abfsConfiguration.getReadAheadBlockSize(), abfsConfiguration);
      ReadBufferManagerV2 bufferManagerV2 = ReadBufferManagerV2.getBufferManager(abfsClient.getAbfsCounters());
      assertThat(bufferManagerV2.getCpuMonitoringThread())
          .as("CPU Monitor thread should not be initialized").isNull();
      bufferManagerV2.resetBufferManager();
    }
  }

  @Test
  public void testThreadPoolDynamicScaling() throws Exception {
    running = true;
    TestAbfsInputStream testAbfsInputStream = new TestAbfsInputStream();
    AbfsClient client = testAbfsInputStream.getMockAbfsClient();
    AbfsInputStream inputStream = testAbfsInputStream.getAbfsInputStream(client, "testFailedReadAhead.txt");
    Configuration configuration = getReadAheadV2Configuration();
    AbfsConfiguration abfsConfig = new AbfsConfiguration(configuration,
        getAccountName());
    ReadBufferManagerV2.setReadBufferManagerConfigs(abfsConfig.getReadAheadBlockSize(), abfsConfig);
    ReadBufferManagerV2.getBufferManager(client.getAbfsCounters()).testResetReadBufferManager();
    ReadBufferManagerV2.setReadBufferManagerConfigs(abfsConfig.getReadAheadBlockSize(), abfsConfig);
    ReadBufferManagerV2 bufferManagerV2 = ReadBufferManagerV2.getBufferManager(client.getAbfsCounters());
    assertThat(bufferManagerV2.getCurrentThreadPoolSize()).isEqualTo(2);
    int[] reqOffset = {0};
    int reqLength = 1;
    Thread t = new Thread(() -> {
      while (running) {
        bufferManagerV2.queueReadAhead(inputStream, reqOffset[0], reqLength,
            inputStream.getTracingContext());
        reqOffset[0] += reqLength;
      }
    });
    t.start();
    Thread.sleep(2L * bufferManagerV2.getCpuMonitoringIntervalInMilliSec());
    assertThat(bufferManagerV2.getCurrentThreadPoolSize()).isGreaterThanOrEqualTo(2);
    running = false;
    t.join();
    Thread.sleep(4L * bufferManagerV2.getCpuMonitoringIntervalInMilliSec());
    assertThat(bufferManagerV2.getCurrentThreadPoolSize()).isLessThanOrEqualTo(4);
  }

  @Test
  public void testCpuUpscaleNotAllowedIfCpuAboveThreshold() throws Exception {
    TestAbfsInputStream testAbfsInputStream = new TestAbfsInputStream();
    AbfsClient client = testAbfsInputStream.getMockAbfsClient();
    AbfsInputStream inputStream = testAbfsInputStream.getAbfsInputStream(client, "testFailedReadAhead.txt");
    Configuration configuration = getReadAheadV2Configuration();
    configuration.set(FS_AZURE_READAHEAD_V2_CPU_USAGE_THRESHOLD_PERCENT, "0"); // set low threshold
    AbfsConfiguration abfsConfig = new AbfsConfiguration(configuration,
        getAccountName());
    ReadBufferManagerV2.setReadBufferManagerConfigs(abfsConfig.getReadAheadBlockSize(), abfsConfig);
    ReadBufferManagerV2.getBufferManager(client.getAbfsCounters()).testResetReadBufferManager();
    ReadBufferManagerV2.setReadBufferManagerConfigs(abfsConfig.getReadAheadBlockSize(), abfsConfig);
    ReadBufferManagerV2 bufferManagerV2 = ReadBufferManagerV2.getBufferManager(client.getAbfsCounters());
    assertThat(bufferManagerV2.getCurrentThreadPoolSize()).isEqualTo(2);
    int[] reqOffset = {0};
    int reqLength = 1;
    running = true;
    Thread t = new Thread(() -> {
      while (running) {
        bufferManagerV2.queueReadAhead(inputStream, reqOffset[0], reqLength,
            inputStream.getTracingContext());
        reqOffset[0] += reqLength;
      }
    });
    t.start();
    Thread.sleep(2L * bufferManagerV2.getCpuMonitoringIntervalInMilliSec());
    assertThat(bufferManagerV2.getCurrentThreadPoolSize()).isEqualTo(2);
    running = false;
    t.join();
  }

  @Test
  public void testScheduledEviction() throws Exception {
    TestAbfsInputStream testAbfsInputStream = new TestAbfsInputStream();
    AbfsClient client = testAbfsInputStream.getMockAbfsClient();
    AbfsInputStream inputStream = testAbfsInputStream.getAbfsInputStream(client, "testFailedReadAhead.txt");
    Configuration configuration = getReadAheadV2Configuration();
    AbfsConfiguration abfsConfig = new AbfsConfiguration(configuration,
        getAccountName());
    ReadBufferManagerV2.getBufferManager(client.getAbfsCounters()).testResetReadBufferManager();
    ReadBufferManagerV2.setReadBufferManagerConfigs(abfsConfig.getReadAheadBlockSize(), abfsConfig);
    ReadBufferManagerV2 bufferManagerV2 = ReadBufferManagerV2.getBufferManager(client.getAbfsCounters());
    // Add a failed buffer to completed queue and set to no free buffers to read ahead.
    ReadBuffer buff = new ReadBuffer();
    buff.setStatus(ReadBufferStatus.READ_FAILED);
    buff.setStream(inputStream);
    bufferManagerV2.testMimicFullUseAndAddFailedBuffer(buff);
    bufferManagerV2.testMimicFullUseAndAddFailedBuffer(buff);
    assertThat(bufferManagerV2.getCompletedReadListSize()).isEqualTo(2);
    Thread.sleep(2L * bufferManagerV2.getMemoryMonitoringIntervalInMilliSec());
    assertThat(bufferManagerV2.getCompletedReadListSize()).isEqualTo(0);
  }

  @Test
  public void testMemoryUpscaleNotAllowedIfMemoryAboveThreshold() throws Exception {
    TestAbfsInputStream testAbfsInputStream = new TestAbfsInputStream();
    AbfsClient client = testAbfsInputStream.getMockAbfsClient();
    AbfsInputStream inputStream = testAbfsInputStream.getAbfsInputStream(client, "testFailedReadAhead.txt");
    Configuration configuration = getReadAheadV2Configuration();
    configuration.set(FS_AZURE_READAHEAD_V2_MEMORY_USAGE_THRESHOLD_PERCENT, "0"); // set low threshold
    AbfsConfiguration abfsConfig = new AbfsConfiguration(configuration,
        getAccountName());
    ReadBufferManagerV2.setReadBufferManagerConfigs(abfsConfig.getReadAheadBlockSize(), abfsConfig);
    ReadBufferManagerV2.getBufferManager(client.getAbfsCounters()).testResetReadBufferManager();
    ReadBufferManagerV2.setReadBufferManagerConfigs(abfsConfig.getReadAheadBlockSize(), abfsConfig);
    ReadBufferManagerV2 bufferManagerV2 = ReadBufferManagerV2.getBufferManager(client.getAbfsCounters());
    // Add a failed buffer to completed queue and set to no free buffers to read ahead.
    ReadBuffer buff = new ReadBuffer();
    buff.setStatus(ReadBufferStatus.READ_FAILED);
    buff.setStream(inputStream);
    bufferManagerV2.testMimicFullUseAndAddFailedBuffer(buff);
    assertThat(bufferManagerV2.getNumBuffers()).isEqualTo(bufferManagerV2.getMinBufferPoolSize());
    bufferManagerV2.queueReadAhead(inputStream, 0, ONE_KB,
        inputStream.getTracingContext());
    assertThat(bufferManagerV2.getNumBuffers()).isEqualTo(bufferManagerV2.getMinBufferPoolSize());
  }

  @Test
  public void testMemoryUpscaleIfMemoryBelowThreshold() throws Exception {
    TestAbfsInputStream testAbfsInputStream = new TestAbfsInputStream();
    AbfsClient client = testAbfsInputStream.getMockAbfsClient();
    AbfsInputStream inputStream = testAbfsInputStream.getAbfsInputStream(client, "testFailedReadAhead.txt");
    Configuration configuration = getReadAheadV2Configuration();
    configuration.set(FS_AZURE_READAHEAD_V2_MEMORY_USAGE_THRESHOLD_PERCENT, "100");
    AbfsConfiguration abfsConfig = new AbfsConfiguration(configuration,
        getAccountName());
    ReadBufferManagerV2.setReadBufferManagerConfigs(abfsConfig.getReadAheadBlockSize(), abfsConfig);
    ReadBufferManagerV2.getBufferManager(client.getAbfsCounters()).testResetReadBufferManager();
    ReadBufferManagerV2.setReadBufferManagerConfigs(abfsConfig.getReadAheadBlockSize(), abfsConfig);
    ReadBufferManagerV2 bufferManagerV2 = ReadBufferManagerV2.getBufferManager(client.getAbfsCounters());
    // Add a failed buffer to completed queue and set to no free buffers to read ahead.
    ReadBuffer buff = new ReadBuffer();
    buff.setStatus(ReadBufferStatus.READ_FAILED);
    buff.setStream(inputStream);
    bufferManagerV2.testMimicFullUseAndAddFailedBuffer(buff);
    assertThat(bufferManagerV2.getNumBuffers()).isEqualTo(bufferManagerV2.getMinBufferPoolSize());
    bufferManagerV2.queueReadAhead(inputStream, 0, ONE_KB,
        inputStream.getTracingContext());
    assertThat(bufferManagerV2.getNumBuffers()).isGreaterThan(bufferManagerV2.getMinBufferPoolSize());
  }

  @Test
  public void testMemoryDownscaleIfMemoryAboveThreshold() throws Exception {
    Configuration configuration = getReadAheadV2Configuration();
    configuration.set(FS_AZURE_READAHEAD_V2_MEMORY_USAGE_THRESHOLD_PERCENT, "2");
    AbfsConfiguration abfsConfig = new AbfsConfiguration(configuration,
        getAccountName());
    AbfsClient abfsClient = getFileSystem().getAbfsStore().getClient();
    ReadBufferManagerV2.setReadBufferManagerConfigs(abfsConfig.getReadAheadBlockSize(), abfsConfig);
    ReadBufferManagerV2.getBufferManager(abfsClient.getAbfsCounters()).testResetReadBufferManager();
    ReadBufferManagerV2.setReadBufferManagerConfigs(abfsConfig.getReadAheadBlockSize(), abfsConfig);
    ReadBufferManagerV2 bufferManagerV2 = ReadBufferManagerV2.getBufferManager(abfsClient.getAbfsCounters());
    int initialBuffers = bufferManagerV2.getMinBufferPoolSize();
    assertThat(bufferManagerV2.getNumBuffers()).isEqualTo(initialBuffers);
    bufferManagerV2.setMinBufferPoolSize(initialBuffers - 5); // allow downscale
    running = true;
    Thread t = new Thread(() -> {
      while (running) {
        long maxMemory = Runtime.getRuntime().maxMemory();
        long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        double usage = (double) usedMemory / maxMemory;

        if (usage < HIGH_MEMORY_USAGE_THRESHOLD_PERCENT) {
          // Allocate more memory
          allocations.add(new byte[10 * 1024 * 1024]); // 10MB
        }
      }
    }, "MemoryLoadThread");
    t.setDaemon(true);
    t.start();
    Thread.sleep(2L * bufferManagerV2.getMemoryMonitoringIntervalInMilliSec());
    assertThat(bufferManagerV2.getNumBuffers()).isLessThan(initialBuffers);
    running = false;
    t.join();
  }

  @Test
  public void testReadMetricUpdation() throws Exception {
    Configuration configuration = getReadAheadV2Configuration();
    configuration.set(FS_AZURE_READAHEAD_V2_MEMORY_USAGE_THRESHOLD_PERCENT, "2");
    FileSystem fileSystem = FileSystem.newInstance(configuration);
    try (AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem) {
      AbfsClient abfsClient = abfs.getAbfsStore().getClient();
      AbfsConfiguration abfsConfig = new AbfsConfiguration(configuration,
          getAccountName());
      ReadBufferManagerV2.setReadBufferManagerConfigs(
          abfsConfig.getReadAheadBlockSize(), abfsConfig);
      ReadBufferManagerV2.getBufferManager(abfsClient.getAbfsCounters()).testResetReadBufferManager();
      ReadBufferManagerV2.setReadBufferManagerConfigs(
          abfsConfig.getReadAheadBlockSize(), abfsConfig);
      ReadBufferManagerV2 bufferManagerV2
          = ReadBufferManagerV2.getBufferManager(abfsClient.getAbfsCounters());

      // --- Capture initial metrics and stats ---
      AbfsReadResourceUtilizationMetrics metrics =
          abfsClient.getAbfsCounters().getAbfsReadResourceUtilizationMetrics();

      ReadBufferManagerV2.ReadThreadPoolStats statsBefore =
          bufferManagerV2.getCurrentStats(ResourceUtilizationUtils.getJvmCpuLoad());
      int initialBuffers = bufferManagerV2.getMinBufferPoolSize();
      assertThat(bufferManagerV2.getNumBuffers()).isEqualTo(initialBuffers);
      bufferManagerV2.setMinBufferPoolSize(initialBuffers - 5); // allow downscale
      running = true;
      Thread t = new Thread(() -> {
        while (running) {
          long maxMemory = Runtime.getRuntime().maxMemory();
          long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
          double usage = (double) usedMemory / maxMemory;

          if (usage < HIGH_MEMORY_USAGE_THRESHOLD_PERCENT) {
            // Allocate more memory
            allocations.add(new byte[10 * 1024 * 1024]); // 10MB
          }
        }
      }, "MemoryLoadThread");
      t.setDaemon(true);
      t.start();
      Thread.sleep(2L * bufferManagerV2.getMemoryMonitoringIntervalInMilliSec());
      assertThat(bufferManagerV2.getNumBuffers()).isLessThan(initialBuffers);
      running = false;
      t.join();

      ReadBufferManagerV2.ReadThreadPoolStats statsAfter
          = bufferManagerV2.getCurrentStats(ResourceUtilizationUtils.getJvmCpuLoad());

      // --- Validate that metrics and stats changed ---
      Assertions.assertThat(statsAfter)
          .as("Thread pool stats should update after CPU load")
          .isNotEqualTo(statsBefore);

      boolean updatedMetrics = metrics.isUpdated();

      Assertions.assertThat(updatedMetrics)
          .as("Metrics should be updated at least once after CPU load")
          .isTrue();

      String metricsOutput = metrics.toString();

      // Assertions for metrics correctness
      Assertions.assertThat(metricsOutput)
          .as("Metrics output should not be empty")
          .isNotEmpty();

      Assertions.assertThat(metricsOutput)
          .as("Metrics must include CPU utilization data")
          .contains("SC=");

      Assertions.assertThat(metricsOutput)
          .as("Metrics must include memory utilization data")
          .contains("AM=");

      Assertions.assertThat(metricsOutput)
          .as("Metrics must include current thread pool size")
          .contains("CP=");
    }
  }


  private Configuration getReadAheadV2Configuration() {
    Configuration conf = new Configuration(getRawConfiguration());
    conf.setBoolean(FS_AZURE_ENABLE_READAHEAD_V2, true);
    conf.setBoolean(FS_AZURE_ENABLE_READAHEAD_V2_DYNAMIC_SCALING, true);
    conf.setInt(FS_AZURE_READAHEAD_V2_MIN_THREAD_POOL_SIZE, 2);
    conf.setInt(FS_AZURE_READAHEAD_V2_MAX_THREAD_POOL_SIZE, 4);
    conf.setInt(FS_AZURE_READAHEAD_V2_CPU_USAGE_THRESHOLD_PERCENT, HUNDRED);
    conf.setInt(FS_AZURE_READAHEAD_V2_CPU_MONITORING_INTERVAL_MILLIS, 1_000);
    conf.setInt(FS_AZURE_READAHEAD_V2_MEMORY_MONITORING_INTERVAL_MILLIS, 1_000);
    conf.setInt(FS_AZURE_READAHEAD_V2_CACHED_BUFFER_TTL_MILLIS, 1_000);
    return conf;
  }
}

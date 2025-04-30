/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WriteThreadPoolSizeManagerTest extends AbstractAbfsIntegrationTest {

  private AbfsConfiguration mockConfig;

  public WriteThreadPoolSizeManagerTest() throws Exception {
    super.setup();
  }

  /**
   * Common setup to prepare a mock configuration for each test.
   */
  @Before
  public void setUp() {
    mockConfig = mock(AbfsConfiguration.class);
    when(mockConfig.getWriteMaxConcurrentRequestCount()).thenReturn(4);
    when(mockConfig.getWriteCorePoolSize()).thenReturn(1);
    when(mockConfig.getWriteThreadPoolKeepAliveTime()).thenReturn(10);
  }

  /**
   * Ensures that {@link WriteThreadPoolSizeManager#getInstance(String, AbfsConfiguration)} returns a singleton per key.
   */
  @Test
  public void testGetInstanceReturnsSingleton() {
    WriteThreadPoolSizeManager instance1
        = WriteThreadPoolSizeManager.getInstance("testfs", mockConfig);
    WriteThreadPoolSizeManager instance2
        = WriteThreadPoolSizeManager.getInstance("testfs", mockConfig);
    Assertions.assertThat(instance1)
        .as("Expected the same singleton instance for the same key")
        .isSameAs(instance2);
  }

  /**
   /**
   * Tests that high CPU usage results in thread pool downscaling.
   */
  @Test
  public void testAdjustThreadPoolSizeBasedOnHighCPU()
      throws InterruptedException, IOException {
    // Get the executor service (ThreadPoolExecutor)
    WriteThreadPoolSizeManager instance
        = WriteThreadPoolSizeManager.getInstance("testfsHigh",
        getAbfsStore(getFileSystem()).getAbfsConfiguration());
    ExecutorService executor = instance.getExecutorService();
    ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executor;

    // Simulate high CPU usage (e.g., 95% CPU utilization)
    int initialMaxSize = threadPoolExecutor.getMaximumPoolSize();
    instance.adjustThreadPoolSizeBasedOnCPU(0.95);  // High CPU

    // Get the new maximum pool size after adjustment
    int newMaxSize = threadPoolExecutor.getMaximumPoolSize();

    // Assert that the pool size has decreased or is equal to initial PoolSize based on high CPU usage
    Assertions.assertThat(newMaxSize)
        .as("Expected pool size to decrease under high CPU usage")
        .isLessThanOrEqualTo(initialMaxSize);
  }

  /**
   * Tests that low CPU usage results in thread pool upscaling or remains the same.
   */
  @Test
  public void testAdjustThreadPoolSizeBasedOnLowCPU()
      throws InterruptedException, IOException {
    WriteThreadPoolSizeManager instance
        = WriteThreadPoolSizeManager.getInstance("testfsLow",
        getAbfsStore(getFileSystem()).getAbfsConfiguration());
    ExecutorService executor = instance.getExecutorService();
    int initialSize = ((ThreadPoolExecutor) executor).getMaximumPoolSize();

    instance.adjustThreadPoolSizeBasedOnCPU(0.05); // Low CPU

    int newSize = ((ThreadPoolExecutor) executor).getMaximumPoolSize();
    Assertions.assertThat(newSize)
        .as("Expected pool size to increase or stay the same under low CPU usage")
        .isGreaterThanOrEqualTo(initialSize);
  }


  /**
   * Confirms that the thread pool executor is initialized and not shut down.
   */
  @Test
  public void testExecutorServiceIsNotNull() {
    WriteThreadPoolSizeManager instance
        = WriteThreadPoolSizeManager.getInstance("testfsExec", mockConfig);
    ExecutorService executor = instance.getExecutorService();
    Assertions.assertThat(executor).as("Executor service should be initialized")
        .isNotNull();
    Assertions.assertThat(executor.isShutdown())
        .as("Executor service should not be shut down")
        .isFalse();
  }


  /**
   * Ensures that calling {@link WriteThreadPoolSizeManager#close()} cleans up resources.
   */
  @Test
  public void testCloseCleansUp() throws Exception {
    WriteThreadPoolSizeManager instance
        = WriteThreadPoolSizeManager.getInstance("testfsClose", mockConfig);
    ExecutorService executor = instance.getExecutorService();

    instance.close();

    Assertions.assertThat(executor.isShutdown() || executor.isTerminated())
        .as("Executor service should be shut down or terminated after close()")
        .isTrue();
  }

  /**
   * Test that the CPU monitoring task is scheduled properly when startCPUMonitoring() is called.
   * <p>
   * This test checks the following:
   * 1. That the CPU monitoring task gets scheduled by verifying that the CPU monitor executor is not null.
   * 2. Ensures that the thread pool executor has at least one thread running, confirming that the task is being executed.
   * </p>
   * @throws InterruptedException if the test is interrupted during the sleep time
   */
  @Test
  public void testStartCPUMonitoringSchedulesTask()
      throws InterruptedException {
    // Create a new instance of WriteThreadPoolSizeManager using a mock configuration
    WriteThreadPoolSizeManager instance
        = WriteThreadPoolSizeManager.getInstance("testScheduler", mockConfig);

    // Call startCPUMonitoring to schedule the monitoring task
    instance.startCPUMonitoring();

    // Wait for a short period to allow the task to run and be scheduled
    Thread.sleep(200);

    // Retrieve the CPU monitor executor (ScheduledThreadPoolExecutor) from the instance
    ScheduledThreadPoolExecutor monitor
        = (ScheduledThreadPoolExecutor) instance.getCpuMonitorExecutor();

    // Assert that the monitor executor is not null, ensuring that it was properly initialized
    Assertions.assertThat(monitor)
        .as("CPU Monitor Executor should not be null")
        .isNotNull();

    // Assert that the thread pool size is greater than 0, confirming that the task has been scheduled and threads are active
    Assertions.assertThat(monitor.getPoolSize())
        .as("Thread pool size should be greater than 0, indicating that the task is running")
        .isGreaterThan(0);
  }
}


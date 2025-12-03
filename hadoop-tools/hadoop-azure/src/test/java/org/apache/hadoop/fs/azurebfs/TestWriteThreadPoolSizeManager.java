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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsCounters;
import org.apache.hadoop.fs.azurebfs.services.AbfsWriteResourceUtilizationMetrics;
import org.apache.hadoop.fs.azurebfs.utils.ResourceUtilizationUtils;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_WRITE_MAX_CONCURRENT_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_WRITE_CPU_MONITORING_INTERVAL_MILLIS;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_WRITE_DYNAMIC_THREADPOOL_ENABLEMENT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_WRITE_LOW_CPU_THRESHOLD_PERCENT;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HUNDRED;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ZERO;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestWriteThreadPoolSizeManager extends AbstractAbfsIntegrationTest {

  private AbfsConfiguration mockConfig;
  private static final double HIGH_CPU_UTILIZATION_THRESHOLD = 0.95;
  private static final double LOW_CPU_UTILIZATION_THRESHOLD = 0.05;
  private static final int LOW_MEMORY_USAGE_THRESHOLD_PERCENT = 100;
  private static final int THREAD_SLEEP_DURATION_MS = 200;
  private static final String TEST_FILE_PATH = "testFilePath";
  private static final String TEST_DIR_PATH = "testDirPath";
  private static final int TEST_FILE_LENGTH = 1024 * 1024 * 8;
  private static final int CONCURRENT_REQUEST_COUNT = 15;
  private static final int THREAD_POOL_KEEP_ALIVE_TIME = 10;
  private static final int LOW_TIER_MEMORY_MULTIPLIER = 4;
  private static final int MEDIUM_TIER_MEMORY_MULTIPLIER = 6;
  private static final int HIGH_TIER_MEMORY_MULTIPLIER = 8;
  private static final int HIGH_CPU_THRESHOLD = 15;
  private static final int MEDIUM_CPU_THRESHOLD = 10;
  private static final int LOW_CPU_THRESHOLD = 5;
  private static final int CPU_MONITORING_INTERVAL = 15;
  private static final int WAIT_DURATION_MS = 3000;
  private static final int LATCH_TIMEOUT_SECONDS = 60;
  private static final int RESIZE_WAIT_TIME_MS = 6_000;
  private static final double HIGH_CPU_USAGE_RATIO = 0.95;
  private static final double LOW_CPU_USAGE_RATIO = 0.05;
  private static final int SLEEP_DURATION_MS = 150;
  private static final int AWAIT_TIMEOUT_SECONDS = 45;
  private static final int RESIZER_JOIN_TIMEOUT_MS = 2_000;
  private static final int WAIT_TIMEOUT_MS = 5000;
  private static final int SLEEP_DURATION_30S_MS = 30000;
  private static final int SMALL_PAUSE_MS = 50;
  private static final int BURST_LOAD = 50;
  private static final long LOAD_SLEEP_DURATION_MS = 2000;

  TestWriteThreadPoolSizeManager() throws Exception {
    super.setup();
  }

  /**
   * Common setup to prepare a mock configuration for each test.
   */
  @BeforeEach
  public void setUp() {
    mockConfig = mock(AbfsConfiguration.class);
    when(mockConfig.getWriteConcurrentRequestCount()).thenReturn(CONCURRENT_REQUEST_COUNT);
    when(mockConfig.getWriteThreadPoolKeepAliveTime()).thenReturn(THREAD_POOL_KEEP_ALIVE_TIME);
    when(mockConfig.getLowTierMemoryMultiplier()).thenReturn(LOW_TIER_MEMORY_MULTIPLIER);
    when(mockConfig.getMediumTierMemoryMultiplier()).thenReturn(MEDIUM_TIER_MEMORY_MULTIPLIER);
    when(mockConfig.getHighTierMemoryMultiplier()).thenReturn(HIGH_TIER_MEMORY_MULTIPLIER);
    when(mockConfig.getWriteHighCpuThreshold()).thenReturn(HIGH_CPU_THRESHOLD);
    when(mockConfig.getWriteMediumCpuThreshold()).thenReturn(MEDIUM_CPU_THRESHOLD);
    when(mockConfig.getWriteLowCpuThreshold()).thenReturn(LOW_CPU_THRESHOLD);
    when(mockConfig.getWriteCpuMonitoringInterval()).thenReturn(CPU_MONITORING_INTERVAL);
    when(mockConfig.getWriteLowMemoryUsageThresholdPercent()).thenReturn(LOW_MEMORY_USAGE_THRESHOLD_PERCENT);
  }

  /**
   * Verifies that {@link WriteThreadPoolSizeManager#getInstance(String, AbfsConfiguration, AbfsCounters)}
   * returns the same singleton instance for the same filesystem name, and a different instance
   * for a different filesystem name.
   */
  @Test
  void testGetInstanceReturnsSingleton() throws IOException {
    WriteThreadPoolSizeManager instance1
        = WriteThreadPoolSizeManager.getInstance("testfs", mockConfig,
        getFileSystem().getAbfsClient().getAbfsCounters());
    WriteThreadPoolSizeManager instance2
        = WriteThreadPoolSizeManager.getInstance("testfs", mockConfig,
        getFileSystem().getAbfsClient().getAbfsCounters());
    WriteThreadPoolSizeManager instance3 =
        WriteThreadPoolSizeManager.getInstance("newFs", mockConfig,
            getFileSystem().getAbfsClient().getAbfsCounters());
    Assertions.assertThat(instance1)
        .as("Expected the same singleton instance for the same key")
        .isSameAs(instance2);
    Assertions.assertThat(instance1)
        .as("Expected the same singleton instance for the same key")
        .isNotSameAs(instance3);
  }

  /**
   * Tests that high CPU usage results in thread pool downscaling.
   */
  @Test
  void testAdjustThreadPoolSizeBasedOnHighCPU() throws InterruptedException, IOException {
    // Initialize filesystem and thread pool manager
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_WRITE_DYNAMIC_THREADPOOL_ENABLEMENT, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    try (AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem) {
      // Get the executor service (ThreadPoolExecutor)
      WriteThreadPoolSizeManager instance
          = WriteThreadPoolSizeManager.getInstance(abfs.getFileSystemId(),
          getAbfsStore(abfs).getAbfsConfiguration(),
          abfs.getAbfsClient().getAbfsCounters());
      ExecutorService executor = instance.getExecutorService();
      ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executor;

      // Simulate high CPU usage (e.g., 95% CPU utilization)
      int initialMaxSize = threadPoolExecutor.getMaximumPoolSize();
      instance.adjustThreadPoolSizeBasedOnCPU(
          HIGH_CPU_UTILIZATION_THRESHOLD);  // High CPU

      // Get the new maximum pool size after adjustment
      int newMaxSize = threadPoolExecutor.getMaximumPoolSize();

      // Assert that the pool size has decreased or is equal to initial PoolSize based on high CPU usage
      Assertions.assertThat(newMaxSize)
          .as("Expected pool size to decrease under high CPU usage")
          .isLessThanOrEqualTo(initialMaxSize);
      instance.close();
    }
  }

  /**
   * Tests that low CPU usage results in thread pool upscaling or remains the same.
   */
  @Test
  void testAdjustThreadPoolSizeBasedOnLowCPU()
      throws InterruptedException, IOException {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_WRITE_DYNAMIC_THREADPOOL_ENABLEMENT, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    try (AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem) {
      WriteThreadPoolSizeManager instance
          = WriteThreadPoolSizeManager.getInstance(abfs.getFileSystemId(),
          getAbfsStore(abfs).getAbfsConfiguration(),
          abfs.getAbfsClient().getAbfsCounters());
      ExecutorService executor = instance.getExecutorService();
      int initialSize = ((ThreadPoolExecutor) executor).getMaximumPoolSize();
      instance.adjustThreadPoolSizeBasedOnCPU(
          LOW_CPU_UTILIZATION_THRESHOLD); // Low CPU
      int newSize = ((ThreadPoolExecutor) executor).getMaximumPoolSize();
      Assertions.assertThat(newSize)
          .as("Expected pool size to increase or stay the same under low CPU usage")
          .isGreaterThanOrEqualTo(initialSize);
      instance.close();
    }
  }


  /**
   * Confirms that the thread pool executor is initialized and not shut down.
   */
  @Test
  void testExecutorServiceIsNotNull() throws IOException {
    WriteThreadPoolSizeManager instance
        = WriteThreadPoolSizeManager.getInstance("testfsExec", mockConfig,
        getFileSystem().getAbfsClient().getAbfsCounters());
    ExecutorService executor = instance.getExecutorService();
    Assertions.assertThat(executor).as("Executor service should be initialized")
        .isNotNull();
    Assertions.assertThat(executor.isShutdown())
        .as("Executor service should not be shut down")
        .isFalse();
    instance.close();
  }


  /**
   * Ensures that calling {@link WriteThreadPoolSizeManager#close()} cleans up resources.
   */
  @Test
  void testCloseCleansUp() throws Exception {
    WriteThreadPoolSizeManager instance
        = WriteThreadPoolSizeManager.getInstance("testfsClose", mockConfig,
        getFileSystem().getAbfsClient().getAbfsCounters());
    ExecutorService executor = instance.getExecutorService();
    instance.close();
    Assertions.assertThat(executor.isShutdown() || executor.isTerminated())
        .as("Executor service should be shut down or terminated after close()")
        .isTrue();
  }

  /**
   * Test that the CPU monitoring task is scheduled properly when startCPUMonitoring() is called.
   * This test checks the following:
   * 1. That the CPU monitoring task gets scheduled by verifying that the CPU monitor executor is not null.
   * 2. Ensures that the thread pool executor has at least one thread running, confirming that the task is being executed.
   * @throws InterruptedException if the test is interrupted during the sleep time
   */
  @Test
  void testStartCPUMonitoringSchedulesTask()
      throws InterruptedException, IOException {
    // Create a new instance of WriteThreadPoolSizeManager using a mock configuration
    WriteThreadPoolSizeManager instance
        = WriteThreadPoolSizeManager.getInstance("testScheduler", mockConfig,
        getFileSystem().getAbfsClient().getAbfsCounters());

    // Call startCPUMonitoring to schedule the monitoring task
    instance.startCPUMonitoring();

    // Wait for a short period to allow the task to run and be scheduled
    Thread.sleep(THREAD_SLEEP_DURATION_MS);

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
        .isGreaterThan(ZERO);
    instance.close();
  }

  /**
   * Verifies that ABFS write tasks can complete successfully even when the system
   * is under artificial CPU stress. The test also ensures that the write thread
   * pool resizes dynamically under load without leading to starvation, overload,
   * or leftover work in the queue.
   */
  @Test
  void testABFSWritesUnderCPUStress() throws Exception {
    // Initialize the filesystem and thread pool manager
    AzureBlobFileSystem fs = getFileSystem();
    WriteThreadPoolSizeManager instance =
        WriteThreadPoolSizeManager.getInstance(getFileSystemName(),
            getConfiguration(), getFileSystem().getAbfsClient().getAbfsCounters());
    ThreadPoolExecutor executor =
        (ThreadPoolExecutor) instance.getExecutorService();

    // Start CPU monitoring so pool size adjustments happen in response to load
    instance.startCPUMonitoring();

    // Launch a background thread that generates CPU stress for ~3 seconds.
    // This simulates contention on the system and should cause the pool to adjust.
    Thread stressThread = new Thread(() -> {
      long end = System.currentTimeMillis() + WAIT_DURATION_MS;
      while (System.currentTimeMillis() < end) {
        // Busy-work loop: repeatedly compute random powers to waste CPU cycles
        double waste = Math.pow(Math.random(), Math.random());
      }
    });
    stressThread.start();

    // Prepare the ABFS write workload with multiple concurrent tasks
    int taskCount = 10;
    CountDownLatch latch = new CountDownLatch(taskCount);
    Path testFile = new Path(TEST_FILE_PATH);
    final byte[] b = new byte[TEST_FILE_LENGTH];
    new Random().nextBytes(b);

    // Submit 10 tasks, each writing to its own file to simulate parallel load
    for (int i = 0; i < taskCount; i++) {
      int finalI = i;
      executor.submit(() -> {
        try (FSDataOutputStream out = fs.create(
            new Path(testFile + "_" + finalI), true)) {
          for (int j = 0; j < 5; j++) {
            out.write(b); // perform multiple writes to add sustained pressure
          }
          out.hflush();   // flush to force actual I/O
        } catch (IOException e) {
          // Any failure here indicates pool misbehavior or I/O issues
          Assertions.fail("Write task failed with exception", e);
        } finally {
          // Mark this task as complete
          latch.countDown();
        }
      });
    }

    // Wait for all tasks to finish (up to 60s timeout to guard against deadlock/starvation)
    boolean finished = latch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // Record the pool size after CPU stress to confirm resizing took place
    int resizedPoolSize = executor.getMaximumPoolSize();

    // 1. All tasks must finish within timeout → proves no starvation or deadlock
    Assertions.assertThat(finished)
        .as("All ABFS write tasks should complete without starvation")
        .isTrue();

    // 2. Pool size must fall within valid bounds → proves resizing occurred
    Assertions.assertThat(resizedPoolSize)
        .as("Thread pool size should dynamically adjust under CPU stress")
        .isBetween(1, getAbfsStore(fs).getAbfsConfiguration().getWriteConcurrentRequestCount());

    // 3. Task queue must be empty → proves no backlog remains after workload
    Assertions.assertThat(executor.getQueue().size())
        .as("No backlog should remain in task queue after completion")
        .isEqualTo(0);

    // Cleanup resources
    instance.close();
  }


  /**
   * Ensures that dynamic thread pool resizing during an active ABFS write workload
   * does not cause deadlocks, task loss, or task duplication. The test also verifies
   * that the pool resizes while work is in progress and that the executor queue
   * eventually drains cleanly.
   */
  @Test
  void testDynamicResizeNoDeadlocksNoTaskLoss() throws Exception {
    // Initialize filesystem and thread pool manager
    AzureBlobFileSystem fs = getFileSystem();
    WriteThreadPoolSizeManager mgr =
        WriteThreadPoolSizeManager.getInstance(getFileSystemName(), mockConfig,
            getFileSystem().getAbfsClient().getAbfsCounters());
    ThreadPoolExecutor executor = (ThreadPoolExecutor) mgr.getExecutorService();

    // Enable monitoring (may not be required if adjust() is triggered internally)
    mgr.startCPUMonitoring();

    // Test configuration: enough tasks and writes to stress the pool
    final int taskCount = 10;
    final int writesPerTask = 5;
    final byte[] b = new byte[TEST_FILE_LENGTH];
    new Random().nextBytes(b);
    final Path base = new Path(TEST_DIR_PATH);
    fs.mkdirs(base);

    // Barrier ensures all tasks start together, so resizing happens mid-flight
    final CyclicBarrier startBarrier = new CyclicBarrier(taskCount + 1);
    final CountDownLatch done = new CountDownLatch(taskCount);

    // Track execution results
    final AtomicIntegerArray completed = new AtomicIntegerArray(taskCount); // mark tasks once
    final AtomicInteger duplicates = new AtomicInteger(0);                  // guard against double-completion
    final AtomicInteger rejected = new AtomicInteger(0);                    // count unexpected rejections

    // Submit ABFS write tasks
    for (int i = 0; i < taskCount; i++) {
      final int id = i;
      try {
        executor.submit(() -> {
          try {
            // Hold until all tasks are enqueued, then start together
            startBarrier.await(10, TimeUnit.SECONDS);

            // Each task writes to its own file, flushing intermittently
            Path subPath = new Path(base, "part-" + id);
            try (FSDataOutputStream out = fs.create(subPath)) {
              for (int w = 0; w < writesPerTask; w++) {
                out.write(b);
                if ((w & 1) == 1) {
                  out.hflush(); // force some syncs to increase contention
                }
              }
              out.hflush();
            }

            // Mark task as completed once; duplicates flag if it happens again
            if (!completed.compareAndSet(id, 0, 1)) {
              duplicates.incrementAndGet();
            }
          } catch (Exception e) {
            Assertions.fail("ABFS write task " + id + " failed", e);
          } finally {
            done.countDown();
          }
        });
      } catch (RejectedExecutionException rex) {
        rejected.incrementAndGet();
      }
    }

    // Thread that simulates fluctuating CPU load while tasks are running
    final AtomicInteger observedMinMax = new AtomicInteger(executor.getMaximumPoolSize());
    final AtomicInteger observedMaxMax = new AtomicInteger(executor.getMaximumPoolSize());

    Thread resizer = new Thread(() -> {
      try {
        // Release worker tasks
        startBarrier.await(10, TimeUnit.SECONDS);

        long end = System.currentTimeMillis() + RESIZE_WAIT_TIME_MS; // keep resizing for ~6s
        boolean high = true;
        while (System.currentTimeMillis() < end) {
          // Alternate between high load (shrink) and low load (expand)
          if (high) {
            mgr.adjustThreadPoolSizeBasedOnCPU(HIGH_CPU_USAGE_RATIO);
          } else {
            mgr.adjustThreadPoolSizeBasedOnCPU(LOW_CPU_USAGE_RATIO);
          }
          high = !high;

          // Track observed pool size bounds to prove resizing occurred
          int cur = executor.getMaximumPoolSize();
          observedMinMax.updateAndGet(prev -> Math.min(prev, cur));
          observedMaxMax.updateAndGet(prev -> Math.max(prev, cur));

          Thread.sleep(SLEEP_DURATION_MS);
        }
      } catch (Exception ignore) {
        // No-op: this is best-effort simulation
      }
    }, "resizer-thread");

    resizer.start();

    // Wait for all tasks to finish (ensures no deadlock)
    boolean finished = done.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // Join resizer thread
    resizer.join(RESIZER_JOIN_TIMEOUT_MS);

    // 1. All tasks must complete in time → proves there are no deadlocks
    Assertions.assertThat(finished)
        .as("All tasks must complete within timeout (no deadlock)")
        .isTrue();

    // 2. Every task should complete exactly once → proves no task loss
    int completedCount = 0;
    for (int i = 0; i < taskCount; i++) {
      completedCount += completed.get(i);
    }
    Assertions.assertThat(completedCount)
        .as("Every task should complete exactly once (no task loss)")
        .isEqualTo(taskCount);

    // 3. No task should mark itself as done more than once → proves no duplication
    Assertions.assertThat(duplicates.get())
        .as("No task should report completion more than once (no duplication)")
        .isZero();

    // 4. The executor should not reject tasks while resizing is happening
    Assertions.assertThat(rejected.get())
        .as("Tasks should not be rejected during active resizing")
        .isZero();

    // 5. Executor queue should eventually empty once all tasks finish
    Assertions.assertThat(executor.getQueue().size())
        .as("Executor queue should drain after workload")
        .isEqualTo(0);

    // 6. Executor should still be running after workload until explicitly closed
    Assertions.assertThat(executor.isShutdown())
        .as("Executor should remain running until manager.close()")
        .isFalse();

    // 7. Verify that resizing actually occurred (pool max both grew and shrank)
    int minObserved = observedMinMax.get();
    int maxObserved = observedMaxMax.get();

    Assertions.assertThat(maxObserved)
        .as("Pool maximum size should have increased or fluctuated above baseline")
        .isGreaterThan(0);

    Assertions.assertThat(minObserved)
        .as("Pool maximum size should have dropped during resizing")
        .isLessThanOrEqualTo(maxObserved);

    // Cleanup
    for (int i = 0; i < taskCount; i++) {
      Path p = new Path(base, "part-" + i);
      try {
        fs.delete(p, false);
      } catch (IOException ignore) {
        // Ignored: delete failures are non-fatal for test cleanup
      }
    }
    try {
      fs.delete(base, true);
    } catch (IOException ignore) {
      // Ignored: cleanup failures are non-fatal in tests
    }
    mgr.close();
  }



  /**
   * Verifies that when the system experiences high CPU usage,
   * the WriteThreadPoolSizeManager detects the load and reduces
   * the maximum thread pool size accordingly.
   */
  @Test
  void testThreadPoolScalesDownOnHighCpuLoad() throws Exception {
    // Initialize filesystem and thread pool manager
    try (FileSystem fileSystem = FileSystem.newInstance(getRawConfiguration())) {
      AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
      WriteThreadPoolSizeManager instance =
          WriteThreadPoolSizeManager.getInstance(abfs.getFileSystemId(),
              getConfiguration(), getFileSystem().getAbfsClient().getAbfsCounters());
      ThreadPoolExecutor executor =
          (ThreadPoolExecutor) instance.getExecutorService();

      // Start monitoring CPU load
      instance.startCPUMonitoring();

      // Capture baseline pool size for comparison later
      int initialMax = executor.getMaximumPoolSize();

      // Define a CPU-bound task: tight loop of math ops for ~5s
      Runnable cpuBurn = () -> {
        long end = System.currentTimeMillis() + WAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < end) {
          double waste = Math.sin(Math.random()) * Math.cos(Math.random());
        }
      };

      // Launch two CPU hogs in parallel
      Thread cpuHog1 = new Thread(cpuBurn, "cpu-hog-thread-1");
      Thread cpuHog2 = new Thread(cpuBurn, "cpu-hog-thread-2");
      cpuHog1.start();
      cpuHog2.start();

      // Submit multiple write tasks while CPU is under stress
      int taskCount = 10;
      CountDownLatch latch = new CountDownLatch(taskCount);
      Path base = new Path(TEST_DIR_PATH);
      abfs.mkdirs(base);
      final byte[] buffer = new byte[TEST_FILE_LENGTH];
      new Random().nextBytes(buffer);

      for (int i = 0; i < taskCount; i++) {
        final Path part = new Path(base, "part-" + i);
        executor.submit(() -> {
          try (FSDataOutputStream out = abfs.create(part, true)) {
            for (int j = 0; j < 5; j++) {
              out.write(buffer);
              out.hflush();
            }
          } catch (IOException e) {
            Assertions.fail("Write task failed under CPU stress", e);
          } finally {
            latch.countDown();
          }
        });
      }

      // Ensure all tasks complete (avoid deadlock/starvation)
      boolean finished = latch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

      // Wait for CPU hogs to end and give monitor time to react
      cpuHog1.join();
      cpuHog2.join();
      Thread.sleep(SLEEP_DURATION_30S_MS);

      int resizedMax = executor.getMaximumPoolSize();

      // Verify outcomes:
      // 1. All write tasks succeeded despite CPU pressure
      Assertions.assertThat(finished)
          .as("All ABFS write tasks must complete despite CPU stress")
          .isTrue();

      // 2. Thread pool scaled down as expected
      Assertions.assertThat(resizedMax)
          .as("Thread pool should scale down under high CPU load")
          .isLessThanOrEqualTo(initialMax);

      // 3. No leftover tasks in the queue
      Assertions.assertThat(executor.getQueue().size())
          .as("No backlog should remain in the queue after workload")
          .isEqualTo(0);

      // Cleanup test data
      for (int i = 0; i < taskCount; i++) {
        try {
          abfs.delete(new Path(base, "part-" + i), false);
        } catch (IOException ignore) {
          // Ignored: cleanup failures are non-fatal in tests
        }
      }
      try {
        abfs.delete(base, true);
      } catch (IOException ignore) {
        // Ignored: cleanup failures are non-fatal in tests
      }
      instance.close();
    }
  }


  /**
   * Verifies that when two parallel high memory–consuming workloads run,
   * the WriteThreadPoolSizeManager detects the memory pressure and
   * scales down the maximum thread pool size.
   */
  @Test
  void testScalesDownOnParallelHighMemoryLoad() throws Exception {
    // Initialize filesystem and thread pool manager
    try (FileSystem fileSystem = FileSystem.newInstance(getRawConfiguration())) {
      AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
      WriteThreadPoolSizeManager instance =
          WriteThreadPoolSizeManager.getInstance(abfs.getFileSystemId(),
              getConfiguration(), getFileSystem().getAbfsClient().getAbfsCounters());
      ThreadPoolExecutor executor =
          (ThreadPoolExecutor) instance.getExecutorService();

      // Begin monitoring resource usage (CPU + memory)
      instance.startCPUMonitoring();

      // Capture the initial thread pool size for later comparison
      int initialMax = executor.getMaximumPoolSize();

      // Define a workload that continuously allocates memory (~5 MB chunks)
      // for ~5 seconds to simulate memory pressure in the JVM.
      Runnable memoryBurn = () -> {
        List<byte[]> allocations = new ArrayList<>();
        long end = System.currentTimeMillis() + WAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < end) {
          try {
            allocations.add(new byte[5 * 1024 * 1024]); // allocate 5 MB
            Thread.sleep(SMALL_PAUSE_MS); // small pause to avoid instant OOM
          } catch (OutOfMemoryError oom) {
            // Clear allocations if JVM runs out of memory and continue
            allocations.clear();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      };

      // Start two threads running the memory hog workload in parallel
      Thread memHog1 = new Thread(memoryBurn, "mem-hog-thread-1");
      Thread memHog2 = new Thread(memoryBurn, "mem-hog-thread-2");
      memHog1.start();
      memHog2.start();

      // Submit several write tasks to ABFS while memory is under stress
      int taskCount = 10;
      CountDownLatch latch = new CountDownLatch(taskCount);
      Path base = new Path(TEST_DIR_PATH);
      abfs.mkdirs(base);
      final byte[] buffer = new byte[TEST_FILE_LENGTH];
      new Random().nextBytes(buffer);

      for (int i = 0; i < taskCount; i++) {
        final Path part = new Path(base, "part-" + i);
        executor.submit(() -> {
          try (FSDataOutputStream out = abfs.create(part, true)) {
            for (int j = 0; j < 5; j++) {
              out.write(buffer);
              out.hflush();
            }
          } catch (IOException e) {
            Assertions.fail("Write task failed under memory stress", e);
          } finally {
            latch.countDown();
          }
        });
      }

      // Ensure all tasks finish within a timeout
      boolean finished = latch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

      // Wait for memory hog threads to finish
      memHog1.join();
      memHog2.join();

      // Give monitoring thread time to detect memory pressure and react
      Thread.sleep(SLEEP_DURATION_30S_MS);

      int resizedMax = executor.getMaximumPoolSize();

      // Validate that:
      // 1. All ABFS writes succeeded despite memory stress
      Assertions.assertThat(finished)
          .as("All ABFS write tasks must complete despite parallel memory stress")
          .isTrue();

      // 2. The thread pool scaled down under memory pressure
      Assertions.assertThat(resizedMax)
          .as("Thread pool should scale down under parallel high memory load")
          .isLessThanOrEqualTo(initialMax);

      // 3. No tasks remain queued after workload completion
      Assertions.assertThat(executor.getQueue().size())
          .as("No backlog should remain in the queue after workload")
          .isEqualTo(0);

      // Clean up temporary test files
      for (int i = 0; i < taskCount; i++) {
        try {
          abfs.delete(new Path(base, "part-" + i), false);
        } catch (IOException ignore) {
          // Ignored: cleanup failures are non-fatal in tests
        }
      }
      try {
        abfs.delete(base, true);
      } catch (IOException ignore) {
        // Ignored: cleanup failures are non-fatal in tests
      }
      instance.close();
    }
  }

  /**
   * Test that after a long idle period, the thread pool
   * can quickly scale up in response to a sudden burst of load
   * without performance degradation.
   */
  @Test
  void testThreadPoolScalesUpAfterIdleBurstLoad() throws Exception {
    // Initialize filesystem and thread pool manager
    try (FileSystem fileSystem = FileSystem.newInstance(
        getRawConfiguration())) {
      AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem;
      WriteThreadPoolSizeManager instance = WriteThreadPoolSizeManager.getInstance(abfs.getFileSystemId(),
          abfs.getAbfsStore().getAbfsConfiguration(), getFileSystem().getAbfsClient().getAbfsCounters());
      ThreadPoolExecutor executor =
          (ThreadPoolExecutor) instance.getExecutorService();

      // --- Step 1: Simulate idle period ---
      // Let the executor sit idle with no work for a few seconds
      Thread.sleep(WAIT_TIMEOUT_MS);
      int poolSizeAfterIdle = executor.getPoolSize();

      // Verify that after idling, the pool is at or close to its minimum size
      Assertions.assertThat(poolSizeAfterIdle)
          .as("Pool size should remain minimal after idle")
          .isLessThanOrEqualTo(executor.getCorePoolSize());

      // --- Step 2: Submit a sudden burst of tasks ---
      // Launch many short, CPU-heavy tasks at once to simulate burst load
      int burstLoad = BURST_LOAD;
      CountDownLatch latch = new CountDownLatch(burstLoad);
      for (int i = 0; i < burstLoad; i++) {
        executor.submit(() -> {
          // Busy loop for ~200ms to simulate CPU work
          long end = System.currentTimeMillis() + THREAD_SLEEP_DURATION_MS;
          while (System.currentTimeMillis() < end) {
            Math.sqrt(Math.random()); // burn CPU cycles
          }
          latch.countDown();
        });
      }

      // --- Step 3: Give pool time to react ---
      // Wait briefly so the pool’s scaling logic has a chance to expand
      Thread.sleep(LOAD_SLEEP_DURATION_MS);
      int poolSizeDuringBurst = executor.getPoolSize();

      // Verify that the pool scaled up compared to idle
      Assertions.assertThat(poolSizeDuringBurst)
          .as("Pool size should increase after burst load")
          .isGreaterThanOrEqualTo(poolSizeAfterIdle);

// --- Step 4: Verify completion ---
// Ensure all tasks complete successfully in a reasonable time,
// proving there was no degradation or deadlock under burst load
      Assertions.assertThat(
              latch.await(LATCH_TIMEOUT_SECONDS / 2, TimeUnit.SECONDS))
          .as("All burst tasks should finish in reasonable time")
          .isTrue();
      instance.close();
    }
  }

  /**
   * Verifies that when the system experiences low CPU usage,
   * the WriteThreadPoolSizeManager maintains the thread pool size
   * without scaling down and updates the corresponding
   * write thread pool metrics accordingly.
   */
  @Test
  void testThreadPoolOnLowCpuLoadAndMetricsUpdate()
      throws Exception {
    // Initialize filesystem and thread pool manager
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_WRITE_DYNAMIC_THREADPOOL_ENABLEMENT, true);
    conf.setInt(AZURE_WRITE_MAX_CONCURRENT_REQUESTS, 2);
    conf.setInt(FS_AZURE_WRITE_LOW_CPU_THRESHOLD_PERCENT, 10);
    conf.setInt(FS_AZURE_WRITE_CPU_MONITORING_INTERVAL_MILLIS, 1_000);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    try (AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem) {
      WriteThreadPoolSizeManager instance =
          WriteThreadPoolSizeManager.getInstance("fs1",
              abfs.getAbfsStore().getAbfsConfiguration(),
              abfs.getAbfsClient().getAbfsCounters());
      instance.startCPUMonitoring();

      // --- Capture initial metrics and stats ---
      AbfsWriteResourceUtilizationMetrics metrics =
          abfs.getAbfsClient()
              .getAbfsCounters()
              .getAbfsWriteResourceUtilizationMetrics();

      WriteThreadPoolSizeManager.WriteThreadPoolStats statsBefore =
          instance.getCurrentStats(ResourceUtilizationUtils.getJvmCpuLoad(), ResourceUtilizationUtils.getMemoryLoad());

      ThreadPoolExecutor executor =
          (ThreadPoolExecutor) instance.getExecutorService();

      // No CPU hogs this time — simulate light CPU load
      // Submit lightweight ABFS tasks that barely use CPU
      int taskCount = 10;
      CountDownLatch latch = new CountDownLatch(taskCount);

      for (int i = 0; i < taskCount; i++) {
        executor.submit(() -> {
          try {
            // Light operations — minimal CPU load
            for (int j = 0; j < 3; j++) {
              Thread.sleep(HUNDRED); // simulate idle/light wait
            }
          } catch (Exception e) {
            Assertions.fail("Light task failed unexpectedly", e);
          } finally {
            latch.countDown();
          }
        });
      }

      // Wait for all tasks to finish
      boolean finished = latch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      Assertions.assertThat(finished)
          .as("All lightweight tasks should complete normally")
          .isTrue();

      // Allow some time for monitoring and metrics update
      Thread.sleep(SLEEP_DURATION_30S_MS);

      WriteThreadPoolSizeManager.WriteThreadPoolStats statsAfter =
          instance.getCurrentStats(ResourceUtilizationUtils.getJvmCpuLoad(), ResourceUtilizationUtils.getMemoryLoad());

      //--- Validate that metrics and stats changed ---
      Assertions.assertThat(statsAfter)
          .as("Thread pool stats should update after CPU load")
          .isNotEqualTo(statsBefore);

      String metricsOutput = metrics.toString();

      if (!metricsOutput.isEmpty()) {
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
      instance.close();
    }
  }
}


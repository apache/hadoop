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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.services.AbfsCounters;
import org.apache.hadoop.fs.azurebfs.services.AbfsWriteResourceUtilizationMetrics;
import org.apache.hadoop.fs.azurebfs.services.ResourceUtilizationStats;
import org.apache.hadoop.fs.azurebfs.utils.ResourceUtilizationUtils;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.LOW_HEAP_SPACE_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.MEDIUM_HEAP_SPACE_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HIGH_CPU_LOW_MEMORY_REDUCTION_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HIGH_CPU_REDUCTION_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HUNDRED_D;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.LOW_CPU_HIGH_MEMORY_DECREASE_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.LOW_CPU_POOL_SIZE_INCREASE_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MEDIUM_CPU_LOW_MEMORY_REDUCTION_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MEDIUM_CPU_REDUCTION_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.SCALE_DIRECTION_DOWN;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.SCALE_DIRECTION_NO_DOWN_AT_MIN;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.SCALE_DIRECTION_NO_UP_AT_MAX;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.SCALE_DIRECTION_UP;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.THIRTY_SECONDS;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ZERO;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ZERO_D;

/**
 * Manages a thread pool for writing operations, adjusting the pool size based on CPU utilization.
 */
public final class WriteThreadPoolSizeManager implements Closeable {

  /* Maximum allowed size for the thread pool. */
  private final int maxThreadPoolSize;
  /* Executor for periodically monitoring CPU usage. */
  private final ScheduledExecutorService cpuMonitorExecutor;
  /* Thread pool whose size is dynamically managed. */
  private volatile ExecutorService boundedThreadPool;
  /* Lock to ensure thread-safe updates to the thread pool. */
  private final Lock lock = new ReentrantLock();
  /* New computed max size for the thread pool after adjustment. */
  private volatile int newMaxPoolSize;
  /* Logger instance for logging events from WriteThreadPoolSizeManager. */
  private static final Logger LOG = LoggerFactory.getLogger(
      WriteThreadPoolSizeManager.class);
  /* Map to maintain a WriteThreadPoolSizeManager instance per filesystem. */
  private static final ConcurrentHashMap<String, WriteThreadPoolSizeManager>
      POOL_SIZE_MANAGER_MAP = new ConcurrentHashMap<>();
  /* Name of the filesystem associated with this manager. */
  private final String filesystemName;
  /* Initial size for the thread pool when created. */
  private final int initialPoolSize;
  /* The configuration instance. */
  private final AbfsConfiguration abfsConfiguration;
  /* Metrics collector for monitoring the performance of the ABFS write thread pool.  */
  private final AbfsWriteResourceUtilizationMetrics writeThreadPoolMetrics;
  /* Flag indicating if CPU monitoring has started. */
  private volatile boolean isMonitoringStarted = false;
  /* Tracks the last scale direction applied, or empty if none. */
  private volatile String lastScaleDirection = EMPTY_STRING;
  /* Maximum CPU utilization observed during the monitoring interval. */
  private volatile double maxJvmCpuUtilization = 0.0;
  /** High memory usage threshold used to trigger thread pool downscaling. */
  private final double highMemoryThreshold;
  /** Low memory usage threshold used to allow thread pool upscaling. */
  private final double lowMemoryThreshold;

  /**
   * Private constructor to initialize the write thread pool and CPU monitor executor
   * based on system resources and ABFS configuration.
   *
   * @param filesystemName       Name of the ABFS filesystem.
   * @param abfsConfiguration    Configuration containing pool size parameters.
   * @param abfsCounters                  ABFS counters instance used for metrics.
   */
  private WriteThreadPoolSizeManager(String filesystemName,
      AbfsConfiguration abfsConfiguration, AbfsCounters abfsCounters) {
    /* Retrieves and assigns the write thread pool metrics from the ABFS client counters. */
    this.writeThreadPoolMetrics = abfsCounters.getAbfsWriteResourceUtilizationMetrics();
    this.filesystemName = filesystemName;
    this.abfsConfiguration = abfsConfiguration;
    int availableProcessors = Runtime.getRuntime().availableProcessors();
    /* Compute the max pool size */
    int computedMaxPoolSize = getComputedMaxPoolSize(availableProcessors, ResourceUtilizationUtils.getAvailableMaxHeapMemory());

    /* Get the initial pool size from config, fallback to at least 1 */
    this.initialPoolSize = Math.max(1,
        abfsConfiguration.getWriteConcurrentRequestCount());

    /* Set the upper bound for the thread pool size */
    this.maxThreadPoolSize = Math.max(computedMaxPoolSize, initialPoolSize);
    AtomicInteger threadCount = new AtomicInteger(1);
    this.boundedThreadPool = Executors.newFixedThreadPool(
        initialPoolSize,
        r -> {
          Thread t = new Thread(r);
          t.setName("abfs-boundedwrite-" + threadCount.getAndIncrement());
          return t;
        }
    );
    ThreadPoolExecutor executor = (ThreadPoolExecutor) this.boundedThreadPool;
    int keepAlive = Math.max(1, abfsConfiguration.getWriteThreadPoolKeepAliveTime());
    executor.setKeepAliveTime(keepAlive, TimeUnit.SECONDS);
    executor.allowCoreThreadTimeOut(true);
    /* Create a scheduled executor for CPU monitoring and pool adjustment */
    this.cpuMonitorExecutor = Executors.newScheduledThreadPool(1);
    highMemoryThreshold = abfsConfiguration.getWriteHighMemoryUsageThresholdPercent() / HUNDRED_D;
    lowMemoryThreshold = abfsConfiguration.getWriteLowMemoryUsageThresholdPercent() / HUNDRED_D;
  }

  /** Returns the internal {@link AbfsConfiguration}. */
  private AbfsConfiguration getAbfsConfiguration() {
    return abfsConfiguration;
  }

  /**
   * Computes the maximum thread pool size based on the available processors
   * and the initial available heap memory. The calculation uses a tiered
   * multiplier derived from the memory-to-core ratio — systems with higher
   * memory per core allow for a larger thread pool.
   *
   * @param availableProcessors the number of available CPU cores.
   * @param initialAvailableHeapMemory the initial available heap memory, in bytes or GB (depending on implementation).
   * @return the computed maximum thread pool size.
   */
  private int getComputedMaxPoolSize(final int availableProcessors, long initialAvailableHeapMemory) {
    int maxpoolSize = getMemoryTierMaxThreads(initialAvailableHeapMemory, availableProcessors);
    LOG.debug("Computed max thread pool size: {} | Available processors: {} | Heap memory (GB): {}",
        maxpoolSize, availableProcessors, initialAvailableHeapMemory);
    return maxpoolSize;
  }

  /**
   * Determines the maximum thread count based on available heap memory and CPU cores.
   * Calculates the thread count as {@code availableProcessors × multiplier}, where the
   * multiplier is selected according to the heap memory tier (low, medium, or high).
   *
   * @param availableHeapGB       the available heap memory in gigabytes.
   * @param availableProcessors   the number of available CPU cores.
   * @return the maximum thread count based on memory tier and processor count.
   */
  private int getMemoryTierMaxThreads(long availableHeapGB, int availableProcessors) {
    int multiplier;
    if (availableHeapGB <= LOW_HEAP_SPACE_FACTOR) {
      multiplier = abfsConfiguration.getLowTierMemoryMultiplier();
    } else if (availableHeapGB <= MEDIUM_HEAP_SPACE_FACTOR) {
      multiplier = abfsConfiguration.getMediumTierMemoryMultiplier();
    } else {
      multiplier = abfsConfiguration.getHighTierMemoryMultiplier();
    }
    return availableProcessors * multiplier;
  }

  /**
   * Returns the singleton {@link WriteThreadPoolSizeManager} instance for the specified filesystem.
   * If an active instance already exists in the manager map for the given filesystem, it is returned.
   * Otherwise, a new instance is created, registered in the map, and returned.
   *
   * @param filesystemName     the name of the filesystem.
   * @param abfsConfiguration  the {@link AbfsConfiguration} associated with the filesystem.
   * @param abfsCounters                the {@link AbfsCounters} used to initialize the manager.
   * @return  the singleton {@link WriteThreadPoolSizeManager} instance for the given filesystem.
   */
  public static synchronized WriteThreadPoolSizeManager getInstance(
      String filesystemName, AbfsConfiguration abfsConfiguration, AbfsCounters abfsCounters) {
    /* Check if an instance already exists in the map for the given filesystem */
    WriteThreadPoolSizeManager existingInstance = POOL_SIZE_MANAGER_MAP.get(
        filesystemName);

    /* If an existing instance is found, return it */
    if (existingInstance != null && existingInstance.boundedThreadPool != null
        && !existingInstance.boundedThreadPool.isShutdown()) {
      return existingInstance;
    }

    /* Otherwise, create a new instance, put it in the map, and return it */
    LOG.debug(
        "Creating new WriteThreadPoolSizeManager instance for filesystem: {}",
        filesystemName);
    WriteThreadPoolSizeManager newInstance = new WriteThreadPoolSizeManager(
        filesystemName, abfsConfiguration, abfsCounters);
    POOL_SIZE_MANAGER_MAP.put(filesystemName, newInstance);
    return newInstance;
  }

  /**
   * Adjusts the thread pool size to the specified maximum pool size.
   *
   * @param newMaxPoolSize the new maximum pool size.
   */
  private void adjustThreadPoolSize(int newMaxPoolSize) {
    synchronized (this) {
      ThreadPoolExecutor threadPoolExecutor
          = ((ThreadPoolExecutor) boundedThreadPool);
      int currentCorePoolSize = threadPoolExecutor.getCorePoolSize();

      if (newMaxPoolSize >= currentCorePoolSize) {
        threadPoolExecutor.setMaximumPoolSize(newMaxPoolSize);
        threadPoolExecutor.setCorePoolSize(newMaxPoolSize);
      } else {
        threadPoolExecutor.setCorePoolSize(newMaxPoolSize);
        threadPoolExecutor.setMaximumPoolSize(newMaxPoolSize);
      }
      LOG.debug("ThreadPool Info - New max pool size: {}, Current pool size: {}, Active threads: {}",
          newMaxPoolSize, threadPoolExecutor.getPoolSize(), threadPoolExecutor.getActiveCount());
    }
  }

  /**
   * Starts monitoring the CPU utilization and adjusts the thread pool size accordingly.
   */
  public synchronized void startCPUMonitoring() {
    if (!isMonitoringStarted()) {
      isMonitoringStarted = true;
      cpuMonitorExecutor.scheduleAtFixedRate(() -> {
            double cpuUtilization = ResourceUtilizationUtils.getJvmCpuLoad();
            LOG.debug("Current CPU Utilization is this: {}", cpuUtilization);
            try {
              adjustThreadPoolSizeBasedOnCPU(cpuUtilization);
            } catch (InterruptedException e) {
              throw new RuntimeException(String.format(
                  "Thread pool size adjustment interrupted for filesystem %s",
                  filesystemName), e);
            }
          }, 0, getAbfsConfiguration().getWriteCpuMonitoringInterval(),
          TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Dynamically adjusts the thread pool size based on current CPU utilization
   * and available heap memory relative to the initially available heap.
   *
   * @param cpuUtilization Current system CPU utilization (0.0 to 1.0)
   *  @throws InterruptedException if the resizing operation is interrupted while acquiring the lock
   */
  public void adjustThreadPoolSizeBasedOnCPU(double cpuUtilization) throws InterruptedException {
    lock.lock();
    try {
      ThreadPoolExecutor executor = (ThreadPoolExecutor) this.boundedThreadPool;
      int currentPoolSize = executor.getMaximumPoolSize();
      double memoryLoad = ResourceUtilizationUtils.getMemoryLoad();
      LOG.debug("The memory load is {} and CPU utilization is {}", memoryLoad, cpuUtilization);
      if (cpuUtilization > (abfsConfiguration.getWriteHighCpuThreshold()/HUNDRED_D)) {
        newMaxPoolSize = calculateReducedPoolSizeHighCPU(currentPoolSize, memoryLoad);
        if (currentPoolSize == initialPoolSize && newMaxPoolSize == initialPoolSize) {
          lastScaleDirection = SCALE_DIRECTION_NO_DOWN_AT_MIN;
        }
      } else if (cpuUtilization > (abfsConfiguration.getWriteMediumCpuThreshold()/HUNDRED_D)) {
        newMaxPoolSize = calculateReducedPoolSizeMediumCPU(currentPoolSize, memoryLoad);
        if (currentPoolSize == initialPoolSize && newMaxPoolSize == initialPoolSize) {
          lastScaleDirection = SCALE_DIRECTION_NO_DOWN_AT_MIN;
        }
      } else if (cpuUtilization < (abfsConfiguration.getWriteLowCpuThreshold()/HUNDRED_D)) {
        newMaxPoolSize = calculateIncreasedPoolSizeLowCPU(currentPoolSize, memoryLoad);
        if (currentPoolSize == maxThreadPoolSize && newMaxPoolSize == maxThreadPoolSize) {
          lastScaleDirection = SCALE_DIRECTION_NO_UP_AT_MAX;
        }
      } else {
        newMaxPoolSize = currentPoolSize;
        LOG.debug("CPU load normal ({}). No change: current={}", cpuUtilization, currentPoolSize);
      }
      boolean willResize = newMaxPoolSize != currentPoolSize;
      if (!willResize && !lastScaleDirection.equals(EMPTY_STRING)) {
        WriteThreadPoolStats stats = getCurrentStats(cpuUtilization, memoryLoad);
        // Update the write thread pool metrics with the latest statistics snapshot.
        writeThreadPoolMetrics.update(stats);
      }
      // Case 1: CPU increased — push metrics ONLY if not resizing
      if (cpuUtilization > maxJvmCpuUtilization) {
        maxJvmCpuUtilization = cpuUtilization;
        if (!willResize) {
          try {
            // Capture the latest thread pool statistics (pool size, CPU, memory, etc.).
            WriteThreadPoolStats stats = getCurrentStats(cpuUtilization, memoryLoad);
            // Update the write thread pool metrics with the latest statistics snapshot.
            writeThreadPoolMetrics.update(stats);
          } catch (Exception e) {
            LOG.debug("Error updating write thread pool metrics", e);
          }
        }
      }
      // Case 2: Resize — always push metrics
      if (willResize) {
        LOG.debug("Resizing thread pool from {} to {}", currentPoolSize, newMaxPoolSize);
        // Record scale direction
        lastScaleDirection = (newMaxPoolSize > currentPoolSize) ? SCALE_DIRECTION_UP: SCALE_DIRECTION_DOWN;
        adjustThreadPoolSize(newMaxPoolSize);
        try {
          // Capture the latest thread pool statistics (pool size, CPU, memory, etc.).
          WriteThreadPoolStats stats = getCurrentStats(cpuUtilization, memoryLoad);
          // Update the write thread pool metrics with the latest statistics snapshot.
          writeThreadPoolMetrics.update(stats);
        } catch (Exception e) {
          LOG.debug("Error updating write thread pool metrics after resizing.", e);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Calculates a reduced thread pool size when high CPU utilization is detected.
   * The reduction strategy depends on available heap memory:
   * if heap usage is high (low free memory), the pool size is reduced aggressively;
   * otherwise, it is reduced moderately to prevent resource contention.
   *
   * @param currentPoolSize  the current size of the thread pool.
   *  @param memoryLoad      the current JVM heap load (0.0–1.0)
   * @return the adjusted (reduced) pool size based on CPU and memory conditions.
   */
  private int calculateReducedPoolSizeHighCPU(int currentPoolSize, double memoryLoad) {
    LOG.debug("The high cpu memory load is {}", memoryLoad);
    if (memoryLoad > highMemoryThreshold) {
      LOG.debug("High CPU & high memory load ({}). Aggressive reduction: current={}, new={}",
          memoryLoad, currentPoolSize, currentPoolSize / HIGH_CPU_LOW_MEMORY_REDUCTION_FACTOR);
      return Math.max(initialPoolSize, currentPoolSize / HIGH_CPU_LOW_MEMORY_REDUCTION_FACTOR);
    }
    int reduced = Math.max(initialPoolSize, currentPoolSize - currentPoolSize / HIGH_CPU_REDUCTION_FACTOR);
    LOG.debug("High CPU ({}). Reducing pool size moderately: current={}, new={}",
        abfsConfiguration.getWriteHighCpuThreshold(), currentPoolSize, reduced);
    return reduced;
  }

  /**
   * Calculates a reduced thread pool size when medium CPU utilization is detected.
   * The reduction is based on available heap memory: if memory is low, the pool size
   * is reduced more aggressively; otherwise, a moderate reduction is applied to
   * maintain balanced performance.
   *
   * @param currentPoolSize  the current size of the thread pool.
   * @param memoryLoad      the current JVM heap load (0.0–1.0)
   * @return the adjusted (reduced) pool size based on medium CPU and memory conditions.
   */
  private int calculateReducedPoolSizeMediumCPU(int currentPoolSize, double memoryLoad) {
    LOG.debug("The medium cpu memory load is {}", memoryLoad);
    if (memoryLoad > highMemoryThreshold) {
      int reduced = Math.max(initialPoolSize, currentPoolSize - currentPoolSize / MEDIUM_CPU_LOW_MEMORY_REDUCTION_FACTOR);
      LOG.debug("Medium CPU & high memory load ({}). Reducing: current={}, new={}",
          memoryLoad, currentPoolSize, reduced);
      return reduced;
    }
    int reduced = Math.max(initialPoolSize, currentPoolSize - currentPoolSize / MEDIUM_CPU_REDUCTION_FACTOR);
    LOG.debug("Medium CPU ({}). Moderate reduction: current={}, new={}",
        abfsConfiguration.getWriteMediumCpuThreshold(), currentPoolSize, reduced);
    return reduced;
  }

  /**
   * Calculates an adjusted thread pool size when low CPU utilization is detected.
   * If sufficient heap memory is available, the pool size is increased to improve throughput.
   * Otherwise, it is slightly decreased to conserve memory resources.
   *
   * @param currentPoolSize  the current size of the thread pool.
   * @param memoryLoad      the current JVM heap load (0.0–1.0)
   * @return the adjusted (increased or decreased) pool size based on CPU and memory conditions.
   */
  private int calculateIncreasedPoolSizeLowCPU(int currentPoolSize, double memoryLoad) {
    LOG.debug("The low cpu memory load is {}", memoryLoad);
    if (memoryLoad <= lowMemoryThreshold) {
      int increased = Math.min(maxThreadPoolSize, (int) (currentPoolSize * LOW_CPU_POOL_SIZE_INCREASE_FACTOR));
      LOG.debug("Low CPU & low memory load ({}). Increasing: current={}, new={}",
          memoryLoad, currentPoolSize, increased);
      return increased;
    } else {
      // Decrease by 10%
      int decreased = Math.max(1, (int) (currentPoolSize * LOW_CPU_HIGH_MEMORY_DECREASE_FACTOR));
      LOG.debug("Low CPU but insufficient heap. Decreasing: current={}, new={}", currentPoolSize, decreased);
      return decreased;
    }
  }

  /**
   * Returns the executor service for the thread pool.
   *
   * @return the executor service.
   */
  public ExecutorService getExecutorService() {
    return boundedThreadPool;
  }

  /**
   * Returns the scheduled executor responsible for CPU monitoring and dynamic pool adjustment.
   *
   * @return the {@link ScheduledExecutorService} used for CPU monitoring.
   */
  public ScheduledExecutorService getCpuMonitorExecutor() {
    return cpuMonitorExecutor;
  }

  /**
   * Checks if monitoring has started.
   *
   * @return true if monitoring has started, false otherwise.
   */
  public synchronized boolean isMonitoringStarted() {
    return isMonitoringStarted;
  }

  /**
   * Returns the maximum JVM CPU utilization observed during the current
   * monitoring interval or since the last reset.
   *
   * @return the highest JVM CPU utilization percentage recorded
   */
  @VisibleForTesting
  public double getMaxJvmCpuUtilization() {
    return maxJvmCpuUtilization;
  }

  /**
   * Closes this manager by shutting down executors and cleaning up resources.
   * Removes the instance from the active manager map.
   *
   * @throws IOException if an error occurs during shutdown.
   */
  @Override
  public void close() throws IOException {
    synchronized (this) {
      try {
        // Shutdown CPU monitor
        if (cpuMonitorExecutor != null && !cpuMonitorExecutor.isShutdown()) {
          cpuMonitorExecutor.shutdown();
        }
        // Gracefully shutdown the bounded thread pool
        if (boundedThreadPool != null && !boundedThreadPool.isShutdown()) {
          boundedThreadPool.shutdown();
          if (!boundedThreadPool.awaitTermination(THIRTY_SECONDS, TimeUnit.SECONDS)) {
            LOG.warn("Bounded thread pool did not terminate in time, forcing shutdownNow for filesystem: {}", filesystemName);
            boundedThreadPool.shutdownNow();
          }
          boundedThreadPool = null;
        }
        // Remove from the map
        POOL_SIZE_MANAGER_MAP.remove(filesystemName);
        LOG.debug("Closed and removed instance for filesystem: {}", filesystemName);
      } catch (Exception e) {
        LOG.warn("Failed to properly close instance for filesystem: {}", filesystemName, e);
      }
    }
  }

  /**
   * Represents current statistics of the write thread pool and system.
   */
  public static class WriteThreadPoolStats extends ResourceUtilizationStats {

    /**
     * Constructs a {@link WriteThreadPoolStats} instance containing thread pool
     * metrics and JVM/system resource utilization details.
     *
     * @param currentPoolSize the current number of threads in the pool
     * @param maxPoolSize the maximum number of threads permitted in the pool
     * @param activeThreads the number of threads actively executing tasks
     * @param idleThreads the number of idle threads in the pool
     * @param jvmCpuLoad the current JVM CPU load (0.0–1.0)
     * @param systemCpuUtilization the current system-wide CPU utilization (0.0–1.0)
     * @param availableHeapGB the available heap memory in gigabytes
     * @param committedHeapGB the committed heap memory in gigabytes
     * @param usedHeapGB the available heap memory in gigabytes
     * @param maxHeapGB the committed heap memory in gigabytes
     * @param memoryLoad the JVM memory load (used / max)
     * @param lastScaleDirection the last scaling action performed: "I" (increase),
     * "D" (decrease), or empty if no scaling occurred
     * @param maxCpuUtilization the peak JVM CPU utilization observed during this interval
     * @param jvmProcessId the process ID of the JVM
     */
    public WriteThreadPoolStats(int currentPoolSize,
        int maxPoolSize, int activeThreads, int idleThreads,
        double jvmCpuLoad, double systemCpuUtilization, double availableHeapGB,
        double committedHeapGB, double usedHeapGB, double maxHeapGB, double memoryLoad, String lastScaleDirection,
        double maxCpuUtilization, long jvmProcessId) {
      super(currentPoolSize, maxPoolSize, activeThreads, idleThreads,
          jvmCpuLoad, systemCpuUtilization, availableHeapGB,
          committedHeapGB, usedHeapGB, maxHeapGB, memoryLoad, lastScaleDirection,
          maxCpuUtilization, jvmProcessId);
    }
  }

  /**
   * Returns the latest statistics for the write thread pool and system resources.
   * The snapshot includes thread counts, JVM and system CPU utilization, and the
   * current heap usage. These metrics are used for monitoring and making dynamic
   * sizing decisions for the write thread pool.
   *
   * @param jvmCpuUtilization current JVM CPU usage (%)
   * @param memoryLoad        current JVM memory load (used/committed)
   * @return a {@link WriteThreadPoolStats} object containing the current metrics
   */
  synchronized WriteThreadPoolStats getCurrentStats(double jvmCpuUtilization,
      double memoryLoad) {

    if (boundedThreadPool == null) {
      return new WriteThreadPoolStats(
          ZERO, ZERO, ZERO, ZERO, ZERO_D, ZERO_D, ZERO_D, ZERO_D, ZERO_D,
          ZERO_D, ZERO_D, EMPTY_STRING, ZERO_D, ZERO);
    }

    ThreadPoolExecutor exec = (ThreadPoolExecutor) this.boundedThreadPool;

    String currentScaleDirection = lastScaleDirection;
    lastScaleDirection = EMPTY_STRING;

    int poolSize = exec.getPoolSize();
    int activeThreads = exec.getActiveCount();
    int idleThreads = poolSize - activeThreads;

    return new WriteThreadPoolStats(
        poolSize,                      // Current thread count
        exec.getMaximumPoolSize(),     // Max allowed threads
        activeThreads,                 // Busy threads
        idleThreads,                   // Idle threads
        jvmCpuUtilization,             // JVM CPU usage (ratio)
        ResourceUtilizationUtils.getSystemCpuLoad(),     // System CPU usage (ratio)
        ResourceUtilizationUtils.getAvailableHeapMemory(),      // Free heap (GB)
        ResourceUtilizationUtils.getCommittedHeapMemory(),      // Committed heap (GB)
        ResourceUtilizationUtils.getUsedHeapMemory(),   // Used heap (GB)
        ResourceUtilizationUtils.getMaxHeapMemory(),    // Max heap (GB)
        memoryLoad,                    // used/max
        currentScaleDirection,         // "I", "D", or ""
        getMaxJvmCpuUtilization(),              // Peak JVM CPU usage so far
        ResourceUtilizationUtils.getJvmProcessId()              // JVM PID
    );
  }
}

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

import com.sun.management.OperatingSystemMXBean;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.LOW_HEAP_SPACE_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.MEDIUM_HEAP_SPACE_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.BYTES_PER_GIGABYTE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HIGH_CPU_LOW_MEMORY_REDUCTION_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HIGH_CPU_REDUCTION_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HIGH_MEDIUM_HEAP_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HUNDRED_D;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.LOW_CPU_HEAP_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.LOW_CPU_HIGH_MEMORY_DECREASE_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.LOW_CPU_POOL_SIZE_INCREASE_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MEDIUM_CPU_LOW_MEMORY_REDUCTION_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MEDIUM_CPU_REDUCTION_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.THIRTY_SECONDS;

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
  /* Initially available heap memory. */
  private final long initialAvailableHeapMemory;
  /* The configuration instance. */
  private final AbfsConfiguration abfsConfiguration;

  /**
   * Private constructor to initialize the write thread pool and CPU monitor executor
   * based on system resources and ABFS configuration.
   *
   * @param filesystemName       Name of the ABFS filesystem.
   * @param abfsConfiguration    Configuration containing pool size parameters.
   */
  private WriteThreadPoolSizeManager(String filesystemName,
      AbfsConfiguration abfsConfiguration) {
    this.filesystemName = filesystemName;
    this.abfsConfiguration = abfsConfiguration;
    int availableProcessors = Runtime.getRuntime().availableProcessors();
    /* Get the heap space available when the instance is created */
    this.initialAvailableHeapMemory = getAvailableHeapMemory();
    /* Compute the max pool size */
    int computedMaxPoolSize = getComputedMaxPoolSize(availableProcessors, initialAvailableHeapMemory);

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
    executor.setKeepAliveTime(
        abfsConfiguration.getWriteThreadPoolKeepAliveTime(), TimeUnit.SECONDS);
    executor.allowCoreThreadTimeOut(true);
    /* Create a scheduled executor for CPU monitoring and pool adjustment */
    this.cpuMonitorExecutor = Executors.newScheduledThreadPool(1);
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
   * Calculates the available heap memory in gigabytes.
   * This method uses {@link Runtime#getRuntime()} to obtain the maximum heap memory
   * allowed for the JVM and subtracts the currently used memory (total - free)
   * to determine how much heap memory is still available.
   * The result is rounded up to the nearest gigabyte.
   *
   * @return the available heap memory in gigabytes
   */
  private long getAvailableHeapMemory() {
    MemoryMXBean osBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memoryUsage = osBean.getHeapMemoryUsage();
    long availableHeapBytes = memoryUsage.getMax() - memoryUsage.getUsed();
    return (availableHeapBytes + BYTES_PER_GIGABYTE - 1) / BYTES_PER_GIGABYTE;
  }

  /**
   * Returns aggressive thread count = CPU cores × multiplier based on heap tier.
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
   * Returns the singleton instance of WriteThreadPoolSizeManager for the given filesystem.
   *
   * @param filesystemName the name of the filesystem.
   * @param abfsConfiguration the configuration for the ABFS.
   *
   * @return the singleton instance.
   */
  public static synchronized WriteThreadPoolSizeManager getInstance(
      String filesystemName, AbfsConfiguration abfsConfiguration) {
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
        filesystemName, abfsConfiguration);
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
  synchronized void startCPUMonitoring() {
    cpuMonitorExecutor.scheduleAtFixedRate(() -> {
      double cpuUtilization = getCpuUtilization();
      LOG.debug("Current CPU Utilization is this: {}", cpuUtilization);
      try {
        adjustThreadPoolSizeBasedOnCPU(cpuUtilization);
      } catch (InterruptedException e) {
        throw new RuntimeException(String.format(
            "Thread pool size adjustment interrupted for filesystem %s",
            filesystemName), e);
      }
    }, 0, getAbfsConfiguration().getWriteCpuMonitoringInterval(), TimeUnit.MILLISECONDS);
  }

  /**
   * Gets the current system CPU utilization.
   *
   * @return the CPU utilization as a fraction (0.0 to 1.0), or 0.0 if unavailable.
   */
  private double getCpuUtilization() {
    OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(
        OperatingSystemMXBean.class);
    double cpuLoad = osBean.getSystemCpuLoad();
    if (cpuLoad < 0) {
      LOG.warn("System CPU load value unavailable (returned -1.0). Defaulting to 0.0.");
      return 0.0;
    }
    return cpuLoad;
  }

  /**
   * Dynamically adjusts the thread pool size based on current CPU utilization
   * and available heap memory relative to the initially available heap.
   *
   * @param cpuUtilization Current system CPU utilization (0.0 to 1.0)
   * @throws InterruptedException if thread locking is interrupted
   */
  public void adjustThreadPoolSizeBasedOnCPU(double cpuUtilization) throws InterruptedException {
    lock.lock();
    try {
      ThreadPoolExecutor executor = (ThreadPoolExecutor) this.boundedThreadPool;
      int currentPoolSize = executor.getMaximumPoolSize();
      long currentHeap = getAvailableHeapMemory();
      long initialHeap = initialAvailableHeapMemory;
      LOG.debug("Available heap memory: {} GB, Initial heap memory: {} GB", currentHeap, initialHeap);
      LOG.debug("Current CPU Utilization: {}", cpuUtilization);

      if (cpuUtilization > (abfsConfiguration.getWriteHighCpuThreshold()/HUNDRED_D)) {
        newMaxPoolSize = calculateReducedPoolSizeHighCPU(currentPoolSize, currentHeap, initialHeap);
      } else if (cpuUtilization > (abfsConfiguration.getWriteMediumCpuThreshold()/HUNDRED_D)) {
        newMaxPoolSize = calculateReducedPoolSizeMediumCPU(currentPoolSize, currentHeap, initialHeap);
      } else if (cpuUtilization < (abfsConfiguration.getWriteLowCpuThreshold()/HUNDRED_D)) {
        newMaxPoolSize = calculateIncreasedPoolSizeLowCPU(currentPoolSize, currentHeap, initialHeap);
      } else {
        newMaxPoolSize = currentPoolSize;
        LOG.debug("CPU load normal ({}). No change: current={}", cpuUtilization, currentPoolSize);
      }
      if (newMaxPoolSize != currentPoolSize) {
        LOG.debug("Resizing thread pool from {} to {}", currentPoolSize, newMaxPoolSize);
        adjustThreadPoolSize(newMaxPoolSize);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Calculates reduced pool size under high CPU utilization.
   */
  private int calculateReducedPoolSizeHighCPU(int currentPoolSize, long currentHeap, long initialHeap) {
    if (currentHeap <= initialHeap / HIGH_MEDIUM_HEAP_FACTOR) {
      LOG.debug("High CPU & low heap. Aggressively reducing: current={}, new={}",
          currentPoolSize, currentPoolSize / HIGH_CPU_LOW_MEMORY_REDUCTION_FACTOR);
      return Math.max(initialPoolSize, currentPoolSize / HIGH_CPU_LOW_MEMORY_REDUCTION_FACTOR);
    }
    int reduced = Math.max(initialPoolSize, currentPoolSize - currentPoolSize / HIGH_CPU_REDUCTION_FACTOR);
    LOG.debug("High CPU ({}). Reducing pool size moderately: current={}, new={}",
        abfsConfiguration.getWriteHighCpuThreshold(), currentPoolSize, reduced);
    return reduced;
  }

  /**
   * Calculates reduced pool size under medium CPU utilization.
   */
  private int calculateReducedPoolSizeMediumCPU(int currentPoolSize, long currentHeap, long initialHeap) {
    if (currentHeap <= initialHeap / HIGH_MEDIUM_HEAP_FACTOR) {
      int reduced = Math.max(initialPoolSize, currentPoolSize - currentPoolSize / MEDIUM_CPU_LOW_MEMORY_REDUCTION_FACTOR);
      LOG.debug("Medium CPU & low heap. Reducing: current={}, new={}", currentPoolSize, reduced);
      return reduced;
    }
    int reduced = Math.max(initialPoolSize, currentPoolSize - currentPoolSize / MEDIUM_CPU_REDUCTION_FACTOR);
    LOG.debug("Medium CPU ({}). Moderate reduction: current={}, new={}",
        abfsConfiguration.getWriteMediumCpuThreshold(), currentPoolSize, reduced);
    return reduced;
  }

  /**
   * Calculates increased pool size under low CPU utilization.
   */
  private int calculateIncreasedPoolSizeLowCPU(int currentPoolSize, long currentHeap, long initialHeap) {
    if (currentHeap >= initialHeap * LOW_CPU_HEAP_FACTOR) {
      int increased = Math.min(maxThreadPoolSize, (int) (currentPoolSize * LOW_CPU_POOL_SIZE_INCREASE_FACTOR));
      LOG.debug("Low CPU & healthy heap. Increasing: current={}, new={}", currentPoolSize, increased);
      return increased;
    } else {
      // Decrease by 10%
      int decreased = Math.max(1, (int) (currentPoolSize * LOW_CPU_HIGH_MEMORY_DECREASE_FACTOR));
      LOG.debug("Low CPU but insufficient heap ({} GB). Decreasing: current={}, new={}", currentHeap, currentPoolSize, decreased);
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
}

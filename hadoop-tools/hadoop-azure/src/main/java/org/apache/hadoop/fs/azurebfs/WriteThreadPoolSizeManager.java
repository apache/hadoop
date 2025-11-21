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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsWriteThreadPoolMetrics;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.LOW_HEAP_SPACE_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.MEDIUM_HEAP_SPACE_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.BYTES_PER_GIGABYTE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HIGH_CPU_LOW_MEMORY_REDUCTION_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HIGH_CPU_REDUCTION_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HUNDRED_D;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.LOW_CPU_HIGH_MEMORY_DECREASE_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.LOW_CPU_POOL_SIZE_INCREASE_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MEDIUM_CPU_LOW_MEMORY_REDUCTION_FACTOR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MEDIUM_CPU_REDUCTION_FACTOR;
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
  private final AbfsWriteThreadPoolMetrics writeThreadPoolMetrics;
  /* Flag indicating if CPU monitoring has started. */
  private volatile boolean isMonitoringStarted = false;
  /* Tracks the last scale direction applied, or empty if none. */
  private volatile String lastScaleDirection = EMPTY_STRING;
  /* Maximum CPU utilization observed during the monitoring interval. */
  private volatile double maxCpuUtilization = 0.0;
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
   * @param abfsClient                  ABFS client instance used for communication.
   */
  private WriteThreadPoolSizeManager(String filesystemName,
      AbfsConfiguration abfsConfiguration, AbfsClient abfsClient) {
    /* Retrieves and assigns the write thread pool metrics from the ABFS client counters. */
    this.writeThreadPoolMetrics = abfsClient.getAbfsCounters()
        .getAbfsWriteThreadPoolMetrics();
    this.filesystemName = filesystemName;
    this.abfsConfiguration = abfsConfiguration;
    int availableProcessors = Runtime.getRuntime().availableProcessors();
    /* Compute the max pool size */
    int computedMaxPoolSize = getComputedMaxPoolSize(availableProcessors, getAvailableMaxHeapMemory());

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
   * Calculates the available heap memory in gigabytes.
   * This method uses {@link Runtime#getRuntime()} to obtain the maximum heap memory
   * allowed for the JVM and subtracts the currently used memory (total - free)
   * to determine how much heap memory is still available.
   * The result is rounded up to the nearest gigabyte.
   *
   * @return the available heap memory in gigabytes
   */
  @VisibleForTesting
  public long getAvailableHeapMemory() {
    MemoryMXBean osBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memoryUsage = osBean.getHeapMemoryUsage();
    long availableHeapBytes = memoryUsage.getCommitted() - memoryUsage.getUsed();
    return (availableHeapBytes + BYTES_PER_GIGABYTE - 1) / BYTES_PER_GIGABYTE;
  }

  /**
   * Returns the currently committed JVM heap memory in bytes.
   * This reflects the amount of heap the JVM has reserved from the OS and may grow as needed.
   *
   * @return committed heap memory in bytes
   */
  @VisibleForTesting
  public long getCommittedHeapMemory() {
    MemoryMXBean osBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memoryUsage = osBean.getHeapMemoryUsage();
    return memoryUsage.getCommitted();
  }

  /**
   * Calculates the available max heap memory in gigabytes.
   * This method uses {@link Runtime#getRuntime()} to obtain the maximum heap memory
   * allowed for the JVM and subtracts the currently used memory (total - free)
   * to determine how much heap memory is still available.
   * The result is rounded up to the nearest gigabyte.
   *
   * @return the available heap memory in gigabytes
   */
  private long getAvailableMaxHeapMemory() {
    MemoryMXBean osBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memoryUsage = osBean.getHeapMemoryUsage();
    long availableHeapBytes = memoryUsage.getMax() - memoryUsage.getUsed();
    return (availableHeapBytes + BYTES_PER_GIGABYTE - 1) / BYTES_PER_GIGABYTE;
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
   * @param abfsClient                the {@link AbfsClient} used to initialize the manager.
   * @return  the singleton {@link WriteThreadPoolSizeManager} instance for the given filesystem.
   */
  public static synchronized WriteThreadPoolSizeManager getInstance(
      String filesystemName, AbfsConfiguration abfsConfiguration, AbfsClient abfsClient) {
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
        filesystemName, abfsConfiguration, abfsClient);
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
            double cpuUtilization = getJvmCpuLoad();
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
   * Gets the current system CPU utilization.
   *
   * @return the CPU utilization as a fraction (0.0 to 1.0), or 0.0 if unavailable.
   */
  private double getSystemCpuUtilization() {
    OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(
        OperatingSystemMXBean.class);
    double cpuLoad = osBean.getSystemCpuLoad();
    if (cpuLoad < ZERO) {
      LOG.warn("System CPU load value unavailable (returned -1.0). Defaulting to 0.0.");
      return ZERO_D;
    }
    return cpuLoad;
  }

  /**
   * Gets the current system CPU utilization.
   *
   * @return the CPU utilization as a fraction (0.0 to 1.0), or 0.0 if unavailable.
   */
  @VisibleForTesting
  public double getJvmCpuLoad() {
    OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(
        OperatingSystemMXBean.class);
    double cpuLoad = osBean.getProcessCpuLoad();
    if (cpuLoad < ZERO) {
      LOG.warn("System CPU load value unavailable (returned -1.0). Defaulting to 0.0.");
      return ZERO_D;
    }
    return cpuLoad;
  }

  /**
   * Get the current memory load of the JVM.
   * @return the memory load as a double value between 0.0 and 1.0
   */
  @VisibleForTesting
  double getMemoryLoad() {
    MemoryMXBean osBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memoryUsage = osBean.getHeapMemoryUsage();
    return (double) memoryUsage.getUsed() / memoryUsage.getMax();
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
      double memoryLoad = getMemoryLoad();
      LOG.debug("Current CPU Utilization: {}", cpuUtilization);
      if (cpuUtilization > (abfsConfiguration.getWriteHighCpuThreshold()/HUNDRED_D)) {
        newMaxPoolSize = calculateReducedPoolSizeHighCPU(currentPoolSize, memoryLoad);
        if (newMaxPoolSize == initialPoolSize) {
          lastScaleDirection = "-D";
        }
      } else if (cpuUtilization > (abfsConfiguration.getWriteMediumCpuThreshold()/HUNDRED_D)) {
        newMaxPoolSize = calculateReducedPoolSizeMediumCPU(currentPoolSize, memoryLoad);
        if (newMaxPoolSize == initialPoolSize) {
          lastScaleDirection = "-D";
        }
      } else if (cpuUtilization < (abfsConfiguration.getWriteLowCpuThreshold()/HUNDRED_D)) {
        newMaxPoolSize = calculateIncreasedPoolSizeLowCPU(currentPoolSize, memoryLoad);
        if (newMaxPoolSize == maxThreadPoolSize) {
          lastScaleDirection = "+F";
        }
      } else {
        newMaxPoolSize = currentPoolSize;
        LOG.debug("CPU load normal ({}). No change: current={}", cpuUtilization, currentPoolSize);
      }
      boolean willResize = newMaxPoolSize != currentPoolSize;
      if (!willResize && !lastScaleDirection.equals(EMPTY_STRING)) {
        WriteThreadPoolStats stats = getCurrentStats(cpuUtilization,
            maxCpuUtilization, memoryLoad);
        // Update the write thread pool metrics with the latest statistics snapshot.
        writeThreadPoolMetrics.update(stats);
      }
      // Case 1: CPU increased — push metrics ONLY if not resizing
      if (cpuUtilization > maxCpuUtilization) {
        maxCpuUtilization = cpuUtilization;
        if (!willResize) {
          try {
            // Capture the latest thread pool statistics (pool size, CPU, memory, etc.).
            WriteThreadPoolStats stats = getCurrentStats(cpuUtilization,
                maxCpuUtilization, memoryLoad);
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
        lastScaleDirection = (newMaxPoolSize > currentPoolSize) ? "I" : "D";
        adjustThreadPoolSize(newMaxPoolSize);
        try {
          // Capture the latest thread pool statistics (pool size, CPU, memory, etc.).
          WriteThreadPoolStats stats = getCurrentStats(cpuUtilization,
              maxCpuUtilization, memoryLoad);
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
  public double getMaxCpuUtilization() {
    return maxCpuUtilization;
  }

  /**
   * Returns the process ID (PID) of the currently running JVM.
   * This method uses {@link ProcessHandle#current()} to obtain the ID of the
   * Java process.
   *
   * @return the PID of the current JVM process
   */
  public long getJvmProcessId() {
    return ProcessHandle.current().pid();
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
  public static class WriteThreadPoolStats {
    private final int currentPoolSize;  // Current number of threads in the pool
    private final int maxPoolSize;        // Maximum allowed pool size
    private final int activeThreads;    // Number of threads currently executing tasks
    private final int idleThreads;        // Number of threads not executing tasks
    private final double jvmCpuLoad;    // Current JVM CPU utilization (%)
    private final double systemCpuUtilization;  // Current system CPU utilization (%)
    private final long availableHeapGB;       // Available heap memory (GB)
    private final long committedHeapGB;  // Total committed heap memory (GB)
    private final double memoryLoad;  // Heap usage ratio (used/committed)
    private final String lastScaleDirection;  // Last resize direction: "I" (increase) or "D" (decrease)
    private final double maxCpuUtilization;  // Peak JVM CPU observed in the current interval
    private final long jvmProcessId;   // JVM Process ID

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
     * @param memoryLoad the JVM memory load (used / committed)
     * @param lastScaleDirection the last scaling action performed: "I" (increase),
     * "D" (decrease), or empty if no scaling occurred
     * @param maxCpuUtilization the peak JVM CPU utilization observed during this interval
     * @param jvmProcessId the process ID of the JVM
     */
    public WriteThreadPoolStats(int currentPoolSize,
        int maxPoolSize, int activeThreads, int idleThreads,
        double jvmCpuLoad, double systemCpuUtilization, long availableHeapGB,
        long committedHeapGB, double memoryLoad, String lastScaleDirection,
        double maxCpuUtilization, long jvmProcessId) {
      this.currentPoolSize = currentPoolSize;
      this.maxPoolSize = maxPoolSize;
      this.activeThreads = activeThreads;
      this.idleThreads = idleThreads;
      this.jvmCpuLoad = jvmCpuLoad;
      this.systemCpuUtilization = systemCpuUtilization;
      this.availableHeapGB = availableHeapGB;
      this.committedHeapGB = committedHeapGB;
      this.memoryLoad = memoryLoad;
      this.lastScaleDirection = lastScaleDirection;
      this.maxCpuUtilization = maxCpuUtilization;
      this.jvmProcessId = jvmProcessId;
    }

    /** @return the current number of threads in the pool. */
    public int getCurrentPoolSize() {
      return currentPoolSize;
    }

    /** @return the maximum allowed size of the thread pool. */
    public int getMaxPoolSize() {
      return maxPoolSize;
    }

    /** @return the number of threads currently executing tasks. */
    public int getActiveThreads() {
      return activeThreads;
    }

    /** @return the number of threads currently idle. */
    public int getIdleThreads() {
      return idleThreads;
    }

    /** @return the overall system CPU utilization percentage. */
    public double getSystemCpuUtilization() {
      return systemCpuUtilization;
    }

    /** @return the available heap memory in gigabytes. */
    public long getMemoryUtilization() {
      return availableHeapGB;
    }

    /** @return the total committed heap memory in gigabytes */
    public long getCommittedHeapGB() {
      return committedHeapGB;
    }

    /** @return the current JVM memory load (used / committed) as a value between 0.0 and 1.0 */
    public double getMemoryLoad() {
      return memoryLoad;
    }

    /** @return "I" (increase), "D" (decrease), or empty. */
    public String getLastScaleDirection() {
      return lastScaleDirection;
    }

    /** @return the JVM process CPU utilization percentage. */
    public double getJvmCpuLoad() {
      return jvmCpuLoad;
    }

    /** @return the max JVM process CPU utilization percentage. */
    public double getMaxCpuUtilization() {
      return maxCpuUtilization;
    }

    /** @return the JVM process ID. */
    public long getJvmProcessId() {
      return jvmProcessId;
    }

    /**
     * Converts the scale direction string into numeric value.
     *
     * @param lastScaleDirection the scale direction ("I", "D", or empty)
     *
     * @return 1 for increase, -1 for decrease, 0 for none
     */
    public int getLastScaleDirectionNumeric(String lastScaleDirection) {
      switch (lastScaleDirection) {
      case "I":
        return 1;    // Scaled up
      case "D":
        return -1;   // Scaled down
      case "-D":
        return -2;   // Attempted down-scale, already at minimum
      case "+F":
        return 2;    // Attempted up-scale, already at maximum
      default:
        return 0;  // No scaling
      }
    }

    @Override
    public String toString() {
      return String.format(
          "currentPoolSize=%d, maxPoolSize=%d, activeThreads=%d, idleThreads=%d, "
              + "jvmCpuLoad=%.2f%%, systemCpuUtilization=%.2f%%, "
              + "availableHeap=%dGB, committedHeap=%dGB, memoryLoad=%.2f%%, "
              + "scaleDirection=%s, maxCpuUtilization=%.2f%%, jvmProcessId=%d",
          currentPoolSize, maxPoolSize, activeThreads,
          idleThreads, jvmCpuLoad * HUNDRED_D, systemCpuUtilization * HUNDRED_D,
          availableHeapGB, committedHeapGB, memoryLoad,
          lastScaleDirection, maxCpuUtilization * HUNDRED_D, jvmProcessId
      );
    }
  }

  /**
   * Returns the latest statistics for the write thread pool and system resources.
   * The snapshot includes thread counts, JVM and system CPU utilization, and the
   * current heap usage. These metrics are used for monitoring and making dynamic
   * sizing decisions for the write thread pool.
   *
   * @param jvmCpuUtilization current JVM CPU usage (%)
   * @param maxCpuUtilization highest observed CPU utilization (%)
   * @param memoryLoad        current JVM memory load (used/committed)
   * @return a {@link WriteThreadPoolStats} object containing the current metrics
   */
  synchronized WriteThreadPoolStats getCurrentStats(
      double jvmCpuUtilization,
      double maxCpuUtilization,
      double memoryLoad) {

    if (boundedThreadPool == null) {
      return new WriteThreadPoolStats(
          ZERO, ZERO, ZERO, ZERO, ZERO_D, ZERO_D, ZERO, ZERO, ZERO_D, EMPTY_STRING, ZERO_D, ZERO);
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
        getSystemCpuUtilization(),     // System CPU usage (ratio)
        getAvailableHeapMemory(),      // Free heap (GB)
        getCommittedHeapMemory(),      // Committed heap (GB)
        memoryLoad,                    // used/max
        currentScaleDirection,         // "I", "D", or ""
        maxCpuUtilization,              // Peak JVM CPU usage so far
        getJvmProcessId()              // JVM PID
    );
  }
}

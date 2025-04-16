package org.apache.hadoop.fs.azurebfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import static java.lang.Boolean.TRUE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HIGH_CPU_THRESHOLD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.LOW_CPU_THRESHOLD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.MEDIUM_CPU_THRESHOLD;

/**
 * Manages a thread pool for writing operations, adjusting the pool size based on CPU utilization.
 */
public class WriteThreadPoolSizeManager implements Closeable {
  private final int maxThreadPoolSize;
  private final ScheduledExecutorService cpuMonitorExecutor;
  private volatile ExecutorService boundedThreadPool;
  private final Lock lock = new ReentrantLock();
  private volatile int newMaxPoolSize;
  private static final Logger LOG = LoggerFactory.getLogger(WriteThreadPoolSizeManager.class);
  private static final ConcurrentHashMap<String, WriteThreadPoolSizeManager>
      poolSizeManagerMap = new ConcurrentHashMap<>();
  String filesystemName;

  /**
   * Private constructor to initialize the thread pool and CPU monitor executor.
   */
  private WriteThreadPoolSizeManager(String filesystemName, AbfsConfiguration abfsConfiguration) {
    this.filesystemName = filesystemName;
    // Get total available memory in GB
    long totalMemoryInBytes = getTotalMemoryInBytes(); // Get total system memory in bytes
    long totalMemoryInGB = totalMemoryInBytes / (1024 * 1024 * 1024); // Convert bytes to GB

    int calculatedMaxPoolSize = Math.max(1, (int) (totalMemoryInGB * 4));
    LOG.debug("Using 4");
    int maxPoolSize = Math.max(1, abfsConfiguration.getWriteMaxConcurrentRequestCount());
    // Adjust maxThreadPoolSize based on calculated value
    this.maxThreadPoolSize = Math.max(calculatedMaxPoolSize, maxPoolSize);
    //this.maxThreadPoolSize = Math.max(maxPoolSize, abfsConfiguration.getWriteMaxThreadPoolSize());
    boundedThreadPool = Executors.newFixedThreadPool(maxPoolSize);
    ((ThreadPoolExecutor) boundedThreadPool).setKeepAliveTime(
        abfsConfiguration.getWriteThreadPoolKeepAliveTime(), TimeUnit.SECONDS);
    ((ThreadPoolExecutor) boundedThreadPool).allowCoreThreadTimeOut(TRUE);
    cpuMonitorExecutor = Executors.newScheduledThreadPool(
        abfsConfiguration.getWriteCorePoolSize());
  }

  /**
   * Get total system memory in bytes using OperatingSystemMXBean
   *
   * @return Total memory in bytes
   */
  private long getTotalMemoryInBytes() {
    OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
    if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
      com.sun.management.OperatingSystemMXBean sunOsBean = (com.sun.management.OperatingSystemMXBean) osBean;
      return sunOsBean.getTotalPhysicalMemorySize();  // This returns total memory in bytes
    }
    return 0;
  }

  /**
   * Returns the singleton instance of WriteThreadPoolSizeManager for the given filesystem.
   *
   * @param filesystemName the name of the filesystem.
   * @param abfsConfiguration the configuration for the ABFS.
   * @return the singleton instance.
   */
  public static synchronized WriteThreadPoolSizeManager getInstance(
      String filesystemName, AbfsConfiguration abfsConfiguration) {
    // Check if an instance already exists in the map for the given filesystem
    WriteThreadPoolSizeManager existingInstance = poolSizeManagerMap.get(filesystemName);

    // If an existing instance is found, return it
    if (existingInstance != null && existingInstance.boundedThreadPool != null
        && !existingInstance.boundedThreadPool.isShutdown()) {
      return existingInstance;
    }

    // Otherwise, create a new instance, put it in the map, and return it
    LOG.warn("Creating new WriteThreadPoolSizeManager instance for filesystem: {}", filesystemName);
    WriteThreadPoolSizeManager newInstance = new WriteThreadPoolSizeManager(filesystemName, abfsConfiguration);
    poolSizeManagerMap.put(filesystemName, newInstance);
    return newInstance;
  }

  /**
   * Adjusts the thread pool size to the specified maximum pool size.
   *
   * @param newMaxPoolSize the new maximum pool size.
   */
  public void adjustThreadPoolSize(int newMaxPoolSize) {
    synchronized (this) {
      ThreadPoolExecutor threadPoolExecutor = ((ThreadPoolExecutor) boundedThreadPool);
      int currentCorePoolSize = threadPoolExecutor.getCorePoolSize();
      if (newMaxPoolSize >= currentCorePoolSize) {
        threadPoolExecutor.setMaximumPoolSize(newMaxPoolSize);
        threadPoolExecutor.setCorePoolSize(newMaxPoolSize);
      } else {
        threadPoolExecutor.setCorePoolSize(newMaxPoolSize);
        threadPoolExecutor.setMaximumPoolSize(newMaxPoolSize);
      }
      LOG.debug("The thread pool size is: {} ", newMaxPoolSize);
      LOG.debug("The pool size is: {} ", threadPoolExecutor.getPoolSize());
      LOG.debug("The active thread count is: {}", threadPoolExecutor.getActiveCount());
    }
  }

  /**
   * Starts monitoring the CPU utilization and adjusts the thread pool size accordingly.
   */
  public synchronized void startCPUMonitoring() {
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
    }, 0, 60, TimeUnit.SECONDS);
  }

  /**
   * Gets the current CPU utilization.
   *
   * @return the CPU utilization as a percentage (0.0 to 1.0).
   */
  private double getCpuUtilization() {
    OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
    if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
      com.sun.management.OperatingSystemMXBean sunOsBean = (com.sun.management.OperatingSystemMXBean) osBean;
      double cpuLoad = sunOsBean.getSystemCpuLoad();
      if (cpuLoad >= 0) {
        return cpuLoad;
      }
    }
    return 0.0;
  }

  /**
   * Adjusts the thread pool size based on the current CPU utilization.
   *
   * @param cpuUtilization the current CPU utilization.
   * @throws InterruptedException if the thread pool adjustment is interrupted.
   */
  public void adjustThreadPoolSizeBasedOnCPU(double cpuUtilization) throws InterruptedException {
    lock.lock();
    int currentPoolSize = ((ThreadPoolExecutor) boundedThreadPool).getMaximumPoolSize();
    try {
      if (cpuUtilization > HIGH_CPU_THRESHOLD) {
        newMaxPoolSize = Math.max(1, currentPoolSize - currentPoolSize / 3);
      } else if (cpuUtilization > MEDIUM_CPU_THRESHOLD) {
        newMaxPoolSize = Math.max(1, currentPoolSize - currentPoolSize / 5);
      } else if (cpuUtilization < LOW_CPU_THRESHOLD) {
        newMaxPoolSize = Math.min(maxThreadPoolSize, (int) (currentPoolSize * 1.5));
      } else {
        newMaxPoolSize = currentPoolSize;
      }
      synchronized (this) {
        newMaxPoolSize = Math.max(1, newMaxPoolSize); // Ensure newMaxPoolSize is not 0
      }
      LOG.debug("Adjusting pool size from " + currentPoolSize + " to " + newMaxPoolSize);
    } finally {
      lock.unlock();
    }
    if (newMaxPoolSize != currentPoolSize) {
      this.adjustThreadPoolSize(newMaxPoolSize);
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

  @Override
  public void close() throws IOException {
    synchronized (this) {
      try {
        // Shutdown executors
        cpuMonitorExecutor.shutdown();
        HadoopExecutors.shutdown(boundedThreadPool, LOG, 30, TimeUnit.SECONDS);
        boundedThreadPool = null;

        // Remove from the map
        poolSizeManagerMap.remove(filesystemName);
        LOG.debug("Closed and removed instance for filesystem: {}",
            filesystemName);
      } catch (Exception e) {
        LOG.warn("Failed to properly close instance for filesystem: {}",
            filesystemName, e);
      }
    }
  }
}
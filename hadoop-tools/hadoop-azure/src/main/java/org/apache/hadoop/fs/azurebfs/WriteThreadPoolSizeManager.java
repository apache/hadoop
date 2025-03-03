package org.apache.hadoop.fs.azurebfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.Boolean.TRUE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HIGH_CPU_THRESHOLD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.LOW_CPU_THRESHOLD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.MEDIUM_CPU_THRESHOLD;

/**
 * Manages a thread pool for writing operations, adjusting the pool size based on CPU utilization.
 */
public class WriteThreadPoolSizeManager {
  private static WriteThreadPoolSizeManager instance;
  private final int maxThreadPoolSize;
  private final ScheduledExecutorService cpuMonitorExecutor;
  private final ExecutorService boundedThreadPool;
  private final Lock lock = new ReentrantLock();
  private volatile int newMaxPoolSize;
  private static final Logger LOG = LoggerFactory.getLogger(WriteThreadPoolSizeManager.class);

  /**
   * Private constructor to initialize the thread pool and CPU monitor executor.
   */
  private WriteThreadPoolSizeManager(AbfsConfiguration abfsConfiguration) {
    int maxPoolSize = Math.max(1, abfsConfiguration.getWriteMaxConcurrentRequestCount());
    this.maxThreadPoolSize = Math.max(maxPoolSize, abfsConfiguration.getWriteMaxThreadPoolSize());
    boundedThreadPool = Executors.newFixedThreadPool(maxPoolSize);
    ((ThreadPoolExecutor) boundedThreadPool).setKeepAliveTime(
        abfsConfiguration.getWriteThreadPoolKeepAliveTime(), TimeUnit.SECONDS);
    ((ThreadPoolExecutor) boundedThreadPool).allowCoreThreadTimeOut(TRUE);
    cpuMonitorExecutor = Executors.newScheduledThreadPool(
        abfsConfiguration.getWriteCorePoolSize());
  }

  /**
   * Returns the singleton instance of WriteThreadPoolSizeManager.
   *
   * @return the singleton instance.
   */
  public static synchronized WriteThreadPoolSizeManager getInstance(AbfsConfiguration abfsConfiguration) {
    if (instance == null) {
      instance = new WriteThreadPoolSizeManager(abfsConfiguration);
    }
    return instance;
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
        e.printStackTrace();
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
      LOG.debug("Adjusting pool size from " + currentPoolSize + " to " + newMaxPoolSize);
    } finally {
      lock.unlock();
    }
    if (newMaxPoolSize != currentPoolSize) {
      this.adjustThreadPoolSize(newMaxPoolSize);
    }
  }

  /**
   * Shuts down the thread pool and CPU monitor executor.
   *
   * @throws InterruptedException if the shutdown process is interrupted.
   */
  public void shutdown() throws InterruptedException {
    instance = null;
    cpuMonitorExecutor.shutdown();
    boundedThreadPool.shutdown();
    if (!boundedThreadPool.awaitTermination(30, TimeUnit.SECONDS)) {
      boundedThreadPool.shutdownNow();
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
}
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.utils;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

import com.sun.management.OperatingSystemMXBean;

import org.apache.hadoop.classification.VisibleForTesting;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.BYTES_PER_GIGABYTE;

/**
 * Utility class for retrieving JVM- and system-level resource utilization
 * metrics such as CPU load, memory usage, and available heap memory.
 * All metrics are returned as long values with 2-decimal precision stored as integer (scaled by 100).
 */
public final class ResourceUtilizationUtils {

  private static final long SCALE_FACTOR = 100L; // 2 decimal places

  private ResourceUtilizationUtils() {
    // Prevent instantiation
  }

  /**
   * Scales a double value by {@link #SCALE_FACTOR} to store 2-decimal precision as long.
   *
   * @param value value to scale
   * @return scaled long value
   */
  private static long scale(double value) {
    return Math.round(value * SCALE_FACTOR);
  }

  /**
   * Returns the available heap memory in gigabytes, calculated as the difference between
   * the committed heap and used heap memory.
   * <p>
   * The result is scaled by 100 for 2-decimal precision.
   * </p>
   *
   * @return available heap memory in GB (scaled by 100)
   */
  public static long getAvailableHeapMemory() {
    MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    double gb = (mu.getCommitted() - mu.getUsed()) / (double) BYTES_PER_GIGABYTE;
    return scale(gb);
  }

  /**
   * Returns the JVM heap memory currently committed.
   * <p>
   * Committed memory is the amount of memory guaranteed to be available for the JVM.
   * </p>
   *
   * @return committed heap memory in GB (scaled by 100)
   */
  @VisibleForTesting
  public static long getCommittedHeapMemory() {
    MemoryMXBean osBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memoryUsage = osBean.getHeapMemoryUsage();
    double gb = memoryUsage.getCommitted() / (double) BYTES_PER_GIGABYTE;
    return scale(gb);
  }

  /**
   * Returns the system-wide CPU load as a fraction (scaled by 100 for 2-decimal precision).
   * <p>
   * The value ranges between 0 (no load) and 100 (full load). Returns 0 if CPU load cannot be obtained.
   * </p>
   *
   * @return system CPU load (scaled by 100)
   */
  @VisibleForTesting
  public static long getSystemCpuLoad() {
    OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
    double cpuLoad = osBean.getSystemCpuLoad();
    if (cpuLoad < 0) {
      return 0L;
    }
    return scale(cpuLoad);
  }

  /**
   * Returns the JVM process CPU load as a fraction (scaled by 100 for 2-decimal precision).
   * <p>
   * The value ranges between 0 (no load) and 100 (full CPU used by this process). Returns 0 if CPU load cannot be obtained.
   * </p>
   *
   * @return JVM process CPU load (scaled by 100)
   */
  @VisibleForTesting
  public static long getJvmCpuLoad() {
    OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
    double cpuLoad = osBean.getProcessCpuLoad();
    if (cpuLoad < 0) {
      return 0L;
    }
    return scale(cpuLoad);
  }

  /**
   * Returns the heap memory usage as a fraction of max heap (scaled by 100).
   *
   * @return memory load (used/max heap) scaled by 100
   */
  @VisibleForTesting
  public static long getMemoryLoad() {
    MemoryMXBean osBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memoryUsage = osBean.getHeapMemoryUsage();
    double memLoad = (double) memoryUsage.getUsed() / memoryUsage.getMax();
    return scale(memLoad);
  }

  /**
   * Returns the currently used heap memory in gigabytes.
   *
   * @return used heap memory in GB (scaled by 100)
   */
  public static long getUsedHeapMemory() {
    MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    double gb = mu.getUsed() / (double) BYTES_PER_GIGABYTE;
    return scale(gb);
  }

  /**
   * Returns the maximum heap memory that the JVM can use.
   *
   * @return max heap memory in GB (scaled by 100)
   */
  public static long getMaxHeapMemory() {
    MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    double gb = mu.getMax() / (double) BYTES_PER_GIGABYTE;
    return scale(gb);
  }

  /**
   * Returns the available heap memory relative to the max heap.
   * <p>
   * This method calculates the difference between max heap and currently used heap,
   * then converts it to gigabytes rounded up.
   * </p>
   *
   * @return available heap memory in GB (rounded up)
   */
  public static long getAvailableMaxHeapMemory() {
    MemoryMXBean osBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memoryUsage = osBean.getHeapMemoryUsage();
    long availableHeapBytes = memoryUsage.getMax() - memoryUsage.getUsed();
    return (availableHeapBytes + BYTES_PER_GIGABYTE - 1) / BYTES_PER_GIGABYTE;
  }
}


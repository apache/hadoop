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
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ZERO;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ZERO_D;

/**
 * Utility class for retrieving JVM- and system-level resource utilization
 * metrics such as CPU load, memory usage, and available heap memory.
 */
public final class ResourceUtilizationUtils {

  private ResourceUtilizationUtils() {
    // Prevent instantiation
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
  public static long getAvailableHeapMemory() {
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
  public static double getCommittedHeapMemory() {
    MemoryMXBean osBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memoryUsage = osBean.getHeapMemoryUsage();
    return (double) memoryUsage.getCommitted() / BYTES_PER_GIGABYTE;
  }

  /**
   * Get the current CPU load of the system.
   * @return the CPU load as a double value between 0.0 and 1.0
   */
  @VisibleForTesting
  public static double getSystemCpuLoad() {
    OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(
        OperatingSystemMXBean.class);
    double cpuLoad = osBean.getSystemCpuLoad();
    if (cpuLoad < 0) {
      // If the CPU load is not available, return 0.0
      return 0.0;
    }
    return cpuLoad;
  }


  /**
   * Gets the current system CPU utilization.
   *
   * @return the CPU utilization as a fraction (0.0 to 1.0), or 0.0 if unavailable.
   */
  @VisibleForTesting
  public static double getJvmCpuLoad() {
    OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(
        OperatingSystemMXBean.class);
    double cpuLoad = osBean.getProcessCpuLoad();
    if (cpuLoad < ZERO) {
      return ZERO_D;
    }
    return cpuLoad;
  }

  /**
   * Get the current memory load of the JVM.
   * @return the memory load as a double value between 0.0 and 1.0
   */
  @VisibleForTesting
  public static double getMemoryLoad() {
    MemoryMXBean osBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memoryUsage = osBean.getHeapMemoryUsage();
    return (double) memoryUsage.getUsed() / memoryUsage.getMax();
  }

  /**
   * Calculates the used heap memory in gigabytes.
   * This method returns the amount of heap memory currently used by the JVM.
   * The result is rounded up to the nearest gigabyte.
   *
   * @return the used heap memory in gigabytes
   */
  public static long getUsedHeapMemory() {
    MemoryMXBean osBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memoryUsage = osBean.getHeapMemoryUsage();
    long usedHeapBytes = memoryUsage.getUsed();
    return (usedHeapBytes + BYTES_PER_GIGABYTE - 1) / BYTES_PER_GIGABYTE;
  }

  /**
   * Calculates the maximum heap memory allowed for the JVM in gigabytes.
   * This is the upper bound the JVM may expand its heap to.
   *
   * @return the maximum heap memory in gigabytes
   */
  public static long getMaxHeapMemory() {
    MemoryMXBean osBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memoryUsage = osBean.getHeapMemoryUsage();
    long maxHeapBytes = memoryUsage.getMax();
    return (maxHeapBytes + BYTES_PER_GIGABYTE - 1) / BYTES_PER_GIGABYTE;
  }


  /**
   * Returns the process ID (PID) of the currently running JVM.
   * This method uses {@link ProcessHandle#current()} to obtain the ID of the
   * Java process.
   *
   * @return the PID of the current JVM process
   */
  public static long getJvmProcessId() {
    return ProcessHandle.current().pid();
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
  public static long getAvailableMaxHeapMemory() {
    MemoryMXBean osBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memoryUsage = osBean.getHeapMemoryUsage();
    long availableHeapBytes = memoryUsage.getMax() - memoryUsage.getUsed();
    return (availableHeapBytes + BYTES_PER_GIGABYTE - 1) / BYTES_PER_GIGABYTE;
  }
}

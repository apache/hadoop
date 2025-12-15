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

  private static long scale(double value) {
    return Math.round(value * SCALE_FACTOR);
  }

  public static long getAvailableHeapMemory() {
    MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    double gb = (mu.getCommitted() - mu.getUsed()) / (double) BYTES_PER_GIGABYTE;
    return scale(gb);
  }

  @VisibleForTesting
  public static long getCommittedHeapMemory() {
    MemoryMXBean osBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memoryUsage = osBean.getHeapMemoryUsage();
    double gb = memoryUsage.getCommitted() / (double) BYTES_PER_GIGABYTE;
    return scale(gb);
  }

  @VisibleForTesting
  public static long getSystemCpuLoad() {
    OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
    double cpuLoad = osBean.getSystemCpuLoad();
    if (cpuLoad < 0) {
      return 0L;
    }
    return scale(cpuLoad); // store as fraction * 100
  }

  @VisibleForTesting
  public static long getJvmCpuLoad() {
    OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
    double cpuLoad = osBean.getProcessCpuLoad();
    if (cpuLoad < 0) {
      return 0L;
    }
    return scale(cpuLoad);
  }

  @VisibleForTesting
  public static long getMemoryLoad() {
    MemoryMXBean osBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memoryUsage = osBean.getHeapMemoryUsage();
    double memLoad = (double) memoryUsage.getUsed() / memoryUsage.getMax();
    return scale(memLoad);
  }

  public static long getUsedHeapMemory() {
    MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    double gb = mu.getUsed() / (double) BYTES_PER_GIGABYTE;
    return scale(gb);
  }

  public static long getMaxHeapMemory() {
    MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    double gb = mu.getMax() / (double) BYTES_PER_GIGABYTE;
    return scale(gb);
  }

  public static long getAvailableMaxHeapMemory() {
    MemoryMXBean osBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage memoryUsage = osBean.getHeapMemoryUsage();
    long availableHeapBytes = memoryUsage.getMax() - memoryUsage.getUsed();
    return (availableHeapBytes + BYTES_PER_GIGABYTE - 1) / BYTES_PER_GIGABYTE;
  }
}

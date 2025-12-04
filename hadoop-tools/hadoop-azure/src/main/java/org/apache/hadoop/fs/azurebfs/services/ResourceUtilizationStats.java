/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HUNDRED_D;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.NO_ACTION_NEEDED;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.NO_SCALE_DOWN_AT_MIN;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.NO_SCALE_UP_AT_MAX;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.SCALE_DIRECTION_DOWN;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.SCALE_DIRECTION_NO_ACTION_NEEDED;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.SCALE_DIRECTION_NO_DOWN_AT_MIN;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.SCALE_DIRECTION_NO_UP_AT_MAX;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.SCALE_DIRECTION_UP;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.SCALE_DOWN;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.SCALE_NONE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.SCALE_UP;

/**
 * Represents current statistics of the thread pool and system.
 */
public abstract class ResourceUtilizationStats {

  private final int currentPoolSize;  // Current number of threads in the pool
  private final int maxPoolSize;        // Maximum allowed pool size
  private final int activeThreads;    // Number of threads currently executing tasks
  private final int idleThreads;        // Number of threads not executing tasks
  private final double jvmCpuLoad;    // Current JVM CPU utilization (%)
  private final double systemCpuUtilization; // Current system CPU utilization (%)
  private final double availableHeapGB;       // Available heap memory (GB)
  private final double committedHeapGB;  // Total committed heap memory (GB)
  private final double usedHeapGB;        // Used heap memory (GB)
  private final double maxHeapGB;         // Max heap memory (GB)
  private final double memoryLoad;  // Heap usage ratio (used/max)
  private final String lastScaleDirection;  // Last resize direction: "I" (increase) or "D" (decrease)
  private final double maxCpuUtilization; // Peak JVM CPU observed in the current interval
  private final long jvmProcessId;   // JVM Process ID

  /**
   * Constructs a {@link ResourceUtilizationStats} instance containing thread pool
   * metrics and JVM/system resource utilization details.
   *
   * @param currentPoolSize the current number of threads in the pool
   * @param maxPoolSize the maximum number of threads permitted in the pool
   * @param activeThreads the number of threads actively executing tasks
   * @param idleThreads the number of idle threads in the pool
   * @param jvmCpuLoad the current JVM CPU load (0.0–1.0)
   * @param systemCpuUtilization the current system-wide CPU utilization (0.0–1.0)
   * @param availableHeapGB the available JVM memory in gigabytes
   * @param committedHeapGB the committed heap memory in gigabytes
   * @param usedHeapGB the available heap memory in gigabytes
   * @param maxHeapGB the committed heap memory in gigabytes
   * @param memoryLoad the JVM memory load (used / max)
   * @param lastScaleDirection the last scaling action performed: "I" (increase),
   * "D" (decrease), or empty if no scaling occurred
   * @param maxCpuUtilization the peak JVM CPU utilization observed during this interval
   * @param jvmProcessId the process ID of the JVM
   */
  public ResourceUtilizationStats(int currentPoolSize,
      int maxPoolSize, int activeThreads, int idleThreads,
      double jvmCpuLoad, double systemCpuUtilization, double availableHeapGB,
      double committedHeapGB, double usedHeapGB, double maxHeapGB, double memoryLoad, String lastScaleDirection,
      double maxCpuUtilization, long jvmProcessId) {
    this.currentPoolSize = currentPoolSize;
    this.maxPoolSize = maxPoolSize;
    this.activeThreads = activeThreads;
    this.idleThreads = idleThreads;
    this.jvmCpuLoad = jvmCpuLoad;
    this.systemCpuUtilization = systemCpuUtilization;
    this.availableHeapGB = availableHeapGB;
    this.committedHeapGB = committedHeapGB;
    this.usedHeapGB = usedHeapGB;
    this.maxHeapGB = maxHeapGB;
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
  public double getMemoryUtilization() {
    return availableHeapGB;
  }

  /** @return the total committed heap memory in gigabytes */
  public double getCommittedHeapGB() {
    return committedHeapGB;
  }

  /** @return the used heap memory in gigabytes */
  public double getUsedHeapGB() {
    return usedHeapGB;
  }

  /** @return the max heap memory in gigabytes */
  public double getMaxHeapGB() {
    return maxHeapGB;
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
    case SCALE_DIRECTION_UP:
      return SCALE_UP;    // Scaled up
    case SCALE_DIRECTION_DOWN:
      return SCALE_DOWN;   // Scaled down
    case SCALE_DIRECTION_NO_DOWN_AT_MIN:
      return NO_SCALE_DOWN_AT_MIN;   // Attempted down-scale, already at minimum
    case SCALE_DIRECTION_NO_UP_AT_MAX:
      return NO_SCALE_UP_AT_MAX;    // Attempted up-scale, already at maximum
    case SCALE_DIRECTION_NO_ACTION_NEEDED:
      return NO_ACTION_NEEDED; // No action needed
    default:
      return SCALE_NONE;  // No scaling
    }
  }

  @Override
  public String toString() {
    return String.format(
        "currentPoolSize=%d, maxPoolSize=%d, activeThreads=%d, idleThreads=%d, "
            + "jvmCpuLoad=%.2f%%, systemCpuUtilization=%.2f%%, "
            + "availableHeap=%.2fGB, committedHeap=%.2fGB, memoryLoad=%.2f%%, "
            + "scaleDirection=%s, maxCpuUtilization=%.2f%%, jvmProcessId=%d",
        currentPoolSize, maxPoolSize, activeThreads,
        idleThreads, jvmCpuLoad * HUNDRED_D, systemCpuUtilization * HUNDRED_D,
        availableHeapGB, committedHeapGB, memoryLoad * HUNDRED_D,
        lastScaleDirection, maxCpuUtilization * HUNDRED_D, jvmProcessId
    );
  }
}

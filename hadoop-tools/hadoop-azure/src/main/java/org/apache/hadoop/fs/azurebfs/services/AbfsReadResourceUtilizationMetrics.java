
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

import org.apache.hadoop.fs.azurebfs.enums.AbfsReadResourceUtilizationMetricsEnum;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;

/**
 * Metrics container for the ABFS read thread pool.
 * <p>
 * This class captures thread-pool sizing, CPU utilization, memory usage,
 * scaling direction, and other runtime statistics reported by
 * {@link ReadBufferManagerV2.ReadThreadPoolStats}.
 * </p>
 */
public class AbfsReadResourceUtilizationMetrics
    extends
    AbstractAbfsResourceUtilizationMetrics<AbfsReadResourceUtilizationMetricsEnum> {

  /**
   * Creates a metrics set for read operations, initializing all
   * metric keys defined in {@link AbfsReadResourceUtilizationMetricsEnum}.
   */
  public AbfsReadResourceUtilizationMetrics() {
    super(AbfsReadResourceUtilizationMetricsEnum.values(), FSOperationType.READ.toString());
  }

  /**
   * Updates all read-thread-pool metrics using the latest stats snapshot.
   * <p>
   * Each value from {@link ReadBufferManagerV2.ReadThreadPoolStats} is
   * mapped to the corresponding metric, including:
   * </p>
   * <ul>
   *   <li>Thread pool size (current, max, active, idle)</li>
   *   <li>JVM and system CPU load (converted to percentage)</li>
   *   <li>Available and committed memory</li>
   *   <li>Memory load percentage</li>
   *   <li>Scaling direction</li>
   *   <li>Maximum CPU utilization observed</li>
   *   <li>JVM process ID</li>
   * </ul>
   *
   * @param stats the latest read-thread-pool statistics; ignored if {@code null}
   */
  public synchronized void update(ReadBufferManagerV2.ReadThreadPoolStats stats) {
    if (stats == null) {
      return;
    }

    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.CURRENT_POOL_SIZE, stats.getCurrentPoolSize());
    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.MAX_POOL_SIZE, stats.getMaxPoolSize());
    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.ACTIVE_THREADS, stats.getActiveThreads());
    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.IDLE_THREADS, stats.getIdleThreads());
    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.JVM_CPU_UTILIZATION, stats.getJvmCpuLoad() * HUNDRED_D);
    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.SYSTEM_CPU_UTILIZATION, stats.getSystemCpuUtilization() * HUNDRED_D);
    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.AVAILABLE_MEMORY, stats.getMemoryUtilization());
    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.COMMITTED_MEMORY, stats.getCommittedHeapGB());
    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.USED_MEMORY, stats.getUsedHeapGB());
    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.MAX_HEAP_MEMORY, stats.getMaxHeapGB());
    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.MEMORY_LOAD, stats.getMemoryLoad() * HUNDRED_D);
    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.LAST_SCALE_DIRECTION,
        stats.getLastScaleDirectionNumeric(stats.getLastScaleDirection()));
    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.MAX_CPU_UTILIZATION, stats.getMaxCpuUtilization() * HUNDRED_D);
    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.JVM_PROCESS_ID, stats.getJvmProcessId());

    markUpdated();
  }
}

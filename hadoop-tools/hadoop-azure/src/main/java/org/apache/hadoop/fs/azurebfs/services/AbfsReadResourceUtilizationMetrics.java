
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

import java.util.concurrent.atomic.AtomicLong;

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
   * A version counter incremented each time a metric update occurs.
   * Used to detect whether metrics have changed since the last serialization.
   */
  private final AtomicLong updateVersion = new AtomicLong(0);

  /**
   * The last version number that was serialized and pushed out.
   */
  private final AtomicLong lastPushedVersion = new AtomicLong(0);

  @Override
  protected boolean isUpdated() {
    return updateVersion.get() > lastPushedVersion.get();
  }

  protected synchronized void markUpdated() {
    updateVersion.incrementAndGet();
  }

  @Override
  protected long getUpdateVersion() {
    return updateVersion.get();
  }

  @Override
  protected long getLastPushedVersion() {
    return lastPushedVersion.get();
  }

  /**
   * Creates a metrics set for read operations, initializing all
   * metric keys defined in {@link AbfsReadResourceUtilizationMetricsEnum}.
   */
  public AbfsReadResourceUtilizationMetrics() {
    super(AbfsReadResourceUtilizationMetricsEnum.values(), FSOperationType.READ.toString());
  }

  /**
   * Marks the current metrics version as pushed.
   * Must be called only after the metrics string is actually emitted.
   */
  @Override
  public synchronized void markPushed() {
    lastPushedVersion.set(updateVersion.get());
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
    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.JVM_CPU_UTILIZATION, stats.getJvmCpuLoad());
    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.SYSTEM_CPU_UTILIZATION, stats.getSystemCpuUtilization());
    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.AVAILABLE_MEMORY, stats.getMemoryUtilization());
    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.COMMITTED_MEMORY, stats.getCommittedHeapGB());
    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.USED_MEMORY, stats.getUsedHeapGB());
    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.MAX_HEAP_MEMORY, stats.getMaxHeapGB());
    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.MEMORY_LOAD, stats.getMemoryLoad());
    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.LAST_SCALE_DIRECTION,
        stats.getLastScaleDirectionNumeric(stats.getLastScaleDirection()));
    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.MAX_CPU_UTILIZATION, stats.getMaxCpuUtilization());
    setMetricValue(AbfsReadResourceUtilizationMetricsEnum.JVM_PROCESS_ID, stats.getJvmProcessId());

    markUpdated();
  }
}

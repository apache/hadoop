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

import org.apache.hadoop.fs.azurebfs.enums.AbfsWriteResourceUtilizationMetricsEnum;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.WriteThreadPoolSizeManager;

/**
 * Metrics container for the ABFS write thread pool.
 * <p>
 * This class records pool size, CPU utilization, memory usage,
 * scaling direction, and other runtime indicators reported by
 * {@link WriteThreadPoolSizeManager.WriteThreadPoolStats}.
 * </p>
 */
public class AbfsWriteResourceUtilizationMetrics
    extends
    AbstractAbfsResourceUtilizationMetrics<AbfsWriteResourceUtilizationMetricsEnum> {

  /**
   * A version counter incremented each time a metric update occurs.
   * Used to detect whether metrics have changed since the last serialization.
   */
  private final AtomicLong updateVersion = new AtomicLong(0);

  /**
   * The last version number that was serialized and pushed out.
   */
  private final AtomicLong lastPushedVersion = new AtomicLong(0);

  /**
   * Creates a metrics set for write operations, pre-initializing
   * all metric keys defined in {@link AbfsWriteResourceUtilizationMetricsEnum}.
   */
  public AbfsWriteResourceUtilizationMetrics() {
    super(AbfsWriteResourceUtilizationMetricsEnum.values(), FSOperationType.WRITE.toString());
  }

  @Override
  protected boolean isUpdated() {
    return updateVersion.get() > lastPushedVersion.get();
  }

  protected void markUpdated() {
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
   * Marks the current metrics version as pushed.
   * Must be called only after the metrics string is actually emitted.
   */
  @Override
  public synchronized void markPushed() {
    lastPushedVersion.set(updateVersion.get());
  }

  /**
   * Updates all write-thread-pool metrics using the latest stats snapshot.
   * Each field in {@link WriteThreadPoolSizeManager.WriteThreadPoolStats}
   * is mapped to a corresponding metric.
   *
   * @param stats the latest thread-pool statistics; ignored if {@code null}
   */
  public synchronized void update(WriteThreadPoolSizeManager.WriteThreadPoolStats stats) {
    if (stats == null) {
      return;
    }

    setMetricValue(AbfsWriteResourceUtilizationMetricsEnum.CURRENT_POOL_SIZE, stats.getCurrentPoolSize());
    setMetricValue(AbfsWriteResourceUtilizationMetricsEnum.MAX_POOL_SIZE, stats.getMaxPoolSize());
    setMetricValue(AbfsWriteResourceUtilizationMetricsEnum.ACTIVE_THREADS, stats.getActiveThreads());
    setMetricValue(AbfsWriteResourceUtilizationMetricsEnum.IDLE_THREADS, stats.getIdleThreads());
    setMetricValue(AbfsWriteResourceUtilizationMetricsEnum.JVM_CPU_UTILIZATION, stats.getJvmCpuLoad());
    setMetricValue(AbfsWriteResourceUtilizationMetricsEnum.SYSTEM_CPU_UTILIZATION, stats.getSystemCpuUtilization());
    setMetricValue(AbfsWriteResourceUtilizationMetricsEnum.AVAILABLE_MEMORY, stats.getMemoryUtilization());
    setMetricValue(AbfsWriteResourceUtilizationMetricsEnum.COMMITTED_MEMORY, stats.getCommittedHeapGB());
    setMetricValue(AbfsWriteResourceUtilizationMetricsEnum.USED_MEMORY, stats.getUsedHeapGB());
    setMetricValue(AbfsWriteResourceUtilizationMetricsEnum.MAX_HEAP_MEMORY, stats.getMaxHeapGB());
    setMetricValue(AbfsWriteResourceUtilizationMetricsEnum.MEMORY_LOAD, stats.getMemoryLoad());
    setMetricValue(AbfsWriteResourceUtilizationMetricsEnum.LAST_SCALE_DIRECTION,
        stats.getLastScaleDirectionNumeric(stats.getLastScaleDirection()));
    setMetricValue(AbfsWriteResourceUtilizationMetricsEnum.MAX_CPU_UTILIZATION, stats.getMaxCpuUtilization());
    setMetricValue(AbfsWriteResourceUtilizationMetricsEnum.JVM_PROCESS_ID, stats.getJvmProcessId());

    markUpdated();
  }
}

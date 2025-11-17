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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_EQUALS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HUNDRED_D;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.CHAR_DOLLAR;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

import org.apache.hadoop.fs.azurebfs.enums.AbfsWriteThreadPoolMetricsEnum;
import org.apache.hadoop.fs.azurebfs.WriteThreadPoolSizeManager;


/**
 * Collects and updates metrics related to the ABFS Write Thread Pool.
 */
public class AbfsWriteThreadPoolMetrics extends AbstractAbfsStatisticsSource {

  private static final Logger LOG = LoggerFactory.getLogger(AbfsWriteThreadPoolMetrics.class);

  /* Flag indicating whether metrics have been updated at least once. */
  private final AtomicBoolean updatedAtLeastOnce = new AtomicBoolean(false);

  /* Tracks the current version of metric updates. */
  private final AtomicLong updateVersion = new AtomicLong(0);

  /* Tracks the last version of metrics that was pushed or reported. */
  private final AtomicLong lastPushedVersion = new AtomicLong(-1);

  /**
   * Initializes the IOStatistics store for write thread pool metrics,
   * registering all gauge-type metrics for monitoring.
   */
  public AbfsWriteThreadPoolMetrics() {
    IOStatisticsStore ioStatisticsStore = iostatisticsStore()
        .withGauges(getMetricNames(StatisticTypeEnum.TYPE_GAUGE))
        .build();
    setIOStatistics(ioStatisticsStore);
  }

  /**
   * Returns the list of metric names corresponding to the specified statistic type.
   * Filters the {@link AbfsWriteThreadPoolMetricsEnum} values by their
   * {@link StatisticTypeEnum} and collects the matching metric names.
   *
   * @param type the {@link StatisticTypeEnum} used to filter metrics (e.g., gauge or counter).
   * @return an array of metric names matching the given statistic type.
   */
  private String[] getMetricNames(StatisticTypeEnum type) {
    return Arrays.stream(AbfsWriteThreadPoolMetricsEnum.values())
        .filter(metricEnum -> metricEnum.getStatisticType().equals(type))
        .flatMap(metricEnum -> Stream.of(metricEnum.getName()))
        .toArray(String[]::new);
  }

  /**
   * Sets the metric value for the specified write thread pool metric.
   * Depending on the metric’s statistic type, the value is recorded either as a gauge
   * (representing the current state) or as a counter (representing a cumulative count).
   *
   * @param metric the {@link AbfsWriteThreadPoolMetricsEnum} representing the metric to update.
   * @param value  the value to assign to the metric.
   */
  public void setMetricValue(AbfsWriteThreadPoolMetricsEnum metric, double value) {
    switch (metric.getStatisticType()) {
    case TYPE_GAUGE:
      setGaugeValue(metric.getName(), (long) value);
      break;
    case TYPE_COUNTER:
      setCounterValue(metric.getName(), (long) value);
      break;
    default:
      LOG.warn("Unsupported metric type: {}", metric.getStatisticType());
    }
  }

  /**
   * Updates the write thread pool metrics using the provided statistics snapshot.
   * This synchronized method ensures thread-safe updates of metrics including current pool size,
   * maximum pool size, active threads, JVM CPU utilization, overall CPU utilization, and
   * available memory. Once updated, it marks the metrics as refreshed and increments the
   * internal version counter for tracking metric changes.
   *
   * @param stats the {@link WriteThreadPoolSizeManager.WriteThreadPoolStats} instance containing
   *              the latest thread pool and system statistics; ignored if {@code null}.
   */
  public synchronized void update(WriteThreadPoolSizeManager.WriteThreadPoolStats stats) {
    if (stats == null) {
      LOG.warn("Attempted to update WriteThreadPoolMetrics with null stats");
      return;
    }
    setMetricValue(AbfsWriteThreadPoolMetricsEnum.CURRENT_POOL_SIZE, stats.getCurrentPoolSize());
    setMetricValue(AbfsWriteThreadPoolMetricsEnum.MAX_POOL_SIZE, stats.getMaxPoolSize());
    setMetricValue(AbfsWriteThreadPoolMetricsEnum.ACTIVE_THREADS, stats.getActiveThreads());
    setMetricValue(AbfsWriteThreadPoolMetricsEnum.JVM_CPU_UTILIZATION, stats.getJvmCpuUtilization());
    setMetricValue(AbfsWriteThreadPoolMetricsEnum.JVM_CPU_LOAD, stats.getJvmCpuLoad() * HUNDRED_D);
    setMetricValue(AbfsWriteThreadPoolMetricsEnum.JVM_CPU_LOAD_OSHI, stats.getJvmLoadOshi() * HUNDRED_D);
    setMetricValue(AbfsWriteThreadPoolMetricsEnum.CPU_UTILIZATION, (stats.getCpuUtilization() * HUNDRED_D));
    setMetricValue(AbfsWriteThreadPoolMetricsEnum.MEMORY_UTILIZATION, stats.getMemoryUtilization());
    setMetricValue(AbfsWriteThreadPoolMetricsEnum.LAST_SCALE_DIRECTION, stats.getLastScaleDirectionNumeric(
        stats.getLastScaleDirection()));
    updatedAtLeastOnce.set(true);
    updateVersion.incrementAndGet();
  }

  /**
   * Returns metrics as a string only once per update version.
   */
  @Override
  public String toString() {
    if (!updatedAtLeastOnce.get()) {
      return EMPTY_STRING;
    }
    long currentVersion = updateVersion.get();
    if (currentVersion == lastPushedVersion.get()) {
      return EMPTY_STRING;
    }
    synchronized (this) {
      // double check for thread safety
      if (currentVersion == lastPushedVersion.get()) {
        return EMPTY_STRING;
      }
      StringBuilder sb = new StringBuilder(String.valueOf(FSOperationType.WRITE)).append(CHAR_EQUALS);
      for (AbfsWriteThreadPoolMetricsEnum metric : AbfsWriteThreadPoolMetricsEnum.values()) {
        sb.append(metric.getName())
            .append(CHAR_EQUALS)
            .append(lookupGaugeValue(metric.getName()))
            .append(CHAR_DOLLAR);
      }
      lastPushedVersion.set(currentVersion); // mark this version as pushed
      return sb.toString();
    }
  }
}

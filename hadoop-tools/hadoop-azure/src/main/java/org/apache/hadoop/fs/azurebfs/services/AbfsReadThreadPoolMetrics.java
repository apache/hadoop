
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
import org.apache.hadoop.fs.azurebfs.enums.AbfsReadThreadPoolMetricsEnum;
import org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_EQUALS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HUNDRED_D;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.CHAR_DOLLAR;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

/**
 * Collects and updates metrics related to the ABFS Read Thread Pool.
 */
public class AbfsReadThreadPoolMetrics extends AbstractAbfsStatisticsSource {

  private static final Logger LOG = LoggerFactory.getLogger(AbfsReadThreadPoolMetrics.class);

  /* Flag indicating whether metrics have been updated at least once. */
  private final AtomicBoolean updatedAtLeastOnce = new AtomicBoolean(false);

  /* Tracks the current version of metric updates. */
  private final AtomicLong updateVersion = new AtomicLong(0);

  /* Tracks the last version of metrics that was pushed or reported. */
  private final AtomicLong lastPushedVersion = new AtomicLong(-1);

  /**
   * Initializes the IOStatistics store for read thread pool metrics,
   * registering all gauge-type metrics for monitoring.
   */
  public AbfsReadThreadPoolMetrics() {
    IOStatisticsStore ioStatisticsStore = iostatisticsStore()
        .withGauges(getMetricNames(StatisticTypeEnum.TYPE_GAUGE))
        .build();
    setIOStatistics(ioStatisticsStore);
  }

  /**
   * Returns the list of metric names corresponding to the given statistic type.
   * Filters all available read thread pool metrics and collects names of those
   * matching the specified {@link StatisticTypeEnum}.
   *
   * @param type the type of statistic (e.g., gauge or counter).
   * @return an array of metric names matching the given type.
   */
  private String[] getMetricNames(StatisticTypeEnum type) {
    return Arrays.stream(AbfsReadThreadPoolMetricsEnum.values())
        .filter(metricEnum -> metricEnum.getStatisticType().equals(type))
        .flatMap(metricEnum -> Stream.of(metricEnum.getName()))
        .toArray(String[]::new);
  }

  /**
   * Updates the metric value for the specified read thread pool metric.
   * Depending on the {@link StatisticTypeEnum} of the metric, this method sets
   * the corresponding gauge or counter value.
   *
   * @param metric the {@link AbfsReadThreadPoolMetricsEnum} representing the metric to update.
   * @param value  the new value to assign to the metric.
   */
  public void setMetricValue(AbfsReadThreadPoolMetricsEnum metric, double value) {
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
   * Updates the read thread pool metrics using the provided statistics snapshot.
   * It records values such as current pool size, maximum pool size, active threads, JVM CPU usage,
   * system CPU usage, and available memory.
   *
   * @param stats the {@link ReadBufferManagerV2.ReadThreadPoolStats} instance containing
   *              the latest thread pool and system statistics; ignored if {@code null}.
   */
  public synchronized void update(ReadBufferManagerV2.ReadThreadPoolStats stats) {
    if (stats == null) {
      LOG.warn("Attempted to update WriteThreadPoolMetrics with null stats");
      return;
    }
    setMetricValue(AbfsReadThreadPoolMetricsEnum.CURRENT_POOL_SIZE, stats.getCurrentPoolSize());
    setMetricValue(AbfsReadThreadPoolMetricsEnum.MAX_POOL_SIZE, stats.getMaxPoolSize());
    setMetricValue(AbfsReadThreadPoolMetricsEnum.ACTIVE_THREADS, stats.getActiveThreads());
    setMetricValue(AbfsReadThreadPoolMetricsEnum.JVM_CPU_UTILIZATION, stats.getJvmCpuUtilization());
    setMetricValue(AbfsReadThreadPoolMetricsEnum.CPU_UTILIZATION, (stats.getCpuUtilization() * HUNDRED_D));
    setMetricValue(AbfsReadThreadPoolMetricsEnum.MEMORY_UTILIZATION, stats.getMemoryUtilization());
    updatedAtLeastOnce.set(true);
    updateVersion.incrementAndGet();
  }

  /**
   * Returns a flag indicating whether the metrics have been updated at least once.
   * Used to verify if metric updates have occurred since initialization.
   */
  public boolean getUpdatedAtLeastOnce() {
    return updatedAtLeastOnce.get();
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
      StringBuilder sb = new StringBuilder(String.valueOf(FSOperationType.READ)).append(CHAR_EQUALS);
      for (AbfsReadThreadPoolMetricsEnum metric : AbfsReadThreadPoolMetricsEnum.values()) {
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

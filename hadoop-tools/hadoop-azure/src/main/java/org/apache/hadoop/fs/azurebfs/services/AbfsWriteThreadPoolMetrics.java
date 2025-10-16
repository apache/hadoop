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
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HUNDRED_D;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

import org.apache.hadoop.fs.azurebfs.enums.AbfsWriteThreadPoolMetricsEnum;
import org.apache.hadoop.fs.azurebfs.WriteThreadPoolSizeManager;


/**
 * Collects and updates metrics related to the ABFS Write Thread Pool.
 */
public class AbfsWriteThreadPoolMetrics extends AbstractAbfsStatisticsSource {

  private static final Logger LOG = LoggerFactory.getLogger(AbfsWriteThreadPoolMetrics.class);
  private final AtomicBoolean updatedAtLeastOnce = new AtomicBoolean(false);

  public AbfsWriteThreadPoolMetrics() {
    IOStatisticsStore ioStatisticsStore = iostatisticsStore()
        .withGauges(getMetricNames(StatisticTypeEnum.TYPE_GAUGE))
        .build();
    setIOStatistics(ioStatisticsStore);
  }

  /**
   * Retrieves metric names based on the statistic type.
   */
  private String[] getMetricNames(StatisticTypeEnum type) {
    return Arrays.stream(AbfsWriteThreadPoolMetricsEnum.values())
        .filter(metricEnum -> metricEnum.getStatisticType().equals(type))
        .flatMap(metricEnum -> Stream.of(metricEnum.getName()))
        .toArray(String[]::new);
  }

  /**
   * Sets the metric value for a given enum.
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
   * Updates the thread pool metrics from the given stats.
   */
  public void update(WriteThreadPoolSizeManager.WriteThreadPoolStats stats) {
    if (stats == null) {
      LOG.warn("Attempted to update WriteThreadPoolMetrics with null stats");
      return;
    }
    setMetricValue(AbfsWriteThreadPoolMetricsEnum.CURRENT_POOL_SIZE, stats.getCurrentPoolSize());
    setMetricValue(AbfsWriteThreadPoolMetricsEnum.MAX_POOL_SIZE, stats.getMaxPoolSize());
    setMetricValue(AbfsWriteThreadPoolMetricsEnum.ACTIVE_THREADS, stats.getActiveThreads());
    setMetricValue(AbfsWriteThreadPoolMetricsEnum.CPU_UTILIZATION, (stats.getCpuUtilization() * HUNDRED_D));
    setMetricValue(AbfsWriteThreadPoolMetricsEnum.MEMORY_UTILIZATION, stats.getMemoryUtilization());
    updatedAtLeastOnce.set(true);
  }

  public void reset() {
    IOStatisticsStore ioStatisticsStore = iostatisticsStore()
        .withGauges(getMetricNames(StatisticTypeEnum.TYPE_GAUGE))
        .build();
    setIOStatistics(ioStatisticsStore);
  }

  @Override
  public String toString() {
    if (!updatedAtLeastOnce.get()) {
      return EMPTY_STRING;
    }
    StringBuilder sb = new StringBuilder("Write");
    for (AbfsWriteThreadPoolMetricsEnum metric : AbfsWriteThreadPoolMetricsEnum.values()) {
      sb.append(metric.getName())
          .append("=")
          .append(lookupGaugeValue(metric.getName()))
          .append(" ");
    }
    return sb.toString();
  }
}
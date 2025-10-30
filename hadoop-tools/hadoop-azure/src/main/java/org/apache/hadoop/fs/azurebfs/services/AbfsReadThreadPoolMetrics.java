
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

import org.apache.hadoop.fs.azurebfs.enums.AbfsReadThreadPoolMetricsEnum;
import org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_EQUALS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_HUNDRED;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.CHAR_DOLLAR;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

/**
 * Collects and updates metrics related to the ABFS Write Thread Pool.
 */
public class AbfsReadThreadPoolMetrics extends AbstractAbfsStatisticsSource {

  private static final Logger LOG = LoggerFactory.getLogger(AbfsReadThreadPoolMetrics.class);
  private final AtomicBoolean updatedAtLeastOnce = new AtomicBoolean(false);
  private final AtomicLong updateVersion = new AtomicLong(0);
  private final AtomicLong lastPushedVersion = new AtomicLong(-1);

  public AbfsReadThreadPoolMetrics() {
    IOStatisticsStore ioStatisticsStore = iostatisticsStore()
        .withGauges(getMetricNames(StatisticTypeEnum.TYPE_GAUGE))
        .build();
    setIOStatistics(ioStatisticsStore);
  }

  /**
   * Retrieves metric names based on the statistic type.
   */
  private String[] getMetricNames(StatisticTypeEnum type) {
    return Arrays.stream(AbfsReadThreadPoolMetricsEnum.values())
        .filter(metricEnum -> metricEnum.getStatisticType().equals(type))
        .flatMap(metricEnum -> Stream.of(metricEnum.getName()))
        .toArray(String[]::new);
  }

  /**
   * Sets the metric value for a given enum.
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
   * Updates the thread pool metrics from the given stats.
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
    setMetricValue(AbfsReadThreadPoolMetricsEnum.CPU_UTILIZATION, (stats.getCpuUtilization() * ONE_HUNDRED));
    setMetricValue(AbfsReadThreadPoolMetricsEnum.MEMORY_UTILIZATION, stats.getMemoryUtilization());
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
      StringBuilder sb = new StringBuilder("RE").append(CHAR_EQUALS);
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
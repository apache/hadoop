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
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.enums.AbfsReadResourceUtilizationMetricsEnum;
import org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum;
import org.apache.hadoop.fs.azurebfs.enums.AbfsResourceUtilizationMetricsEnum;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_EQUALS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HUNDRED_D;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.CHAR_DOLLAR;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

/**
 * Abstract base class for tracking ABFS resource metrics, handling metric registration,
 * updates, versioning, and compact serialization for diagnostics.
 *
 * @param <T> enum type implementing {@link AbfsResourceUtilizationMetricsEnum}
 */
public abstract class AbstractAbfsResourceUtilizationMetrics<T extends Enum<T> & AbfsResourceUtilizationMetricsEnum>
    extends AbstractAbfsStatisticsSource {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractAbfsResourceUtilizationMetrics.class);

  protected abstract boolean isUpdated();
  protected abstract long getUpdateVersion();
  protected abstract long getLastPushedVersion();
  public abstract void markPushed();

  /**
   * The set of metrics supported by this metrics instance.
   */
  private final T[] metrics;

  /**
   * A short identifier describing the operation or subsystem these metrics represent.
   * This prefix appears in the serialized results string.
   */
  private final String operationType;

  /**
   * Constructs the resource metrics abstraction.
   * Registers gauges (and later counters) with the Hadoop {@link IOStatisticsStore}
   * based on the metric enum values.
   *
   * @param metrics        all metric enum constants supported by this instance
   * @param operationType  a short label used as the prefix when serializing metrics
   */
  protected AbstractAbfsResourceUtilizationMetrics(T[] metrics, String operationType) {
    this.metrics = metrics;
    this.operationType = operationType;

    IOStatisticsStore store = iostatisticsStore()
        .withGauges(getMetricNames(StatisticTypeEnum.TYPE_GAUGE))
        .build();
    setIOStatistics(store);
  }

  /**
   * Extracts the names of metrics of the specified type.
   *
   * @param type the type of metrics to return (e.g., gauge, counter)
   * @return an array of metric names of the given type
   */
  private String[] getMetricNames(StatisticTypeEnum type) {
    return Arrays.stream(metrics)
        .filter(m -> m.getStatisticType().equals(type))
        .flatMap(m -> Stream.of(m.getName()))
        .toArray(String[]::new);
  }

  /**
   * Sets the value of a metric using its configured statistic type.
   * <ul>
   *   <li>For {@code TYPE_GAUGE}, the value overwrites the existing gauge value.</li>
   *   <li>For {@code TYPE_COUNTER}, the value increments the counter.</li>
   * </ul>
   *
   * @param metric the metric to update
   * @param value  the numeric value to assign or increment
   */
  protected void setMetricValue(T metric, double value) {
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

  @Override
  public String toString() {
    if (!isUpdated()) {
      return EMPTY_STRING;
    }

    long currentVersion = getUpdateVersion();
    if (currentVersion == getLastPushedVersion()) {
      return EMPTY_STRING;
    }

    StringBuilder sb = new StringBuilder(operationType).append(CHAR_EQUALS);

    for (T metric : metrics) {
      long value = lookupGaugeValue(metric.getName());

      if (isMemoryMetric(metric.getName())) {
        sb.append(metric.getName())
            .append(CHAR_EQUALS)
            .append(convertMemoryValue(value))
            .append(CHAR_DOLLAR);
      } else {
        sb.append(metric.getName())
            .append(CHAR_EQUALS)
            .append(value)
            .append(CHAR_DOLLAR);
      }
    }

    return sb.toString();
  }

  /**
   * Determines whether the given metric represents a JVM heap memory metric.
   *
   * <p>Memory metrics are identified by their names as defined in
   * {@link AbfsReadResourceUtilizationMetricsEnum}.</p>
   *
   * @param metricName the metric name
   * @return {@code true} if the metric is a memory metric, {@code false} otherwise
   */
  private boolean isMemoryMetric(String metricName) {
    return metricName.equals(
        AbfsReadResourceUtilizationMetricsEnum.AVAILABLE_MEMORY.getName())
        || metricName.equals(
        AbfsReadResourceUtilizationMetricsEnum.COMMITTED_MEMORY.getName())
        || metricName.equals(
        AbfsReadResourceUtilizationMetricsEnum.USED_MEMORY.getName())
        || metricName.equals(
        AbfsReadResourceUtilizationMetricsEnum.MAX_HEAP_MEMORY.getName());
  }

  /**
   * Converts a raw memory metric value to a decimal representation for logging.
   *
   * @param value the raw memory value
   * @return the converted memory value
   */
  private double convertMemoryValue(long value) {
    return value / HUNDRED_D;
  }
}

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
import java.util.List;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum;
import org.apache.hadoop.fs.azurebfs.enums.RetryValue;
import org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COLON;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EQUAL;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.THOUSAND;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.DOUBLE_PRECISION_FORMAT;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.RETRY;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.REQUESTS;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.REQUEST_COUNT;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.SECONDS;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.MIN_MAX_AVERAGE;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.BANDWIDTH_THROTTLED_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.IOPS_THROTTLED_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.OTHER_THROTTLED_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.PERCENTAGE_THROTTLED_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.NETWORK_ERROR_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.SUCCESS_REQUESTS_WITHOUT_RETRY;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.FAILED_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.TOTAL_REQUESTS_COUNT;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.MAX_RETRY;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.MAX_BACK_OFF;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.MIN_BACK_OFF;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.MAX_RETRY_COUNT;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.NUMBER_OF_BANDWIDTH_THROTTLED_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.NUMBER_OF_IOPS_THROTTLED_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.NUMBER_OF_NETWORK_FAILED_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.NUMBER_OF_OTHER_THROTTLED_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.NUMBER_OF_REQUESTS_FAILED;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.NUMBER_OF_REQUESTS_SUCCEEDED;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.NUMBER_OF_REQUESTS_SUCCEEDED_WITHOUT_RETRYING;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.TOTAL_BACK_OFF;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.TOTAL_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.TOTAL_NUMBER_OF_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum.TYPE_COUNTER;
import static org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum.TYPE_GAUGE;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;
import static org.apache.hadoop.util.StringUtils.format;
import static org.apache.hadoop.util.StringUtils.formatPercent;

/**
 * This class is responsible for tracking and
 * updating metrics related to backoff and
 * retry operations in Azure Blob File System (ABFS).
 */
public class AbfsBackoffMetrics extends AbstractAbfsStatisticsSource {
  private static final Logger LOG = LoggerFactory.getLogger(AbfsBackoffMetrics.class);
  private static final List<RetryValue> RETRY_LIST = Arrays.asList(
          RetryValue.values());

  /**
   * Constructor to initialize the IOStatisticsStore with counters and gauges.
   */
  public AbfsBackoffMetrics() {
    IOStatisticsStore ioStatisticsStore = iostatisticsStore()
            .withCounters(getMetricNames(TYPE_COUNTER))
            .withGauges(getMetricNames(TYPE_GAUGE))
            .build();
    setIOStatistics(ioStatisticsStore);
  }

  /**
   * Retrieves the metric names based on the statistic type.
   *
   * @param type the type of the statistic (counter or gauge)
   * @return an array of metric names
   */
  private String[] getMetricNames(StatisticTypeEnum type) {
    return Arrays.stream(AbfsBackoffMetricsEnum.values())
            .filter(backoffMetricsEnum -> backoffMetricsEnum
                    .getStatisticType()
                    .equals(type))
            .flatMap(backoffMetricsEnum ->
                    RETRY.equals(backoffMetricsEnum.getType())
                            ? RETRY_LIST.stream().map(retryCount ->
                            getMetricName(backoffMetricsEnum, retryCount))
                            : Stream.of(backoffMetricsEnum.getName())
            ).toArray(String[]::new);
  }

  /**
   * Constructs the metric name based on the metric and retry value.
   *
   * @param metric the metric enum
   * @param retryValue the retry value
   * @return the constructed metric name
   */
  private String getMetricName(AbfsBackoffMetricsEnum metric, RetryValue retryValue) {
    if (metric == null) {
      LOG.error("ABFS Backoff Metric should not be null");
      return EMPTY_STRING;
    }
    if (RETRY.equals(metric.getType()) && retryValue != null) {
      return retryValue.getValue() + COLON + metric.getName();
    }
    return metric.getName();
  }

  /**
   * Retrieves the value of a specific metric.
   *
   * @param metric the metric enum
   * @param retryValue the retry value
   * @return the value of the metric
   */
  public long getMetricValue(AbfsBackoffMetricsEnum metric, RetryValue retryValue) {
    String metricName = getMetricName(metric, retryValue);
    switch (metric.getStatisticType()) {
      case TYPE_COUNTER:
        return lookupCounterValue(metricName);
      case TYPE_GAUGE:
        return lookupGaugeValue(metricName);
      default:
        return 0;
    }
  }

  /**
   * Retrieves the value of a specific metric.
   *
   * @param metric the metric enum
   * @return the value of the metric
   */
  public long getMetricValue(AbfsBackoffMetricsEnum metric) {
    return getMetricValue(metric, null);
  }

  /**
   * Increments the value of a specific metric.
   *
   * @param metric the metric enum
   * @param retryValue the retry value
   */
  public void incrementMetricValue(AbfsBackoffMetricsEnum metric, RetryValue retryValue) {
    String metricName = getMetricName(metric, retryValue);
    switch (metric.getStatisticType()) {
      case TYPE_COUNTER:
        incCounterValue(metricName);
        break;
      case TYPE_GAUGE:
        incGaugeValue(metricName);
        break;
      default:
        // Do nothing
        break;
    }
  }

  /**
   * Increments the value of a specific metric.
   *
   * @param metric the metric enum
   */
  public void incrementMetricValue(AbfsBackoffMetricsEnum metric) {
    incrementMetricValue(metric, null);
  }

  /**
   * Sets the value of a specific metric.
   *
   * @param metric the metric enum
   * @param value the new value of the metric
   * @param retryValue the retry value
   */
  public void setMetricValue(AbfsBackoffMetricsEnum metric, long value, RetryValue retryValue) {
    String metricName = getMetricName(metric, retryValue);
    switch (metric.getStatisticType()) {
      case TYPE_COUNTER:
        setCounterValue(metricName, value);
        break;
      case TYPE_GAUGE:
        setGaugeValue(metricName, value);
        break;
      default:
        // Do nothing
        break;
    }
  }

  /**
   * Sets the value of a specific metric.
   *
   * @param metric the metric enum
   * @param value the new value of the metric
   */
  public void setMetricValue(AbfsBackoffMetricsEnum metric, long value) {
    setMetricValue(metric, value, null);
  }

  /**
   * Get the precision metrics.
   *
   * @param metricName the metric name
   * @param retryCount the retry count
   * @param denominator the denominator
   * @return String metrics value with precision
   */
  private String getPrecisionMetrics(AbfsBackoffMetricsEnum metricName,
                                     RetryValue retryCount,
                                     long denominator) {
    return format(DOUBLE_PRECISION_FORMAT, (double) getMetricValue(metricName, retryCount) / denominator);
  }

  /**
   * Retrieves the retry metrics.
   *
   * @param metricBuilder the string builder to append the metrics
   */
  private void getRetryMetrics(StringBuilder metricBuilder) {
    for (RetryValue retryCount : RETRY_LIST) {
      long totalRequests = getMetricValue(TOTAL_REQUESTS, retryCount);
      metricBuilder.append(REQUEST_COUNT)
              .append(retryCount.getValue())
              .append(REQUESTS)
              .append(getMetricValue(NUMBER_OF_REQUESTS_SUCCEEDED, retryCount));

      if (totalRequests > 0) {
        metricBuilder.append(MIN_MAX_AVERAGE)
                .append(retryCount.getValue())
                .append(REQUESTS)
                .append(getPrecisionMetrics(MIN_BACK_OFF, retryCount, THOUSAND))
                .append(SECONDS)
                .append(getPrecisionMetrics(MAX_BACK_OFF, retryCount, THOUSAND))
                .append(SECONDS)
                .append(getPrecisionMetrics(TOTAL_BACK_OFF, retryCount, totalRequests * THOUSAND))
                .append(SECONDS);
      } else {
        metricBuilder.append(MIN_MAX_AVERAGE)
                .append(retryCount.getValue())
                .append(REQUESTS + EQUAL + 0 + SECONDS);
      }
    }
  }

  /**
   * Retrieves the base metrics.
   *
   * @param metricBuilder the string builder to append the metrics
   */
  private void getBaseMetrics(StringBuilder metricBuilder) {
    long totalRequestsThrottled = getMetricValue(NUMBER_OF_NETWORK_FAILED_REQUESTS)
            + getMetricValue(NUMBER_OF_IOPS_THROTTLED_REQUESTS)
            + getMetricValue(NUMBER_OF_OTHER_THROTTLED_REQUESTS)
            + getMetricValue(NUMBER_OF_BANDWIDTH_THROTTLED_REQUESTS);

    metricBuilder.append(BANDWIDTH_THROTTLED_REQUESTS)
            .append(getMetricValue(NUMBER_OF_BANDWIDTH_THROTTLED_REQUESTS))
            .append(IOPS_THROTTLED_REQUESTS)
            .append(getMetricValue(NUMBER_OF_IOPS_THROTTLED_REQUESTS))
            .append(OTHER_THROTTLED_REQUESTS)
            .append(getMetricValue(NUMBER_OF_OTHER_THROTTLED_REQUESTS))
            .append(PERCENTAGE_THROTTLED_REQUESTS)
            .append(formatPercent(totalRequestsThrottled/ (double) getMetricValue(TOTAL_NUMBER_OF_REQUESTS), 3))
            .append(NETWORK_ERROR_REQUESTS)
            .append(getMetricValue(NUMBER_OF_NETWORK_FAILED_REQUESTS))
            .append(SUCCESS_REQUESTS_WITHOUT_RETRY)
            .append(getMetricValue(NUMBER_OF_REQUESTS_SUCCEEDED_WITHOUT_RETRYING))
            .append(FAILED_REQUESTS)
            .append(getMetricValue(NUMBER_OF_REQUESTS_FAILED))
            .append(TOTAL_REQUESTS_COUNT)
            .append(getMetricValue(TOTAL_NUMBER_OF_REQUESTS))
            .append(MAX_RETRY)
            .append(getMetricValue(MAX_RETRY_COUNT));
  }

  /**
   * Retrieves the string representation of the metrics.
   *
   * @return the string representation of the metrics
   */
  @Override
  public String toString() {
    if (getMetricValue(TOTAL_NUMBER_OF_REQUESTS) == 0) {
      return EMPTY_STRING;
    }
    StringBuilder metricBuilder = new StringBuilder();
    getRetryMetrics(metricBuilder);
    getBaseMetrics(metricBuilder);
    return metricBuilder.toString();
  }

  /**
   * Retrieves the metric names based on the statistic type.
   *
   * @param type the type of the statistic (counter or gauge)
   * @return an array of metric names
   */
  @VisibleForTesting
  String[] getMetricNamesByType(StatisticTypeEnum type) {
    return getMetricNames(type);
  }
}

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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum;
import org.apache.hadoop.fs.azurebfs.enums.RetryValue;
import org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum;
import org.apache.hadoop.fs.azurebfs.statistics.AbstractAbfsStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HUNDRED;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.THOUSAND;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.RETRY;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.COLON;
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
import static org.apache.hadoop.fs.azurebfs.enums.RetryValue.ONE;
import static org.apache.hadoop.fs.azurebfs.enums.RetryValue.TWO;
import static org.apache.hadoop.fs.azurebfs.enums.RetryValue.THREE;
import static org.apache.hadoop.fs.azurebfs.enums.RetryValue.FOUR;
import static org.apache.hadoop.fs.azurebfs.enums.RetryValue.FIVE_FIFTEEN;
import static org.apache.hadoop.fs.azurebfs.enums.RetryValue.FIFTEEN_TWENTY_FIVE;
import static org.apache.hadoop.fs.azurebfs.enums.RetryValue.TWENTY_FIVE_AND_ABOVE;
import static org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum.TYPE_COUNTER;
import static org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum.TYPE_GAUGE;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

/**
 * This class is responsible for tracking and
 * updating metrics related to backoff and
 * retry operations in Azure Blob File System (ABFS).
 */
public class AbfsBackoffMetrics extends AbstractAbfsStatisticsSource {
  private static final List<RetryValue> RETRY_LIST = Arrays.asList(ONE, TWO, THREE, FOUR, FIVE_FIFTEEN, FIFTEEN_TWENTY_FIVE, TWENTY_FIVE_AND_ABOVE);

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
            .filter(backoffMetricsEnum -> backoffMetricsEnum.getStatisticType().equals(type))
            .flatMap(backoffMetricsEnum ->
                    RETRY.equals(backoffMetricsEnum.getType())
                            ? RETRY_LIST.stream().map(retryCount -> retryCount.getValue() + COLON + backoffMetricsEnum.getName())
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
    if (RETRY.equals(metric.getType())) {
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

  /*
  Acronyms :-
  1.RCTSI :- Request count that succeeded in x retries
  2.MMA :- Min Max Average (This refers to the backoff or sleep time between 2 requests)
  3.s :- seconds
  4.BWT :- Number of Bandwidth throttled requests
  5.IT :- Number of IOPS throttled requests
  6.OT :- Number of Other throttled requests
  7.NFR :- Number of requests which failed due to network errors
  8.%RT :- Percentage of requests that are throttled
  9.TRNR :- Total number of requests which succeeded without retrying
  10.TRF :- Total number of requests which failed
  11.TR :- Total number of requests which were made
  12.MRC :- Max retry count across all requests
   */
  @Override
  public String toString() {
    if (getMetricValue(TOTAL_NUMBER_OF_REQUESTS) == 0) {
      return "";
    }
    StringBuilder metricString = new StringBuilder();
    long totalRequestsThrottled = getMetricValue(NUMBER_OF_NETWORK_FAILED_REQUESTS)
            + getMetricValue(NUMBER_OF_IOPS_THROTTLED_REQUESTS)
            + getMetricValue(NUMBER_OF_OTHER_THROTTLED_REQUESTS)
            + getMetricValue(NUMBER_OF_BANDWIDTH_THROTTLED_REQUESTS);
    double percentageOfRequestsThrottled = ((double) totalRequestsThrottled / getMetricValue(TOTAL_NUMBER_OF_REQUESTS)) * HUNDRED;

    for (RetryValue retryCount : RETRY_LIST) {
      long totalRequests = getMetricValue(TOTAL_REQUESTS, retryCount);
      metricString.append("$RCTSI$_").append(retryCount.getValue()).append("R=").append(getMetricValue(NUMBER_OF_REQUESTS_SUCCEEDED, retryCount));
      if (totalRequests > 0) {
        metricString.append("$MMA$_").append(retryCount.getValue()).append("R=")
                .append(String.format("%.3f", (double) getMetricValue(MIN_BACK_OFF, retryCount) / THOUSAND)).append("s")
                .append(String.format("%.3f", (double) getMetricValue(MAX_BACK_OFF, retryCount) / THOUSAND)).append("s")
                .append(String.format("%.3f", (double) getMetricValue(TOTAL_BACK_OFF, retryCount) / totalRequests / THOUSAND)).append("s");
      } else {
        metricString.append("$MMA$_").append(retryCount.getValue()).append("R=0s");
      }
    }
    metricString.append("$BWT=").append(getMetricValue(NUMBER_OF_BANDWIDTH_THROTTLED_REQUESTS))
            .append("$IT=").append(getMetricValue(NUMBER_OF_IOPS_THROTTLED_REQUESTS))
            .append("$OT=").append(getMetricValue(NUMBER_OF_OTHER_THROTTLED_REQUESTS))
            .append("$RT=").append(String.format("%.3f", percentageOfRequestsThrottled))
            .append("$NFR=").append(getMetricValue(NUMBER_OF_NETWORK_FAILED_REQUESTS))
            .append("$TRNR=").append(getMetricValue(NUMBER_OF_REQUESTS_SUCCEEDED_WITHOUT_RETRYING))
            .append("$TRF=").append(getMetricValue(NUMBER_OF_REQUESTS_FAILED))
            .append("$TR=").append(getMetricValue(TOTAL_NUMBER_OF_REQUESTS))
            .append("$MRC=").append(getMetricValue(MAX_RETRY_COUNT));

    return metricString.toString();
  }

  @VisibleForTesting
  String[] getMetricNamesByType(StatisticTypeEnum type) {
    return getMetricNames(type);
  }
}

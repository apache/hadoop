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

import org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum;
import org.apache.hadoop.fs.azurebfs.statistics.AbstractAbfsStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HUNDRED;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.THOUSAND;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.RETRY;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.RETRY_LIST;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

public class AbfsBackoffMetrics extends AbstractAbfsStatisticsSource {

  public AbfsBackoffMetrics() {
    IOStatisticsStore ioStatisticsStore = iostatisticsStore()
            .withCounters(getCountersName())
            .build();
    setIOStatistics(ioStatisticsStore);
  }

  private String[] getCountersName() {
    return Arrays.stream(AbfsBackoffMetricsEnum.values())
            .flatMap(backoffMetricsEnum -> {
              if (RETRY.equals(backoffMetricsEnum.getType())) {
                return RETRY_LIST.stream()
                        .map(retryCount -> retryCount + ":" + backoffMetricsEnum.getName());
              } else {
                return Stream.of(backoffMetricsEnum.getName());
              }
            })
            .toArray(String[]::new);
  }

  public void incrementCounter(AbfsBackoffMetricsEnum metric, String retryCount, long value) {
    incCounter(retryCount + ":" + metric.getName(), value);
  }

  public void incrementCounter(AbfsBackoffMetricsEnum metric, String retryCount) {
    incrementCounter(metric, retryCount, 1);
  }

  public void incrementCounter(AbfsBackoffMetricsEnum metric, long value) {
    incCounter(metric.getName(), value);
  }

  public void incrementCounter(AbfsBackoffMetricsEnum metric) {
    incrementCounter(metric, 1);
  }

  public Long getCounter(AbfsBackoffMetricsEnum metric, String retryCount) {
    return lookupCounterValue(retryCount + ":" + metric.getName());
  }

  public Long getCounter(AbfsBackoffMetricsEnum metric) {
    return lookupCounterValue(metric.getName());
  }

  public void setCounter(AbfsBackoffMetricsEnum metric, String retryCount, long value) {
    setCounterValue(retryCount + ":" + metric.getName(), value);
  }

  public void setCounter(AbfsBackoffMetricsEnum metric, long value) {
    setCounterValue(metric.getName(), value);
  }

  public long getNumberOfIOPSThrottledRequests() {
    return getCounter(AbfsBackoffMetricsEnum.NUMBER_OF_IOPS_THROTTLED_REQUESTS);
  }

  public void incrementNumberOfIOPSThrottledRequests() {
    incrementCounter(AbfsBackoffMetricsEnum.NUMBER_OF_IOPS_THROTTLED_REQUESTS);
  }

  public long getNumberOfBandwidthThrottledRequests() {
    return getCounter(AbfsBackoffMetricsEnum.NUMBER_OF_BANDWIDTH_THROTTLED_REQUESTS);
  }

  public void incrementNumberOfBandwidthThrottledRequests() {
    incrementCounter(AbfsBackoffMetricsEnum.NUMBER_OF_BANDWIDTH_THROTTLED_REQUESTS);
  }

  public long getNumberOfOtherThrottledRequests() {
    return getCounter(AbfsBackoffMetricsEnum.NUMBER_OF_OTHER_THROTTLED_REQUESTS);
  }

  public void incrementNumberOfOtherThrottledRequests() {
    incrementCounter(AbfsBackoffMetricsEnum.NUMBER_OF_OTHER_THROTTLED_REQUESTS);
  }

  public long getNumberOfNetworkFailedRequests() {
    return getCounter(AbfsBackoffMetricsEnum.NUMBER_OF_NETWORK_FAILED_REQUESTS);
  }

  public void incrementNumberOfNetworkFailedRequests() {
    incrementCounter(AbfsBackoffMetricsEnum.NUMBER_OF_NETWORK_FAILED_REQUESTS);
  }

  public long getMaxRetryCount() {
    return getCounter(AbfsBackoffMetricsEnum.MAX_RETRY_COUNT);
  }

  public void setMaxRetryCount(long value) {
    setCounter(AbfsBackoffMetricsEnum.MAX_RETRY_COUNT, value);
  }

  public long getTotalNumberOfRequests() {
    return getCounter(AbfsBackoffMetricsEnum.TOTAL_NUMBER_OF_REQUESTS);
  }

  public void incrementTotalNumberOfRequests() {
    incrementCounter(AbfsBackoffMetricsEnum.TOTAL_NUMBER_OF_REQUESTS);
  }

  public long getNumberOfRequestsSucceededWithoutRetrying() {
    return getCounter(AbfsBackoffMetricsEnum.NUMBER_OF_REQUESTS_SUCCEEDED_WITHOUT_RETRYING);
  }

  public void incrementNumberOfRequestsSucceededWithoutRetrying() {
    incrementCounter(AbfsBackoffMetricsEnum.NUMBER_OF_REQUESTS_SUCCEEDED_WITHOUT_RETRYING);
  }

  public long getNumberOfRequestsFailed() {
    return getCounter(AbfsBackoffMetricsEnum.NUMBER_OF_REQUESTS_FAILED);
  }

  public void incrementNumberOfRequestsFailed() {
    incrementCounter(AbfsBackoffMetricsEnum.NUMBER_OF_REQUESTS_FAILED);
  }

  public long getNumberOfRequestsSucceeded(String retryCount) {
    return getCounter(AbfsBackoffMetricsEnum.NUMBER_OF_REQUESTS_SUCCEEDED, retryCount);
  }

  public void incrementNumberOfRequestsSucceeded(String retryCount) {
    incrementCounter(AbfsBackoffMetricsEnum.NUMBER_OF_REQUESTS_SUCCEEDED, retryCount);
  }

  public long getMinBackoff(String retryCount) {
    return getCounter(AbfsBackoffMetricsEnum.MIN_BACK_OFF, retryCount);
  }

  public void setMinBackoff(String retryCount, long value) {
    setCounter(AbfsBackoffMetricsEnum.MIN_BACK_OFF, retryCount, value);
  }

  public long getMaxBackoff(String retryCount) {
    return getCounter(AbfsBackoffMetricsEnum.MAX_BACK_OFF, retryCount);
  }

  public void setMaxBackoff(String retryCount, long value) {
    setCounter(AbfsBackoffMetricsEnum.MAX_BACK_OFF, retryCount, value);
  }

  public long getTotalBackoff(String retryCount) {
    return getCounter(AbfsBackoffMetricsEnum.TOTAL_BACK_OFF, retryCount);
  }

  public void setTotalBackoff(String retryCount, long value) {
    setCounter(AbfsBackoffMetricsEnum.TOTAL_BACK_OFF, retryCount, value);
  }

  public long getTotalRequests(String retryCount) {
    return getCounter(AbfsBackoffMetricsEnum.TOTAL_REQUESTS, retryCount);
  }

  public void incrementTotalRequests(String retryCount) {
    incrementCounter(AbfsBackoffMetricsEnum.TOTAL_REQUESTS, retryCount);
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
    StringBuilder metricString = new StringBuilder();
    long totalRequestsThrottled = getNumberOfBandwidthThrottledRequests()
            + getNumberOfIOPSThrottledRequests()
            + getNumberOfOtherThrottledRequests();
    double percentageOfRequestsThrottled =
        ((double) totalRequestsThrottled / getTotalNumberOfRequests()) * HUNDRED;
    for (String retryCount: RETRY_LIST) {
      metricString.append("$RCTSI$_").append(retryCount)
          .append("R_").append("=")
          .append(getNumberOfRequestsSucceeded(retryCount));
      long totalRequests = getTotalRequests(retryCount);
      if (totalRequests > 0) {
        metricString.append("$MMA$_").append(retryCount)
            .append("R_").append("=")
            .append(String.format("%.3f",
                (double) getMinBackoff(retryCount) / THOUSAND))
            .append("s")
            .append(String.format("%.3f",
                (double) getMaxBackoff(retryCount) / THOUSAND))
            .append("s")
            .append(String.format("%.3f",
                ((double) getTotalBackoff(retryCount) / totalRequests)
                    / THOUSAND))
            .append("s");
      } else {
        metricString.append("$MMA$_").append(retryCount)
            .append("R_").append("=0s");
      }
    }
    metricString.append("$BWT=")
        .append(getNumberOfBandwidthThrottledRequests())
        .append("$IT=")
        .append(getNumberOfIOPSThrottledRequests())
        .append("$OT=")
        .append(getNumberOfOtherThrottledRequests())
        .append("$RT=")
        .append(String.format("%.3f", percentageOfRequestsThrottled))
        .append("$NFR=")
        .append(getNumberOfNetworkFailedRequests())
        .append("$TRNR=")
        .append(getNumberOfRequestsSucceededWithoutRetrying())
        .append("$TRF=")
        .append(getNumberOfRequestsFailed())
        .append("$TR=")
        .append(getTotalNumberOfRequests())
        .append("$MRC=")
        .append(getMaxRetryCount());

    return metricString + "";
  }
}


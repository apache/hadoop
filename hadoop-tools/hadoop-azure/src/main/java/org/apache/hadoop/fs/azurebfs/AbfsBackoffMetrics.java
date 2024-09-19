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

package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum;
import org.apache.hadoop.fs.azurebfs.statistics.AbstractAbfsStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

import java.util.Arrays;
import java.util.stream.Stream;

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
    setCounterValue(metric.getName(), 1);
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
    long totalRequestsThrottled = getCounter(AbfsBackoffMetricsEnum.NUMBER_OF_BANDWIDTH_THROTTLED_REQUESTS)
        + getCounter(AbfsBackoffMetricsEnum.NUMBER_OF_IOPS_THROTTLED_REQUESTS)
        + getCounter(AbfsBackoffMetricsEnum.NUMBER_OF_OTHER_THROTTLED_REQUESTS);
    double percentageOfRequestsThrottled =
        ((double) totalRequestsThrottled / getCounter(AbfsBackoffMetricsEnum.TOTAL_NUMBER_OF_REQUESTS)) * HUNDRED;
    for (String retryCount: RETRY_LIST) {
      metricString.append("$RCTSI$_").append(retryCount)
          .append("R_").append("=")
          .append(getCounter(AbfsBackoffMetricsEnum.NUMBER_OF_REQUESTS_SUCCEEDED, retryCount));
      long totalRequests = getCounter(AbfsBackoffMetricsEnum.TOTAL_REQUESTS, retryCount);
      if (totalRequests > 0) {
        metricString.append("$MMA$_").append(retryCount)
            .append("R_").append("=")
            .append(String.format("%.3f",
                (double) getCounter(AbfsBackoffMetricsEnum.MIN_BACK_OFF, retryCount) / THOUSAND))
            .append("s")
            .append(String.format("%.3f",
                (double) getCounter(AbfsBackoffMetricsEnum.MAX_BACK_OFF, retryCount) / THOUSAND))
            .append("s")
            .append(String.format("%.3f",
                ((double) getCounter(AbfsBackoffMetricsEnum.TOTAL_BACK_OFF, retryCount) / totalRequests)
                    / THOUSAND))
            .append("s");
      } else {
        metricString.append("$MMA$_").append(retryCount)
            .append("R_").append("=0s");
      }
    }
    metricString.append("$BWT=")
        .append(getCounter(AbfsBackoffMetricsEnum.NUMBER_OF_BANDWIDTH_THROTTLED_REQUESTS))
        .append("$IT=")
        .append(getCounter(AbfsBackoffMetricsEnum.NUMBER_OF_IOPS_THROTTLED_REQUESTS))
        .append("$OT=")
        .append(getCounter(AbfsBackoffMetricsEnum.NUMBER_OF_OTHER_THROTTLED_REQUESTS))
        .append("$RT=")
        .append(String.format("%.3f", percentageOfRequestsThrottled))
        .append("$NFR=")
        .append(getCounter(AbfsBackoffMetricsEnum.NUMBER_OF_NETWORK_FAILED_REQUESTS))
        .append("$TRNR=")
        .append(getCounter(AbfsBackoffMetricsEnum.NUMBER_OF_REQUESTS_SUCCEEDED_WITHOUT_RETRYING))
        .append("$TRF=")
        .append(getCounter(AbfsBackoffMetricsEnum.NUMBER_OF_REQUESTS_FAILED))
        .append("$TR=")
        .append(getCounter(AbfsBackoffMetricsEnum.TOTAL_NUMBER_OF_REQUESTS))
        .append("$MRC=")
        .append(getCounter(AbfsBackoffMetricsEnum.MAX_RETRY_COUNT));

    return metricString + "";
  }
}


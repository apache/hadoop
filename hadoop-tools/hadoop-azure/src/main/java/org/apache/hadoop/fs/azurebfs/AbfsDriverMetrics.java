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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;
import java.util.ArrayList;
import java.util.Arrays;

public class AbfsDriverMetrics {

  private AtomicLong numberOfRequestsSucceeded;

  private AtomicLong minBackoff;

  private AtomicLong maxBackoff;

  private AtomicLong totalRequests;

  private AtomicLong totalBackoff;

  private String retryCount;

  private AtomicLong numberOfIOPSThrottledRequests;

  private AtomicLong numberOfBandwidthThrottledRequests;

  private AtomicLong numberOfOtherThrottledRequests;

  private AtomicLong numberOfNetworkFailedRequests;

  private AtomicLong maxRetryCount;

  private AtomicLong totalNumberOfRequests;

  private AtomicLong numberOfRequestsSucceededWithoutRetrying;

  private AtomicLong numberOfRequestsFailed;

  private final Map<String, AbfsDriverMetrics> metricsMap
      = new ConcurrentHashMap<>();

  public AbfsDriverMetrics() {
    initializeMap();
    this.numberOfIOPSThrottledRequests = new AtomicLong();
    this.numberOfBandwidthThrottledRequests = new AtomicLong();
    this.numberOfOtherThrottledRequests = new AtomicLong();
    this.totalNumberOfRequests = new AtomicLong();
    this.maxRetryCount = new AtomicLong();
    this.numberOfRequestsSucceededWithoutRetrying = new AtomicLong();
    this.numberOfRequestsFailed = new AtomicLong();
    this.numberOfNetworkFailedRequests = new AtomicLong();
  }

  public AbfsDriverMetrics(String retryCount) {
    this.retryCount = retryCount;
    this.numberOfRequestsSucceeded = new AtomicLong();
    this.minBackoff = new AtomicLong(Long.MAX_VALUE);
    this.maxBackoff = new AtomicLong();
    this.totalRequests = new AtomicLong();
    this.totalBackoff = new AtomicLong();
  }

  private void initializeMap() {
    ArrayList<String> retryCountList = new ArrayList<String>(
        Arrays.asList("1", "2", "3", "4", "5_15", "15_25", "25AndAbove"));
    for (String s : retryCountList) {
      metricsMap.put(s, new AbfsDriverMetrics(s));
    }
  }

  public AtomicLong getNumberOfRequestsSucceeded() {
    return numberOfRequestsSucceeded;
  }

  public AtomicLong getMinBackoff() {
    return minBackoff;
  }

  public AtomicLong getMaxBackoff() {
    return maxBackoff;
  }

  public AtomicLong getTotalRequests() {
    return totalRequests;
  }

  public AtomicLong getTotalBackoff() {
    return totalBackoff;
  }

  public String getRetryCount() {
    return retryCount;
  }

  public AtomicLong getNumberOfIOPSThrottledRequests() {
    return numberOfIOPSThrottledRequests;
  }

  public AtomicLong getNumberOfBandwidthThrottledRequests() {
    return numberOfBandwidthThrottledRequests;
  }

  public AtomicLong getNumberOfOtherThrottledRequests() {
    return numberOfOtherThrottledRequests;
  }

  public AtomicLong getMaxRetryCount() {
    return maxRetryCount;
  }

  public AtomicLong getTotalNumberOfRequests() {
    return totalNumberOfRequests;
  }

  public Map<String, AbfsDriverMetrics> getMetricsMap() {
    return metricsMap;
  }

  public AtomicLong getNumberOfRequestsSucceededWithoutRetrying() {
    return numberOfRequestsSucceededWithoutRetrying;
  }

  public AtomicLong getNumberOfRequestsFailed() {
    return numberOfRequestsFailed;
  }

  public AtomicLong getNumberOfNetworkFailedRequests() {
    return numberOfNetworkFailedRequests;
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
    7.%RT :- Percentage of requests that are throttled
    8.TRNR :- Total number of requests which succeeded without retrying
    9.TRF :- Total number of requests which failed
    10.TR :- Total number of requests which were made
    11.MRC :- Max retry count across all requests
     */
  @Override
  public String toString() {
    StringBuilder metricString = new StringBuilder();
    long totalRequestsThrottled = numberOfBandwidthThrottledRequests.get()
        + numberOfIOPSThrottledRequests.get()
        + numberOfOtherThrottledRequests.get();
    double percentageOfRequestsThrottled =
        ((double) totalRequestsThrottled / totalNumberOfRequests.get()) * 100;
    for (Map.Entry<String, AbfsDriverMetrics> entry : metricsMap.entrySet()) {
      metricString.append("#RCTSI#_").append(entry.getKey())
          .append("R_").append("=")
          .append(entry.getValue().getNumberOfRequestsSucceeded()).append(" ");
      long totalRequests = entry.getValue().getTotalRequests().get();
      if (totalRequests > 0) {
        metricString.append("#MMA#_").append(entry.getKey())
            .append("R_").append("=")
            .append(String.format("%.3f",
                (double) entry.getValue().getMinBackoff().get() / 1000L))
            .append("s ")
            .append(String.format("%.3f",
                (double) entry.getValue().getMaxBackoff().get() / 1000L))
            .append("s ")
            .append(String.format("%.3f",
                ((double)entry.getValue().getTotalBackoff().get() / totalRequests)
                    / 1000L))
            .append("s ");
      } else {
        metricString.append("#MMA#_").append(entry.getKey())
            .append("R_").append("=0s ");
      }
    }
    metricString.append("#BWT=")
        .append(numberOfBandwidthThrottledRequests)
        .append(" #IT=")
        .append(numberOfIOPSThrottledRequests)
        .append(" #OT=")
        .append(numberOfOtherThrottledRequests)
        .append(" #%RT=")
        .append(String.format("%.3f", percentageOfRequestsThrottled))
        .append(" #NFR=")
        .append(numberOfNetworkFailedRequests)
        .append(" #TRNR=")
        .append(numberOfRequestsSucceededWithoutRetrying)
        .append(" #TRF=")
        .append(numberOfRequestsFailed)
        .append(" #TR=")
        .append(totalNumberOfRequests)
        .append(" #MRC=")
        .append(maxRetryCount);

    return metricString + " ";
  }
}


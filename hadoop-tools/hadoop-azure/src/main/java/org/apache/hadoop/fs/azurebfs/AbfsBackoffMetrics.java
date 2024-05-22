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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HUNDRED;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.THOUSAND;

public class AbfsBackoffMetrics {

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

  private final Map<String, AbfsBackoffMetrics> metricsMap
      = new ConcurrentHashMap<>();

  public AbfsBackoffMetrics() {
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

  public AbfsBackoffMetrics(String retryCount) {
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
      metricsMap.put(s, new AbfsBackoffMetrics(s));
    }
  }

  public long getNumberOfRequestsSucceeded() {
    return this.numberOfRequestsSucceeded.get();
  }

  public void setNumberOfRequestsSucceeded(long numberOfRequestsSucceeded) {
    this.numberOfRequestsSucceeded.set(numberOfRequestsSucceeded);
  }

  public void incrementNumberOfRequestsSucceeded() {
    this.numberOfRequestsSucceeded.getAndIncrement();
  }

  public long getMinBackoff() {
    return this.minBackoff.get();
  }

  public void setMinBackoff(long minBackoff) {
    this.minBackoff.set(minBackoff);
  }

  public long getMaxBackoff() {
    return this.maxBackoff.get();
  }

  public void setMaxBackoff(long maxBackoff) {
    this.maxBackoff.set(maxBackoff);
  }

  public long getTotalRequests() {
    return this.totalRequests.get();
  }

  public void incrementTotalRequests() {
    this.totalRequests.incrementAndGet();
  }

  public void setTotalRequests(long totalRequests) {
    this.totalRequests.set(totalRequests);
  }

  public long getTotalBackoff() {
    return this.totalBackoff.get();
  }

  public void setTotalBackoff(long totalBackoff) {
    this.totalBackoff.set(totalBackoff);
  }

  public String getRetryCount() {
    return this.retryCount;
  }

  public long getNumberOfIOPSThrottledRequests() {
    return this.numberOfIOPSThrottledRequests.get();
  }

  public void setNumberOfIOPSThrottledRequests(long numberOfIOPSThrottledRequests) {
    this.numberOfIOPSThrottledRequests.set(numberOfIOPSThrottledRequests);
  }

  public void incrementNumberOfIOPSThrottledRequests() {
    this.numberOfIOPSThrottledRequests.getAndIncrement();
  }

  public long getNumberOfBandwidthThrottledRequests() {
    return this.numberOfBandwidthThrottledRequests.get();
  }

  public void setNumberOfBandwidthThrottledRequests(long numberOfBandwidthThrottledRequests) {
    this.numberOfBandwidthThrottledRequests.set(numberOfBandwidthThrottledRequests);
  }

  public void incrementNumberOfBandwidthThrottledRequests() {
    this.numberOfBandwidthThrottledRequests.getAndIncrement();
  }

  public long getNumberOfOtherThrottledRequests() {
    return this.numberOfOtherThrottledRequests.get();
  }

  public void setNumberOfOtherThrottledRequests(long numberOfOtherThrottledRequests) {
    this.numberOfOtherThrottledRequests.set(numberOfOtherThrottledRequests);
  }

  public void incrementNumberOfOtherThrottledRequests() {
    this.numberOfOtherThrottledRequests.getAndIncrement();
  }

  public long getMaxRetryCount() {
    return this.maxRetryCount.get();
  }

  public void setMaxRetryCount(long maxRetryCount) {
    this.maxRetryCount.set(maxRetryCount);
  }

  public void incrementMaxRetryCount() {
    this.maxRetryCount.getAndIncrement();
  }

  public long getTotalNumberOfRequests() {
    return this.totalNumberOfRequests.get();
  }

  public void setTotalNumberOfRequests(long totalNumberOfRequests) {
    this.totalNumberOfRequests.set(totalNumberOfRequests);
  }

  public void incrementTotalNumberOfRequests() {
    this.totalNumberOfRequests.getAndIncrement();
  }

  public Map<String, AbfsBackoffMetrics> getMetricsMap() {
    return metricsMap;
  }

  public long getNumberOfRequestsSucceededWithoutRetrying() {
    return this.numberOfRequestsSucceededWithoutRetrying.get();
  }

  public void setNumberOfRequestsSucceededWithoutRetrying(long numberOfRequestsSucceededWithoutRetrying) {
    this.numberOfRequestsSucceededWithoutRetrying.set(numberOfRequestsSucceededWithoutRetrying);
  }

  public void incrementNumberOfRequestsSucceededWithoutRetrying() {
    this.numberOfRequestsSucceededWithoutRetrying.getAndIncrement();
  }

  public long getNumberOfRequestsFailed() {
    return this.numberOfRequestsFailed.get();
  }

  public void setNumberOfRequestsFailed(long numberOfRequestsFailed) {
    this.numberOfRequestsFailed.set(numberOfRequestsFailed);
  }

  public void incrementNumberOfRequestsFailed() {
    this.numberOfRequestsFailed.getAndIncrement();
  }

  public long getNumberOfNetworkFailedRequests() {
    return this.numberOfNetworkFailedRequests.get();
  }

  public void setNumberOfNetworkFailedRequests(long numberOfNetworkFailedRequests) {
    this.numberOfNetworkFailedRequests.set(numberOfNetworkFailedRequests);
  }

  public void incrementNumberOfNetworkFailedRequests() {
    this.numberOfNetworkFailedRequests.getAndIncrement();
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
    for (Map.Entry<String, AbfsBackoffMetrics> entry : metricsMap.entrySet()) {
      metricString.append("$RCTSI$_").append(entry.getKey())
          .append("R_").append("=")
          .append(entry.getValue().getNumberOfRequestsSucceeded());
      long totalRequests = entry.getValue().getTotalRequests();
      if (totalRequests > 0) {
        metricString.append("$MMA$_").append(entry.getKey())
            .append("R_").append("=")
            .append(String.format("%.3f",
                (double) entry.getValue().getMinBackoff() / THOUSAND))
            .append("s")
            .append(String.format("%.3f",
                (double) entry.getValue().getMaxBackoff() / THOUSAND))
            .append("s")
            .append(String.format("%.3f",
                ((double) entry.getValue().getTotalBackoff() / totalRequests)
                    / THOUSAND))
            .append("s");
      } else {
        metricString.append("$MMA$_").append(entry.getKey())
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


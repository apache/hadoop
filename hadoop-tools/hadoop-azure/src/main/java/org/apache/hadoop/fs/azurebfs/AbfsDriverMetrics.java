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

import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
  private AtomicLong maxRetryCount;
  private AtomicLong totalNumberOfRequests;
  private AtomicLong numberOfRequestsSucceededWithoutRetrying;
  private AtomicLong numberOfRequestsFailed;
  private final Map<String, AbfsDriverMetrics> metricsMap = new ConcurrentHashMap<>();

  public AbfsDriverMetrics() {
    initializeMap();
    this.numberOfIOPSThrottledRequests = new AtomicLong();
    this.numberOfBandwidthThrottledRequests = new AtomicLong();
    this.numberOfOtherThrottledRequests = new AtomicLong();
    this.totalNumberOfRequests = new AtomicLong();
    this.maxRetryCount = new AtomicLong();
    this.numberOfRequestsSucceededWithoutRetrying = new AtomicLong();
    this.numberOfRequestsFailed = new AtomicLong();
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
    ArrayList<String> retryCountList = new ArrayList<String>(Arrays.asList("1","2","3","4","5_15","15_25","25_30"));
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

  @Override
  public String toString() {
    StringBuilder metricString = new StringBuilder();
    long totalRequestsThrottled = numberOfBandwidthThrottledRequests.get() + numberOfIOPSThrottledRequests.get() + numberOfOtherThrottledRequests.get();
    double percentageOfRequestsThrottled = ((double)totalRequestsThrottled/totalNumberOfRequests.get())*100;
    for (Map.Entry<String, AbfsDriverMetrics> entry : metricsMap.entrySet()) {
      metricString.append(" #RCTSI#_").append(entry.getKey())
          .append("R_").append("=")
          .append(entry.getValue().getNumberOfRequestsSucceeded()).append(" ");
      long totalRequests = entry.getValue().getTotalRequests().get();
      if(totalRequests > 0) {
        metricString.append(" MinMaxAvg#_").append(entry.getKey())
            .append("R_").append("=")
            .append(String.format("%.5f", (double) entry.getValue().getMinBackoff().get() / 1000L))
            .append(" seconds ")
            .append(String.format("%.5f", (double) entry.getValue().getMaxBackoff().get() / 1000L))
            .append(" seconds ")
            .append(String.format("%.5f", (double) ((entry.getValue().getTotalBackoff().get() / totalRequests) / 1000L)))
            .append(" seconds ");
      }else {
        metricString.append("MinMaxAvg#_").append(entry.getKey())
            .append("R_").append("= 0 seconds ");
      }
    }
    metricString.append(" BandwidthThrottled = ").append(numberOfBandwidthThrottledRequests)
        .append(" IOPSThrottled = ").append(numberOfIOPSThrottledRequests)
        .append(" OtherThrottled = ").append(numberOfOtherThrottledRequests).append('\n');
    metricString.append(" Percentage of throttled requests = ").append(percentageOfRequestsThrottled).append('\n');
    metricString.append(" Total number of requests which succeeded without retrying = ").append(numberOfRequestsSucceededWithoutRetrying).append('\n');
    metricString.append(" Total number of requests which failed = ").append(numberOfRequestsFailed).append('\n');
    metricString.append(" Total number of requests = ").append(totalNumberOfRequests).append('\n');
    metricString.append(" Max retry count = ").append(maxRetryCount).append('\n');

    return metricString + " ";
  }
}


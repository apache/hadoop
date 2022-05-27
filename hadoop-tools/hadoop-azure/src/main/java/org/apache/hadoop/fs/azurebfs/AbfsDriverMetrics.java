package org.apache.hadoop.fs.azurebfs;

import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;
import java.util.LinkedHashMap;
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

  private static final Map<String, AbfsDriverMetrics> metricsMap = new LinkedHashMap<>();

  public AbfsDriverMetrics() {
    initializeMap();
    this.numberOfIOPSThrottledRequests = new AtomicLong();
    this.numberOfBandwidthThrottledRequests = new AtomicLong();
    this.numberOfOtherThrottledRequests = new AtomicLong();
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

  public Map<String, AbfsDriverMetrics> getMetricsMap() {
    return metricsMap;
  }

  @Override
  public String toString() {
    StringBuilder requestsSucceeded = new StringBuilder();
    StringBuilder minMaxAvg = new StringBuilder();
    StringBuilder throttledRequests = new StringBuilder();
    for (Map.Entry<String, AbfsDriverMetrics> entry : metricsMap.entrySet()) {
      requestsSucceeded.append("#RCTSI#_").append(entry.getKey())
          .append("R_").append("=")
          .append(entry.getValue().getNumberOfRequestsSucceeded()).append(" ");
      long totalRequests = entry.getValue().getTotalRequests().get();
      if(totalRequests > 0) {
        minMaxAvg.append("MinMaxAvg#_").append(entry.getKey())
            .append("R_").append("=")
            .append(String.format("%.5f", (double) entry.getValue().getMinBackoff().get() / 1000L))
            .append(" seconds ")
            .append(String.format("%.5f", (double) entry.getValue().getMaxBackoff().get() / 1000L))
            .append(" seconds ")
            .append(String.format("%.5f", (double) ((entry.getValue().getTotalBackoff().get() / totalRequests) / 1000L)))
            .append(" seconds ");
      }else {
        minMaxAvg.append("MinMaxAvg#_").append(entry.getKey())
            .append("R_").append("= 0 seconds ");
      }
    }
    throttledRequests.append("BandwidthThrottled = ").append(numberOfBandwidthThrottledRequests)
        .append("IOPSThrottled = ").append(numberOfIOPSThrottledRequests)
        .append("OtherThrottled = ").append(numberOfOtherThrottledRequests);

    return requestsSucceeded + " " + minMaxAvg + " " + throttledRequests;
  }
}


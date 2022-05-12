package org.apache.hadoop.fs.azurebfs;

import  java.util.concurrent.atomic.AtomicLong;

public class AbfsDriverMetrics {
  private AtomicLong numberOfRequestsSucceededInFirstRetry;
  private AtomicLong numberOfRequestsSucceededInSecondRetry;
  private AtomicLong numberOfRequestsSucceededInThirdRetry;
  private AtomicLong numberOfRequestsSucceededInFourthRetry;
  private AtomicLong numberOfRequestsSucceededInFiveToFifteenRetry;
  private AtomicLong numberOfRequestsSucceededInFifteenToTwentyFiveRetry;
  private AtomicLong numberOfRequestsSucceededInTwentyFiveToThirtyRetry;

  public AbfsDriverMetrics() {
    this.numberOfRequestsSucceededInFirstRetry = new AtomicLong();
    this.numberOfRequestsSucceededInSecondRetry = new AtomicLong();
    this.numberOfRequestsSucceededInThirdRetry = new AtomicLong();
    this.numberOfRequestsSucceededInFourthRetry = new AtomicLong();
    this.numberOfRequestsSucceededInFiveToFifteenRetry = new AtomicLong();
    this.numberOfRequestsSucceededInFifteenToTwentyFiveRetry = new AtomicLong();
    this.numberOfRequestsSucceededInTwentyFiveToThirtyRetry = new AtomicLong();
  }
}

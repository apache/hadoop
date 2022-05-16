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

  public AtomicLong getNumberOfRequestsSucceededInFirstRetry() {
    return numberOfRequestsSucceededInFirstRetry;
  }

  public AtomicLong getNumberOfRequestsSucceededInSecondRetry() {
    return numberOfRequestsSucceededInSecondRetry;
  }

  public AtomicLong getNumberOfRequestsSucceededInThirdRetry() {
    return numberOfRequestsSucceededInThirdRetry;
  }

  public AtomicLong getNumberOfRequestsSucceededInFourthRetry() {
    return numberOfRequestsSucceededInFourthRetry;
  }

  public AtomicLong getNumberOfRequestsSucceededInFiveToFifteenRetry() {
    return numberOfRequestsSucceededInFiveToFifteenRetry;
  }

  public AtomicLong getNumberOfRequestsSucceededInFifteenToTwentyFiveRetry() {
    return numberOfRequestsSucceededInFifteenToTwentyFiveRetry;
  }

  public AtomicLong getNumberOfRequestsSucceededInTwentyFiveToThirtyRetry() {
    return numberOfRequestsSucceededInTwentyFiveToThirtyRetry;
  }

  @Override
  public String toString() {
    return "#RCTSI_1R_ " + numberOfRequestsSucceededInFirstRetry + "#RCTSI_2R_ " + numberOfRequestsSucceededInSecondRetry + "#RCTSI_3R_ " +
        numberOfRequestsSucceededInThirdRetry + "#RCTSI_4R_ " + numberOfRequestsSucceededInFourthRetry + "#RCTSI_5-15R_ " + numberOfRequestsSucceededInFiveToFifteenRetry +
        "#RCTSI_15-25R_ " + numberOfRequestsSucceededInFifteenToTwentyFiveRetry + "#RCTSI_25-30R_ " + numberOfRequestsSucceededInTwentyFiveToThirtyRetry;
 }
}

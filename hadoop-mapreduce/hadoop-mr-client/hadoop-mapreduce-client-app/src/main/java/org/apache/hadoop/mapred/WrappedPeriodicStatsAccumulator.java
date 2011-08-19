package org.apache.hadoop.mapred;

//Workaround for PeriodicStateAccumulator being package access
public class WrappedPeriodicStatsAccumulator {

  private PeriodicStatsAccumulator real;

  public WrappedPeriodicStatsAccumulator(PeriodicStatsAccumulator real) {
    this.real = real;
  }
  
  public void extend(double newProgress, int newValue) {
    real.extend(newProgress, newValue);
  }
}

package org.apache.hadoop.fs.azurebfs.services;

import java.util.LinkedList;

public class RollingWindow {
  private final LinkedList<DataPoint> dataPoints = new LinkedList<>();
  private final long windowSize; // window size in milliseconds

  public RollingWindow(long windowSizeInSeconds) {
    this.windowSize = windowSizeInSeconds * 1000;
  }

  public synchronized void add(long value) {
    dataPoints.add(new DataPoint(System.currentTimeMillis(), value));
    cleanup();
  }

  public synchronized long getSum() {
    cleanup();
    long sum = 0;
    for (DataPoint dataPoint : dataPoints) {
      sum += dataPoint.getValue();
    }
    return sum;
  }

  private void cleanup() {
    long currentTime = System.currentTimeMillis();
    while (!dataPoints.isEmpty() && currentTime - dataPoints.getFirst().getTime() > windowSize) {
      dataPoints.removeFirst();
    }
  }

  private static class DataPoint {
    private final long time;
    private final long value;

    public DataPoint(long time, long value) {
      this.time = time;
      this.value = value;
    }

    public long getTime() {
      return time;
    }

    public long getValue() {
      return value;
    }
  }
}

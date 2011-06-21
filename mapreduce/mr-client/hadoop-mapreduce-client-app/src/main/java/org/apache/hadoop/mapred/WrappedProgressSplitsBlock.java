package org.apache.hadoop.mapred;

// Workaround for ProgressSplitBlock being package access
public class WrappedProgressSplitsBlock extends ProgressSplitsBlock {

  public static final int DEFAULT_NUMBER_PROGRESS_SPLITS = 12;

  private WrappedPeriodicStatsAccumulator wrappedProgressWallclockTime;
  private WrappedPeriodicStatsAccumulator wrappedProgressCPUTime;
  private WrappedPeriodicStatsAccumulator wrappedProgressVirtualMemoryKbytes;
  private WrappedPeriodicStatsAccumulator wrappedProgressPhysicalMemoryKbytes;

  public WrappedProgressSplitsBlock(int numberSplits) {
    super(numberSplits);
  }

  public int[][] burst() {
    return super.burst();
  }

  public WrappedPeriodicStatsAccumulator getProgressWallclockTime() {
    if (wrappedProgressWallclockTime == null) {
      wrappedProgressWallclockTime = new WrappedPeriodicStatsAccumulator(
          progressWallclockTime);
    }
    return wrappedProgressWallclockTime;
  }

  public WrappedPeriodicStatsAccumulator getProgressCPUTime() {
    if (wrappedProgressCPUTime == null) {
      wrappedProgressCPUTime = new WrappedPeriodicStatsAccumulator(
          progressCPUTime);
    }
    return wrappedProgressCPUTime;
  }

  public WrappedPeriodicStatsAccumulator getProgressVirtualMemoryKbytes() {
    if (wrappedProgressVirtualMemoryKbytes == null) {
      wrappedProgressVirtualMemoryKbytes = new WrappedPeriodicStatsAccumulator(
          progressVirtualMemoryKbytes);
    }
    return wrappedProgressVirtualMemoryKbytes;
  }

  public WrappedPeriodicStatsAccumulator getProgressPhysicalMemoryKbytes() {
    if (wrappedProgressPhysicalMemoryKbytes == null) {
      wrappedProgressPhysicalMemoryKbytes = new WrappedPeriodicStatsAccumulator(
          progressPhysicalMemoryKbytes);
    }
    return wrappedProgressPhysicalMemoryKbytes;
  }
}
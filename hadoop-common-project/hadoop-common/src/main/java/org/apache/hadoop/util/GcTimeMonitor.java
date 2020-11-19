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

package org.apache.hadoop.util;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This class monitors the percentage of time the JVM is paused in GC within
 * the specified observation window, say 1 minute. The user can provide a
 * hook which will be called whenever this percentage exceeds the specified
 * threshold.
 */
public class GcTimeMonitor extends Thread {

  private final long maxGcTimePercentage;
  private final long observationWindowMs, sleepIntervalMs;
  private final GcTimeAlertHandler alertHandler;

  private final List<GarbageCollectorMXBean> gcBeans =
      ManagementFactory.getGarbageCollectorMXBeans();
  // Ring buffers containing GC timings and timestamps when timings were taken
  private final TsAndData[] gcDataBuf;
  private int bufSize, startIdx, endIdx;

  private long startTime;
  private final GcData curData = new GcData();
  private volatile boolean shouldRun = true;

  public static class Builder {

    private long observationWindowMs = TimeUnit.MINUTES.toMillis(1);
    private long sleepIntervalMs = TimeUnit.SECONDS.toMillis(5);
    private int maxGcTimePercentage = 100;
    private GcTimeAlertHandler handler = null;

    /**
     * Set observation window size in milliseconds.
     */
    public Builder observationWindowMs(long value) {
      this.observationWindowMs = value;
      return this;
    }

    /**
     * Set sleep interval in milliseconds.
     */
    public Builder sleepIntervalMs(long value) {
      this.sleepIntervalMs = value;
      return this;
    }

    /**
     * Set the max GC time percentage that triggers the alert handler.
     */
    public Builder maxGcTimePercentage(int value) {
      this.maxGcTimePercentage = value;
      return this;
    }

    /**
     * Set the GC alert handler.
     */
    public Builder gcTimeAlertHandler(GcTimeAlertHandler value) {
      this.handler = value;
      return this;
    }

    public GcTimeMonitor build() {
      return new GcTimeMonitor(observationWindowMs, sleepIntervalMs,
          maxGcTimePercentage, handler);
    }
  }


  /**
   * Create an instance of GCTimeMonitor. Once it's started, it will stay alive
   * and monitor GC time percentage until shutdown() is called. If you don't
   * put a limit on the number of GCTimeMonitor instances that you create, and
   * alertHandler != null, you should necessarily call shutdown() once the given
   * instance is not needed. Otherwise, you may create a memory leak, because
   * each running GCTimeMonitor will keep its alertHandler object in memory,
   * which in turn may reference and keep in memory many more other objects.
   *
   * @param observationWindowMs the interval over which the percentage
   *   of GC time should be calculated. A practical value would be somewhere
   *   between 30 sec and several minutes.
   * @param sleepIntervalMs how frequently this thread should wake up to check
   *   GC timings. This is also a frequency with which alertHandler will be
   *   invoked if GC time percentage exceeds the specified limit. A practical
   *   value would likely be 500..1000 ms.
   * @param maxGcTimePercentage A GC time percentage limit (0..100) within
   *   observationWindowMs. Once this is exceeded, alertHandler will be
   *   invoked every sleepIntervalMs milliseconds until GC time percentage
   *   falls below this limit.
   * @param alertHandler a single method in this interface is invoked when GC
   *   time percentage exceeds the specified limit.
   */
  public GcTimeMonitor(long observationWindowMs, long sleepIntervalMs,
      int maxGcTimePercentage, GcTimeAlertHandler alertHandler) {
    Preconditions.checkArgument(observationWindowMs > 0);
    Preconditions.checkArgument(
        sleepIntervalMs > 0 && sleepIntervalMs < observationWindowMs);
    Preconditions.checkArgument(
        maxGcTimePercentage >= 0 && maxGcTimePercentage <= 100);

    this.observationWindowMs = observationWindowMs;
    this.sleepIntervalMs = sleepIntervalMs;
    this.maxGcTimePercentage = maxGcTimePercentage;
    this.alertHandler = alertHandler;

    bufSize = (int) (observationWindowMs / sleepIntervalMs + 2);
    // Prevent the user from accidentally creating an abnormally big buffer,
    // which will result in slow calculations and likely inaccuracy.
    Preconditions.checkArgument(bufSize <= 128 * 1024);
    gcDataBuf = new TsAndData[bufSize];
    for (int i = 0; i < bufSize; i++) {
      gcDataBuf[i] = new TsAndData();
    }

    this.setDaemon(true);
    this.setName("GcTimeMonitor obsWindow = " + observationWindowMs +
        ", sleepInterval = " + sleepIntervalMs +
        ", maxGcTimePerc = " + maxGcTimePercentage);
  }

  @Override
  public void run() {
    startTime = System.currentTimeMillis();
    curData.timestamp = startTime;
    gcDataBuf[startIdx].setValues(startTime, 0);

    while (shouldRun) {
      try {
        Thread.sleep(sleepIntervalMs);
      } catch (InterruptedException ie) {
        return;
      }

      calculateGCTimePercentageWithinObservedInterval();
      if (alertHandler != null &&
          curData.gcTimePercentage > maxGcTimePercentage) {
        alertHandler.alert(curData.clone());
      }
    }
  }

  public void shutdown() {
    shouldRun = false;
  }

  /** Returns a copy of the most recent data measured by this monitor. */
  public GcData getLatestGcData() {
    return curData.clone();
  }

  private void calculateGCTimePercentageWithinObservedInterval() {
    long prevTotalGcTime = curData.totalGcTime;
    long totalGcTime = 0;
    long totalGcCount = 0;
    for (GarbageCollectorMXBean gcBean : gcBeans) {
      totalGcTime += gcBean.getCollectionTime();
      totalGcCount += gcBean.getCollectionCount();
    }
    long gcTimeWithinSleepInterval = totalGcTime - prevTotalGcTime;

    long ts = System.currentTimeMillis();
    long gcMonitorRunTime = ts - startTime;

    endIdx = (endIdx + 1) % bufSize;
    gcDataBuf[endIdx].setValues(ts, gcTimeWithinSleepInterval);

    // Move startIdx forward until we reach the first buffer entry with
    // timestamp within the observation window.
    long startObsWindowTs = ts - observationWindowMs;
    while (gcDataBuf[startIdx].ts < startObsWindowTs && startIdx != endIdx) {
      startIdx = (startIdx + 1) % bufSize;
    }

    // Calculate total GC time within observationWindowMs.
    // We should be careful about GC time that passed before the first timestamp
    // in our observation window.
    long gcTimeWithinObservationWindow = Math.min(
        gcDataBuf[startIdx].gcPause, gcDataBuf[startIdx].ts - startObsWindowTs);
    if (startIdx != endIdx) {
      for (int i = (startIdx + 1) % bufSize; i != endIdx;
           i = (i + 1) % bufSize) {
        gcTimeWithinObservationWindow += gcDataBuf[i].gcPause;
      }
    }

    curData.update(ts, gcMonitorRunTime, totalGcTime, totalGcCount,
        (int) (gcTimeWithinObservationWindow * 100 /
          Math.min(observationWindowMs, gcMonitorRunTime)));
  }

  /**
   * The user can provide an instance of a class implementing this interface
   * when initializing a GcTimeMonitor to receive alerts when GC time
   * percentage exceeds the specified threshold.
   */
  public interface GcTimeAlertHandler {
    void alert(GcData gcData);
  }

  /** Encapsulates data about GC pauses measured at the specific timestamp. */
  public static class GcData implements Cloneable {
    private long timestamp;
    private long gcMonitorRunTime, totalGcTime, totalGcCount;
    private int gcTimePercentage;

    /** Returns the absolute timestamp when this measurement was taken. */
    public long getTimestamp() {
      return timestamp;
    }

    /** Returns the time since the start of the associated GcTimeMonitor. */
    public long getGcMonitorRunTime() {
      return gcMonitorRunTime;
    }

    /** Returns accumulated GC time since this JVM started. */
    public long getAccumulatedGcTime() {
      return totalGcTime;
    }

    /** Returns the accumulated number of GC pauses since this JVM started. */
    public long getAccumulatedGcCount() {
      return totalGcCount;
    }

    /**
     * Returns the percentage (0..100) of time that the JVM spent in GC pauses
     * within the observation window of the associated GcTimeMonitor.
     */
    public int getGcTimePercentage() {
      return gcTimePercentage;
    }

    private synchronized void update(long inTimestamp, long inGcMonitorRunTime,
        long inTotalGcTime, long inTotalGcCount, int inGcTimePercentage) {
      this.timestamp = inTimestamp;
      this.gcMonitorRunTime = inGcMonitorRunTime;
      this.totalGcTime = inTotalGcTime;
      this.totalGcCount = inTotalGcCount;
      this.gcTimePercentage = inGcTimePercentage;
    }

    @Override
    public synchronized GcData clone() {
      try {
        return (GcData) super.clone();
      } catch (CloneNotSupportedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class TsAndData {
    private long ts;      // Timestamp when this measurement was taken
    private long gcPause; // Total GC pause time within the interval between ts
                          // and the timestamp of the previous measurement.

    void setValues(long inTs, long inGcPause) {
      this.ts = inTs;
      this.gcPause = inGcPause;
    }
  }
}

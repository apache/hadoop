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

package org.apache.hadoop.fs.azurebfs.services;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.VisibleForTesting;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HUNDRED;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ZERO;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ZERO_D;

/**
 * Sliding Window HdrHistogram for tracking latencies over a time window.
 * Uses a ring buffer of histograms to represent time segments within the window.
 * Thread-safe for concurrent recording and querying.
 */
public final class SlidingWindowHdrHistogram {

  private static final Logger LOG = LoggerFactory.getLogger(
      SlidingWindowHdrHistogram.class);
  private static final int PERCENTILE_50 = 50;
  private static final int PERCENTILE_90 = 90;
  private static final int PERCENTILE_99 = 99;

  // Configuration
  private final long windowSizeMillis;          // Total analysis window
  private final long timeSegmentDurationMillis; // Subdivision on analysis window
  private final int numSegments;
  private final long highestTrackableValue;
  private final int significantFigures;

  // Ring buffer of immutable snapshots for completed time segments
  private final Histogram[] completedSegments;
  private final AtomicInteger currentIndex = new AtomicInteger(0);

  // Active Time Segment
  private volatile Recorder activeSegmentRecorder;
  private Histogram currentSegmentAccumulation;
  private volatile long currentSegmentStartMillis;
  private final AtomicLong currentTotalCount = new AtomicLong(0L);

  // Synchronization
  // Writers never take locks. Readers (queries) and rotation use this lock
  // to mutate currentAccumulation and ring-buffer pointers safely.
  private final ReentrantLock rotateLock = new ReentrantLock();

  // Reusable temp histograms to minimize allocations
  private Histogram tmpForDelta;
  private Histogram tmpForMerge;

  private final AbfsRestOperationType operationType;

  private boolean isAnalysisWindowFilled = false;
  private int minSampleSize;
  private double tailLatencyPercentile;
  private int tailLatencyMinDeviation;

  private double p50 = ZERO_D;
  private double p90 = ZERO_D;
  private double p99 = ZERO_D;
  private double tailLatency = ZERO_D;
  private int deviation = ZERO;

  public SlidingWindowHdrHistogram(long windowSizeMillis,
      int numberOfSegments,
      int minSampleSize,
      int tailLatencyPercentile,
      int tailLatencyMinDeviation,
      long highestTrackableValue,
      int significantFigures,
      final AbfsRestOperationType operationType) {
    if (windowSizeMillis <= ZERO) {
      throw new IllegalArgumentException("windowSizeMillis > 0");
    }
    if (numberOfSegments <= ZERO) {
      throw new IllegalArgumentException("numberOfSegments > 0");
    }
    if (highestTrackableValue <= ZERO) {
      throw new IllegalArgumentException("highestTrackableValue > 0");
    }
    if (significantFigures < 1 || significantFigures > 5) {
      throw new IllegalArgumentException("significantFigures in [1,5]");
    }

    this.windowSizeMillis = windowSizeMillis;
    this.numSegments = numberOfSegments;
    this.timeSegmentDurationMillis = windowSizeMillis / numberOfSegments;
    this.highestTrackableValue = highestTrackableValue;
    this.significantFigures = significantFigures;
    this.operationType = operationType;
    this.minSampleSize = minSampleSize;
    this.tailLatencyPercentile = adjustPercentile(tailLatencyPercentile);
    this.tailLatencyMinDeviation = tailLatencyMinDeviation; // 5ms

    this.completedSegments = new Histogram[numSegments];
    long now = System.currentTimeMillis();
    this.currentSegmentStartMillis = alignToSegmentDuration(now);
    currentIndex.set(0);
    this.activeSegmentRecorder = new Recorder(highestTrackableValue,
        significantFigures);
    this.currentSegmentAccumulation = new Histogram(highestTrackableValue,
        significantFigures);
    this.tmpForDelta = new Histogram(highestTrackableValue, significantFigures);
    this.tmpForMerge = new Histogram(highestTrackableValue, significantFigures);

    LOG.debug(
        "[{}] Initialized SlidingWindowHdrHistogram with WindowSize {}, TimeSegmentDur: {}, "
            + "NumOfSegments: {}", operationType, windowSizeMillis, timeSegmentDurationMillis,
        numSegments);
  }

  /**
   * Record a single latency value (in your chosen time unit). Thread-safe and lock-free.
   * @param value latency value to record
   */
  public void recordValue(long value) {
    if (value < 0 || value > highestTrackableValue) {
      LOG.warn("[{}] Value {} outside of range [0, {}]. Ignoring",
          operationType, value, highestTrackableValue);
      return;
    }
    activeSegmentRecorder.recordValue(value);
    currentTotalCount.incrementAndGet();
    LOG.debug("[{}] Recorded latency value: {}. Current total count: {}",
        operationType, value, currentTotalCount.get());
  }

  /**
   * Get any percentile over the current sliding window.
   */
  public void computeLatency() {
    if (getCurrentTotalCount() < minSampleSize) {
      LOG.debug(
          "[{}] Not enough data to report percentiles. Current total count: {}",
          operationType, getCurrentTotalCount());
      return;
    } else {
      rotateLock.lock();
      try {
        tmpForMerge.reset();
        for (int i = 0; i < numSegments; i++) {
          Histogram h = completedSegments[i];
          if (h != null && h.getTotalCount() > 0) {
            tmpForMerge.add(h);
          }
        }

        if (tmpForMerge.getTotalCount() == 0) {
          return;
        }

        tailLatency = tmpForMerge.getValueAtPercentile(tailLatencyPercentile);
        p50 = tmpForMerge.getValueAtPercentile(PERCENTILE_50);
        p90 = tmpForMerge.getValueAtPercentile(PERCENTILE_90);
        p99 = tmpForMerge.getValueAtPercentile(PERCENTILE_99);
        if (p50 == ZERO || tailLatency < p50) {
          deviation = ZERO;
        } else {
          deviation = (int) ((tailLatency - p50) / p50 * HUNDRED);
        }
      } finally {
        rotateLock.unlock();
      }
    }
    LOG.debug(
        "[{}] Computed Latencies. p50: {}, p90: {}, p99: {}, tailLatency: {}, "
            + "deviation with p50: {} Current total count: {}",
        operationType, p50, p90, p99, tailLatency, deviation,
        getCurrentTotalCount());
  }

  private long alignToSegmentDuration(long timeMs) {
    return timeMs - (timeMs % timeSegmentDurationMillis);
  }

  /**
   * Ensure active bucket is aligned to current time; rotate if we've crossed a boundary.
   */
  public void rotateIfNeeded() {
    LOG.debug("[{}] Triggering Histogram Rotation", operationType);
    long expectedStart = alignToSegmentDuration(System.currentTimeMillis());
    if (expectedStart == currentSegmentStartMillis) {
      LOG.debug(
          "[{}] Current Time Segment Still Active at {}. Skipping Rotation",
          operationType, expectedStart);
      return; // still current
    }

    rotateLock.lock();
    try {
      // Re-check inside lock
      expectedStart = alignToSegmentDuration(System.currentTimeMillis());
      if (expectedStart == currentSegmentStartMillis) {
        return;
      }

      // Finalize the current bucket:
      // Pull any remaining deltas from active recorder and add to currentAccumulation
      tmpForDelta.reset();
      activeSegmentRecorder.getIntervalHistogramInto(tmpForDelta);
      currentSegmentAccumulation.add(tmpForDelta);

      if (currentSegmentAccumulation.getTotalCount() <= ZERO) {
        currentSegmentStartMillis = alignToSegmentDuration(
            System.currentTimeMillis());
        LOG.debug(
            "[{}] No data recorded in current time segment at {}. Skipping Rotation. Current Index is {}.",
            operationType, currentSegmentStartMillis, currentIndex.get());
        return;
      }

      LOG.debug(
          "[{}] Rotating current segment with total count {} into slot {}",
          operationType, currentSegmentAccumulation.getTotalCount(),
          currentIndex.get());

      // Place the finished currentAccumulation into the ring buffer slot ahead.
      int currentIdx = (currentIndex.getAndIncrement()) % numSegments;
      // Next slot is now going to be eradicated. Remove its count from total.
      currentTotalCount.set(
          currentTotalCount.get() - (completedSegments[currentIdx] == null
              ? ZERO
              : completedSegments[currentIdx].getTotalCount()));
      // Store an immutable snapshot (make sure we don't mutate the instance after storing)
      completedSegments[currentIdx] = currentSegmentAccumulation;
      currentSegmentStartMillis = alignToSegmentDuration(
          System.currentTimeMillis());

      // Start a fresh current bucket
      currentSegmentAccumulation = new Histogram(highestTrackableValue,
          significantFigures);
      activeSegmentRecorder = new Recorder(highestTrackableValue,
          significantFigures);

      if (currentIndex.get() >= numSegments) {
        LOG.debug("[{}] Analysis window is now filled", operationType);
        isAnalysisWindowFilled = true;
        // Prevent overflow of currentIndex
        currentIndex.set(currentIndex.get() % numSegments);
      }
      LOG.debug(
          "[{}] Completed rotation. New current index {}, New segment start time {}, New total count {}",
          operationType, currentIndex.get(), currentSegmentStartMillis,
          currentTotalCount.get());
    } finally {
      rotateLock.unlock();
    }
  }

  /**
   * If percentile is configured to more than 100, adjust it to a decimal value.
   * @param number configured percentile
   * @return adjusted percentile
   */
  public static double adjustPercentile(int number) {
    if (number <= HUNDRED) {
      return number; // No change for numbers ≤ 100
    }

    String numStr = String.valueOf(number);
    String withDecimal = numStr.substring(0, 2) + "." + numStr.substring(2);
    return Double.parseDouble(withDecimal);
  }

  @VisibleForTesting
  public double getTailLatency() {
    LOG.debug(
        "[{}] Getting Tail Latency. Current total count: {}, Deviation: {}%, "
            + "p50: {}, Tail Latency: {}, isAnalysisWindowFilled: {}",
        operationType, getCurrentTotalCount(), deviation, p50, tailLatency,
        isAnalysisWindowFilled);
    if (!isAnalysisWindowFilled()) {
      LOG.debug(
          "[{}] Analysis window not yet filled. Not reporting tail latency",
          operationType);
      return ZERO_D;
    }
    if (deviation < tailLatencyMinDeviation) {
      LOG.debug(
          "[{}] Tail latency deviation {}% is less than minimum required {}%. Not reporting tail latency",
          operationType, deviation, tailLatencyMinDeviation);
      return ZERO_D;
    }
    return tailLatency;
  }

  @VisibleForTesting
  public long getCurrentTotalCount() {
    return currentTotalCount.get();
  }

  @VisibleForTesting
  public int getCurrentIndex() {
    return currentIndex.get();
  }

  @VisibleForTesting
  public double getP50() {
    return p50;
  }

  @VisibleForTesting
  public boolean isAnalysisWindowFilled() {
    return isAnalysisWindowFilled;
  }
}

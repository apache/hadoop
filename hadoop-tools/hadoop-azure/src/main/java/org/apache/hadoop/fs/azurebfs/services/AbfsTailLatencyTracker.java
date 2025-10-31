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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;

/**
 * Account Specific Latency Tracker.
 * This class tracks the latency of various operations like read, write etc for a single account.
 * It maintains a sliding window histogram for each operation type to analyze latency patterns over time.
 */
public class AbfsTailLatencyTracker {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsTailLatencyTracker.class);
  private static AbfsTailLatencyTracker singletonLatencyTracker;
  private static final ReentrantLock LOCK = new ReentrantLock();
  private static final int HISTOGRAM_MAX_VALUE = 60_000;
  private static final int HISTOGRAM_SIGNIFICANT_FIGURES = 3;

  private final Map<AbfsRestOperationType, SlidingWindowHdrHistogram>
      operationLatencyMap = new HashMap<>();
  private final int talLatencyAnalysisWindowInMillis;
  private final int tailLatencyAnalysisWindowGranularity;
  private final int tailLatencyPercentile;
  private final int tailLatencyMinSampleSize;
  private final int tailLatencyMinDeviation;
  private final int tailLatencyComputationIntervalInMillis;

  /**
   * Constructor to initialize the latency tracker with configuration.
   * @param abfsConfiguration Configuration settings for latency tracking.
   */
  public AbfsTailLatencyTracker(final AbfsConfiguration abfsConfiguration) {
    this.talLatencyAnalysisWindowInMillis = abfsConfiguration.getTailLatencyAnalysisWindowInMillis();
    this.tailLatencyAnalysisWindowGranularity = abfsConfiguration.getTailLatencyAnalysisWindowGranularity();
    this.tailLatencyPercentile = abfsConfiguration.getTailLatencyPercentile();
    this.tailLatencyMinSampleSize = abfsConfiguration.getTailLatencyMinSampleSize();
    this.tailLatencyMinDeviation = abfsConfiguration.getTailLatencyMinDeviation();
    this.tailLatencyComputationIntervalInMillis = abfsConfiguration.getTailLatencyComputationIntervalInMillis();
    ScheduledExecutorService histogramRotatorThread = Executors.newSingleThreadScheduledExecutor(
        r -> {
          Thread t = new Thread(r, "Histogram-Rotator-Thread");
          t.setDaemon(true);
          return t;
        });
    long rotationInterval = talLatencyAnalysisWindowInMillis/tailLatencyAnalysisWindowGranularity;
    histogramRotatorThread.scheduleAtFixedRate(this::rotateHistograms,
        rotationInterval, rotationInterval, TimeUnit.MILLISECONDS);


    ScheduledExecutorService tailLatencyComputationThread = Executors.newSingleThreadScheduledExecutor(
        r -> {
          Thread t = new Thread(r, "Tail-Latency-Computation-Thread");
          t.setDaemon(true);
          return t;
        });
    tailLatencyComputationThread.scheduleAtFixedRate(this::computePercentiles,
        tailLatencyComputationIntervalInMillis, tailLatencyComputationIntervalInMillis, TimeUnit.MILLISECONDS);
  }

  /**
   * Rotates all histograms to ensure they reflect the most recent latency data.
   * This method is called periodically based on the configured rotation interval.
   */
  private void rotateHistograms() {
    for (SlidingWindowHdrHistogram histogram : operationLatencyMap.values()) {
      histogram.rotateIfNeeded();
    }
  }

  /**
   * Computes the tail latency percentiles for all operation types.
   * This method is called periodically based on the configured computation interval.
   */
  private void computePercentiles() {
    for (SlidingWindowHdrHistogram histogram : operationLatencyMap.values()) {
      histogram.computeLatency();
    }
  }

  /**
   * Creates a singleton object of the {@link SlidingWindowHdrHistogram}.
   * which is shared across all filesystem instances.
   * @param abfsConfiguration configuration set.
   * @return singleton object of intercept.
   */
  static AbfsTailLatencyTracker initializeSingleton(AbfsConfiguration abfsConfiguration) {
    if (singletonLatencyTracker == null) {
      LOCK.lock();
      try {
        if (singletonLatencyTracker == null) {
          singletonLatencyTracker = new AbfsTailLatencyTracker(abfsConfiguration);
        }
      } finally {
        LOCK.unlock();
      }
    }
    return singletonLatencyTracker;
  }

  /**
   * Updates the latency for a specific operation type.
   * @param latency Latency value to be recorded.
   * @param operationType Only applicable for read and write operations.
   */
  public void updateLatency(final AbfsRestOperationType operationType,
      final long latency) {
    SlidingWindowHdrHistogram histogram = operationLatencyMap.get(operationType);
    if (histogram == null) {
      LOCK.lock();
      try {
        if (operationLatencyMap.get(operationType) == null) {
          LOG.debug("Creating new histogram for operation: {}", operationType);
          histogram = new SlidingWindowHdrHistogram(
              talLatencyAnalysisWindowInMillis,
              tailLatencyAnalysisWindowGranularity,
              tailLatencyMinSampleSize,
              tailLatencyPercentile,
              tailLatencyMinDeviation,
              HISTOGRAM_MAX_VALUE, HISTOGRAM_SIGNIFICANT_FIGURES,
              operationType);
          operationLatencyMap.put(operationType, histogram);
        }
      } finally {
        LOCK.unlock();
      }
    } else {
      LOG.debug("Using existing histogram for operation: {}",  operationType);
    }
    if (histogram == null) {
      LOG.error("Unable to find/create histogram for: {}", operationType);
      return;
    }
    histogram.recordValue(latency);
    LOG.debug("Updated latency for operation: {} with latency: {}",
        operationType, latency);
  }

  /**
   * Gets the tail latency for a specific operation type.
   * @param operationType for which tail latency is required.
   * @return Tail latency value.
   */
  public double getTailLatency(final AbfsRestOperationType operationType) {
    SlidingWindowHdrHistogram histogram = operationLatencyMap.get(operationType);
    if (histogram != null) {
      return histogram.getTailLatency();
    }
    LOG.debug("No histogram yet created for operation: {}", operationType);
    return 0;
  }
}

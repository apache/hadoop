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

package org.apache.hadoop.metrics2.lib;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import javax.annotation.Nullable;

import static org.apache.hadoop.metrics2.lib.Interns.*;

/**
 * <p>
 * This class maintains a group of rolling average metrics. It implements the
 * algorithm of rolling average, i.e. a number of sliding windows are kept to
 * roll over and evict old subsets of samples. Each window has a subset of
 * samples in a stream, where sub-sum and sub-total are collected. All sub-sums
 * and sub-totals in all windows will be aggregated to final-sum and final-total
 * used to compute final average, which is called rolling average.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MutableRollingAverages extends MutableMetric implements Closeable {

  private MutableRatesWithAggregation innerMetrics =
      new MutableRatesWithAggregation();

  @VisibleForTesting
  static final ScheduledExecutorService SCHEDULER = Executors
      .newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true)
          .setNameFormat("MutableRollingAverages-%d").build());

  private ScheduledFuture<?> scheduledTask = null;

  @Nullable
  private Map<String, MutableRate> currentSnapshot;

  private final String avgInfoNameTemplate;
  private final String avgInfoDescTemplate;
  private int numWindows;

  private static class SumAndCount {
    private final double sum;
    private final long count;

    SumAndCount(final double sum, final long count) {
      this.sum = sum;
      this.count = count;
    }

    public double getSum() {
      return sum;
    }

    public long getCount() {
      return count;
    }
  }

  /**
   * <p>
   * key: metric name
   * </p>
   * <p>
   * value: deque where sub-sums and sub-totals for sliding windows are
   * maintained.
   * </p>
   */
  private Map<String, LinkedBlockingDeque<SumAndCount>> averages =
      new ConcurrentHashMap<>();

  private static final long WINDOW_SIZE_MS_DEFAULT = 300_000;
  private static final int NUM_WINDOWS_DEFAULT = 36;

  /**
   * Constructor for {@link MutableRollingAverages}.
   * @param metricValueName
   */
  public MutableRollingAverages(String metricValueName) {
    if (metricValueName == null) {
      metricValueName = "";
    }
    avgInfoNameTemplate = "[%s]" + "RollingAvg" +
        StringUtils.capitalize(metricValueName);
    avgInfoDescTemplate = "Rolling average " +
        StringUtils.uncapitalize(metricValueName) +" for "+ "%s";
    numWindows = NUM_WINDOWS_DEFAULT;
    scheduledTask = SCHEDULER.scheduleAtFixedRate(new RatesRoller(this),
        WINDOW_SIZE_MS_DEFAULT, WINDOW_SIZE_MS_DEFAULT, TimeUnit.MILLISECONDS);
  }

  /**
   * This method is for testing only to replace the scheduledTask.
   */
  @VisibleForTesting
  synchronized void replaceScheduledTask(int windows, long interval,
                                         TimeUnit timeUnit) {
    numWindows = windows;
    scheduledTask.cancel(true);
    scheduledTask = SCHEDULER.scheduleAtFixedRate(new RatesRoller(this),
        interval, interval, timeUnit);
  }

  @Override
  public void snapshot(MetricsRecordBuilder builder, boolean all) {
    if (all || changed()) {
      for (final Entry<String, LinkedBlockingDeque<SumAndCount>> entry
          : averages.entrySet()) {
        final String name = entry.getKey();
        final MetricsInfo avgInfo = info(
            String.format(avgInfoNameTemplate, StringUtils.capitalize(name)),
            String.format(avgInfoDescTemplate, StringUtils.uncapitalize(name)));
        double totalSum = 0;
        long totalCount = 0;

        for (final SumAndCount sumAndCount : entry.getValue()) {
          totalCount += sumAndCount.getCount();
          totalSum += sumAndCount.getSum();
        }

        if (totalCount != 0) {
          builder.addGauge(avgInfo, totalSum / totalCount);
        }
      }
      if (changed()) {
        clearChanged();
      }
    }
  }

  /**
   * Collects states maintained in {@link ThreadLocal}, if any.
   */
  public void collectThreadLocalStates() {
    innerMetrics.collectThreadLocalStates();
  }

  /**
   * @param name
   *          name of metric
   * @param value
   *          value of metric
   */
  public void add(final String name, final long value) {
    innerMetrics.add(name, value);
  }

  private static class RatesRoller implements Runnable {
    private final MutableRollingAverages parent;

    RatesRoller(final MutableRollingAverages parent) {
      this.parent = parent;
    }

    @Override
    public void run() {
      synchronized (parent) {
        final MetricsCollectorImpl mc = new MetricsCollectorImpl();
        final MetricsRecordBuilder rb = mc.addRecord("RatesRoller");
        /**
         * snapshot all metrics regardless of being changed or not, in case no
         * ops since last snapshot, we will get 0.
         */
        parent.innerMetrics.snapshot(rb, true);
        Preconditions.checkState(mc.getRecords().size() == 1,
            "There must be only one record and it's named with 'RatesRoller'");

        parent.currentSnapshot = parent.innerMetrics.getGlobalMetrics();
        parent.rollOverAvgs();
      }
      parent.setChanged();
    }
  }

  /**
   * Iterates over snapshot to capture all Avg metrics into rolling structure
   * {@link MutableRollingAverages#averages}.
   */
  private synchronized void rollOverAvgs() {
    if (currentSnapshot == null) {
      return;
    }

    for (Map.Entry<String, MutableRate> entry : currentSnapshot.entrySet()) {
      final MutableRate rate = entry.getValue();
      final LinkedBlockingDeque<SumAndCount> deque = averages.computeIfAbsent(
          entry.getKey(),
          new Function<String, LinkedBlockingDeque<SumAndCount>>() {
            @Override
            public LinkedBlockingDeque<SumAndCount> apply(String k) {
              return new LinkedBlockingDeque<>(numWindows);
            }
          });
      final SumAndCount sumAndCount = new SumAndCount(
          rate.lastStat().total(),
          rate.lastStat().numSamples());
      /* put newest sum and count to the end */
      if (!deque.offerLast(sumAndCount)) {
        deque.pollFirst();
        deque.offerLast(sumAndCount);
      }
    }

    setChanged();
  }

  @Override
  public void close() throws IOException {
    if (scheduledTask != null) {
      scheduledTask.cancel(false);
    }
    scheduledTask = null;
  }

  /**
   * Retrieve a map of metric name -> (aggregate).
   * Filter out entries that don't have at least minSamples.
   *
   * @return a map of peer DataNode Id to the average latency to that
   *         node seen over the measurement period.
   */
  public synchronized Map<String, Double> getStats(long minSamples) {
    final Map<String, Double> stats = new HashMap<>();

    for (final Entry<String, LinkedBlockingDeque<SumAndCount>> entry
        : averages.entrySet()) {
      final String name = entry.getKey();
      double totalSum = 0;
      long totalCount = 0;

      for (final SumAndCount sumAndCount : entry.getValue()) {
        totalCount += sumAndCount.getCount();
        totalSum += sumAndCount.getSum();
      }

      if (totalCount > minSamples) {
        stats.put(name, totalSum / totalCount);
      }
    }
    return stats;
  }
}

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
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
public class RollingAverages extends MutableMetric implements Closeable {

  private final MutableRatesWithAggregation innerMetrics =
      new MutableRatesWithAggregation();

  private static final ScheduledExecutorService SCHEDULER = Executors
      .newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true)
          .setNameFormat("RollingAverages-%d").build());

  private ScheduledFuture<?> scheduledTask = null;
  private Map<String, MutableRate> currentSnapshot;
  private final int numWindows;
  private final String avgInfoNameTemplate;
  private final String avgInfoDescTemplate;

  private static class SumAndCount {
    private final double sum;
    private final long count;

    public SumAndCount(final double sum, final long count) {
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

  /**
   * Constructor of {@link RollingAverages}.
   * @param windowSize
   *          The number of seconds of each window for which sub set of samples
   *          are gathered to compute the rolling average, A.K.A. roll over
   *          interval.
   * @param numWindows
   *          The number of windows maintained to compute the rolling average.
   * @param valueName
   *          of the metric (e.g. "Time", "Latency")
   */
  public RollingAverages(
      final int windowSize,
      final int numWindows,
      final String valueName) {
    String uvName = StringUtils.capitalize(valueName);
    String lvName = StringUtils.uncapitalize(valueName);
    avgInfoNameTemplate = "%s" + "RollingAvg"+ uvName;
    avgInfoDescTemplate = "Rolling average "+ lvName +" for "+ "%s";
    this.numWindows = numWindows;
    scheduledTask = SCHEDULER.scheduleAtFixedRate(new RatesRoller(this),
        windowSize, windowSize, TimeUnit.SECONDS);
  }

  /**
   * Constructor of {@link RollingAverages}.
   * @param windowSize
   *          The number of seconds of each window for which sub set of samples
   *          are gathered to compute rolling average, also A.K.A roll over
   *          interval.
   * @param numWindows
   *          The number of windows maintained in the same time to compute the
   *          average of the rolling averages.
   */
  public RollingAverages(
      final int windowSize,
      final int numWindows) {
    this(windowSize, numWindows, "Time");
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
    private final RollingAverages parent;

    public RatesRoller(final RollingAverages parent) {
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
   * {@link RollingAverages#averages}.
   */
  private void rollOverAvgs() {
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
              return new LinkedBlockingDeque<SumAndCount>(numWindows);
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
}

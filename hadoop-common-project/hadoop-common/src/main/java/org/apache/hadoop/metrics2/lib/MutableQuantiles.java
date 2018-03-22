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

import static org.apache.hadoop.metrics2.lib.Interns.info;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.util.Quantile;
import org.apache.hadoop.metrics2.util.QuantileEstimator;
import org.apache.hadoop.metrics2.util.SampleQuantiles;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Watches a stream of long values, maintaining online estimates of specific
 * quantiles with provably low error bounds. This is particularly useful for
 * accurate high-percentile (e.g. 95th, 99th) latency metrics.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MutableQuantiles extends MutableMetric {

  @VisibleForTesting
  public static final Quantile[] quantiles = { new Quantile(0.50, 0.050),
      new Quantile(0.75, 0.025), new Quantile(0.90, 0.010),
      new Quantile(0.95, 0.005), new Quantile(0.99, 0.001) };

  private final MetricsInfo numInfo;
  private final MetricsInfo[] quantileInfos;
  private final int interval;

  private QuantileEstimator estimator;
  private long previousCount = 0;
  private ScheduledFuture<?> scheduledTask = null;

  @VisibleForTesting
  protected Map<Quantile, Long> previousSnapshot = null;

  private static final ScheduledExecutorService scheduler = Executors
      .newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true)
          .setNameFormat("MutableQuantiles-%d").build());

  /**
   * Instantiates a new {@link MutableQuantiles} for a metric that rolls itself
   * over on the specified time interval.
   * 
   * @param name
   *          of the metric
   * @param description
   *          long-form textual description of the metric
   * @param sampleName
   *          type of items in the stream (e.g., "Ops")
   * @param valueName
   *          type of the values
   * @param interval
   *          rollover interval (in seconds) of the estimator
   */
  public MutableQuantiles(String name, String description, String sampleName,
      String valueName, int interval) {
    String ucName = StringUtils.capitalize(name);
    String usName = StringUtils.capitalize(sampleName);
    String uvName = StringUtils.capitalize(valueName);
    String desc = StringUtils.uncapitalize(description);
    String lsName = StringUtils.uncapitalize(sampleName);
    String lvName = StringUtils.uncapitalize(valueName);

    numInfo = info(ucName + "Num" + usName, String.format(
        "Number of %s for %s with %ds interval", lsName, desc, interval));
    // Construct the MetricsInfos for the quantiles, converting to percentiles
    quantileInfos = new MetricsInfo[quantiles.length];
    String nameTemplate = ucName + "%dthPercentile" + uvName;
    String descTemplate = "%d percentile " + lvName + " with " + interval
        + " second interval for " + desc;
    for (int i = 0; i < quantiles.length; i++) {
      int percentile = (int) (100 * quantiles[i].quantile);
      quantileInfos[i] = info(String.format(nameTemplate, percentile),
          String.format(descTemplate, percentile));
    }

    estimator = new SampleQuantiles(quantiles);

    this.interval = interval;
    scheduledTask = scheduler.scheduleAtFixedRate(new RolloverSample(this),
        interval, interval, TimeUnit.SECONDS);
  }

  @Override
  public synchronized void snapshot(MetricsRecordBuilder builder, boolean all) {
    if (all || changed()) {
      builder.addGauge(numInfo, previousCount);
      for (int i = 0; i < quantiles.length; i++) {
        long newValue = 0;
        // If snapshot is null, we failed to update since the window was empty
        if (previousSnapshot != null) {
          newValue = previousSnapshot.get(quantiles[i]);
        }
        builder.addGauge(quantileInfos[i], newValue);
      }
      if (changed()) {
        clearChanged();
      }
    }
  }

  public synchronized void add(long value) {
    estimator.insert(value);
  }

  public int getInterval() {
    return interval;
  }

  public void stop() {
    if (scheduledTask != null) {
      scheduledTask.cancel(false);
    }
    scheduledTask = null;
  }

  /**
   * Get the quantile estimator.
   *
   * @return the quantile estimator
   */
  @VisibleForTesting
  public synchronized QuantileEstimator getEstimator() {
    return estimator;
  }

  public synchronized void setEstimator(QuantileEstimator quantileEstimator) {
    this.estimator = quantileEstimator;
  }

  /**
   * Runnable used to periodically roll over the internal
   * {@link SampleQuantiles} every interval.
   */
  private static class RolloverSample implements Runnable {

    MutableQuantiles parent;

    public RolloverSample(MutableQuantiles parent) {
      this.parent = parent;
    }

    @Override
    public void run() {
      synchronized (parent) {
        parent.previousCount = parent.estimator.getCount();
        parent.previousSnapshot = parent.estimator.snapshot();
        parent.estimator.clear();
      }
      parent.setChanged();
    }

  }
}

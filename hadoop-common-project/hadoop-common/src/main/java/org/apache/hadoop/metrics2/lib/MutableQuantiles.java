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

import java.text.DecimalFormat;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.util.Quantile;
import org.apache.hadoop.metrics2.util.QuantileEstimator;
import org.apache.hadoop.metrics2.util.SampleQuantiles;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Watches a stream of long values, maintaining online estimates of specific
 * quantiles with provably low error bounds. This is particularly useful for
 * accurate high-percentile (e.g. 95th, 99th) latency metrics.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MutableQuantiles extends MutableMetric {

  @VisibleForTesting
  public static final Quantile[] QUANTILES = {new Quantile(0.50, 0.050),
      new Quantile(0.75, 0.025), new Quantile(0.90, 0.010),
      new Quantile(0.95, 0.005), new Quantile(0.99, 0.001)};

  private MetricsInfo numInfo;
  private MetricsInfo[] quantileInfos;
  private int intervalSecs;
  private static DecimalFormat decimalFormat = new DecimalFormat("###.####");

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

    setInterval(interval);
    setNumInfo(info(ucName + "Num" + usName, String.format(
        "Number of %s for %s with %ds interval", lsName, desc, interval)));
    scheduledTask = scheduler.scheduleWithFixedDelay(new RolloverSample(this),
        interval, interval, TimeUnit.SECONDS);
    // Construct the MetricsInfos for the quantiles, converting to percentiles
    Quantile[] quantilesArray = getQuantiles();
    setQuantileInfos(quantilesArray.length);
    setQuantiles(ucName, uvName, desc, lvName, decimalFormat);
    setEstimator(new SampleQuantiles(quantilesArray));
  }

  /**
   * Sets quantileInfo.
   *
   * @param ucName capitalized name of the metric
   * @param uvName capitalized type of the values
   * @param desc uncapitalized long-form textual description of the metric
   * @param lvName uncapitalized type of the values
   * @param pDecimalFormat Number formatter for percentile value
   */
  void setQuantiles(String ucName, String uvName, String desc, String lvName, DecimalFormat pDecimalFormat) {
    for (int i = 0; i < QUANTILES.length; i++) {
      double percentile = 100 * QUANTILES[i].quantile;
      String nameTemplate = ucName + pDecimalFormat.format(percentile) + "thPercentile" + uvName;
      String descTemplate = pDecimalFormat.format(percentile) + " percentile " + lvName
          + " with " + getInterval() + " second interval for " + desc;
      addQuantileInfo(i, info(nameTemplate, descTemplate));
    }
  }

  public MutableQuantiles() {}

  @Override
  public synchronized void snapshot(MetricsRecordBuilder builder, boolean all) {
    Quantile[] quantilesArray = getQuantiles();
    if (all || changed()) {
      builder.addGauge(numInfo, previousCount);
      for (int i = 0; i < quantilesArray.length; i++) {
        long newValue = 0;
        // If snapshot is null, we failed to update since the window was empty
        if (previousSnapshot != null) {
          newValue = previousSnapshot.get(quantilesArray[i]);
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

  /**
   * Returns the array of Quantiles declared in MutableQuantiles.
   *
   * @return array of Quantiles
   */
  public synchronized Quantile[] getQuantiles() {
    return QUANTILES;
  }

  /**
   * Set info about the metrics.
   *
   * @param pNumInfo info about the metrics.
   */
  public synchronized void setNumInfo(MetricsInfo pNumInfo) {
    this.numInfo = pNumInfo;
  }

  /**
   * Initialize quantileInfos array.
   *
   * @param length of the quantileInfos array.
   */
  public synchronized void setQuantileInfos(int length) {
    this.quantileInfos = new MetricsInfo[length];
  }

  /**
   * Add entry to quantileInfos array.
   *
   * @param i array index.
   * @param info info to be added to  quantileInfos array.
   */
  public synchronized void addQuantileInfo(int i, MetricsInfo info) {
    this.quantileInfos[i] = info;
  }

  /**
   * Set the rollover interval (in seconds) of the estimator.
   *
   * @param pIntervalSecs of the estimator.
   */
  public synchronized void setInterval(int pIntervalSecs) {
    this.intervalSecs = pIntervalSecs;
  }

  /**
   * Get the rollover interval (in seconds) of the estimator.
   *
   * @return  intervalSecs of the estimator.
   */
  public synchronized int getInterval() {
    return intervalSecs;
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

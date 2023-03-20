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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.util.Quantile;
import org.apache.hadoop.metrics2.util.SampleQuantiles;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * Watches a stream of long values, maintaining online estimates of specific
 * quantiles with provably low error bounds. Inverse quantiles are meant for
 * highly accurate low-percentile (e.g. 1st, 5th) latency metrics.
 * InverseQuantiles are used for metrics where higher the value better it is.
 * ( eg: data transfer rate ).
 * The 1st percentile here corresponds to the 99th inverse percentile metric,
 * 5th percentile to 95th and so on.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MutableInverseQuantiles extends MutableQuantiles{

  @VisibleForTesting
  public static final Quantile[] INVERSE_QUANTILES = { new Quantile(0.50, 0.050),
      new Quantile(0.25, 0.025), new Quantile(0.10, 0.010),
      new Quantile(0.05, 0.005), new Quantile(0.01, 0.001) };

  private ScheduledFuture<?> scheduledTask;

  private static final ScheduledExecutorService SCHEDULAR = Executors
      .newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true)
          .setNameFormat("MutableInverseQuantiles-%d").build());

  /**
   * Instantiates a new {@link MutableInverseQuantiles} for a metric that rolls itself
   * over on the specified time interval.
   *
   * @param name        of the metric
   * @param description long-form textual description of the metric
   * @param sampleName  type of items in the stream (e.g., "Ops")
   * @param valueName   type of the values
   * @param interval    rollover interval (in seconds) of the estimator
   */
  public MutableInverseQuantiles(String name, String description, String sampleName,
      String valueName, int interval) {
    String ucName = StringUtils.capitalize(name);
    String usName = StringUtils.capitalize(sampleName);
    String uvName = StringUtils.capitalize(valueName);
    String desc = StringUtils.uncapitalize(description);
    String lsName = StringUtils.uncapitalize(sampleName);
    String lvName = StringUtils.uncapitalize(valueName);

    setNumInfo(info(ucName + "Num" + usName, String.format(
        "Number of %s for %s with %ds interval", lsName, desc, interval)));
    // Construct the MetricsInfos for the inverse quantiles, converting to inverse percentiles
    setQuantileInfos(INVERSE_QUANTILES.length);
    String nameTemplate = ucName + "%dthInversePercentile" + uvName;
    String descTemplate = "%d inverse percentile " + lvName + " with " + interval
        + " second interval for " + desc;
    for (int i = 0; i < INVERSE_QUANTILES.length; i++) {
      int inversePercentile = (int) (100 * (1 - INVERSE_QUANTILES[i].quantile));
      addQuantileInfo(i, info(String.format(nameTemplate, inversePercentile),
          String.format(descTemplate, inversePercentile)));
    }

    setEstimator(new SampleQuantiles(INVERSE_QUANTILES));
    setInterval(interval);
    scheduledTask = SCHEDULAR.scheduleWithFixedDelay(new RolloverSample(this),
        interval, interval, TimeUnit.SECONDS);
  }

  public void stop() {
    if (scheduledTask != null) {
      scheduledTask.cancel(false);
    }
    scheduledTask = null;
  }
}

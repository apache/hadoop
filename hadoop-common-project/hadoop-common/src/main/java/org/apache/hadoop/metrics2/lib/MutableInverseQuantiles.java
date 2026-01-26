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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.metrics2.util.Quantile;
import java.text.DecimalFormat;
import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * Watches a stream of long values, maintaining online estimates of specific
 * quantiles with provably low error bounds. Inverse quantiles are meant for
 * highly accurate low-percentile (e.g. 1st, 5th) metrics.
 * InverseQuantiles are used for metrics where higher the value better it is.
 * ( eg: data transfer rate ).
 * The 1st percentile here corresponds to the 99th inverse percentile metric,
 * 5th percentile to 95th and so on.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MutableInverseQuantiles extends MutableQuantiles{

  static class InversePercentile extends Quantile {
    InversePercentile(double inversePercentile) {
      super(inversePercentile/100, inversePercentile/1000);
    }
  }

  @VisibleForTesting
  public static final Quantile[] INVERSE_QUANTILES = {new InversePercentile(50),
      new InversePercentile(25), new InversePercentile(10),
      new InversePercentile(5), new InversePercentile(1)};

  /**
   * Instantiates a new {@link MutableInverseQuantiles} for a metric that rolls itself
   * over on the specified time interval.
   *
   * @param name          of the metric
   * @param description   long-form textual description of the metric
   * @param sampleName    type of items in the stream (e.g., "Ops")
   * @param valueName     type of the values
   * @param intervalSecs  rollover interval (in seconds) of the estimator
   */
  public MutableInverseQuantiles(String name, String description, String sampleName,
      String valueName, int intervalSecs) {
    super(name, description, sampleName, valueName, intervalSecs);
  }

  /**
   * Sets quantileInfo.
   *
   * @param ucName capitalized name of the metric
   * @param uvName capitalized type of the values
   * @param desc uncapitalized long-form textual description of the metric
   * @param lvName uncapitalized type of the values
   * @param df Number formatter for inverse percentile value
   */
  void setQuantiles(String ucName, String uvName, String desc, String lvName, DecimalFormat df) {
    for (int i = 0; i < INVERSE_QUANTILES.length; i++) {
      double inversePercentile = 100 * (1 - INVERSE_QUANTILES[i].quantile);
      String nameTemplate = ucName + df.format(inversePercentile) + "thInversePercentile" + uvName;
      String descTemplate = df.format(inversePercentile) + " inverse percentile " + lvName
          + " with " + getInterval() + " second interval for " + desc;
      addQuantileInfo(i, info(nameTemplate, descTemplate));
    }
  }

  /**
   * Returns the array of Inverse Quantiles declared in MutableInverseQuantiles.
   *
   * @return array of Inverse Quantiles
   */
  public synchronized Quantile[] getQuantiles() {
    return INVERSE_QUANTILES;
  }
}

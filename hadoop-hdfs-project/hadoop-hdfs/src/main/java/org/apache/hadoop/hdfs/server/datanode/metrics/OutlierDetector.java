/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode.metrics;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * A utility class to help detect resources (nodes/ disks) whose aggregate
 * latency is an outlier within a given set.
 *
 * We use the median absolute deviation for outlier detection as
 * described in the following publication:
 *
 * Leys, C., et al., Detecting outliers: Do not use standard deviation
 * around the mean, use absolute deviation around the median.
 * http://dx.doi.org/10.1016/j.jesp.2013.03.013
 *
 * We augment the above scheme with the following heuristics to be even
 * more conservative:
 *
 *  1. Skip outlier detection if the sample size is too small.
 *  2. Never flag resources whose aggregate latency is below a low threshold.
 *  3. Never flag resources whose aggregate latency is less than a small
 *     multiple of the median.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class OutlierDetector {
  public static final Logger LOG =
      LoggerFactory.getLogger(OutlierDetector.class);

  /**
   * Minimum number of resources to run outlier detection.
   */
  private final long minNumResources;

  /**
   * The multiplier is from Leys, C. et al.
   */
  private static final double MAD_MULTIPLIER = (double) 1.4826;

  /**
   * Threshold in milliseconds below which a node/ disk is definitely not slow.
   */
  private final long lowThresholdMs;

  /**
   * Deviation multiplier. A sample is considered to be an outlier if it
   * exceeds the median by (multiplier * median abs. deviation). 3 is a
   * conservative choice.
   */
  private static final int DEVIATION_MULTIPLIER = 3;

  /**
   * If most of the samples are clustered together, the MAD can be
   * low. The median multiplier introduces another safeguard to avoid
   * overaggressive outlier detection.
   */
  @VisibleForTesting
  static final int MEDIAN_MULTIPLIER = 3;

  public OutlierDetector(long minNumResources, long lowThresholdMs) {
    this.minNumResources = minNumResources;
    this.lowThresholdMs = lowThresholdMs;
  }

  /**
   * Return a set of nodes/ disks whose latency is much higher than
   * their counterparts. The input is a map of (resource {@literal ->} aggregate
   * latency)
   * entries.
   *
   * The aggregate may be an arithmetic mean or a percentile e.g.
   * 90th percentile. Percentiles are a better choice than median
   * since latency is usually not a normal distribution.
   *
   * This method allocates temporary memory O(n) and
   * has run time O(n.log(n)), where n = stats.size().
   *
   * @return
   */
  public Map<String, Double> getOutliers(Map<String, Double> stats) {
    if (stats.size() < minNumResources) {
      LOG.debug("Skipping statistical outlier detection as we don't have " +
              "latency data for enough resources. Have {}, need at least {}",
          stats.size(), minNumResources);
      return ImmutableMap.of();
    }
    // Compute the median absolute deviation of the aggregates.
    final List<Double> sorted = new ArrayList<>(stats.values());
    Collections.sort(sorted);
    final Double median = computeMedian(sorted);
    final Double mad = computeMad(sorted);
    Double upperLimitLatency = Math.max(
        lowThresholdMs, median * MEDIAN_MULTIPLIER);
    upperLimitLatency = Math.max(
        upperLimitLatency, median + (DEVIATION_MULTIPLIER * mad));

    final Map<String, Double> slowResources = new HashMap<>();

    LOG.trace("getOutliers: List={}, MedianLatency={}, " +
        "MedianAbsoluteDeviation={}, upperLimitLatency={}",
        sorted, median, mad, upperLimitLatency);

    // Find resources whose latency exceeds the threshold.
    for (Map.Entry<String, Double> entry : stats.entrySet()) {
      if (entry.getValue() > upperLimitLatency) {
        slowResources.put(entry.getKey(), entry.getValue());
      }
    }

    return slowResources;
  }

  /**
   * Compute the Median Absolute Deviation of a sorted list.
   */
  public static Double computeMad(List<Double> sortedValues) {
    if (sortedValues.size() == 0) {
      throw new IllegalArgumentException(
          "Cannot compute the Median Absolute Deviation " +
              "of an empty list.");
    }

    // First get the median of the values.
    Double median = computeMedian(sortedValues);
    List<Double> deviations = new ArrayList<>(sortedValues);

    // Then update the list to store deviation from the median.
    for (int i = 0; i < sortedValues.size(); ++i) {
      deviations.set(i, Math.abs(sortedValues.get(i) - median));
    }

    // Finally get the median absolute deviation.
    Collections.sort(deviations);
    return computeMedian(deviations) * MAD_MULTIPLIER;
  }

  /**
   * Compute the median of a sorted list.
   */
  public static Double computeMedian(List<Double> sortedValues) {
    if (sortedValues.size() == 0) {
      throw new IllegalArgumentException(
          "Cannot compute the median of an empty list.");
    }

    Double median = sortedValues.get(sortedValues.size() / 2);
    if (sortedValues.size() % 2 == 0) {
      median += sortedValues.get((sortedValues.size() / 2) - 1);
      median /= 2;
    }
    return median;
  }
}

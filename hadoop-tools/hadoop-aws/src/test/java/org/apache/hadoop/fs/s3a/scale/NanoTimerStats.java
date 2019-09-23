/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.s3a.scale;

import org.apache.hadoop.fs.contract.ContractTestUtils;

/**
 * Collect statistics from duration data from
 * {@link ContractTestUtils.NanoTimer} values.
 *
 * The mean and standard deviation is built up as the stats are collected,
 * using "Welford's Online algorithm" for the variance.
 * Trends in statistics (e.g. slowing down) are not tracked.
 * Not synchronized.
 */
public class NanoTimerStats {

  private static final double ONE_NS = 1.0e9;

  private final String operation;

  private int count;

  private double sum;

  private double min;

  private double max;

  private double mean;

  private double m2;

  /**
   * Construct statistics for a given operation.
   * @param operation operation
   */
  public NanoTimerStats(String operation) {
    this.operation = operation;
    reset();
  }

  /**
   * construct from another stats entry;
   * all value are copied.
   * @param that the source statistics
   */
  public NanoTimerStats(NanoTimerStats that) {
    operation = that.operation;
    count = that.count;
    sum = that.sum;
    min = that.min;
    max = that.max;
    mean = that.mean;
    m2 = that.m2;
  }

  /**
   * Add a duration.
   * @param duration the new duration
   */
  public void add(ContractTestUtils.NanoTimer duration) {
    add(duration.elapsedTime());
  }

  /**
   * Add a number.
   * @param x the number
   */
  public void add(long x) {
    count++;
    sum += x;
    double delta = x - mean;
    mean += delta / count;
    double delta2 = x - mean;
    m2 += delta * delta2;
    if (min < 0 || x < min) {
      min = x;
    }
    if (x > max) {
      max = x;
    }
  }

  /**
   * Reset the data.
   */
  public void reset() {
    count = 0;
    sum = 0;
    sum = 0;
    min = -1;
    max = 0;
    mean = 0;
    m2 = 0;
  }

  /**
   * Get the number of entries sampled.
   * @return the number of durations added
   */
  public int getCount() {
    return count;
  }

  /**
   * Get the sum of all durations.
   * @return all the durations
   */
  public double getSum() {
    return sum;
  }

  /**
   * Get the arithmetic mean of the aggregate statistics.
   * @return the arithmetic mean
   */
  public double getArithmeticMean() {
    return mean;
  }

  /**
   * Variance, {@code sigma^2}.
   * @return variance, or, if no samples are there, 0.
   */
  public double getVariance() {
    return count > 0 ? (m2 / (count - 1)) :
        Double.NaN;
  }

  /**
   * Get the std deviation, sigma.
   * @return the stddev, 0 may mean there are no samples.
   */
  public double getDeviation() {
    double variance = getVariance();
    return (!Double.isNaN(variance) && variance > 0)  ? Math.sqrt(variance) : 0;
  }

  private double toSeconds(double nano) {
    return nano / ONE_NS;
  }

  /**
   * Covert to a useful string.
   * @return a human readable summary
   */
  @Override
  public String toString() {
    return String.format(
        "%s count=%d total=%.3fs mean=%.3fs stddev=%.3fs min=%.3fs max=%.3fs",
        operation,
        count,
        toSeconds(sum),
        toSeconds(mean),
        getDeviation() / ONE_NS,
        toSeconds(min),
        toSeconds(max));
  }

  public String getOperation() {
    return operation;
  }

  public double getMin() {
    return min;
  }

  public double getMax() {
    return max;
  }

  public double getMean() {
    return mean;
  }
}

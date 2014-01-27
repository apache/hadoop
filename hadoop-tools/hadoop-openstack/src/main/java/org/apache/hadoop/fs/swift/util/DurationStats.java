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

package org.apache.hadoop.fs.swift.util;

/**
 * Build ongoing statistics from duration data
 */
public class DurationStats {

  final String operation;
  int n;
  long sum;
  long min;
  long max;
  double mean, m2;

  /**
   * Construct statistics for a given operation.
   * @param operation operation
   */
  public DurationStats(String operation) {
    this.operation = operation;
    reset();
  }

  /**
   * construct from anothr stats entry;
   * all value are copied.
   * @param that the source statistics
   */
  public DurationStats(DurationStats that) {
    operation = that.operation;
    n = that.n;
    sum = that.sum;
    min = that.min;
    max = that.max;
    mean = that.mean;
    m2 = that.m2;
  }

  /**
   * Add a duration
   * @param duration the new duration
   */
  public void add(Duration duration) {
    add(duration.value());
  }

  /**
   * Add a number
   * @param x the number
   */
  public void add(long x) {
    n++;
    sum += x;
    double delta = x - mean;
    mean += delta / n;
    m2 += delta * (x - mean);
    if (x < min) {
      min = x;
    }
    if (x > max) {
      max = x;
    }
  }

  /**
   * Reset the data
   */
  public void reset() {
    n = 0;
    sum = 0;
    sum = 0;
    min = 10000000;
    max = 0;
    mean = 0;
    m2 = 0;
  }

  /**
   * Get the number of entries sampled
   * @return the number of durations added
   */
  public int getCount() {
    return n;
  }

  /**
   * Get the sum of all durations
   * @return all the durations
   */
  public long getSum() {
    return sum;
  }

  /**
   * Get the arithmetic mean of the aggregate statistics
   * @return the arithmetic mean
   */
  public double getArithmeticMean() {
    return mean;
  }

  /**
   * Variance, sigma^2
   * @return variance, or, if no samples are there, 0.
   */
  public double getVariance() {
    return n > 0 ? (m2 / (n - 1)) : 0;
  }

  /**
   * Get the std deviation, sigma
   * @return the stddev, 0 may mean there are no samples.
   */
  public double getDeviation() {
    double variance = getVariance();
    return (variance > 0) ? Math.sqrt(variance) : 0;
  }

  /**
   * Covert to a useful string
   * @return a human readable summary
   */
  @Override
  public String toString() {
    return String.format(
      "%s count=%d total=%.3fs mean=%.3fs stddev=%.3fs min=%.3fs max=%.3fs",
      operation,
      n,
      sum / 1000.0,
      mean / 1000.0,
      getDeviation() / 1000000.0,
      min / 1000.0,
      max / 1000.0);
  }

}

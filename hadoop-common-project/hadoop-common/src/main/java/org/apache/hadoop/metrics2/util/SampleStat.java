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

package org.apache.hadoop.metrics2.util;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Helper to compute running sample stats
 */
@InterfaceAudience.Private
public class SampleStat {
  private final MinMax minmax = new MinMax();
  private long numSamples = 0;
  private double a0, a1, s0, s1, total;

  /**
   * Construct a new running sample stat
   */
  public SampleStat() {
    a0 = s0 = 0.0;
    total = 0.0;
  }

  public void reset() {
    numSamples = 0;
    a0 = s0 = 0.0;
    total = 0.0;
    minmax.reset();
  }

  // We want to reuse the object, sometimes.
  void reset(long numSamples, double a0, double a1, double s0, double s1,
      double total, MinMax minmax) {
    this.numSamples = numSamples;
    this.a0 = a0;
    this.a1 = a1;
    this.s0 = s0;
    this.s1 = s1;
    this.total = total;
    this.minmax.reset(minmax);
  }

  /**
   * Copy the values to other (saves object creation and gc.)
   * @param other the destination to hold our values
   */
  public void copyTo(SampleStat other) {
    other.reset(numSamples, a0, a1, s0, s1, total, minmax);
  }

  /**
   * Add a sample the running stat.
   * @param x the sample number
   * @return  self
   */
  public SampleStat add(double x) {
    minmax.add(x);
    return add(1, x);
  }

  /**
   * Add some sample and a partial sum to the running stat.
   * Note, min/max is not evaluated using this method.
   * @param nSamples  number of samples
   * @param x the partial sum
   * @return  self
   */
  public SampleStat add(long nSamples, double x) {
    numSamples += nSamples;
    total += x;

    if (numSamples == 1) {
      a0 = a1 = x;
      s0 = 0.0;
    }
    else {
      // The Welford method for numerical stability
      a1 = a0 + (x - a0) / numSamples;
      s1 = s0 + (x - a0) * (x - a1);
      a0 = a1;
      s0 = s1;
    }
    return this;
  }

  /**
   * @return  the total number of samples
   */
  public long numSamples() {
    return numSamples;
  }

  /**
   * @return the total of all samples added
   */
  public double total() {
    return total;
  }

  /**
   * @return  the arithmetic mean of the samples
   */
  public double mean() {
    return numSamples > 0 ? (total / numSamples) : 0.0;
  }

  /**
   * @return  the variance of the samples
   */
  public double variance() {
    return numSamples > 1 ? s1 / (numSamples - 1) : 0.0;
  }

  /**
   * @return  the standard deviation of the samples
   */
  public double stddev() {
    return Math.sqrt(variance());
  }

  /**
   * @return  the minimum value of the samples
   */
  public double min() {
    return minmax.min();
  }

  /**
   * @return  the maximum value of the samples
   */
  public double max() {
    return minmax.max();
  }

  /**
   * Helper to keep running min/max
   */
  @SuppressWarnings("PublicInnerClass")
  public static class MinMax {

    // Float.MAX_VALUE is used rather than Double.MAX_VALUE, even though the
    // min and max variables are of type double.
    // Float.MAX_VALUE is big enough, and using Double.MAX_VALUE makes 
    // Ganglia core due to buffer overflow.
    // The same reasoning applies to the MIN_VALUE counterparts.
    static final double DEFAULT_MIN_VALUE = Float.MAX_VALUE;
    static final double DEFAULT_MAX_VALUE = Float.MIN_VALUE;

    private double min = DEFAULT_MIN_VALUE;
    private double max = DEFAULT_MAX_VALUE;

    public void add(double value) {
      if (value > max) max = value;
      if (value < min) min = value;
    }

    public double min() { return min; }
    public double max() { return max; }

    public void reset() {
      min = DEFAULT_MIN_VALUE;
      max = DEFAULT_MAX_VALUE;
    }

    public void reset(MinMax other) {
      min = other.min();
      max = other.max();
    }
  }
}

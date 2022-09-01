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
  private double mean, s;

  /**
   * Construct a new running sample stat
   */
  public SampleStat() {
    mean = 0.0;
    s = 0.0;
  }

  public void reset() {
    numSamples = 0;
    mean = 0.0;
    s = 0.0;
    minmax.reset();
  }

  // We want to reuse the object, sometimes.
  void reset(long numSamples1, double mean1, double s1, MinMax minmax1) {
    numSamples = numSamples1;
    mean = mean1;
    s = s1;
    minmax.reset(minmax1);
  }

  /**
   * Copy the values to other (saves object creation and gc.)
   * @param other the destination to hold our values
   */
  public void copyTo(SampleStat other) {
    other.reset(numSamples, mean, s, minmax);
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
   * @param xTotal the partial sum
   * @return  self
   */
  public SampleStat add(long nSamples, double xTotal) {
    numSamples += nSamples;

    // use the weighted incremental version of Welford's algorithm to get
    // numerical stability while treating the samples as being weighted
    // by nSamples
    // see https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance

    double x = xTotal / nSamples;
    double meanOld = mean;

    mean += ((double) nSamples / numSamples) * (x - meanOld);
    s += nSamples * (x - meanOld) * (x - mean);
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
    return mean * numSamples;
  }

  /**
   * @return  the arithmetic mean of the samples
   */
  public double mean() {
    return numSamples > 0 ? mean : 0.0;
  }

  /**
   * @return  the variance of the samples
   */
  public double variance() {
    return numSamples > 1 ? s / (numSamples - 1) : 0.0;
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

  @Override
  public String toString() {
    try {
      return "Samples = " + numSamples() +
          "  Min = " + min() +
          "  Mean = " + mean() +
          "  Std Dev = " + stddev() +
          "  Max = " + max();
    } catch (Throwable t) {
      return super.toString();
    }
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

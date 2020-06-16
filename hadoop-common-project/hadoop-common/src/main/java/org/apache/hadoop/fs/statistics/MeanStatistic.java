/*
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

package org.apache.hadoop.fs.statistics;

import java.io.Serializable;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A mean statistic.
 * The sample size is required so that means can be aggregated.
 * If a statistic has 0 samples then it is considered to be empty.
 */
public final class MeanStatistic implements Serializable, Cloneable {

  private static final long serialVersionUID = 567888327998615425L;

  private double mean;

  private long samples;

  /**
   * Constructor.
   * If the sample count is 0, the mean is set to 0.
   * @param mean mean value
   * @param samples sample count.
   */
  public MeanStatistic(final double mean, final long samples) {
    if (samples != 0) {
      checkArgument(mean >= 0);
      checkArgument(samples > 0);
      this.mean = mean;
      this.samples = samples;
    }
  }

  /**
   * Create from another statistic.
   * @param that source
   */
  public MeanStatistic(MeanStatistic that) {
    this(that.mean, that.samples);
  }

  /**
   * Create an empty statistic.
   */
  public MeanStatistic() {
  }

  /**
   * Get the mean value.
   * @return the mean
   */
  public double getMean() {
    return mean;
  }

  /**
   * Get the sample count.
   * @return the sample count; 0 means empty
   */
  public long getSamples() {
    return samples;
  }

  /**
   * Is a statistic empty?
   * @return true if the sample count is 0
   */
  public boolean isEmpty() {
    return samples == 0;
  }

  /**
   * Add another mean statistic to create a new statistic.
   * When adding two statistics, if either is empty then
   * a copy of the non-empty statistic is returned.
   * If both are empty then a new empty statistic is returned.
   *
   * @param other other value
   * @return the aggregate mean
   */
  public MeanStatistic add(final MeanStatistic other) {
    if (isEmpty()) {
      new MeanStatistic(other);
    }
    if (other.isEmpty()) {
      new MeanStatistic(other);
    }
    long rSamples = other.samples;
    double rSum = other.mean * rSamples;
    long totalSamples = samples + rSamples;
    if (totalSamples == 0) {
      return new MeanStatistic(0, 0);
    } else {
      double sum = mean * samples + rSum;
      return new MeanStatistic(sum / totalSamples, totalSamples);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(mean, samples);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) { return true; }
    if (o == null || getClass() != o.getClass()) { return false; }
    MeanStatistic that = (MeanStatistic) o;
    return Double.compare(that.mean, mean) == 0 &&
        samples == that.samples;
  }

  @Override
  public MeanStatistic clone() {
    return new MeanStatistic(this);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "MeanStatistic{");
    sb.append("mean=").append(mean);
    sb.append(", samples=").append(samples);
    sb.append('}');
    return sb.toString();
  }

  public MeanStatistic copy() {
    return new MeanStatistic(this);
  }
}

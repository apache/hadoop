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
 * A mean statistic represented as the sum and the sample count;
 * the mean is calculated on demand.
 * <p></p>
 * It can be used to accrue values so as to dynamically update
 * the mean. If so, know that there is no synchronization
 * on the methods.
 * <p></p>*
 * If a statistic has 0 samples then it is considered to be empty.
 */
public final class MeanStatistic implements Serializable, Cloneable {

  private static final long serialVersionUID = 567888327998615425L;

  /**
   * sum of the values.
   */
  private long sum;

  /**
   * Number of samples used to calculate
   * the mean.
   */
  private long samples;

  /**
   * Constructor.
   * If the sample count is 0, the sum is set to 0.
   * @param sum mean value
   * @param samples sample count.
   */
  public MeanStatistic(final long sum, final long samples) {
    if (samples != 0) {
      checkArgument(samples > 0);
      this.sum = sum;
      this.samples = samples;
    }
  }

  /**
   * Create from another statistic.
   * @param that source
   */
  public MeanStatistic(MeanStatistic that) {
    this(that.sum, that.samples);
  }

  /**
   * Create an empty statistic.
   */
  public MeanStatistic() {
  }

  /**
   * Get the sum of samples.
   * @return the sum
   */
  public long getSum() {
    return sum;
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


  public void setSum(final long sum) {
    this.sum = sum;
  }

  public void setSamples(final long samples) {
    checkArgument(samples >= 0);
    this.samples = samples;
  }

  /**
   * Get the arithmetic mean value.
   * @return the mean
   */
  public double mean() {
    return samples > 0
        ? ((double)sum)/ samples
        : 0.0;
  }

  /**
   * Add another MeanStatistic.
   * @param other other value
   */
  public void add(final MeanStatistic other) {
    if (other.isEmpty()) {
      return;
    }
    if (isEmpty()) {
      samples = other.samples;
      sum = other.sum;
      return;
    }
    samples += other.samples;
    sum += other.sum;
  }

  /**
   * Add a sample.
   * Unsynchronized/nonatomic
   * @param value value to add to the sum
   */
  public void addSample(int value) {
    samples++;
    sum += value;
  }

  /**
   * The hash code is derived from the mean
   * and sample count: if either is changed
   * the statistic cannot be used as a key
   * for hash tables/maps
   * @return a hash value
   */
  @Override
  public int hashCode() {
    return Objects.hash(sum, samples);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) { return true; }
    if (o == null || getClass() != o.getClass()) { return false; }
    MeanStatistic that = (MeanStatistic) o;
    return sum == that.sum &&
        samples == that.samples;
  }

  @Override
  public MeanStatistic clone() {
    return copy();
  }

  /**
   * Create a copy of this instance.
   * @return copy.
   *
   */
  public MeanStatistic copy() {
    return new MeanStatistic(this);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "MeanStatistic{");
    sb.append("sum=").append(sum);
    sb.append(", samples=").append(samples);
    sb.append(", mean=").append(mean());
    sb.append('}');
    return sb.toString();
  }

}

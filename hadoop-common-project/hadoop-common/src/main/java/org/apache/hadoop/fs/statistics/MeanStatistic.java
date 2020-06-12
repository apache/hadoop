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

public final class MeanStatistic implements Serializable {

  private static final long serialVersionUID = 567888327998615425L;

  private double mean;

  private long samples;

  public MeanStatistic(final double mean, final long samples) {
    setMean(mean);
    setSamples(samples);
  }

  public MeanStatistic() {
  }

  public double getMean() {
    return mean;
  }

  public void setMean(final double mean) {
    checkArgument(mean >= 0);
    this.mean = mean;
  }

  public long getSamples() {
    return samples;
  }

  public void setSamples(final long samples) {
    checkArgument(samples > 0);
    this.samples = samples;
  }

  /**
   * Add another mean statistic to create a new statistic.
   * @param other other value
   * @return the aggregate mean
   */
  public MeanStatistic add(final MeanStatistic other) {
    double lSum = mean * samples;
    long rSamples = other.samples;
    double rSum = other.mean * rSamples;
    long totalSamples = samples + rSamples;
    checkArgument(totalSamples > 0, "total number of samples is %s",
        totalSamples);
    double sum =lSum + rSum;
    return new MeanStatistic(sum / totalSamples, totalSamples);
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

  @Override
  public boolean equals(final Object o) {
    if (this == o) { return true; }
    if (o == null || getClass() != o.getClass()) { return false; }
    MeanStatistic that = (MeanStatistic) o;
    return Double.compare(that.mean, mean) == 0 &&
        samples == that.samples;
  }

  @Override
  public int hashCode() {
    return Objects.hash(mean, samples);
  }
}

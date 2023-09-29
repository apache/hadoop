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

package org.apache.hadoop.fs.s3a.statistics.impl;

import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

/**
 * Base class for implementing IOStatistics sources in the S3 module.
 * <p>
 * A lot of the methods are very terse, because S3AInstrumentation has
 * verbose methods of similar names; the short ones always
 * refer to the inner class and not any superclass method.
 * </p>
 */
public abstract class AbstractS3AStatisticsSource implements
    IOStatisticsSource, DurationTrackerFactory {

  private IOStatisticsStore ioStatistics;

  protected AbstractS3AStatisticsSource() {
  }

  @Override
  public IOStatisticsStore getIOStatistics() {
    return ioStatistics;
  }

  /**
   * Setter.
   * this must be called in the subclass constructor with
   * whatever
   * @param statistics statistics to set
   */
  protected void setIOStatistics(final IOStatisticsStore statistics) {
    this.ioStatistics = statistics;
  }

  /**
   * Increment a named counter by 1.
   * @param name counter name
   * @return the updated value or, if the counter is unknown: 0
   */
  public long incCounter(String name) {
    return incCounter(name, 1);
  }

  /**DefaultS3ClientFactoryDefaultS3ClientFactory
   * Increment a named counter by 1.
   * @param name counter name
   * @param value value to increment by
   * @return the updated value or, if the counter is unknown: 0
   */
  public long incCounter(String name, long value) {
    return ioStatistics.incrementCounter(name, value);
  }

  /**
   * {@inheritDoc}
   */
  public Long lookupCounterValue(final String name) {
    return ioStatistics.counters().get(name);
  }

  /**
   * {@inheritDoc}
   */
  public Long lookupGaugeValue(final String name) {
    return ioStatistics.gauges().get(name);
  }

  public long incGauge(String name, long v) {
    return ioStatistics.incrementGauge(name, v);
  }

  public long incGauge(String name) {
    return incGauge(name, 1);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "AbstractS3AStatisticsSource{");
    sb.append(ioStatistics);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public DurationTracker trackDuration(final String key, final long count) {
    return getIOStatistics().trackDuration(key, count);
  }
}

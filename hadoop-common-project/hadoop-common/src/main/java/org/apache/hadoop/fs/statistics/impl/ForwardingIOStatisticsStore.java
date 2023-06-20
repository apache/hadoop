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

package org.apache.hadoop.fs.statistics.impl;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.MeanStatistic;

/**
 * This may seem odd having an IOStatisticsStore which does nothing
 * but forward to a wrapped store, but it's designed to
 * assist in subclassing of selective methods, such
 * as those to increment counters, get durations etc.
 */
public class ForwardingIOStatisticsStore implements IOStatisticsStore {

  private final IOStatisticsStore innerStatistics;

  public ForwardingIOStatisticsStore(
      final IOStatisticsStore innerStatistics) {
    this.innerStatistics = innerStatistics;
  }

  protected IOStatisticsStore getInnerStatistics() {
    return innerStatistics;
  }

  @Override
  public Map<String, Long> counters() {
    return getInnerStatistics().counters();
  }

  @Override
  public Map<String, Long> gauges() {
    return getInnerStatistics().gauges();
  }

  @Override
  public Map<String, Long> minimums() {
    return getInnerStatistics().minimums();
  }

  @Override
  public Map<String, Long> maximums() {
    return getInnerStatistics().maximums();
  }

  @Override
  public Map<String, MeanStatistic> meanStatistics() {
    return getInnerStatistics().meanStatistics();
  }

  @Override
  public boolean aggregate(@Nullable final IOStatistics statistics) {
    return getInnerStatistics().aggregate(statistics);
  }

  @Override
  public long incrementCounter(final String key, final long value) {
    return getInnerStatistics().incrementCounter(key, value);
  }

  @Override
  public void setCounter(final String key, final long value) {
    getInnerStatistics().setCounter(key, value);
  }

  @Override
  public void setGauge(final String key, final long value) {
    getInnerStatistics().setGauge(key, value);
  }

  @Override
  public long incrementGauge(final String key, final long value) {
    return getInnerStatistics().incrementGauge(key, value);
  }

  @Override
  public void setMaximum(final String key, final long value) {
    getInnerStatistics().setMaximum(key, value);
  }

  @Override
  public long incrementMaximum(final String key, final long value) {
    return getInnerStatistics().incrementMaximum(key, value);
  }

  @Override
  public void setMinimum(final String key, final long value) {
    getInnerStatistics().setMinimum(key, value);

  }

  @Override
  public long incrementMinimum(final String key, final long value) {
    return getInnerStatistics().incrementMinimum(key, value);

  }

  @Override
  public void addMinimumSample(final String key, final long value) {
    getInnerStatistics().addMinimumSample(key, value);

  }

  @Override
  public void addMaximumSample(final String key, final long value) {
    getInnerStatistics().addMaximumSample(key, value);
  }

  @Override
  public void setMeanStatistic(final String key, final MeanStatistic value) {
    getInnerStatistics().setMeanStatistic(key, value);

  }

  @Override
  public void addMeanStatisticSample(final String key, final long value) {
    getInnerStatistics().addMeanStatisticSample(key, value);

  }

  @Override
  public void reset() {
    getInnerStatistics().reset();
  }

  @Override
  public AtomicLong getCounterReference(final String key) {
    return getInnerStatistics().getCounterReference(key);
  }

  @Override
  public AtomicLong getMaximumReference(final String key) {
    return getInnerStatistics().getMaximumReference(key);
  }

  @Override
  public AtomicLong getMinimumReference(final String key) {
    return getInnerStatistics().getMinimumReference(key);
  }

  @Override
  public AtomicLong getGaugeReference(final String key) {
    return getInnerStatistics().getGaugeReference(key);
  }

  @Override
  public MeanStatistic getMeanStatistic(final String key) {
    return getInnerStatistics().getMeanStatistic(key);
  }

  @Override
  public void addTimedOperation(final String prefix,
      final long durationMillis) {
    getInnerStatistics().addTimedOperation(prefix, durationMillis);

  }

  @Override
  public void addTimedOperation(final String prefix,
      final Duration duration) {
    getInnerStatistics().addTimedOperation(prefix, duration);
  }
}

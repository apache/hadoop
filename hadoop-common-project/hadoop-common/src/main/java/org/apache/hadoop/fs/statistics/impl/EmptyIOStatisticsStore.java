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

import static java.util.Collections.emptyMap;

/**
 * An Empty IOStatisticsStore implementation.
 */
final class EmptyIOStatisticsStore implements IOStatisticsStore {

  /**
   * The sole instance of this class.
   */
  private static final EmptyIOStatisticsStore INSTANCE =
      new EmptyIOStatisticsStore();
  /**
   * Get the single instance of this class.
   * @return a shared, empty instance.
   */
  static IOStatisticsStore getInstance() {
    return INSTANCE;
  }

  private EmptyIOStatisticsStore() {
  }

  @Override
  public Map<String, Long> counters() {
    return emptyMap();
  }

  @Override
  public Map<String, Long> gauges() {
    return emptyMap();
  }

  @Override
  public Map<String, Long> minimums() {
    return emptyMap();
  }

  @Override
  public Map<String, Long> maximums() {
    return emptyMap();
  }

  @Override
  public Map<String, MeanStatistic> meanStatistics() {
    return emptyMap();
  }

  @Override
  public boolean aggregate(@Nullable final IOStatistics statistics) {
    return false;
  }

  @Override
  public long incrementCounter(final String key, final long value) {
    return 0;
  }

  @Override
  public void setCounter(final String key, final long value) {

  }

  @Override
  public void setGauge(final String key, final long value) {

  }

  @Override
  public long incrementGauge(final String key, final long value) {
    return 0;
  }

  @Override
  public void setMaximum(final String key, final long value) {

  }

  @Override
  public long incrementMaximum(final String key, final long value) {
    return 0;
  }

  @Override
  public void setMinimum(final String key, final long value) {

  }

  @Override
  public long incrementMinimum(final String key, final long value) {
    return 0;
  }

  @Override
  public void addMinimumSample(final String key, final long value) {

  }

  @Override
  public void addMaximumSample(final String key, final long value) {

  }

  @Override
  public void setMeanStatistic(final String key, final MeanStatistic value) {

  }

  @Override
  public void addMeanStatisticSample(final String key, final long value) {

  }

  @Override
  public void reset() {

  }

  @Override
  public AtomicLong getCounterReference(final String key) {
    return null;
  }

  @Override
  public AtomicLong getMaximumReference(final String key) {
    return null;
  }

  @Override
  public AtomicLong getMinimumReference(final String key) {
    return null;
  }

  @Override
  public AtomicLong getGaugeReference(final String key) {
    return null;
  }

  @Override
  public MeanStatistic getMeanStatistic(final String key) {
    return null;
  }

  @Override
  public void addTimedOperation(final String prefix,
      final long durationMillis) {

  }

  @Override
  public void addTimedOperation(final String prefix, final Duration duration) {

  }
}

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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsAggregator;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.statistics.IOStatisticsSetters;
import org.apache.hadoop.fs.statistics.MeanStatistic;

/**
 * Interface of an IOStatistics store intended for
 * use in classes which track statistics for reporting.
 */
public interface IOStatisticsStore extends IOStatistics,
    IOStatisticsSetters,
    IOStatisticsAggregator,
    DurationTrackerFactory {

  /**
   * Increment a counter by one.
   *
   * No-op if the counter is unknown.
   * @param key statistics key
   * @return old value or, if the counter is unknown: 0
   */
  default long incrementCounter(String key) {
    return incrementCounter(key, 1);
  }

  /**
   * Increment a counter.
   *
   * No-op if the counter is unknown.
   * If the value is negative, it is ignored.
   * @param key statistics key
   * @param value value to increment
   * @return the updated value or, if the counter is unknown: 0
   */
  long incrementCounter(String key, long value);

  /**
   * Increment a gauge.
   * <p>
   * No-op if the gauge is unknown.
   * </p>
   * @param key statistics key
   * @param value value to increment
   * @return new value or 0 if the key is unknown
   */
  long incrementGauge(String key, long value);

  /**
   * Increment a maximum.
   * <p>
   * No-op if the maximum is unknown.
   * </p>
   * @param key statistics key
   * @param value value to increment
   * @return new value or 0 if the key is unknown
   */
  long incrementMaximum(String key, long value);

  /**
   * Increment a minimum.
   * <p>
   * No-op if the minimum is unknown.
   * </p>
   * @param key statistics key
   * @param value value to increment
   * @return new value or 0 if the key is unknown
   */
  long incrementMinimum(String key, long value);

  /**
   * Add a minimum sample: if less than the current value,
   * updates the value.
   * <p>
   * No-op if the minimum is unknown.
   * </p>
   * @param key statistics key
   * @param value sample value
   */
  void addMinimumSample(String key, long value);

  /**
   * Add a maximum sample: if greater than the current value,
   * updates the value.
   * <p>
   * No-op if the key is unknown.
   * </p>
   * @param key statistics key
   * @param value sample value
   */
  void addMaximumSample(String key, long value);

  /**
   * Add a sample to the mean statistics.
   * <p>
   * No-op if the key is unknown.
   * </p>
   * @param key key
   * @param value sample value.
   */
  void addMeanStatisticSample(String key, long value);

  /**
   * Reset all statistics.
   * Unsynchronized.
   */
  void reset();

  /**
   * Get a reference to the atomic instance providing the
   * value for a specific counter. This is useful if
   * the value is passed around.
   * @param key statistic name
   * @return the reference
   * @throws NullPointerException if there is no entry of that name
   */
  AtomicLong getCounterReference(String key);

  /**
   * Get a reference to the atomic instance providing the
   * value for a specific maximum. This is useful if
   * the value is passed around.
   * @param key statistic name
   * @return the reference
   * @throws NullPointerException if there is no entry of that name
   */
  AtomicLong getMaximumReference(String key);

  /**
   * Get a reference to the atomic instance providing the
   * value for a specific minimum. This is useful if
   * the value is passed around.
   * @param key statistic name
   * @return the reference
   * @throws NullPointerException if there is no entry of that name
   */
  AtomicLong getMinimumReference(String key);

  /**
   * Get a reference to the atomic instance providing the
   * value for a specific gauge. This is useful if
   * the value is passed around.
   * @param key statistic name
   * @return the reference
   * @throws NullPointerException if there is no entry of that name
   */
  AtomicLong getGaugeReference(String key);

  /**
   * Get a reference to the atomic instance providing the
   * value for a specific meanStatistic. This is useful if
   * the value is passed around.
   * @param key statistic name
   * @return the reference
   * @throws NullPointerException if there is no entry of that name
   */
  MeanStatistic getMeanStatistic(String key);

  /**
   * Add a duration to the min/mean/max statistics, using the
   * given prefix and adding a suffix for each specific value.
   *
   * The update is not-atomic, even though each individual statistic
   * is updated thread-safely. If two threads update the values
   * simultaneously, at the end of each operation the state will
   * be correct. It is only during the sequence that the statistics
   * may be observably inconsistent.
   * @param prefix statistic prefix
   * @param durationMillis duration in milliseconds.
   */
  void addTimedOperation(String prefix, long durationMillis);

  /**
   * Add a duration to the min/mean/max statistics, using the
   * given prefix and adding a suffix for each specific value.;
   * increment tha counter whose name == prefix.
   *
   * If any of the statistics are not registered, that part of
   * the sequence will be omitted -the rest will proceed.
   *
   * The update is not-atomic, even though each individual statistic
   * is updated thread-safely. If two threads update the values
   * simultaneously, at the end of each operation the state will
   * be correct. It is only during the sequence that the statistics
   * may be observably inconsistent.
   * @param prefix statistic prefix
   * @param duration duration
   */
  void addTimedOperation(String prefix, Duration duration);

  /**
   * Add a statistics sample as a min, max and mean and count.
   * @param key key to add.
   * @param count count.
   */
  default void addSample(String key, long count) {
    incrementCounter(key, count);
    addMeanStatisticSample(key, count);
    addMaximumSample(key, count);
    addMinimumSample(key, count);
  }
}

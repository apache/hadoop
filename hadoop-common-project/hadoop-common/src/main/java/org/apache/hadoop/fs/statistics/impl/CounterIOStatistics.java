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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.MeanStatistic;

/**
 * Interface an IOStatistics source where all the counters,
 * gauges, minimums/maximums
 * are implemented as a static set of counters with atomic
 * set/increment operations.
 * the means are implemented as a map to AtomicReferences.
 * <p></p>
 * Thread safe.
 */
public interface CounterIOStatistics extends IOStatistics {

  /**
   * Increment the counter by one.
   * No-op if the counter is unknown.
   * @param key statistics key
   * @return old value or 0
   */
  default long incrementCounter(String key) {
    return incrementCounter(key, 1);
  }

  long incrementCounter(String key, long value);

  void setCounter(String key, long value);

  void setMaximum(String key, long value);

  long incrementMaximum(String key, long value);

  void setMinimum(String key, long value);

  long incrementMinimum(String key, long value);

  void setGauge(String key, long value);

  long incrementGauge(String key, long value);

  void setMeanStatistic(String key, MeanStatistic value);


  /**
   * Reset all statistics.
   * Unsynchronized.
   */
  void reset();

  /**
   * Update the counter values from a statistics source.
   * The source must have all keys in this instance;
   * extra keys are ignored.
   * @param source source of statistics.
   */
  void copy(IOStatistics source);

  /**
   * Aggregate all statistics from a source into this instance.
   */
  void aggregate(IOStatistics source);

  /**
   * Subtract the counter values from a statistics source.
   * <p></p>
   * All entries must be counters.
   * <p></p>
   * The source must have all keys in this instance;
   * extra keys are ignored.
   * @param source source of statistics.
   */
  void subtractCounters(IOStatistics source);

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
  AtomicReference<MeanStatistic> getMeanStatisticReference(String key);
}

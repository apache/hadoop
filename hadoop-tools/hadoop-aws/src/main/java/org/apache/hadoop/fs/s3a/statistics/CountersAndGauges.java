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

package org.apache.hadoop.fs.s3a.statistics;

import java.time.Duration;

import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;

/**
 * This is the foundational API for collecting S3A statistics.
 */
public interface CountersAndGauges extends DurationTrackerFactory {

  /**
   * Increment a specific counter.
   * No-op if not defined.
   * @param op operation
   * @param count increment value
   */
  void incrementCounter(Statistic op, long count);

  /**
   * Increment a specific gauge.
   * No-op if not defined.
   * @param op operation
   * @param count increment value
   * @throws ClassCastException if the metric is of the wrong type
   */
  void incrementGauge(Statistic op, long count);

  /**
   * Decrement a specific gauge.
   * No-op if not defined.
   * @param op operation
   * @param count increment value
   * @throws ClassCastException if the metric is of the wrong type
   */
  void decrementGauge(Statistic op, long count);

  /**
   * Add a value to a quantiles statistic. No-op if the quantile
   * isn't found.
   * @param op operation to look up.
   * @param value value to add.
   * @throws ClassCastException if the metric is not a Quantiles.
   */
  void addValueToQuantiles(Statistic op, long value);

  /**
   * Record a duration.
   * @param op operation
   * @param success was the operation a success?
   * @param duration how long did it take
   */
  void recordDuration(Statistic op, boolean success, Duration duration);
}

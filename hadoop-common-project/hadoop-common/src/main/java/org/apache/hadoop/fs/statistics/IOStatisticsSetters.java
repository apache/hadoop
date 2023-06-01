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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Setter for IOStatistics entries.
 * These operations have been in the read/write API
 * {@code IOStatisticsStore} since IOStatistics
 * was added; extracting into its own interface allows for
 * {@link IOStatisticsSnapshot} to also support it.
 * These are the simple setters, they don't provide for increments,
 * decrements, calculation of min/max/mean etc.
 * @since The interface and IOStatisticsSnapshot support was added <i>after</i> Hadoop 3.3.5
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface IOStatisticsSetters extends IOStatistics {

  /**
   * Set a counter.
   *
   * No-op if the counter is unknown.
   * @param key statistics key
   * @param value value to set
   */
  void setCounter(String key, long value);

  /**
   * Set a gauge.
   *
   * @param key statistics key
   * @param value value to set
   */
  void setGauge(String key, long value);

  /**
   * Set a maximum.
   * @param key statistics key
   * @param value value to set
   */
  void setMaximum(String key, long value);

  /**
   * Set a minimum.
   * @param key statistics key
   * @param value value to set
   */
  void setMinimum(String key, long value);

  /**
   * Set a mean statistic to a given value.
   * @param key statistic key
   * @param value new value.
   */
  void setMeanStatistic(String key, MeanStatistic value);
}

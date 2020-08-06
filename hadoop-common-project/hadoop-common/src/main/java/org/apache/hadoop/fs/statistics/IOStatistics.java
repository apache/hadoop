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

import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * IO Statistics.
 * <p>
 * These are low-cost per-instance statistics provided by any Hadoop
 * I/O class instance.
 * <p>
 * Consult the filesystem specification document for the requirements
 * of an implementation of this interface.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface IOStatistics {

  /**
   * Map of counters.
   * @return the current map of counters.
   */
  Map<String, Long> counters();

  /**
   * Map of gauges.
   * @return the current map of gauges.
   */
  Map<String, Long> gauges();

  /**
   * Map of minimums.
   * @return the current map of minimums.
   */
  Map<String, Long> minimums();

  /**
   * Map of maximums.
   * @return the current map of maximums.
   */
  Map<String, Long> maximums();

  /**
   * Map of meanStatistics.
   * @return the current map of MeanStatistic statistics.
   */
  Map<String, MeanStatistic> meanStatistics();

  /**
   * Value when a minimum value has never been set.
   */
  long MIN_UNSET_VALUE = -1;

  /**
   * Value when a max value has never been set.
   */
  long MAX_UNSET_VALUE = -1;
}

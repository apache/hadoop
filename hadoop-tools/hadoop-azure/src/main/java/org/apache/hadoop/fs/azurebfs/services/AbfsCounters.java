/**
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

package org.apache.hadoop.fs.azurebfs.services;

import java.util.Map;
import java.util.List;
import org.apache.hadoop.classification.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azurebfs.AbfsStatistic;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.azurebfs.AbfsDriverMetrics;
/**
 * An interface for Abfs counters.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface AbfsCounters extends IOStatisticsSource, DurationTrackerFactory {

  /**
   * Increment a AbfsStatistic by a long value.
   *
   * @param statistic AbfsStatistic to be incremented.
   * @param value     the value to increment the statistic by.
   */
  void incrementCounter(AbfsStatistic statistic, long value);

  /**
   * Form a String of the all the statistics and present in an organized manner.
   *
   * @param prefix    the prefix to be set.
   * @param separator the separator between the statistic name and value.
   * @param suffix    the suffix to be used.
   * @param all       enable all the statistics to be displayed or not.
   * @return String of all the statistics and their values.
   */
  String formString(String prefix, String separator, String suffix,
      boolean all);

  /**
   * Convert all the statistics into a key-value pair map to be used for
   * testing.
   *
   * @return map with statistic name as key and statistic value as the map
   * value.
   */
  @VisibleForTesting
  Map<String, Long> toMap();

  /**
   * Start a DurationTracker for a request.
   *
   * @param key Name of the DurationTracker statistic.
   * @return an instance of DurationTracker.
   */
  @Override
  DurationTracker trackDuration(String key);

  AbfsDriverMetrics getAbfsDriverMetrics();

  List<AbfsReadFooterMetrics> getAbfsReadFooterMetrics();
}

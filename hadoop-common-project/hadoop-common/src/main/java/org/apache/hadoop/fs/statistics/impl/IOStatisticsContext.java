/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.statistics.impl;

import org.apache.hadoop.fs.statistics.IOStatisticsAggregator;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

/**
 * An interface defined to capture thread-level IOStatistics by using per
 * thread context.
 * <p>
 * The aggregator should be collected in their constructor by statistics-generating
 * classes to obtain the aggregator to update <i>across all threads</i>.
 * <p>
 * The {@link #snapshot()} call creates a snapshot of the statistics;
 * <p>
 * The {@link #reset()} call resets the statistics in the current thread so
 * that later snapshots will get the incremental data.
 */
public interface IOStatisticsContext extends IOStatisticsSource {

  /**
   * Get the IOStatisticsAggregator for the current thread.
   *
   * @return return the aggregator for current thread.
   */
  IOStatisticsAggregator getAggregator();

  /**
   * Capture the snapshot of current thread's IOStatistics.
   *
   * @return IOStatisticsSnapshot for current thread.
   */
  IOStatisticsSnapshot snapshot();

  /**
   * Reset the current thread's IOStatistics.
   */
  void reset();

  /**
   * Get the current thread's IOStatisticsContext.
   *
   * @return instance of IOStatisticsContext for the current thread.
   */
  static IOStatisticsContext getCurrentIOStatisticsContext() {
    return IOStatisticsContextIntegration.getCurrentIOStatisticsContext();
  }

  /**
   * Set the IOStatisticsContext for the current thread.
   * @param statisticsContext IOStatistics context instance for the
   * current thread.
   */
  static void setThreadIOStatisticsContext(
      IOStatisticsContext statisticsContext) {
    IOStatisticsContextIntegration.setThreadIOStatisticsContext(
        statisticsContext);
  }
}

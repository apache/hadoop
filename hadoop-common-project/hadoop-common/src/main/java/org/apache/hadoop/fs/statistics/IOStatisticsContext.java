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

package org.apache.hadoop.fs.statistics;

import org.apache.hadoop.fs.statistics.impl.IOStatisticsContextIntegration;

import static java.util.Objects.requireNonNull;

/**
 * An interface defined to capture thread-level IOStatistics by using per
 * thread context.
 * <p>
 * The aggregator should be collected in their constructor by statistics-generating
 * classes to obtain the aggregator to update <i>across all threads</i>.
 * <p>
 * The {@link #snapshot()} call creates a snapshot of the statistics;
 * <p>
 * The {@link #reset()} call resets the statistics in the context so
 * that later snapshots will get the incremental data.
 */
public interface IOStatisticsContext extends IOStatisticsSource {

  /**
   * Get the IOStatisticsAggregator for the context.
   *
   * @return return the aggregator for the context.
   */
  IOStatisticsAggregator getAggregator();

  /**
   * Capture the snapshot of the context's IOStatistics.
   *
   * @return IOStatisticsSnapshot for the context.
   */
  IOStatisticsSnapshot snapshot();

  /**
   * Get a unique ID for this context, for logging
   * purposes.
   *
   * @return an ID unique for all contexts in this process.
   */
  long getID();

  /**
   * Reset the context's IOStatistics.
   */
  void reset();

  /**
   * Get the context's IOStatisticsContext.
   *
   * @return instance of IOStatisticsContext for the context.
   */
  static IOStatisticsContext getCurrentIOStatisticsContext() {
    // the null check is just a safety check to highlight exactly where a null value would
    // be returned if HADOOP-18456 has resurfaced.
    return requireNonNull(
        IOStatisticsContextIntegration.getCurrentIOStatisticsContext(),
        "Null IOStatisticsContext");
  }

  /**
   * Set the IOStatisticsContext for the current thread.
   * @param statisticsContext IOStatistics context instance for the
   * current thread. If null, the context is reset.
   */
  static void setThreadIOStatisticsContext(
      IOStatisticsContext statisticsContext) {
    IOStatisticsContextIntegration.setThreadIOStatisticsContext(
        statisticsContext);
  }

  /**
   * Static probe to check if the thread-level IO statistics enabled.
   *
   * @return if the thread-level IO statistics enabled.
   */
  static boolean enabled() {
    return IOStatisticsContextIntegration.isIOStatisticsThreadLevelEnabled();
  }

}

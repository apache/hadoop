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
import org.apache.hadoop.fs.statistics.impl.StubDurationTracker;
import org.apache.hadoop.fs.statistics.impl.StubDurationTrackerFactory;

/**
 * Support for working with IOStatistics.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class IOStatisticsSupport {

  private IOStatisticsSupport() {
  }

  /**
   * Take a snapshot of the current statistics state.
   * <p>
   * This is not an atomic option.
   * <p>
   * The instance can be serialized, and its
   * {@code toString()} method lists all the values.
   * @param statistics statistics
   * @return a snapshot of the current values.
   */
  public static IOStatisticsSnapshot
      snapshotIOStatistics(IOStatistics statistics) {

    return new IOStatisticsSnapshot(statistics);
  }

  /**
   * Create a snapshot statistics instance ready to aggregate data.
   *
   * The instance can be serialized, and its
   * {@code toString()} method lists all the values.
   * @return an empty snapshot
   */
  public static IOStatisticsSnapshot
      snapshotIOStatistics() {

    return new IOStatisticsSnapshot();
  }

  /**
   * Get the IOStatistics of the source, casting it
   * if it is of the relevant type, otherwise,
   * if it implements {@link IOStatisticsSource}
   * extracting the value.
   *
   * Returns null if the source isn't of the write type
   * or the return value of
   * {@link IOStatisticsSource#getIOStatistics()} was null.
   * @return an IOStatistics instance or null
   */

  public static IOStatistics retrieveIOStatistics(
      final Object source) {
    if (source instanceof IOStatistics) {
      return (IOStatistics) source;
    } else if (source instanceof IOStatisticsSource) {
      return ((IOStatisticsSource) source).getIOStatistics();
    } else {
      // null source or interface not implemented
      return null;
    }
  }

  /**
   * Return a stub duration tracker factory whose returned trackers
   * are always no-ops.
   *
   * As singletons are returned, this is very low-cost to use.
   * @return a duration tracker factory.
   */
  public static DurationTrackerFactory stubDurationTrackerFactory() {
    return StubDurationTrackerFactory.STUB_DURATION_TRACKER_FACTORY;
  }

  /**
   * Get a stub duration tracker.
   * @return a stub tracker.
   */
  public static DurationTracker stubDurationTracker() {
    return StubDurationTracker.STUB_DURATION_TRACKER;
  }
}

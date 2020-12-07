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

import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.stubDurationTracker;

/**
 * Interface for a source of duration tracking.
 *
 * This is intended for uses where it can be passed into classes
 * which update operation durations, without tying those
 * classes to internal implementation details.
 */
public interface DurationTrackerFactory {

  /**
   * Initiate a duration tracking operation by creating/returning
   * an object whose {@code close()} call will
   * update the statistics.
   *
   * The statistics counter with the key name will be incremented
   * by the given count.
   *
   * The expected use is within a try-with-resources clause.
   *
   * The default implementation returns a stub duration tracker.
   * @param key statistic key prefix
   * @param count  #of times to increment the matching counter in this
   * operation.
   * @return an object to close after an operation completes.
   */
  default DurationTracker trackDuration(String key, long count) {
    return stubDurationTracker();
  }

  /**
   * Initiate a duration tracking operation by creating/returning
   * an object whose {@code close()} call will
   * update the statistics.
   * The expected use is within a try-with-resources clause.
   * @param key statistic key
   * @return an object to close after an operation completes.
   */
  default DurationTracker trackDuration(String key) {
    return trackDuration(key, 1);
  }
}

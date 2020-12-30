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

import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;

/**
 * A duration tracker factory which aggregates two other trackers
 * to have the same lifecycle.
 *
 * This is to ease having instance-level tracking alongside global
 * values, such as an input stream and a filesystem.
 *
 * It's got some inefficiencies -assuming system time is used for
 * the tracking, System.currentTimeMillis will be invoked twice
 * at each point of the process -and the results may actually be different.
 * However, it enables multiple duration tracker factories to be given the
 * opportunity to collect the statistics.
 */
final class PairedDurationTrackerFactory implements DurationTrackerFactory {

  private final DurationTrackerFactory local;
  private final DurationTrackerFactory global;

  PairedDurationTrackerFactory(final DurationTrackerFactory local,
      final DurationTrackerFactory global) {
    this.local = local;
    this.global = global;
  }

  @Override
  public DurationTracker trackDuration(final String key, final long count) {
    return new PairedDurationTracker(
        global.trackDuration(key, count),
        local.trackDuration(key, count));
  }

  /**
   * Tracker which wraps the two duration trackers created for the operation.
   */
  private static final class PairedDurationTracker
      implements DurationTracker {
    private final DurationTracker firstDuration;
    private final DurationTracker secondDuration;

    private PairedDurationTracker(
        final DurationTracker firstDuration,
        final DurationTracker secondDuration) {
      this.firstDuration = firstDuration;
      this.secondDuration = secondDuration;
    }

    @Override
    public void failed() {
      firstDuration.failed();
      secondDuration.failed();
    }

    @Override
    public void close() {
      firstDuration.close();
      secondDuration.close();
    }

    /**
     * @return the global duration
     */
    @Override
    public Duration asDuration() {
      return firstDuration.asDuration();
    }
  }

}

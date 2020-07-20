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

import org.apache.hadoop.util.OperationDuration;

/**
 * Track the duration of an object; when closed the
 * statistics are updated.
 */
public class SingleStatisticDurationTracker extends OperationDuration
    implements DurationTracker {

  private final IOStatisticsStore iostats;

  private final String prefix;

  /**
   * Constructor.
   * @param iostats statistics to update
   * @param prefix prefix of values.
   */
  public SingleStatisticDurationTracker(final IOStatisticsStore iostats,
      final String prefix) {
    this.iostats = iostats;
    this.prefix = prefix;
  }

  /**
   * Set the finished time and then update the statistics.
   */
  @Override
  public void close() {
    finished();
    iostats.addTimedOperation(prefix, asDuration());
  }
}

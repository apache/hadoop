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

package org.apache.hadoop.fs.azure.metrics;

import java.util.ArrayDeque;
import java.util.Date;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Helper class to calculate rolling-window averages.
 * Used to calculate rolling-window metrics in AzureNativeFileSystem.
 */
@InterfaceAudience.Private
final class RollingWindowAverage {
  private final ArrayDeque<DataPoint> currentPoints =
      new ArrayDeque<DataPoint>();
  private final long windowSizeMs;

  /**
   * Create a new rolling-window average for the given window size.
   * @param windowSizeMs The size of the window in milliseconds.
   */
  public RollingWindowAverage(long windowSizeMs) {
    this.windowSizeMs = windowSizeMs;
  }

  /**
   * Add a new data point that just happened.
   * @param value The value of the data point.
   */
  public synchronized void addPoint(long value) {
    currentPoints.offer(new DataPoint(new Date(), value));
    cleanupOldPoints();
  }

  /**
   * Get the current average.
   * @return The current average.
   */
  public synchronized long getCurrentAverage() {
    cleanupOldPoints();
    if (currentPoints.isEmpty()) {
      return 0;
    }
    long sum = 0;
    for (DataPoint current : currentPoints) {
      sum += current.getValue();
    }
    return sum / currentPoints.size();
  }

  /**
   * Clean up points that don't count any more (are before our
   * rolling window) from our current queue of points.
   */
  private void cleanupOldPoints() {
    Date cutoffTime = new Date(new Date().getTime() - windowSizeMs);
    while (!currentPoints.isEmpty() 
        && currentPoints.peekFirst().getEventTime().before(cutoffTime)) {
      currentPoints.removeFirst();
    }
  }

  /**
   * A single data point.
   */
  private static class DataPoint {
    private final Date eventTime;
    private final long value;

    public DataPoint(Date eventTime, long value) {
      this.eventTime = eventTime;
      this.value = value;
    }

    public Date getEventTime() {
      return eventTime;
    }

    public long getValue() {
      return value;
    }

    
  }
}

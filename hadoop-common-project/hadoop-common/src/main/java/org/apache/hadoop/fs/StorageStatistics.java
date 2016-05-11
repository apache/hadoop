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
package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;

import java.util.Iterator;

/**
 * StorageStatistics contains statistics data for a FileSystem or FileContext
 * instance.
 */
@InterfaceAudience.Public
public abstract class StorageStatistics {
  /**
   * A 64-bit storage statistic.
   */
  public static class LongStatistic {
    private final String name;
    private final long value;

    public LongStatistic(String name, long value) {
      this.name = name;
      this.value = value;
    }

    /**
     * @return    The name of this statistic.
     */
    public String getName() {
      return name;
    }

    /**
     * @return    The value of this statistic.
     */
    public long getValue() {
      return value;
    }
  }

  private final String name;

  public StorageStatistics(String name) {
    this.name = name;
  }

  /**
   * Get the name of this StorageStatistics object.
   */
  public String getName() {
    return name;
  }

  /**
   * Get an iterator over all the currently tracked long statistics.
   *
   * The values returned will depend on the type of FileSystem or FileContext
   * object.  The values do not necessarily reflect a snapshot in time.
   */
  public abstract Iterator<LongStatistic> getLongStatistics();

  /**
   * Get the value of a statistic.
   *
   * @return         null if the statistic is not being tracked or is not a
   *                     long statistic.
   *                 The value of the statistic, otherwise.
   */
  public abstract Long getLong(String key);

  /**
   * Return true if a statistic is being tracked.
   *
   * @return         True only if the statistic is being tracked.
   */
  public abstract boolean isTracked(String key);
}

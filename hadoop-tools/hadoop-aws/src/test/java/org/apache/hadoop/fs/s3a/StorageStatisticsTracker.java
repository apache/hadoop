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

package org.apache.hadoop.fs.s3a;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.StorageStatistics;

/**
 * Class to track storage statistics of a filesystem, generate diffs.
 */
public class StorageStatisticsTracker {

  private final FileSystem fs;
  private Map<String, Long> stats;

  public StorageStatisticsTracker(FileSystem fs) {
    this.fs = fs;
    snapshot();
  }

  public void mark() {
    stats = snapshot();
  }

  public Map<String, Long> compare(Map<String, Long> current) {
    Map<String, Long> diff = new HashMap<>(stats.size());
    for (Map.Entry<String, Long> entry : stats.entrySet()) {
      String key = entry.getKey();
      Long latest = current.get(key);
      if (latest != null && !latest.equals(entry.getValue())) {
        diff.put(key, entry.getValue() - latest);
      }
    }
    return diff;
  }

  public Map<String, Long> compareToCurrent() {
    return compare(snapshot());
  }

  public String toString(Map<String, Long> map) {
    return Joiner.on("\n").withKeyValueSeparator("=").join(map);
  }

  public Map<String, Long> snapshot() {
    StatsIterator values = latestValues();
    Map<String, Long> snapshot = new HashMap<>(
        stats == null ? 0 : stats.size());
    for (StorageStatistics.LongStatistic value : values) {
      snapshot.put(value.getName(), value.getValue());
    }
    return snapshot;
  }

  public StatsIterator latestValues() {
    return new StatsIterator(fs.getStorageStatistics());
  }

  /**
   * Provide an iterator to the stats.
   */
  public static class StatsIterator implements
      Iterable<StorageStatistics.LongStatistic> {
    private final StorageStatistics statistics;

    public StatsIterator(StorageStatistics statistics) {
      this.statistics = statistics;
    }

    @Override
    public Iterator<StorageStatistics.LongStatistic> iterator() {
      return statistics.getLongStatistics();
    }
  }


}

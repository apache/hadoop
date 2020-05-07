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

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsLogging;

/**
 * Snapshot of statistics from a different source.
 * <p>
 * It is serializable so that frameworks which can use java serialization
 * to propagate data (Spark, Flink...) can send the statistics
 * back.
 */
class SnapshotIOStatistics implements IOStatistics, Serializable {


  private static final long serialVersionUID = -1762522703841538084L;

  /**
   * Treemaps sort their insertions so the iterator is ordered.
   * They are also serializable.
   */
  private final TreeMap<String, Long> entries
      = new TreeMap<>();

  /**
   * Construct from a source statistics instance.
   * @param source source stats.
   */
  SnapshotIOStatistics(final IOStatistics source) {
    snapshot(source);
  }

  /**
   * Empty constructor is only for serialization.
   */
  private SnapshotIOStatistics() {
  }

  @Override
  public Long getStatistic(final String key) {
    return entries.get(key);
  }

  @Override
  public boolean isTracked(final String key) {
    return false;
  }

  @Override
  public Iterator<Map.Entry<String, Long>> iterator() {
    return entries.entrySet().iterator();
  }

  @Override
  public Set<String> keys() {
    return entries.keySet();
  }

  /**
   * Take a snapshot.
   * @param source statistics source.
   */
  private void snapshot(IOStatistics source) {
    entries.clear();
    // MUST NOT use iterator() because IOStatistics implementations
    // may create a snapshot when iterator() is invoked;
    // enumerating keys and querying values avoids stack
    // overflows
    for (String key : source.keys()) {
      entries.put(key, source.getStatistic(key));
    }
  }

  @Override
  public String toString() {
    return IOStatisticsLogging.iostatisticsToString(this);
  }
}

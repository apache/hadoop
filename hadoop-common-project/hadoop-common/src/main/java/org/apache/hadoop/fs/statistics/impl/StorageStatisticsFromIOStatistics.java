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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;

/**
 * Returns all the counters of an IOStatistics instance as StorageStatistics.
 * This is dynamic.
 * The {@link #reset()} is downgraded to a no-op.
 */
public class StorageStatisticsFromIOStatistics
    extends StorageStatistics
    implements Iterable<StorageStatistics.LongStatistic> {

  private final IOStatistics ioStatistics;
  private final String scheme;

  /**
   * Instantiate.
   * @param name storage statistics name.
   * @param scheme FS scheme; may be null.
   * @param ioStatistics IOStatistics source.
   */
  public StorageStatisticsFromIOStatistics(
      final String name,
      final String scheme,
      final IOStatistics ioStatistics) {
    super(name);
    this.scheme = scheme;
    this.ioStatistics = ioStatistics;
  }

  @Override
  public Iterator<LongStatistic> iterator() {
    return getLongStatistics();
  }

  /**
   * Take a snapshot of the current counter values
   * and return an iterator over them.
   * @return all the counter statistics.
   */
  @Override
  public Iterator<LongStatistic> getLongStatistics() {
    final Set<Map.Entry<String, Long>> counters = counters()
        .entrySet();
    return counters.stream().map(e ->
        new StorageStatistics.LongStatistic(e.getKey(), e.getValue()))
        .collect(Collectors.toSet()).iterator();
  }

  private Map<String, Long> counters() {
    return ioStatistics.counters();
  }

  @Override
  public Long getLong(final String key) {
    return counters().get(key);
  }

  @Override
  public boolean isTracked(final String key) {
    return counters().containsKey(key);
  }

  @Override
  public void reset() {
    /* no-op */
  }

  @Override
  public String getScheme() {
    return scheme;
  }
}

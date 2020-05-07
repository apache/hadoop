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
import java.util.TreeSet;

import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;

/**
 * This provides an IOStatistics instance from a {@link StorageStatistics}
 * instance.
 */
final class IOStatisticsFromStorageStatistics
    implements IOStatistics {

  /**
   * Source.
   */
  private final StorageStatistics storageStatistics;

  /**
   * Keys, calculated in the constructor.
   */
  private final Set<String> keys;

  /**
   * Instantiate. This will iterate through the
   * statistics to enumerate the keys.
   * @param storageStatistics source
   */
  IOStatisticsFromStorageStatistics(
      final StorageStatistics storageStatistics) {
    Preconditions.checkArgument(storageStatistics != null,
        "Null storage statistics");
    this.storageStatistics = storageStatistics;
    // build the keys.
    keys = new TreeSet<>();
    final Iterator<StorageStatistics.LongStatistic> st
        = storageStatistics.getLongStatistics();
    while (st.hasNext()) {
      keys.add(st.next().getName());
    }
  }

  @Override
  public Long getStatistic(final String key) {
    return storageStatistics.getLong(key);
  }

  @Override
  public boolean isTracked(final String key) {
    return storageStatistics.isTracked(key);
  }

  @Override
  public Iterator<Map.Entry<String, Long>> iterator() {
    return new MapEntryIterator(storageStatistics.getLongStatistics());
  }

  @Override
  public Set<String> keys() {
    return keys;
  }

  /**
   * An iterator which takes a long statistic iterator from StorageStatistics
   * and converts to an IOStatistics-compatible type.
   */
  private static final class MapEntryIterator
      implements Iterator<Map.Entry<String, Long>> {

    /**
     * The iterator over the storage statistics.
     */
    private final Iterator<StorageStatistics.LongStatistic> iterator;

    private MapEntryIterator(
        final Iterator<StorageStatistics.LongStatistic> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Map.Entry<String, Long> next() {
      final StorageStatistics.LongStatistic entry = iterator.next();
      return new StatsMapEntry(entry.getName(), entry.getValue());
    }

  }

}

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
import java.util.Optional;

import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

/**
 * This provides an IOStatistics implementation from a storage statistics
 * instance.
 * If a null statistics instance is passed in, the statistics are empty.
 * This makes it possible to instantiate this from any filesystem.t
 */
public class IOStatisticsFromStorageStatistics
    implements IOStatisticsSource {

  private final Optional<IOStatistics> binding;

  /**
   * Instantiate from a storage statistics instance, which may be null,
   * in which case the statistics are empty.
   * @param storageStatistics from storage statistics.
   */
  public IOStatisticsFromStorageStatistics(
      final StorageStatistics storageStatistics) {
    if (storageStatistics != null) {
      binding = Optional.of(new IOStatisticsBinding(storageStatistics));
    } else {
      binding = Optional.empty();
    }
  }

  /**
   * Get any IO statistics.
   * @return the IO statistics bound to.
   */
  @Override
  public Optional<IOStatistics> getIOStatistics() {
    return binding;
  }

  /**
   * The internal binding.
   */
  private static class IOStatisticsBinding implements IOStatistics {

    /**
     * Source.
     */
    private final StorageStatistics storageStatistics;

    private IOStatisticsBinding(final StorageStatistics storageStatistics) {
      this.storageStatistics = storageStatistics;
    }

    @Override
    public boolean hasAttribute(final Attributes attr) {
      return Attributes.Dynamic == attr;
    }

    @Override
    public Long getLong(final String key) {
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
  }

  /**
   * An iterator which takes a long statistic iterator from StorageStatistics
   * and converts to an IOStatistics-compatible type.
   */
  private static final class MapEntryIterator
      implements Iterator<Map.Entry<String, Long>> {

    /**
     * The iterator over the storage statisticis.
     */
    private final Iterator<StorageStatistics.LongStatistic> iterator;

    private MapEntryIterator(final Iterator<StorageStatistics.LongStatistic> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Map.Entry<String, Long> next() {
      final StorageStatistics.LongStatistic entry = iterator.next();
      return new IOStatisticsImplementationSupport.StatsMapEntry(
          entry.getName(), entry.getValue());
    }

    @Override
    public void remove() {
      iterator.remove();
    }
  }

}

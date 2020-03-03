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

import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

/**
 * Support for implementing IOStatistics interfaces.
 */
public class IOStatisticsImplementationSupport {

  private IOStatisticsImplementationSupport() {
  }

  /**
   * Get the IO Statistics of the source, falling back to
   * Optional.empty if the source does not implement
   * {@link IOStatisticsSource}.
   * @return an optional IOStatistics instance.
   */

  public static Optional<IOStatistics> retrieveIOStatistics(
      final Object source) {
    return (source instanceof IOStatisticsSource)
        ? ((IOStatisticsSource) source).getIOStatistics()
        : Optional.empty();
  }

  /**
   * Wrap a (dynamic) source with a snapshot IOStatistics instance.
   * @param source source
   * @return a wrapped instance.
   */
  public static IOStatistics wrapWithSnapshot(IOStatistics source) {
    return new SnapshotIOStatistics(source);
  }

  /**
   * Create a builder for dynamic IO Statistics.
   * @return a builder to be completed.
   */
  public static DynamicIOStatisticsBuilder
    createDynamicIOStatistics() {
    return new DynamicIOStatisticsBuilder();
  }

  /**
   * Create an IO statistics source from a storage statistics instance.
   * This will be updated as the storage statistics change.
   * @param storageStatistics source data.
   * @return an IO statistics source.
   */
  public static IOStatisticsSource createFromStorageStatistics(
      StorageStatistics storageStatistics) {
    return new IOStatisticsFromStorageStatistics(storageStatistics);
  }

  /**
   * A map entry for implementations to return.
   */
  static final class StatsMapEntry implements Map.Entry<String, Long> {

    /**
     * Key.
     */
    private final String key;

    /**
     * Value.
     */
    private Long value;

    /**
     * Constructor.
     * @param key key
     * @param value value
     */
    StatsMapEntry(final String key, final Long value) {
      this.key = key;
      this.value = value;
    }

    public String getKey() {
      return key;
    }

    public Long getValue() {
      return value;
    }

    @SuppressWarnings("NestedAssignment")
    @Override
    public Long setValue(final Long value) {
      return this.value = value;
    }
  }
}

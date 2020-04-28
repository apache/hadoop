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

import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

/**
 * Support for implementing IOStatistics interfaces.
 */
public final class IOStatisticsBinding {

  private IOStatisticsBinding() {
  }

  /**
   * Take a snapshot of the current statistics state.
   * This is not an atomic option.
   * The instance can be serialized.
   * @param statistics statistics
   * @return a snapshot of the current values.
   */
  public static <X extends IOStatistics & Serializable> X snapshotStatistics(
      IOStatistics statistics) {
    return (X) new SnapshotIOStatistics(statistics);
  }

  /**
   * Create a builder for dynamic IO Statistics.
   * @return a builder to be completed.
   */
  public static DynamicIOStatisticsBuilder dynamicIOStatistics() {
    return new DynamicIOStatisticsBuilder();
  }

  /**
   * Create  IOStatistics from a storage statistics instance.
   * This will be updated as the storage statistics change.
   * @param storageStatistics source data.
   * @return an IO statistics source.
   */
  public static IOStatistics fromStorageStatistics(
      StorageStatistics storageStatistics) {
    return new IOStatisticsFromStorageStatistics(storageStatistics);
  }

  /**
   * Get the shared instance of the immutable empty statistics
   * object.
   * @return an empty statistics object.
   */
  public static IOStatistics emptyStatistics() {
    return EmptyIOStatistics.getInstance();
  }

  /**
   * Take an IOStatistics instance and wrap it in a source.
   * @param statistics statistics.
   * @return a source which will return the values
   */
  public static IOStatisticsSource wrap(IOStatistics statistics) {
    return new SourceWrappedStatistics(statistics);
  }
}

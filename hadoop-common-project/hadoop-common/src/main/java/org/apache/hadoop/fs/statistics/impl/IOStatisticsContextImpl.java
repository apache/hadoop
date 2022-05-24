/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.statistics.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.impl.WeakReferenceThreadMap;
import org.apache.hadoop.fs.statistics.IOStatisticsContext;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;

/**
 * Implementing the IOStatisticsContext interface.
 */
public class IOStatisticsContextImpl implements IOStatisticsContext {
  private static final Logger LOG =
      LoggerFactory.getLogger(IOStatisticsContextImpl.class);

  /**
   * Collecting IOStatistics per thread.
   */
  private final WeakReferenceThreadMap<IOStatisticsSnapshot>
      threadIOStatsContext = new WeakReferenceThreadMap<>(
      this::getIOStatisticsSnapshotFactory,
      this::referenceLost);

  /**
   * A Method to act as an IOStatisticsSnapshot factory, in a
   * WeakReferenceThreadMap.
   *
   * @param key ThreadID.
   * @return an Instance of IOStatisticsSnapshot.
   */
  private IOStatisticsSnapshot getIOStatisticsSnapshotFactory(Long key) {
    return new IOStatisticsSnapshot();
  }

  /**
   * In case of reference loss.
   *
   * @param key ThreadID.
   */
  private void referenceLost(Long key) {
    LOG.info("Reference lost for threadID: {}", key);
  }

  /**
   * A Method to get the IOStatisticsSnapshot of the currentThread. This
   * denotes the aggregated IOStatistics per thread.
   *
   * @return the instance of IOStatisticsSnapshot.
   */
  @Override
  public IOStatisticsSnapshot getThreadIOStatistics() {
    IOStatisticsSnapshot ioStatisticsSnapshot =
        threadIOStatsContext.getForCurrentThread();
    if (ioStatisticsSnapshot == null) {
      // Create and set IOStatsSnapshot in the ThreadPool.
      ioStatisticsSnapshot = new IOStatisticsSnapshot();
      threadIOStatsContext.setForCurrentThread(ioStatisticsSnapshot);
    }
    // If an instance is present return it.
    return ioStatisticsSnapshot;
  }

  @Override
  public String toString() {
    return getThreadIOStatistics().toString();
  }
}

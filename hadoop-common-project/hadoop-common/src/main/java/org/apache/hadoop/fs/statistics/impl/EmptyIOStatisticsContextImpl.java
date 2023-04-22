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

import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsAggregator;
import org.apache.hadoop.fs.statistics.IOStatisticsContext;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;

/**
 * Empty IOStatistics context which serves no-op for all the operations and
 * returns an empty Snapshot if asked.
 *
 */
final class EmptyIOStatisticsContextImpl implements IOStatisticsContext {

  private static final IOStatisticsContext EMPTY_CONTEXT = new EmptyIOStatisticsContextImpl();

  private EmptyIOStatisticsContextImpl() {
  }

  /**
   * Create a new empty snapshot.
   * A new one is always created for isolation.
   *
   * @return a statistics snapshot
   */
  @Override
  public IOStatisticsSnapshot snapshot() {
    return new IOStatisticsSnapshot();
  }

  @Override
  public IOStatisticsAggregator getAggregator() {
    return EmptyIOStatisticsStore.getInstance();
  }

  @Override
  public IOStatistics getIOStatistics() {
    return EmptyIOStatistics.getInstance();
  }

  @Override
  public void reset() {}

  /**
   * The ID is always 0.
   * As the real context implementation counter starts at 1,
   * we are guaranteed to have unique IDs even between them and
   * the empty context.
   * @return 0
   */
  @Override
  public long getID() {
    return 0;
  }

  /**
   * Get the single instance.
   * @return an instance.
   */
  static IOStatisticsContext getInstance() {
    return EMPTY_CONTEXT;
  }
}

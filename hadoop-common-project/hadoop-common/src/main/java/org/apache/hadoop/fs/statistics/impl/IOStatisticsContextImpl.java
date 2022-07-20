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

import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsAggregator;
import org.apache.hadoop.fs.statistics.IOStatisticsContext;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;

/**
 * Implementing the IOStatisticsContext.
 *
 * A Context defined for IOStatistics collection per thread which captures
 * each worker thread's work in FS streams and stores it in the form of
 * IOStatisticsSnapshot.
 *
 * For the current thread the IOStatisticsSnapshot can be used as a way to
 * move the IOStatistics data between applications using the Serializable
 * nature of the class.
 */
public final class IOStatisticsContextImpl implements IOStatisticsContext {
  private static final Logger LOG =
      LoggerFactory.getLogger(IOStatisticsContextImpl.class);

  /**
   * Thread ID.
   */
  private final long threadId;

  /**
   * Unique ID.
   */
  private final long id;

  /**
   * IOStatistics to aggregate.
   */
  private final IOStatisticsSnapshot ioStatistics = new IOStatisticsSnapshot();

  /**
   * Constructor.
   * @param threadId thread ID
   * @param id instance ID.
   */
  public IOStatisticsContextImpl(final long threadId, final long id) {
    this.threadId = threadId;
    this.id = id;
  }

  @Override
  public String toString() {
    return "IOStatisticsContextImpl{" +
        "id=" + id +
        ", threadId=" + threadId +
        ", ioStatistics=" + ioStatistics +
        '}';
  }

  /**
   * Get the IOStatisticsAggregator of the context.
   * @return the instance of IOStatisticsAggregator for this context.
   */
  @Override
  public IOStatisticsAggregator getAggregator() {
    return ioStatistics;
  }

  /**
   * Returns a snapshot of the current thread's IOStatistics.
   *
   * @return IOStatisticsSnapshot of the context.
   */
  @Override
  public IOStatisticsSnapshot snapshot() {
    LOG.debug("Taking snapshot of IOStatisticsContext id {}", id);
    return new IOStatisticsSnapshot(ioStatistics);
  }

  /**
   * Reset the thread +.
   */
  @Override
  public void reset() {
    LOG.debug("clearing IOStatisticsContext id {}", id);
    ioStatistics.clear();
  }

  @Override
  public IOStatistics getIOStatistics() {
    return ioStatistics;
  }

  /**
   * ID of this context.
   * @return ID.
   */
  @Override
  public long getID() {
    return id;
  }

  /**
   * Get the thread ID.
   * @return thread ID.
   */
  public long getThreadID() {
    return threadId;
  }
}

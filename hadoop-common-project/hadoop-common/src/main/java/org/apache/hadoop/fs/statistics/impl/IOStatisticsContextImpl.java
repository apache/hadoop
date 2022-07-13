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

import java.lang.ref.WeakReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.impl.WeakReferenceThreadMap;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsAggregator;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;

import static org.apache.hadoop.fs.CommonConfigurationKeys.THREAD_LEVEL_IOSTATISTICS_ENABLED;
import static org.apache.hadoop.fs.CommonConfigurationKeys.THREAD_LEVEL_IOSTATISTICS_ENABLED_DEFAULT;

/**
 * Implementing the IOStatisticsContext.
 *
 * A Context defined for IOStatistics collection per thread which captures
 * each worker thread's work in FS streams and stores it in the form of
 * IOStatisticsSnapshot if thread level aggregation is enabled else returning
 * an instance of EmptyIOStatisticsStore for the FS. An active instance of
 * the IOStatisticsContext can be used to collect the statistics.
 *
 * For the current thread the IOStatisticsSnapshot can be used as a way to
 * move the IOStatistics data between applications using the Serializable
 * nature of the class.
 */
public class IOStatisticsContextImpl implements IOStatisticsContext {
  private static final Logger LOG =
      LoggerFactory.getLogger(IOStatisticsContextImpl.class);
  private static final boolean IS_THREAD_IOSTATS_ENABLED;

  /**
   * Active IOStatistics Context containing different worker thread's
   * statistics. Weak Reference so that it gets cleaned up during GC and we
   * avoid any memory leak issues due to long lived references.
   */
  private static final WeakReferenceThreadMap<IOStatisticsContext>
      ACTIVE_IOSTATS_CONTEXT =
      new WeakReferenceThreadMap<>(IOStatisticsContextImpl::createNewInstance,
          IOStatisticsContextImpl::referenceLostContext
  );

  /**
   * Collecting IOStatistics per thread.
   */
  private final WeakReferenceThreadMap<IOStatisticsSnapshot>
      threadLevelIOStatisticsMap =
      new WeakReferenceThreadMap<>(this::getIOStatisticsSnapshotFactory,
          this::referenceLost);

  static {
    // Work out if the current context has thread level IOStatistics enabled.
    final Configuration configuration = new Configuration();
    IS_THREAD_IOSTATS_ENABLED =
        configuration.getBoolean(THREAD_LEVEL_IOSTATISTICS_ENABLED,
            THREAD_LEVEL_IOSTATISTICS_ENABLED_DEFAULT);
  }

  /**
   * Creating a new IOStatisticsContext instance for a FS to be used. If
   *
   * @param key Thread ID that represents which thread the context belongs to.
   * @return an instance of IOStatisticsContext.
   */
  private static IOStatisticsContext createNewInstance(Long key) {
    if (!IS_THREAD_IOSTATS_ENABLED) {
      return new EmptyIOStatisticsContextImpl();
    }
    return new IOStatisticsContextImpl();
  }

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
    LOG.debug("Reference lost for threadID: {}", key);
  }

  /**
   * In case of reference loss for IOStatisticsContext.
   *
   * @param key ThreadID.
   */
  private static void referenceLostContext(Long key) {
    LOG.debug("Reference lost for threadID for the context: {}", key);
  }

  /**
   * Get the current thread's IOStatisticsContext instance. If no instance is
   * present for this thread ID, create one using the factory.
   *
   * @return instance of IOStatisticsContext.
   */
  @Override
  public IOStatisticsContext getCurrentIOStatisticsContext() {
    return ACTIVE_IOSTATS_CONTEXT.getForCurrentThread();
  }

  /**
   * A Method to get the IOStatisticsAggregator of the currentThread. This
   * denotes the aggregated IOStatistics per thread.
   *
   * @return the instance of IOStatisticsAggregator for the current thread.
   */
  @Override
  public IOStatisticsAggregator getThreadIOStatisticsAggregator() {
    // If the current Thread ID already have an aggregator assigned, return
    // that.
    boolean isThreadIOStatsPresent =
        threadLevelIOStatisticsMap.containsKey(Thread.currentThread().getId());
    if (isThreadIOStatsPresent) {
      return threadLevelIOStatisticsMap.getForCurrentThread();
    }
    LOG.debug("No thread iostats present creating it for :{}",
        Thread.currentThread().getId());
    // If no aggregator is defined to the thread ID, create one and assign it.
    IOStatisticsSnapshot ioStatisticsSnapshot = new IOStatisticsSnapshot();
    setThreadIOStatistics(ioStatisticsSnapshot);
    return ioStatisticsSnapshot;
  }

  /**
   * Set the IOStatisticsSnapshot for the current context for a specific
   * thread.
   *
   * @param ioStatisticsSnapshot IOStatisticsAggregator instance for the
   *                               current thread.
   */
  public void setThreadIOStatistics(
      IOStatisticsSnapshot ioStatisticsSnapshot) {
    threadLevelIOStatisticsMap.setForCurrentThread(ioStatisticsSnapshot);
  }

  /**
   * Returns a snapshot of the current thread's IOStatistics.
   *
   * @return IOStatisticsSnapshot of the current thread.
   */
  @Override
  public IOStatisticsSnapshot snapshot() {
    if (threadLevelIOStatisticsMap.containsKey(getCurrentThreadID())) {
      return threadLevelIOStatisticsMap.get(getCurrentThreadID());
    }
    return new IOStatisticsSnapshot();
  }

  /**
   * Reset the thread IOStatistics for current thread.
   */
  @Override
  public void reset() {
    WeakReference<IOStatisticsSnapshot> ioStatisticsSnapshotRef =
        threadLevelIOStatisticsMap.lookup(getCurrentThreadID());
    if (ioStatisticsSnapshotRef != null) {
      IOStatisticsSnapshot ioStatisticsSnapshot = ioStatisticsSnapshotRef.get();
      // Get the snapshot for the current thread ID and clear it.
      if(ioStatisticsSnapshot != null) {
        ioStatisticsSnapshot.clear();
      }
    }
  }

  /**
   * Get the current thread's ID.
   * @return long value of the thread ID.
   */
  private Long getCurrentThreadID() {
    return Thread.currentThread().getId();
  }

  /**
   * Get thread ID specific IOStatistics values.
   *
   * @param testThreadId thread ID.
   * @return IOStatistics instance.
   */
  @VisibleForTesting
  public IOStatistics getThreadSpecificIOStatistics(long testThreadId) {
    LOG.debug("IOStatsContext thread ID required: {}", testThreadId);
    // lookup the weakRef IOStatisticsSnapshot for the thread ID in the
    // ThreadMap.
    WeakReference<IOStatisticsSnapshot> ioStatisticsSnapshotWeakReference =
        threadLevelIOStatisticsMap.lookup(testThreadId);
    if (ioStatisticsSnapshotWeakReference != null) {
      return ioStatisticsSnapshotWeakReference.get();
    }
    return new IOStatisticsSnapshot();
  }
}

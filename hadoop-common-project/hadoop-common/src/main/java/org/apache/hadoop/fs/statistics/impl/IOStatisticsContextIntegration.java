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
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.impl.WeakReferenceThreadMap;
import org.apache.hadoop.fs.statistics.IOStatisticsContext;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IOSTATISTICS_THREAD_LEVEL_ENABLED;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IOSTATISTICS_THREAD_LEVEL_ENABLED_DEFAULT;

/**
 * A Utility class for IOStatisticsContext, which helps in creating and
 * getting the current active context. Static methods in this class allows to
 * get the current context to start aggregating the IOStatistics.
 *
 * Static initializer is used to work out if the feature to collect
 * thread-level IOStatistics is enabled or not and the corresponding
 * implementation class is called for it.
 *
 * Weak Reference thread map to be used to keep track of different context's
 * to avoid long-lived memory leakages as these references would be cleaned
 * up at GC.
 */
public final class IOStatisticsContextIntegration {

  private static final Logger LOG =
      LoggerFactory.getLogger(IOStatisticsContextIntegration.class);

  /**
   * Is thread-level IO Statistics enabled?
   */
  private static boolean isThreadIOStatsEnabled;

  /**
   * ID for next instance to create.
   */
  public static final AtomicLong INSTANCE_ID = new AtomicLong(1);

  /**
   * Active IOStatistics Context containing different worker thread's
   * statistics. Weak Reference so that it gets cleaned up during GC and we
   * avoid any memory leak issues due to long lived references.
   */
  private static final WeakReferenceThreadMap<IOStatisticsContext>
      ACTIVE_IOSTATS_CONTEXT =
      new WeakReferenceThreadMap<>(
          IOStatisticsContextIntegration::createNewInstance,
          IOStatisticsContextIntegration::referenceLostContext
      );

  static {
    // Work out if the current context has thread level IOStatistics enabled.
    final Configuration configuration = new Configuration();
    isThreadIOStatsEnabled =
        configuration.getBoolean(IOSTATISTICS_THREAD_LEVEL_ENABLED,
            IOSTATISTICS_THREAD_LEVEL_ENABLED_DEFAULT);
  }

  /**
   * Static probe to check if the thread-level IO statistics enabled.
   *
   * @return if the thread-level IO statistics enabled.
   */
  public static boolean isIOStatisticsThreadLevelEnabled() {
    return isThreadIOStatsEnabled;
  }

  /**
   * Private constructor for a utility class to be used in IOStatisticsContext.
   */
  private IOStatisticsContextIntegration() {}

  /**
   * Creating a new IOStatisticsContext instance for a FS to be used.
   * @param key Thread ID that represents which thread the context belongs to.
   * @return an instance of IOStatisticsContext.
   */
  private static IOStatisticsContext createNewInstance(Long key) {
    IOStatisticsContextImpl instance =
        new IOStatisticsContextImpl(key, INSTANCE_ID.getAndIncrement());
    LOG.debug("Created instance {}", instance);
    return instance;
  }

  /**
   * In case of reference loss for IOStatisticsContext.
   * @param key ThreadID.
   */
  private static void referenceLostContext(Long key) {
    LOG.debug("Reference lost for threadID for the context: {}", key);
  }

  /**
   * Get the current thread's IOStatisticsContext instance. If no instance is
   * present for this thread ID, create one using the factory.
   * @return instance of IOStatisticsContext.
   */
  public static IOStatisticsContext getCurrentIOStatisticsContext() {
    return isThreadIOStatsEnabled
        ? ACTIVE_IOSTATS_CONTEXT.getForCurrentThread()
        : EmptyIOStatisticsContextImpl.getInstance();
  }

  /**
   * Set the IOStatisticsContext for the current thread.
   * @param statisticsContext IOStatistics context instance for the
   * current thread. If null, the context is reset.
   */
  public static void setThreadIOStatisticsContext(
      IOStatisticsContext statisticsContext) {
    if (isThreadIOStatsEnabled) {
      if (statisticsContext == null) {
        // new value is null, so remove it
        ACTIVE_IOSTATS_CONTEXT.removeForCurrentThread();
      } else {
        // the setter is efficient in that it does not create a new
        // reference if the context is unchanged.
        ACTIVE_IOSTATS_CONTEXT.setForCurrentThread(statisticsContext);
      }
    }
  }

  /**
   * Get thread ID specific IOStatistics values if
   * statistics are enabled and the thread ID is in the map.
   * @param testThreadId thread ID.
   * @return IOStatisticsContext if found in the map.
   */
  @VisibleForTesting
  public static IOStatisticsContext getThreadSpecificIOStatisticsContext(long testThreadId) {
    LOG.debug("IOStatsContext thread ID required: {}", testThreadId);

    if (!isThreadIOStatsEnabled) {
      return null;
    }
    // lookup the weakRef IOStatisticsContext for the thread ID in the
    // ThreadMap.
    WeakReference<IOStatisticsContext> ioStatisticsSnapshotWeakReference =
        ACTIVE_IOSTATS_CONTEXT.lookup(testThreadId);
    if (ioStatisticsSnapshotWeakReference != null) {
      return ioStatisticsSnapshotWeakReference.get();
    }
    return null;
  }

  /**
   * A method to enable IOStatisticsContext to override if set otherwise in
   * the configurations for tests.
   */
  @VisibleForTesting
  public static void enableIOStatisticsContext() {
    if (!isThreadIOStatsEnabled) {
      LOG.info("Enabling Thread IOStatistics..");
      isThreadIOStatsEnabled = true;
    }
  }
}

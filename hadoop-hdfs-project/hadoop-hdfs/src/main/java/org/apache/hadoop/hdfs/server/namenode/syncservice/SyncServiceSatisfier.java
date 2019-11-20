/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.syncservice;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Queues;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedSyncMountSnapshotUpdateFactory;
import org.apache.hadoop.hdfs.server.namenode.syncservice.scheduler.SyncTaskScheduler;
import org.apache.hadoop.hdfs.server.namenode.syncservice.scheduler.SyncTaskSchedulerImpl;
import org.apache.hadoop.hdfs.server.protocol.BlockSyncTaskExecutionFeedback;
import org.apache.hadoop.hdfs.server.protocol.BulkSyncTaskExecutionFeedback;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTaskExecutionFeedback;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionOutcome;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionResult;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Queue;
import java.util.UUID;
import java.util.function.Predicate;

import static org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionOutcome.FINISHED_SUCCESSFULLY;

public class SyncServiceSatisfier implements Runnable {
  public static final Logger LOG =
      LoggerFactory.getLogger(SyncServiceSatisfier.class);
  private final Configuration conf;
  private final Namesystem namesystem;
  private Daemon syncServiceSatisfierThread;
  private volatile boolean isRunning = false;
  private boolean manualMode = false;
  private BlockManager blockManager;

  private SyncMonitor syncMonitor;
  private Queue<String> fullResyncQueue;

  public SyncServiceSatisfier(Namesystem namesystem, BlockManager blockManager, Configuration conf) {
    this.namesystem = namesystem;
    this.blockManager = blockManager;
    this.conf = conf;
    this.fullResyncQueue = Queues.newConcurrentLinkedQueue();
  }

  /**
   * Start sync service satisfier demon thread. Also start block storage
   * movements monitor for retry the attempts if needed.
   */
  public synchronized void start(boolean reconfigStart) {
    this.isRunning = true;
    if (reconfigStart) {
      LOG.info("Starting SyncServiceSatisfier, as admin requested to "
          + "start it.");
    } else {
      LOG.info("Starting SyncServiceSatisfier.");
    }

    PhasedSyncMountSnapshotUpdateFactory syncMountSnapshotUpdatePlanFactory
        = new PhasedSyncMountSnapshotUpdateFactory(namesystem, blockManager, conf);

    BlockAliasMap.Writer<FileRegion> aliasMapWriter =
        createAliasMapWriter(blockManager, conf);

    final MountManager mountManager = namesystem.getMountManager();
    SyncTaskScheduler syncTaskScheduler = new SyncTaskSchedulerImpl(this,
        blockManager, mountManager::updateStats, conf);

    this.syncMonitor = new SyncMonitor(namesystem,
        syncMountSnapshotUpdatePlanFactory, syncTaskScheduler, aliasMapWriter,
        conf);


    this.syncServiceSatisfierThread = new Daemon(this);
    this.syncServiceSatisfierThread.setName("SyncServiceSatisfier");
    this.syncServiceSatisfierThread.start();
  }

  /**
   * Sets running flag to false. Also, this will interrupt monitor thread and
   * clear all the queued up tasks.
   */
  public synchronized void stop() {
    isRunning = false;
    if (syncServiceSatisfierThread != null) {
      syncServiceSatisfierThread.interrupt();
    }
  }

  /**
   * Timed wait to stop monitor thread.
   */
  public synchronized void stopGracefully() {
    if (syncServiceSatisfierThread == null) {
      return;
    }
    if (isRunning) {
      stop();
    }
    try {
      syncServiceSatisfierThread.join(3000);
    } catch (InterruptedException ie) {
    }
  }

  @Override
  public void run() {
    while (namesystem.isRunning() && this.isRunning && !manualMode) {
      try {
        scheduleOnce();
      } catch (IOException e) {
        LOG.error("error during scheduling " + e);
      }
    }
  }

  @VisibleForTesting
  public void scheduleOnce() throws IOException {
    String resyncSyncMountId = this.fullResyncQueue.poll();
    if (resyncSyncMountId != null) {
      syncMonitor.fullResync(resyncSyncMountId, createAliasMapReader(blockManager, conf));
    } else {
      syncMonitor.scheduleNextWork();
    }
    synchronized (syncMonitor) {
      try {
        //TODO: Make this wait the maximum wait between two snaphots, handle per SyncMount
        syncMonitor.wait(1000);
      } catch (InterruptedException e) {
        this.isRunning = false;
      }
    }
  }


  public void disable() {
    this.isRunning = false;

    if (this.syncServiceSatisfierThread == null) {
      return;
    }

    this.syncServiceSatisfierThread.interrupt();
  }

  public boolean isRunning() {
    return this.isRunning;
  }

  @VisibleForTesting
  public boolean hasSyncingInProgress() {
    synchronized (syncMonitor) {
      return this.syncMonitor.hasTrackersInProgress();
    }
  }

  public void manualMode() {
    this.manualMode = true;
  }

  private void handleExecutionFeedback(SyncTaskExecutionOutcome outcome,
      Predicate<SyncTaskExecutionOutcome> isSuccessfulOutcome,
      UUID syncTaskId, String syncMountId,
      SyncTaskExecutionResult result) {
    if (isSuccessfulOutcome.test(outcome)) {
      LOG.info("Marking SyncTask {} as finished", syncTaskId);
      synchronized (syncMonitor) {
        this.syncMonitor.markSyncTaskFinished(syncTaskId, syncMountId, result);
      }
    } else {
      synchronized (syncMonitor) {
        boolean trackerStillRunning = this.syncMonitor.markSyncTaskFailed(syncTaskId, syncMountId, result);
        if (!trackerStillRunning) {
          scheduleFullResync(syncMountId);
        }
      }
    }
  }

  private void scheduleFullResync(String syncMountId) {
    this.fullResyncQueue.add(syncMountId);
  }

  public boolean cancelCurrentAndScheduleFullResync(String syncMountId) {
    boolean cancelSuccessful = this.syncMonitor.blockingCancelTracker(syncMountId);
    if (cancelSuccessful) {
      scheduleFullResync(syncMountId);
      return true;
    } else {
      return false;
    }
  }

  public void handleExecutionFeedback(MetadataSyncTaskExecutionFeedback feedback) {
    handleExecutionFeedback(feedback.getOutcome(),
        outcome -> outcome == FINISHED_SUCCESSFULLY,
        feedback.getSyncTaskId(),
        feedback.getSyncMountId(),
        feedback.getResult());
  }

  private void handleExecutionFeedback(BlockSyncTaskExecutionFeedback feedback) {
    handleExecutionFeedback(feedback.getOutcome(),
        outcome -> outcome == FINISHED_SUCCESSFULLY,
        feedback.getSyncTaskId(),
        feedback.getSyncMountId(),
        feedback.getResult());
  }

  public void handleExecutionFeedback(BulkSyncTaskExecutionFeedback bulkSyncTaskExecutionFeedback) {
    for (BlockSyncTaskExecutionFeedback syncTaskExecutionFeedback : bulkSyncTaskExecutionFeedback.getFeedbacks()) {
      handleExecutionFeedback(syncTaskExecutionFeedback);
    }
  }

  private BlockAliasMap.Writer<FileRegion> createAliasMapWriter(
      BlockManager blockManager, Configuration conf) {
    // load block writer into storage
    Class<? extends BlockAliasMap> aliasMapClass = conf.getClass(
        DFSConfigKeys.DFS_PROVIDED_ALIASMAP_CLASS,
        TextFileRegionAliasMap.class, BlockAliasMap.class);
    final BlockAliasMap<FileRegion> aliasMap = ReflectionUtils.newInstance(
        aliasMapClass, conf);
    try {
      return aliasMap.getWriter(null, blockManager.getBlockPoolId());
    } catch (IOException e) {
      throw new RuntimeException("Could not load AliasMap Writer: "
          + e.getMessage());
    }
  }

  private BlockAliasMap.Reader<FileRegion> createAliasMapReader(
      BlockManager blockManager, Configuration conf) {
    // load block writer into storage
    Class<? extends BlockAliasMap> aliasMapClass = conf.getClass(
        DFSConfigKeys.DFS_PROVIDED_ALIASMAP_CLASS,
        TextFileRegionAliasMap.class, BlockAliasMap.class);
    final BlockAliasMap<FileRegion> aliasMap = ReflectionUtils.newInstance(
        aliasMapClass, conf);
    try {
      return aliasMap.getReader(null, blockManager.getBlockPoolId());
    } catch (IOException e) {
      throw new RuntimeException("Could not load AliasMap Reader: "
          + e.getMessage());
    }
  }

  public SyncMonitor getSyncMonitor() {
    return syncMonitor;
  }
}

/**
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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.Queues;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.aliasmap.AliasMapUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.ProvidedVolumeInfo;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.SyncMountManager;
import org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedSyncMountSnapshotUpdateFactory;
import org.apache.hadoop.hdfs.server.namenode.syncservice.scheduler.SyncTaskScheduler;
import org.apache.hadoop.hdfs.server.namenode.syncservice.scheduler.SyncTaskSchedulerImpl;
import org.apache.hadoop.hdfs.server.protocol.BlockSyncTaskExecutionFeedback;
import org.apache.hadoop.hdfs.server.protocol.BulkSyncTaskExecutionFeedback;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTaskExecutionFeedback;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionOutcome;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionResult;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.function.Predicate;

import static org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionOutcome.FINISHED_SUCCESSFULLY;

/**
 * Sync service satisfier to sync data to remote storage, applicable to
 * provided storage backup/writeBack mount.
 */
public class SyncServiceSatisfier implements Runnable {
  public static final Logger LOG =
      LoggerFactory.getLogger(SyncServiceSatisfier.class);
  private final Configuration conf;
  private final Namesystem namesystem;
  private final SyncMountManager syncMountManager;
  private Daemon syncServiceSatisfierThread;
  private volatile boolean isRunning = false;
  private BlockManager blockManager;

  private SyncMonitor syncMonitor;
  private Queue<String> resyncQueue;

  public SyncServiceSatisfier(FSNamesystem fsNamesystem,
      SyncMountManager syncMountManager, Configuration conf) {
    this.namesystem = fsNamesystem;
    this.blockManager = fsNamesystem.getBlockManager();
    this.syncMountManager = syncMountManager;
    this.conf = conf;
    this.resyncQueue = Queues.newConcurrentLinkedQueue();
  }

  /**
   * Start sync service satisfier demon thread. Also start block storage
   * movements monitor for retry the attempts if needed.
   */
  public synchronized void start() throws IOException {
    LOG.info("Starting SyncServiceSatisfier.");
    PhasedSyncMountSnapshotUpdateFactory syncMountSnapshotUpdateFactory =
        new PhasedSyncMountSnapshotUpdateFactory(
            namesystem, blockManager, conf);
    BlockAliasMap.Writer<FileRegion> aliasMapWriter =
        AliasMapUtil.createAliasMapWriter(blockManager.getBlockPoolId(), conf);
    SyncTaskScheduler syncTaskScheduler = new SyncTaskSchedulerImpl(this,
        blockManager, conf);

    this.syncMonitor = new SyncMonitor(namesystem, syncMountManager,
        syncMountSnapshotUpdateFactory, syncTaskScheduler, aliasMapWriter,
        conf);
    this.syncServiceSatisfierThread = new Daemon(this);
    this.syncServiceSatisfierThread.setName("SyncServiceSatisfier");
    this.syncServiceSatisfierThread.start();
    this.isRunning = true;
  }

  /**
   * Sets running flag to false. Also, this will interrupt monitor thread and
   * clear all the queued up tasks.
   */
  public synchronized void stop() {
    LOG.info("Stopping sync service..");
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
    try {
      syncServiceSatisfierThread.join(3000);
    } catch (InterruptedException ie) {
      LOG.error("syncservice interrupted while stopped");
    }
    if (isRunning) {
      stop();
    }
  }

  @Override
  public void run() {
    if (namesystem.isRunning() && this.isRunning) {
      handleFailOver();
    }
    try {
      while (namesystem.isRunning() && this.isRunning) {
        try {
          scheduleOnce();
        } catch (IOException e) {
          LOG.error("Failed in scheduling sync tasks, " +
              "continue new schedule cycle.", e);
        }
      }
    } catch (RuntimeException e) {
      LOG.error("Stopping sync service satisfier " +
          "due to run time exception: " + e);
      stopGracefully();
    }
  }

  private void handleFailOver() {
    List<ProvidedVolumeInfo> syncMounts = syncMountManager.getSyncMounts();
    for (ProvidedVolumeInfo syncMount : syncMounts) {
      if (!syncMountManager.isSyncFinished(syncMount)) {
        LOG.debug("Schedule resync for {}", syncMount.getMountPath());
        scheduleResync(syncMount.getId());
      }
    }
  }

  @VisibleForTesting
  public void scheduleOnce() throws IOException {
    if (!namesystem.isInSafeMode() && isRunning) {
      String resyncSyncMountId = this.resyncQueue.poll();
      if (resyncSyncMountId != null) {
        LOG.info("Scheduling resync for {}", resyncSyncMountId);
        syncMonitor.resync(resyncSyncMountId);
      } else {
        syncMonitor.scheduleNextWork();
      }
    }
    synchronized (syncMonitor) {
      try {
        //TODO: Make it configurable.
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

  private void handleExecutionFeedback(SyncTaskExecutionOutcome outcome,
      Predicate<SyncTaskExecutionOutcome> isSuccessfulOutcome,
      UUID syncTaskId, String syncMountId,
      SyncTaskExecutionResult result) {
    if (isSuccessfulOutcome.test(outcome)) {
      synchronized (syncMonitor) {
        this.syncMonitor.markSyncTaskFinished(syncTaskId, syncMountId, result);
      }
    } else {
      synchronized (syncMonitor) {
        boolean trackerStillRunning = this.syncMonitor.markSyncTaskFailed(
            syncTaskId, syncMountId, result);
        if (!trackerStillRunning) {
          scheduleResync(syncMountId);
        }
      }
    }
  }

  private void scheduleFullResync(String syncMountId) {
    // TODO: use a full queue to maintain resync.
  }

  private void scheduleResync(String syncMountId) {
    if (resyncQueue.contains(syncMountId)) {
      return;
    }
    this.resyncQueue.add(syncMountId);
  }

  @VisibleForTesting
  public boolean cancelCurrentAndScheduleFullResync(String syncMountId) {
    boolean cancelSuccessful =
        this.syncMonitor.blockingCancelTracker(syncMountId);
    if (cancelSuccessful) {
      // Currently nothing to do.
      scheduleFullResync(syncMountId);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Handle MetadataSyncTaskExecutionFeedback, with updating stats.
   */
  public void handleExecutionFeedback(MetadataSyncTaskExecutionFeedback
      feedback) {
    handleExecutionFeedback(feedback.getOutcome(),
        outcome -> outcome == FINISHED_SUCCESSFULLY,
        feedback.getSyncTaskId(),
        feedback.getSyncMountId(),
        feedback.getResult());
    syncMountManager.updateStats(feedback);
  }

  public void handleExecutionFeedback(BlockSyncTaskExecutionFeedback
      feedback) {
    handleExecutionFeedback(feedback.getOutcome(),
        outcome -> outcome == FINISHED_SUCCESSFULLY,
        feedback.getSyncTaskId(),
        feedback.getSyncMountId(),
        feedback.getResult());
    syncMountManager.updateStats(feedback);
    syncMonitor.updateTaskStatus(feedback.getSyncTaskId());
  }

  /**
   * Handle packaged BlockSyncTaskExecutionFeedback, with updating stats.
   */
  public void handleExecutionFeedback(BulkSyncTaskExecutionFeedback
      bulkFeedback) {
    for (BlockSyncTaskExecutionFeedback syncTaskExecutionFeedback :
        bulkFeedback.getFeedbacks()) {
      handleExecutionFeedback(syncTaskExecutionFeedback);
    }
  }
}

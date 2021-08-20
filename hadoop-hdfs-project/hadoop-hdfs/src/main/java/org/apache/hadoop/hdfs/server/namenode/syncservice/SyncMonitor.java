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

import org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.server.common.ProvidedVolumeInfo;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.SyncMountManager;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedPlan;
import org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedSyncMountSnapshotUpdateFactory;
import org.apache.hadoop.hdfs.server.namenode.syncservice.scheduler.SyncTaskScheduler;
import org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.SchedulableSyncPhase;
import org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.SyncMountSnapshotUpdateTracker;
import org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.SyncMountSnapshotUpdateTrackerFactory;
import org.apache.hadoop.hdfs.server.protocol.SyncTask.CreateFileSyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionResult;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Sync service monitor for provided storage.
 */
public class SyncMonitor {

  public static final Logger LOG = LoggerFactory.getLogger(SyncMonitor.class);
  private final Namesystem namesystem;
  private Map<String, SyncMountSnapshotUpdateTracker> inProgress;
  private Map<String, SyncMountSnapshotUpdateTracker> trackersFailed;
  private PhasedSyncMountSnapshotUpdateFactory syncMountSnapshotUpdateFactory;
  private SyncTaskScheduler syncTaskScheduler;
  private BlockAliasMap.Writer<FileRegion> aliasMapWriter;
  private Configuration conf;
  private SyncMountManager syncMountManager;

  public SyncMonitor(Namesystem namesystem, SyncMountManager syncMountManager,
      PhasedSyncMountSnapshotUpdateFactory syncMountSnapshotUpdateFactory,
      SyncTaskScheduler syncTaskScheduler,
      BlockAliasMap.Writer<FileRegion> aliasMapWriter, Configuration conf) {
    this.syncMountSnapshotUpdateFactory = syncMountSnapshotUpdateFactory;
    this.syncTaskScheduler = syncTaskScheduler;
    this.aliasMapWriter = aliasMapWriter;
    this.conf = conf;
    this.inProgress = Maps.newConcurrentMap();
    this.trackersFailed = Maps.newConcurrentMap();
    this.namesystem = namesystem;
    this.syncMountManager = syncMountManager;
  }

  public boolean markSyncTaskFailed(UUID syncTaskId,
      String syncMountId,
      SyncTaskExecutionResult result) {
    Optional<SyncMountSnapshotUpdateTracker> updateTrackerOpt =
        fetchUpdateTracker(syncMountId);
    if (updateTrackerOpt.isPresent()) {
      if (!updateTrackerOpt.get().isTaskUnderTrack(syncTaskId)){
        return true;
      }
    }
    return updateTrackerOpt.map(updateTracker -> {
      boolean isTrackerStillValid =
          updateTracker.markFailed(syncTaskId, result);
      if (isTrackerStillValid) {
        return true;
      } else {
        inProgress.remove(syncMountId);
        this.trackersFailed.put(syncMountId, updateTracker);
        LOG.error("Tracker is invalid, removed from trackers in progress");
        return false;
      }
    })
        .orElse(false);
  }

  public void markSyncTaskFinished(UUID syncTaskId, String syncMountId,
      SyncTaskExecutionResult result) {
    Optional<SyncMountSnapshotUpdateTracker> updateTrackerOpt =
        fetchUpdateTracker(syncMountId);
    if (updateTrackerOpt.isPresent()) {
      if (!updateTrackerOpt.get().isTaskUnderTrack(syncTaskId)){
        return;
      }
    }
    updateTrackerOpt.ifPresent(updateTracker -> {
      updateTracker.markFinished(syncTaskId, result);
      if (updateTracker.isFinished()) {
        appendToEvictQueue(updateTracker.getFinishedFileSync(), syncMountId);
        inProgress.remove(syncMountId);
        syncMountManager.finishSync(syncMountId);
      } else {
        this.notify();
      }
    });
  }

  /**
   * For provided storage write back mount, append the BlockCollectionId
   * to evict queue maintained by {@link WriteCacheEvictor WriteCacheEvictor}.
   */
  public void appendToEvictQueue(List<CreateFileSyncTask> finishedFileSync,
      String syncMountId) {
    if (!syncMountManager.isWriteBackMount(syncMountId)) {
      return;
    }
    finishedFileSync.stream().forEach(
        fileSync -> syncMountManager.getWriteCacheEvictor().add(
            fileSync.getBlockCollectionId()));
  }

  private Optional<SyncMountSnapshotUpdateTracker> fetchUpdateTracker(
      String syncMountId) {
    SyncMountSnapshotUpdateTracker syncMountSnapshotUpdateTracker =
        inProgress.get(syncMountId);
    if (syncMountSnapshotUpdateTracker == null) {
      return Optional.empty();
    }
    return Optional.of(syncMountSnapshotUpdateTracker);
  }

  void scheduleNextWork() throws IOException {
    List<ProvidedVolumeInfo> syncMounts = syncMountManager.getSyncMounts();

    for (ProvidedVolumeInfo syncMount : syncMounts) {
      if (namesystem.isInSafeMode()) {
        LOG.debug("Skipping synchronization of SyncMounts as the " +
            "namesystem is in safe mode");
        break;
      }
      if (syncMount.isPaused()) {
        LOG.info("SyncMount {} is paused", syncMount);
        continue;
      }

      if (inProgress.containsKey(syncMount.getId())) {
        scheduleNextWorkOnTracker(inProgress.get(syncMount.getId()));
      } else {
        if (syncMountManager.isEmptyDiff(syncMount.getMountPath())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Empty diff report since last sync operation for {}",
                syncMount.getMountPath());
          }
          continue;
        }
        scheduleNewSyncMountSnapshotUpdate(syncMount);
      }
    }
  }

  /**
   * Resync from the initial snapshot.
   *
   * TODO: PHILO: whether this method is needed in some situation.
   */
  public void fullResync(String syncMountId, BlockAliasMap.Reader<FileRegion>
      aliasMapReader) throws IOException {
    ProvidedVolumeInfo syncMount =
        syncMountManager.getSyncMountById(syncMountId);
    SnapshotDiffReport diffReport =
        syncMountManager.forceInitialSnapshot(syncMount.getMountPath());
    int targetSnapshotId = getTargetSnapshotId(diffReport);
    Optional<Integer> sourceSnapshotId = getSourceSnapshotId(diffReport);
    PhasedPlan planFromDiffReport = syncMountSnapshotUpdateFactory.
        createPlanFromDiffReport(syncMount, diffReport, sourceSnapshotId,
            targetSnapshotId);
    for (FileRegion fileRegion : aliasMapReader) {
      //TODO add nonce
      Path pathInAliasMap = fileRegion.getProvidedStorageLocation().getPath();
      planFromDiffReport.filter(pathInAliasMap);
    }
    SyncMountSnapshotUpdateTracker tracker =
        SyncMountSnapshotUpdateTrackerFactory.create(planFromDiffReport,
            aliasMapWriter, conf);
    inProgress.put(syncMount.getId(), tracker);
    scheduleNextWorkOnTracker(tracker);
  }

  /**
   * Resync based on the latest snapshot diff report.
   */
  public void resync(String syncMountId) throws IOException {
    ProvidedVolumeInfo syncMount =
        syncMountManager.getSyncMountById(syncMountId);
    SnapshotDiffReport diffReport =
        syncMountManager.performPreviousDiff(syncMount.getMountPath());
    int targetSnapshotId = getTargetSnapshotId(diffReport);
    Optional<Integer> sourceSnapshotId = getSourceSnapshotId(diffReport);
    PhasedPlan planFromDiffReport = syncMountSnapshotUpdateFactory.
        createPlanFromDiffReport(syncMount, diffReport, sourceSnapshotId,
            targetSnapshotId);

    SyncMountSnapshotUpdateTracker tracker =
        SyncMountSnapshotUpdateTrackerFactory.create(planFromDiffReport,
            aliasMapWriter, conf);
    inProgress.put(syncMount.getId(), tracker);
    scheduleNextWorkOnTracker(tracker);
  }

  public boolean blockingCancelTracker(String syncMountId) {
    //Do not remove. Let the mark fail/mark success do this through the
    // normal process
    SyncMountSnapshotUpdateTracker syncMountSnapshotUpdateTracker =
        inProgress.get(syncMountId);
    if (syncMountSnapshotUpdateTracker == null) {
      //Possible that the tracker already finished by the time the request to
      //cancel comes in.
      return true;
    }
    return syncMountSnapshotUpdateTracker.blockingCancel();
  }

  private void scheduleNewSyncMountSnapshotUpdate(ProvidedVolumeInfo syncMount)
      throws IOException {
    LOG.info("Planning new SyncMount {}", syncMount);
    if (inProgress.containsKey(syncMount.getId())) {
      LOG.info("SyncMount {} still has unfinished scheduled work, " +
          "not adding additional work", syncMount);
    } else {
      SnapshotDiffReport diffReport;
      Optional<Integer> sourceSnapshotId;
      int targetSnapshotId;
      try {
        diffReport = syncMountManager.makeSnapshotAndPerformDiff(
            syncMount.getMountPath());
        sourceSnapshotId = getSourceSnapshotId(diffReport);
        targetSnapshotId = getTargetSnapshotId(diffReport);
      } catch (IOException e) {
        LOG.error("Failed to take snapshot for: {}", syncMount, e);
        return;
      }

      PhasedPlan planFromDiffReport = syncMountSnapshotUpdateFactory.
          createPlanFromDiffReport(syncMount, diffReport, sourceSnapshotId,
              targetSnapshotId);

      if (planFromDiffReport.isEmpty()) {
        /**
         * The tracker for an empty plan will never finish as there will
         * be no tasks to trigger the finish marking.
         */
        LOG.info("Empty plan, not starting a tracker");
        syncMountManager.finishSync(syncMount.getId());
      } else {
        SyncMountSnapshotUpdateTracker tracker =
            SyncMountSnapshotUpdateTrackerFactory.create(planFromDiffReport,
                aliasMapWriter, conf);
        inProgress.put(syncMount.getId(), tracker);
        scheduleNextWorkOnTracker(tracker);
      }
    }
  }

  /**
   * TODO: Schedule failed block sync task to other DNs to have a try.
   */
  private void scheduleNextWorkOnTracker(
      SyncMountSnapshotUpdateTracker tracker) {
    SchedulableSyncPhase schedulableSyncPhase =
        tracker.getNextSchedulablePhase();
    syncTaskScheduler.schedule(schedulableSyncPhase);
  }

  private Optional<Integer> getSourceSnapshotId(SnapshotDiffReport diffReport)
      throws UnresolvedLinkException, AccessControlException,
      ParentNotDirectoryException {
    if (diffReport.getFromSnapshot() == null) {
      return Optional.empty();
    }
    INode localBackupPathINode = namesystem.getFSDirectory()
        .getINode(diffReport.getSnapshotRoot());
    INodeDirectory localBackupPathINodeDirectory =
        localBackupPathINode.asDirectory();
    Snapshot toSnapshot = localBackupPathINodeDirectory.getSnapshot(
        diffReport.getFromSnapshot().getBytes());
    return Optional.of(toSnapshot.getId());
  }

  private int getTargetSnapshotId(SnapshotDiffReport diffReport)
      throws UnresolvedLinkException, AccessControlException,
      ParentNotDirectoryException {
    INode localBackupPathINode = namesystem.getFSDirectory()
        .getINode(diffReport.getSnapshotRoot());
    INodeDirectory localBackupPathINodeDirectory =
        localBackupPathINode.asDirectory();
    Snapshot toSnapshot = localBackupPathINodeDirectory.getSnapshot(
        diffReport.getLaterSnapshotName().getBytes());
    return toSnapshot.getId();
  }

  public void updateTaskStatus(UUID syncTaskId) {
    syncTaskScheduler.dropTrackingTasks(syncTaskId);
  }
}

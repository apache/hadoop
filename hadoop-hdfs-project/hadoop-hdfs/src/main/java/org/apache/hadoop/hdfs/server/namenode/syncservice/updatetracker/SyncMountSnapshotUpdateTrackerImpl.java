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
package org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedPlan;
import org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.multipart.MultipartPlan;
import org.apache.hadoop.hdfs.server.protocol.BlockSyncTask;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTask.CreateDirectorySyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTask.CreateFileSyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTask.DeleteDirectorySyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTask.DeleteFileSyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTask.ModifyFileSyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTask.RenameDirectorySyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTask.RenameFileSyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTask.TouchFileSyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionResult;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskOperation;

import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Tracks sync tasks for provided storage backup/writeBack mount.
 */
public class SyncMountSnapshotUpdateTrackerImpl implements
    SyncMountSnapshotUpdateTracker {

  private static final Logger LOG = LoggerFactory
      .getLogger(SyncMountSnapshotUpdateTrackerImpl.class);

  private final CurrentTasksFactory currentTaskFactory;
  private final Configuration config;
  private PhasedPlan.Phases currentPhase;
  private Optional<MultipartPlan> multipartPlanOpt;
  private boolean multipartTrackerFinished;
  private PhasedPlan phasedPlan;
  private CurrentTasks<SyncTask> currentTasks;
  private List<CreateFileSyncTask> finishedFileSync;
  private BlockAliasMap.Writer<FileRegion> aliasMapWriter;
  private boolean cancelling = false;

  public SyncMountSnapshotUpdateTrackerImpl(PhasedPlan phasedPlan,
      BlockAliasMap.Writer<FileRegion> aliasMapWriter, Configuration config) {
    this.phasedPlan = phasedPlan;
    this.aliasMapWriter = aliasMapWriter;
    this.currentPhase = PhasedPlan.Phases.NOT_STARTED;
    this.multipartPlanOpt = Optional.empty();
    this.currentTaskFactory = new CurrentTasksFactory(config);
    this.currentTasks = currentTaskFactory.empty();
    this.finishedFileSync = new LinkedList<>();
    this.config = config;
  }

  @Override
  public Optional<SyncTask> markFinished(UUID syncTaskId,
      SyncTaskExecutionResult result) {
    LOG.info("Marking task as finished {}", syncTaskId);
    // CreateFileSyncTask is not maintained by currentTasks.
    Optional<SyncTask> syncTaskOpt = currentTasks.markFinished(syncTaskId);
    multipartPlanOpt.ifPresent(
        multipartPlan -> multipartPlan.markFinished(syncTaskId, result));
    multipartPlanOpt.ifPresent(multipartPlan -> {
      if (multipartPlan.isFinished()) {
        this.multipartTrackerFinished = true;
        this.multipartPlanOpt = Optional.empty();
      }
    });
    syncTaskOpt.ifPresent(syncTask -> {
      finalizeTask(syncTask, result);
    });
    return syncTaskOpt;
  }

  @Override
  public boolean markFailed(UUID syncTaskId, SyncTaskExecutionResult result) {
    LOG.error("Sync task currentTasks {} failed.", syncTaskId);
    boolean isTrackerStillValid = currentTasks.markFailure(syncTaskId);
    boolean multipartTrackerStillValid = multipartPlanOpt
        .map(multipartPlan -> multipartPlan.markFailed(syncTaskId, result))
        .orElse(true);
    return isTrackerStillValid && multipartTrackerStillValid;
  }

  @Override
  public boolean blockingCancel() {
    this.cancelling = true;
    long st = Time.monotonicNow();
    boolean result = this.canBeCancelled();

    long waitForMillis = 1000000;
    long checkEveryMillis = 5000;
    while (!result && (Time.monotonicNow() - st < waitForMillis)) {
      try {
        Thread.sleep(checkEveryMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      result = this.canBeCancelled();
    }
    return result;
  }

  private boolean canBeCancelled() {
    return (multipartTrackerFinished || !this.multipartPlanOpt.isPresent())
        && currentTasks.isFinished();
  }

  @Override
  public boolean isFinished() {
    return (multipartTrackerFinished || !this.multipartPlanOpt.isPresent()) &&
        phasedPlan.hasNoDownstreamTasksLeft(currentPhase.next())
        && currentTasks.isFinished();
  }

  @Override
  public List<CreateFileSyncTask> getFinishedFileSync() {
    return this.finishedFileSync;
  }

  @Override
  public SchedulableSyncPhase getNextSchedulablePhase() {
    if (this.cancelling) {
      return SchedulableSyncPhase.empty();
    } else {
      return multipartPlanOpt
          .map(MultipartPlan::handlePhase)
          .orElseGet(this::handlePhase);
    }
  }

  /**
   * PHILO: currentTasks contains non-multipart tasks, which are handled
   * firstly.
   * @return
   */
  private SchedulableSyncPhase handlePhase() {
    if (this.currentTasks.isNotFinished()) {
      List<SyncTask> tasksToDo = this.currentTasks.getTasksToDo();
      return createNonMultipartSchedulableTasks(tasksToDo);
    } else {
      this.currentPhase = this.currentPhase.next();
      List<SyncTask> nextSchedulableWork
          = phasedPlan.popNextSchedulableWork(this.currentPhase);

      List<CreateFileSyncTask> multipartSyncTasks =
          setCurrentTasksButSkimOffMultipartables(nextSchedulableWork);

      SchedulableSyncPhase syncPhase = startMultipartPlan(multipartSyncTasks);

      syncPhase.append(createNonMultipartSchedulableTasks(
          this.currentTasks.getTasksToDo()));

      return syncPhase;
    }
  }

  private List<CreateFileSyncTask> setCurrentTasksButSkimOffMultipartables(
      List<SyncTask> syncTasks) {
    //optimization, maybe a bit premature
    if (this.currentPhase != PhasedPlan.Phases.CREATE_FILES) {
      this.currentTasks = currentTaskFactory.create(syncTasks);
      return Collections.emptyList();
    }

    List<CreateFileSyncTask> createsThatNeedToBeMultiparted =
        Lists.newArrayList();
    List<SyncTask> others = Lists.newArrayList();
    for (SyncTask syncTask : syncTasks) {
      if (syncTask.getOperation() == SyncTaskOperation.CREATE_FILE) {
        CreateFileSyncTask createFileSyncTask = (CreateFileSyncTask) syncTask;
        createsThatNeedToBeMultiparted.add(createFileSyncTask);
      } else {
        others.add(syncTask);
      }
    }
    this.currentTasks = this.currentTaskFactory.create(others);
    return createsThatNeedToBeMultiparted;
  }

  private SchedulableSyncPhase createNonMultipartSchedulableTasks(
      Collection<SyncTask> others) {
    List<MetadataSyncTask> metadataSyncTasks = Lists.newArrayList();
    List<BlockSyncTask> blockSyncTasks = Lists.newArrayList();
    for (SyncTask syncTask : others) {
      switch (syncTask.getOperation()) {
      case TOUCH_FILE:
        metadataSyncTasks.add(MetadataSyncTask.touchFile(
            (TouchFileSyncTask) syncTask)
        );
        break;
      case DELETE_FILE:
        metadataSyncTasks.add(MetadataSyncTask.deleteFile(
            (DeleteFileSyncTask) syncTask)
        );
        break;
      case RENAME_FILE:
        metadataSyncTasks.add(MetadataSyncTask.renameFile(
            (RenameFileSyncTask) syncTask)
        );
        break;
      case CREATE_DIRECTORY:
        metadataSyncTasks.add(MetadataSyncTask.createDirectory(
            (CreateDirectorySyncTask) syncTask
        ));
        break;
      case DELETE_DIRECTORY:
        metadataSyncTasks.add(MetadataSyncTask.deleteDirectory(
            (DeleteDirectorySyncTask) syncTask
        ));
        break;
      case RENAME_DIRECTORY:
        metadataSyncTasks.add(MetadataSyncTask.renameDirectory(
            (RenameDirectorySyncTask) syncTask
        ));
        break;
      case CREATE_FILE:
        // File creation tasks are handled in the multipart tracker.
        break;
      case MODIFY_FILE:
      default:
        throw new IllegalArgumentException();
      }
    }
    return SchedulableSyncPhase.create(metadataSyncTasks, blockSyncTasks);
  }

  private SchedulableSyncPhase startMultipartPlan(List<CreateFileSyncTask>
      nextSchedulableWork) {
    if (nextSchedulableWork.isEmpty()) {
      return SchedulableSyncPhase.empty();
    }
    MultipartPlan multipartPlan = MultipartPlan.create(nextSchedulableWork,
        currentTaskFactory,
        t -> r -> this.finalizeTask(t, r));
    this.multipartPlanOpt = Optional.of(multipartPlan);
    return multipartPlan.getInitPhase();
  }

  private void finalizeTask(SyncTask syncTask,
      SyncTaskExecutionResult result) {
    LOG.info("Updating BlockAliasMap for {} : {}", syncTask.getUri(),
        syncTask.getOperation());
    switch (syncTask.getOperation()) {
    case TOUCH_FILE:
    case CREATE_DIRECTORY:
    case DELETE_DIRECTORY:
    case RENAME_DIRECTORY:
      // no blocks to update
      break;
    case DELETE_FILE:
      finalizeDeleteFileTask((DeleteFileSyncTask) syncTask);
      break;
    case RENAME_FILE:
      finalizeRenameFileTask((RenameFileSyncTask) syncTask);
      break;
    case CREATE_FILE:
      finalizeCreateFileTask((CreateFileSyncTask) syncTask);
      break;
    case MODIFY_FILE:
      finalizeModifyFileTask((ModifyFileSyncTask) syncTask);
      break;
    default:
    }
  }

  private void finalizeModifyFileTask(ModifyFileSyncTask syncTask) {
    Path filePath = new Path(syncTask.getUri());
    byte[] nonce;
    try {
      PathHandle pathHandle = getPathHandle(syncTask);
      nonce = pathHandle.toByteArray();
    } catch (IOException e) {
      nonce = new byte[0];
    }

    long offset = 0;
    for (LocatedBlock locatedBlock : syncTask.getLocatedBlocks()) {
      final Block block = locatedBlock.getBlock().getLocalBlock();
      ProvidedStorageLocation providedStorageLocation
          = new ProvidedStorageLocation(filePath, offset,
          block.getNumBytes(), nonce);
      FileRegion fileRegion = new FileRegion(block, providedStorageLocation);
      try {
        aliasMapWriter.store(fileRegion);
      } catch (IOException e) {
        LOG.error("Error updating BlockAliasMap for modify file: {}, " +
                "block: {} ", syncTask.getUri(), block);
      }
      offset += block.getNumBytes();
    }
  }

  /**
   * PHILO: this method is actually used by SingleMultipart instead of
   * {@link #markFinished markFinished}. Since create sync task is not
   * contained in {@link #currentTasks currentTasks}.
   */
  private void finalizeCreateFileTask(CreateFileSyncTask syncTask) {
    Path filePath = new Path(syncTask.getUri());
    byte[] nonce;
    try {
      PathHandle pathHandle = getPathHandle(syncTask);
      nonce = pathHandle.toByteArray();
    } catch (IOException e) {
      nonce = new byte[0];
    }

    long offset = 0;
    for (LocatedBlock locatedBlock : syncTask.getLocatedBlocks()) {
      final Block block = locatedBlock.getBlock().getLocalBlock();
      ProvidedStorageLocation providedStorageLocation
          = new ProvidedStorageLocation(filePath, offset,
          block.getNumBytes(), nonce);
      FileRegion fileRegion = new FileRegion(block, providedStorageLocation);
      try {
        aliasMapWriter.store(fileRegion);
      } catch (IOException e) {
        LOG.error(
            "Error updating BlockAliasMap for create file: {}, block: {} ",
            syncTask.getUri(), block);
      }
      offset += block.getNumBytes();
    }
    finishedFileSync.add(syncTask);
  }

  private void finalizeRenameFileTask(RenameFileSyncTask syncTask) {
    Path filePath = new Path(syncTask.getRenamedTo());
    byte[] nonce;
    try {
      PathHandle pathHandle = getPathHandle(syncTask);
      nonce = pathHandle.toByteArray();
    } catch (IOException e) {
      nonce = new byte[0];
    }

    long offset = 0;
    for (Block block : syncTask.getBlocks()) {
      ProvidedStorageLocation providedStorageLocation
              = new ProvidedStorageLocation(filePath, offset,
          block.getNumBytes(), nonce);
      FileRegion fileRegion = new FileRegion(block, providedStorageLocation);
      try {
        aliasMapWriter.store(fileRegion);
      } catch (IOException e) {
        LOG.error("Error updating BlockAliasMap for rename file: {} ",
            syncTask.getUri());
      }
      offset += block.getNumBytes();
    }
  }

  /**
   * The record may already be deleted from AliasMap, see
   * {@link FSNamesystem#removeFromAliasMap}. We think this remove
   * operation should be a part of delete operation in file level, regardless
   * of whether the deletion is synced to remote storage.
   * TODO: the remove operation here can be discarded.
   * @param syncTask
   */
  private void finalizeDeleteFileTask(DeleteFileSyncTask syncTask) {
    for (Block block : syncTask.getBlocks()) {
      try {
        aliasMapWriter.remove(block);
      } catch (IOException e) {
        LOG.error("Error updating BlockAliasMap for delete file: {}, " +
            "block: {}", syncTask.getUri(), block);
      }
    }
  }

  public PhasedPlan.Phases getCurrentPhase() {
    return currentPhase;
  }

  private PathHandle getPathHandle(SyncTask syncTask) throws IOException {
    URI uri = syncTask.getFinalUri();
    FileSystem fs = FileSystem.get(uri, config);
    Path path = new Path(uri);
    FileStatus fileStatus = fs.getFileStatus(path);
    try {
      return fs.getPathHandle(fileStatus);
    } catch (UnsupportedOperationException e) {
      throw new IOException("Unsupported to get path handle for " + path);
    }
  }

  Optional<MultipartPlan> getMultipartPlanOpt() {
    return multipartPlanOpt;
  }

  @Override
  public boolean isTaskUnderTrack(UUID syncTaskId) {
    boolean isCurrent = currentTasks.isTaskUnderTrack(syncTaskId);
    boolean isMultiPart = false;
    if (multipartPlanOpt.isPresent()) {
      isMultiPart = multipartPlanOpt.get().isTaskUnderTrack(syncTaskId);
    }
    return isCurrent || isMultiPart;
  }

}

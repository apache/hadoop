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
package org.apache.hadoop.hdfs.server.namenode.syncservice.planner;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SyncMount;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PartitionedDiffReport.TranslatedEntry;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.CREATE;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.RemoteSyncURICreator.createRemotePath;

public class PhasedPlanFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(PhasedSyncMountSnapshotUpdateFactory.class);

  private FilePlanner filePlanner;
  private DirectoryPlanner directoryPlanner;

  public PhasedPlanFactory(FilePlanner filePlanner,
      DirectoryPlanner directoryPlanner, Configuration conf) {

    this.filePlanner = filePlanner;
    this.directoryPlanner = directoryPlanner;
  }

  public PhasedPlan createFromPartitionedDiffReport(PartitionedDiffReport
      partitionedDiffReport, SyncMount syncMount, String sourceSnapshot,
      Optional<Integer> sourceSnapshotId, int targetSnapshotId) {

    List<SyncTask> renameToTemporaryName =
        createRenameToTemporaryNameSyncTasks(partitionedDiffReport.getRenames(),
            syncMount, sourceSnapshot, targetSnapshotId);

    // We can't delete if we don't have a source snapshot.
    FileAndDirsSyncTasks deleteSyncTasks;
    if (sourceSnapshotId.isPresent()) {
      deleteSyncTasks = createDeleteSyncTasks(
          partitionedDiffReport.getDeletes(), syncMount,
          sourceSnapshot, sourceSnapshotId.get());
    } else {
      deleteSyncTasks = new FileAndDirsSyncTasks();
    }

    List<SyncTask> renameToFinalName =
        createRenameToFinalNameSyncTasks(partitionedDiffReport.getRenames(),
            syncMount, sourceSnapshot, targetSnapshotId);
    Collections.reverse(renameToFinalName);

    FileAndDirsSyncTasks createsSyncTasks = createCreatesFromRenamesSyncTasks(
        partitionedDiffReport.getCreatesFromRenames(), syncMount,
        targetSnapshotId);
    createsSyncTasks.append(createCreateSyncTasks(
        partitionedDiffReport.getCreates(), syncMount, targetSnapshotId));

    List<SyncTask> modifiedSyncTasks = createModifiedSyncTasks(
        partitionedDiffReport.getModifies(), syncMount, targetSnapshotId);

    /**
     * Currently, all modify tasks are translated to CREATE tasks for
     * idempotency's sake.
     */

    createsSyncTasks.addAllFileSync(modifiedSyncTasks);

    return new PhasedPlan(renameToTemporaryName, deleteSyncTasks.getAllTasks(),
        renameToFinalName, createsSyncTasks.getDirTasks(),
        createsSyncTasks.getFileTasks());
  }

  private List<SyncTask> createRenameToTemporaryNameSyncTasks(
      List<PartitionedDiffReport.RenameEntryWithTemporaryName> renames,
      SyncMount syncMount, String sourceSnapshot, int targetSnapshotId) {
    return renames.stream()
        .map(entry -> convertRenameToTempNameToSyncTask(entry, syncMount,
            sourceSnapshot, targetSnapshotId))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
  }

  private List<SyncTask> createRenameToFinalNameSyncTasks(
      List<PartitionedDiffReport.RenameEntryWithTemporaryName> renames,
      SyncMount syncMount, String sourceSnapshot, int targetSnapshotId) {
    return renames.stream()
        .map(entry -> convertRenameToFinalToSyncTask(entry, syncMount,
            sourceSnapshot, targetSnapshotId))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
  }

  private Optional<SyncTask> convertRenameToTempNameToSyncTask(
      PartitionedDiffReport.RenameEntryWithTemporaryName entryWithTempName,
      SyncMount syncMount, String sourceSnapshot, int targetSnapshotId) {
    DiffReportEntry diffEntry = entryWithTempName.getEntry();
    URI sourceRemoteURI = createRemotePath(syncMount,
        DFSUtil.bytes2String(diffEntry.getSourcePath()));
    URI renameToRemoteURI = createRemotePath(syncMount,
        entryWithTempName.getTemporaryName());
    return createRenameSyncTask(diffEntry, sourceRemoteURI,
        renameToRemoteURI, syncMount, sourceSnapshot, targetSnapshotId);
  }


  private Optional<SyncTask> convertRenameToFinalToSyncTask(
      PartitionedDiffReport.RenameEntryWithTemporaryName entryWithTempName,
      SyncMount syncMount, String sourceSnapshot, int targetSnapshotId) {
    DiffReportEntry diffEntry = entryWithTempName.getEntry();
    URI sourceRemoteURI = createRemotePath(syncMount,
        entryWithTempName.getTemporaryName());
    URI renameToRemoteURI = createRemotePath(syncMount,
        DFSUtil.bytes2String(diffEntry.getTargetPath()));
    return createRenameSyncTask(diffEntry, sourceRemoteURI,
        renameToRemoteURI, syncMount, sourceSnapshot, targetSnapshotId);
  }

  private Optional<SyncTask> createRenameSyncTask(
      DiffReportEntry diffEntry,
      URI sourceRemoteURI, URI renameToRemoteURI, SyncMount syncMount,
      String sourceSnapshot, int targetSnapshotId) {
    if (diffEntry.getInodeType() == SnapshotDiffReport.INodeType.DIRECTORY) {
      return Optional.of(
          SyncTask.renameDirectory(sourceRemoteURI,
              renameToRemoteURI, syncMount.getName()));
    } else if (diffEntry.getInodeType() == SnapshotDiffReport.INodeType.FILE) {
      try {
        INodeFile iNodeFile = filePlanner.getINodeFile(syncMount, diffEntry);
        LocatedBlocks locatedBlocks = filePlanner.getLocatedBlocks(
            targetSnapshotId, iNodeFile);
        List<Block> blocks = locatedBlocks.getLocatedBlocks().stream()
            .map(lb -> lb.getBlock().getLocalBlock())
            .collect(Collectors.toList());
        return Optional.of(
            SyncTask.renameFile(sourceRemoteURI,
                renameToRemoteURI, blocks, syncMount.getName()));
      } catch (IOException e) {
        LOG.error("Could not createRenameSyncTask for {}",
            diffEntry.getTargetPath());
        return Optional.empty();
      }
    } else if (diffEntry.getInodeType() == SnapshotDiffReport.INodeType.SYMLINK) {
      LOG.debug("Symlinks are not supported. Skipping rename task.");
      return Optional.empty();
    } else {
      throw new RuntimeException("Unknown INodeType: cant create rename task");
    }
  }

  private FileAndDirsSyncTasks createDeleteSyncTasks(List<TranslatedEntry> deletes,
      SyncMount syncMount, String sourceSnapshot, int sourceSnapshotId) {
    FileAndDirsSyncTasks plan = new FileAndDirsSyncTasks();
    for (TranslatedEntry delete : deletes) {
      FileAndDirsSyncTasks subPlan = createDeleteSyncTaskTree(delete,
          syncMount, sourceSnapshot, sourceSnapshotId);
      plan.append(subPlan);
    }
    return plan;
  }

  private FileAndDirsSyncTasks createDeleteSyncTaskTree(
      TranslatedEntry deleteEntry, SyncMount syncMount, String sourceSnapshot,
      int sourceSnapshotId) {
    DiffReportEntry diffEntry = deleteEntry.getEntry();
    if (diffEntry.getInodeType() == SnapshotDiffReport.INodeType.DIRECTORY) {
      return directoryPlanner.createPlanForDirectory(diffEntry,
          deleteEntry.getTranslatedName(), syncMount, sourceSnapshotId);
    } else if (diffEntry.getInodeType() == SnapshotDiffReport.INodeType.FILE) {
      URI sourceRemoteURI = createRemotePath(syncMount,
          DFSUtil.bytes2String(diffEntry.getSourcePath()));
      FileAndDirsSyncTasks plan = new FileAndDirsSyncTasks();
      try {
        INodeFile iNodeFile = filePlanner.getINodeFile4Snapshot(syncMount,
            sourceSnapshot, diffEntry);
        LocatedBlocks locatedBlocks =
            filePlanner.getLocatedBlocks(sourceSnapshotId, iNodeFile);
        List<Block> blocks = locatedBlocks.getLocatedBlocks().stream()
            .map(lb -> lb.getBlock().getLocalBlock())
            .collect(Collectors.toList());
        plan.addFileSync(SyncTask.deleteFile(
            sourceRemoteURI, blocks, syncMount.getName()));
      } catch (IOException e) {
        LOG.error("Could not createDeleteSyncTask for {}",
            diffEntry.getTargetPath());
      }
      return plan;
    } else {
      LOG.info("Unsupported INode type: {}", diffEntry.getInodeType());
      return new FileAndDirsSyncTasks();
    }
  }

  private FileAndDirsSyncTasks createCreateSyncTasks(List<TranslatedEntry> creates,
      SyncMount syncMount, int targetSnapshotId) {
    FileAndDirsSyncTasks plan = new FileAndDirsSyncTasks();
    for (TranslatedEntry create : creates) {
      FileAndDirsSyncTasks subPlan = createCreateSyncTaskTree(create, syncMount, targetSnapshotId);
      plan.append(subPlan);
    }
    return plan;
  }

  @VisibleForTesting
  FileAndDirsSyncTasks createCreateSyncTaskTree(TranslatedEntry createEntry,
      SyncMount syncMount, int targetSnapshotId) {

    DiffReportEntry diffEntry = createEntry.getEntry();
    if (diffEntry.getInodeType() == SnapshotDiffReport.INodeType.DIRECTORY) {
      FileAndDirsSyncTasks plan = directoryPlanner.createPlanForDirectory(diffEntry,
          createEntry.getTranslatedName(), syncMount, targetSnapshotId);
      return plan;
    } else if (diffEntry.getInodeType() == SnapshotDiffReport.INodeType.FILE) {
      URI sourceRemoteURI = createRemotePath(syncMount,
          DFSUtil.bytes2String(diffEntry.getSourcePath()));
      try {
        FileAndDirsSyncTasks plan = new FileAndDirsSyncTasks();
        SyncTask createFile = filePlanner.createPlanTreeNodeForCreatedFile(syncMount,
            targetSnapshotId, diffEntry, sourceRemoteURI);
        plan.addFileSync(createFile);
        return plan;
      } catch (IOException e) {
        //TODO Handle errors
        throw new RuntimeException(e);
      }

    } else {
      LOG.info("Unsupported INode type: {}", diffEntry.getInodeType());
      return new FileAndDirsSyncTasks();
    }
  }

  private FileAndDirsSyncTasks createCreatesFromRenamesSyncTasks(
      List<DiffReportEntry> createsFromRenames, SyncMount syncMount,
      int targetSnapshotId) {

    List<DiffReportEntry> createEntries = createsFromRenames.stream()
        .map(diffEntry -> new DiffReportEntry(diffEntry.getInodeType(), CREATE,
            diffEntry.getTargetPath()))
        .collect(Collectors.toList());

    FileAndDirsSyncTasks plan = new FileAndDirsSyncTasks();
    for (DiffReportEntry createEntry : createEntries) {
      try {
        if (createEntry.getInodeType() == SnapshotDiffReport.INodeType.DIRECTORY) {
          FileAndDirsSyncTasks planForCreatedDirectory =
              directoryPlanner.createPlanForDirectory(createEntry,
                  DFSUtil.bytes2String(createEntry.getSourcePath()),
                  syncMount, targetSnapshotId);
          plan.append(planForCreatedDirectory);
        } else if (createEntry.getInodeType() == SnapshotDiffReport.INodeType.FILE) {
          URI sourceRemoteURI = createRemotePath(syncMount,
              DFSUtil.bytes2String(createEntry.getSourcePath()));
          SyncTask metadataSyncTask = filePlanner.createPlanTreeNodeForCreatedFile(syncMount,
              targetSnapshotId, createEntry, sourceRemoteURI);
          plan.addFileSync(metadataSyncTask);
        } else {
          LOG.info("Unsupported INode type: {}", createEntry.getInodeType());
          return null;
        }
      } catch (IOException e) {
        throw new RuntimeException("Error creating Creates from Renames");
      }
    }
    return plan;

  }

  private List<SyncTask> createModifiedSyncTasks(List<TranslatedEntry> modifies,
      SyncMount syncMount, int targetSnapshotId) {

    return modifies.stream()
        .flatMap(translatedEntry ->
            createModifiedSyncTask(translatedEntry, syncMount,
                targetSnapshotId)
                .map(Stream::of)
                .orElseGet(Stream::empty))
        .collect(Collectors.toList());

  }

  /*
   * We translate MODIFY_FILE To CREATE_FILE here because we only
   * rewrite files. This is because append is not idempotent so it's
   * inadequate for a system that may need to retry writes.
   */

  @VisibleForTesting
  Optional<SyncTask> createModifiedSyncTask(TranslatedEntry modifiedEntry,
      SyncMount syncMount, int targetSnapshotId) {
    DiffReportEntry diffEntry = modifiedEntry.getEntry();
    if (diffEntry.getInodeType() == SnapshotDiffReport.INodeType.DIRECTORY) {
      //We don't handle modify directories
      return Optional.empty();
    } else if (diffEntry.getInodeType() == SnapshotDiffReport.INodeType.FILE) {
      try {
        SyncTask modifyFile =
            filePlanner.createModifiedFileSyncTasks(targetSnapshotId,
                diffEntry.getSourcePath(),
                syncMount);
        return Optional.of(modifyFile);
      } catch (IOException e) {
        //TODO Handle errors
        throw new RuntimeException(e);
      }
    } else {
      LOG.info("Unsupported INode type: {}", diffEntry.getInodeType());
      return Optional.empty();
    }

  }

}

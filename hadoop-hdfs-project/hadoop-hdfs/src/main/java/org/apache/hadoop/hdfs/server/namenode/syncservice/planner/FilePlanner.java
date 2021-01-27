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
package org.apache.hadoop.hdfs.server.namenode.syncservice.planner;

import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.ProvidedVolumeInfo;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.protocol.SyncTask;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.server.namenode.syncservice.RemoteSyncURICreator.createRemotePath;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.planner.DirectoryPlanner.convertPathToAbsoluteFile;

/**
 * Sync service planner for file.
 */
public class FilePlanner {

  private static final Logger LOG = LoggerFactory.getLogger(FilePlanner.class);

  private Namesystem namesystem;
  private BlockManager blockManager;

  public FilePlanner(Namesystem namesystem, BlockManager blockManager) {
    this.namesystem = namesystem;
    this.blockManager = blockManager;
  }

  private static byte[] getNodePath(SnapshotDiffReport.DiffReportEntry dre) {
    if (dre.getTargetPath() != null) {
      return dre.getTargetPath();
    } else {
      return dre.getSourcePath();
    }
  }

  public SyncTask createPlanTreeNodeForCreatedFile(ProvidedVolumeInfo syncMount,
      int targetSnapshotId, SnapshotDiffReport.DiffReportEntry entry,
      String targetName) throws IOException {
    INodeFile iNodeFile = getINodeFile(syncMount, entry);
    return createCreatedFileSyncTasks(targetSnapshotId,
          iNodeFile, syncMount, targetName);
  }

  INodeFile getINodeFile4Snapshot(ProvidedVolumeInfo syncMount, String snapshot,
      SnapshotDiffReport.DiffReportEntry entry)
      throws UnresolvedLinkException, AccessControlException,
      ParentNotDirectoryException {
    File path = convertPathToAbsoluteFile(getNodePath(entry),
        new Path(syncMount.getMountPath()), snapshot);
    INode node = namesystem.getFSDirectory().getINode(
        path.getAbsolutePath());
    return node.asFile();
  }

  INodeFile getINodeFile(ProvidedVolumeInfo syncMount,
      SnapshotDiffReport.DiffReportEntry entry)
      throws UnresolvedLinkException, AccessControlException,
      ParentNotDirectoryException {
    File path = convertPathToAbsoluteFile(getNodePath(entry),
        new Path(syncMount.getMountPath()));
    INode node = namesystem.getFSDirectory().getINode(
        path.getAbsolutePath());
    return node.asFile();
  }

  public SyncTask createTouchFileSyncTasks(URI remotePath,
      ProvidedVolumeInfo syncMount, long blockCollectionId) {
    return SyncTask.touchFile(remotePath,
        syncMount.getId(), blockCollectionId);
  }

  public SyncTask createCreatedFileSyncTasks(int targetSnapshotId,
      INodeFile nodeFile, ProvidedVolumeInfo syncMount, String targetName)
      throws IOException {
    URI remotePath = createRemotePath(syncMount, targetName);
    while (nodeFile.isUnderConstruction()) {
      try {
        // If file is under construction, we may miss syncing blocks which
        // are under construction
        Thread.sleep(10);
      } catch (InterruptedException e) {
        LOG.error("Thread interrupted while waiting file under construction. " +
            "This may lead to missing" +
            "blocks under constructing which are supposed to be synced!");
      }
    }
    BlockInfo[] nodeFileBlocks = nodeFile.getBlocks(targetSnapshotId);
    long blockCollectionId = nodeFile.getId();
    if (nodeFileBlocks == null || nodeFileBlocks.length == 0) {
      return createTouchFileSyncTasks(remotePath, syncMount, blockCollectionId);
    }

    LocatedBlocks locatedBlocks = getLocatedBlocks(targetSnapshotId, nodeFile);

    SyncTask createFileSyncTask =
        SyncTask.createFile(remotePath, syncMount.getId(),
            locatedBlocks.getLocatedBlocks(), blockCollectionId);
    return createFileSyncTask;
  }

  public SyncTask createModifiedFileSyncTasks(int targetSnapshotId,
      byte[] sourcePath, String targetName, ProvidedVolumeInfo syncMount)
      throws IOException {
    File source = convertPathToAbsoluteFile(sourcePath,
        new Path(syncMount.getMountPath()));
    INodeFile nodeFile = namesystem.getFSDirectory().getINode(
        source.getAbsolutePath()).asFile();
    while (nodeFile.isUnderConstruction()) {
      try {
        // If file is under construction, we may miss syncing blocks which are
        // under construction
        Thread.sleep(10);
      } catch (InterruptedException e) {
        LOG.error("Thread interrupted while waiting file under construction");
      }
    }
    URI remotePath = createRemotePath(syncMount, targetName);
    BlockInfo[] nodeFileBlocks = nodeFile.getBlocks(targetSnapshotId);
    long blockCollectionId = nodeFile.getId();
    if (nodeFileBlocks == null || nodeFileBlocks.length == 0) {
      return createTouchFileSyncTasks(remotePath, syncMount, blockCollectionId);
    }
    LocatedBlocks locatedBlocks = getLocatedBlocks(targetSnapshotId, nodeFile);

    /*
     * We translate MODIFY_FILE To CREATE_FILE here because we only
     * rewrite files. This is because append is not idempotent so it's
     * inadequate for a system that may need to retry writes.
     */
    SyncTask createFile =
        SyncTask.createFile(remotePath, syncMount.getId(),
            locatedBlocks.getLocatedBlocks(), blockCollectionId);
    return createFile;
  }

  public SyncTask createDeletedFileSyncTasks(int targetSnapshotId,
      INodeFile nodeFile, ProvidedVolumeInfo syncMount, String targetName)
      throws IOException {
    URI remotePath = createRemotePath(syncMount, targetName);
    BlockInfo[] nodeFileBlocks = nodeFile.getBlocks(targetSnapshotId);
    if (nodeFileBlocks == null || nodeFileBlocks.length == 0) {
      return SyncTask.touchFile(remotePath, syncMount.getId(),
          nodeFile.getId());
    }

    LocatedBlocks locatedBlocks = getLocatedBlocks(targetSnapshotId, nodeFile);

    List<Block> blocks = locatedBlocks.getLocatedBlocks().stream()
        .map(lb -> lb.getBlock().getLocalBlock())
        .collect(Collectors.toList());
    SyncTask deleteFileSyncTask =
        SyncTask.deleteFile(remotePath, blocks, syncMount.getId());
    return deleteFileSyncTask;
  }

  LocatedBlocks getLocatedBlocks(int snapshotId, INodeFile nodeFile)
      throws IOException {
    BlockInfo[] blockInfos = nodeFile.getBlocks(snapshotId);
    long fileLength = nodeFile.computeFileSize(snapshotId);
    namesystem.readLock();
    try {
      return blockManager.createLocatedBlocks(blockInfos,
          fileLength,
          false, 0, fileLength, false, true, null, null);
    } finally {
      namesystem.readUnlock();
    }
  }
}

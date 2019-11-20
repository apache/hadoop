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

import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SyncMount;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTask;
import org.apache.hadoop.security.AccessControlException;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.server.namenode.syncservice.RemoteSyncURICreator.createRemotePathFromAbsolutePath;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.planner.DirectoryPlanner.convertPathToAbsoluteFile;

public class FilePlanner {

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

  public SyncTask createPlanTreeNodeForCreatedFile(SyncMount syncMount,
      int targetSnapshotId, SnapshotDiffReport.DiffReportEntry entry,
      URI sourceRemoteURI) throws IOException {
    INodeFile iNodeFile = getINodeFile(syncMount, entry);
    long blockCollectionId = iNodeFile.getId();
    BlockInfo[] nodeFileBlocks = iNodeFile.getBlocks(targetSnapshotId);
    if (nodeFileBlocks == null || nodeFileBlocks.length == 0) {
      return createTouchFileSyncTasks(sourceRemoteURI, syncMount,
          blockCollectionId);
    } else {
      return createCreatedFileSyncTasks(targetSnapshotId,
          iNodeFile, syncMount);
    }
  }

  INodeFile getINodeFile4Snapshot(SyncMount syncMount, String snapshot,
      SnapshotDiffReport.DiffReportEntry entry)
      throws UnresolvedLinkException, AccessControlException,
      ParentNotDirectoryException {
    File path = convertPathToAbsoluteFile(getNodePath(entry),
        syncMount.getLocalPath(), snapshot);
    INode node = namesystem.getFSDirectory().getINode(
        path.getAbsolutePath());
    return node.asFile();
  }

  INodeFile getINodeFile(SyncMount syncMount,
      SnapshotDiffReport.DiffReportEntry entry)
      throws UnresolvedLinkException, AccessControlException,
      ParentNotDirectoryException {
    File path = convertPathToAbsoluteFile(getNodePath(entry),
        syncMount.getLocalPath());
    INode node = namesystem.getFSDirectory().getINode(
        path.getAbsolutePath());
    return node.asFile();
  }

  public SyncTask createTouchFileSyncTasks(URI remotePath,
      SyncMount syncMount, long blockCollectionId) {
    return SyncTask.touchFile(remotePath,
        syncMount.getName(), blockCollectionId);
  }

  public SyncTask createCreatedFileSyncTasks(int targetSnapshotId,
      INodeFile nodeFile, SyncMount syncMount) throws IOException {
    URI remotePath = createRemotePathFromAbsolutePath(syncMount,
        nodeFile.getFullPathName());
    BlockInfo[] nodeFileBlocks = nodeFile.getBlocks(targetSnapshotId);
    long blockCollectionId = nodeFile.getId();
    if (nodeFileBlocks == null || nodeFileBlocks.length == 0) {
      return createTouchFileSyncTasks(remotePath, syncMount, blockCollectionId);
    }

    LocatedBlocks locatedBlocks = getLocatedBlocks(targetSnapshotId, nodeFile);

    SyncTask createFileSyncTask =
        SyncTask.createFile(remotePath, syncMount.getName(),
            locatedBlocks.getLocatedBlocks(), blockCollectionId);
    return createFileSyncTask;
  }

  public SyncTask createModifiedFileSyncTasks(int targetSnapshotId,
      byte[] sourcePath, SyncMount syncMount) throws IOException {
    File source = convertPathToAbsoluteFile(sourcePath, syncMount.getLocalPath());
    INodeFile nodeFile = namesystem.getFSDirectory().getINode(
        source.getAbsolutePath()).asFile();
    long blockCollectionId = nodeFile.getId();
    URI remotePath = createRemotePathFromAbsolutePath(syncMount,
        nodeFile.getFullPathName());
    LocatedBlocks locatedBlocks = getLocatedBlocks(targetSnapshotId, nodeFile);

    /*
     * We translate MODIFY_FILE To CREATE_FILE here because we only
     * rewrite files. This is because append is not idempotent so it's
     * inadequate for a system that may need to retry writes.
     */
    SyncTask createFile =
        SyncTask.createFile(remotePath, syncMount.getName(),
            locatedBlocks.getLocatedBlocks(), blockCollectionId);
    return createFile;
  }

  public SyncTask createDeletedFileSyncTasks(int targetSnapshotId,
      INodeFile nodeFile, SyncMount syncMount) throws IOException {
    URI remotePath = createRemotePathFromAbsolutePath(syncMount,
        nodeFile.getFullPathName());
    BlockInfo[] nodeFileBlocks = nodeFile.getBlocks(targetSnapshotId);
    if (nodeFileBlocks == null || nodeFileBlocks.length == 0) {
      return SyncTask.touchFile(remotePath, syncMount.getName(),
          nodeFile.getId());
    }

    LocatedBlocks locatedBlocks = getLocatedBlocks(targetSnapshotId, nodeFile);

    List<Block> blocks = locatedBlocks.getLocatedBlocks().stream()
        .map(lb -> lb.getBlock().getLocalBlock())
        .collect(Collectors.toList());
    SyncTask deleteFileSyncTask =
        SyncTask.deleteFile(remotePath, blocks, syncMount.getName());
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

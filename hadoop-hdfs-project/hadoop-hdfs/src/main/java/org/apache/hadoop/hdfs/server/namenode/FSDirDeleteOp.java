/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.util.ChunkedArrayList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.CURRENT_STATE_ID;
import static org.apache.hadoop.util.Time.now;

class FSDirDeleteOp {
  /**
   * Delete the target directory and collect the blocks under it
   *
   * @param iip the INodesInPath instance containing all the INodes for the path
   * @param collectedBlocks Blocks under the deleted directory
   * @param removedINodes INodes that should be removed from inodeMap
   * @return the number of files that have been removed
   */
  static long delete(
      FSDirectory fsd, INodesInPath iip, BlocksMapUpdateInfo collectedBlocks,
      List<INode> removedINodes, long mtime) throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.delete: " + iip.getPath());
    }
    final long filesRemoved;
    fsd.writeLock();
    try {
      if (!deleteAllowed(iip, iip.getPath()) ) {
        filesRemoved = -1;
      } else {
        List<INodeDirectory> snapshottableDirs = new ArrayList<>();
        FSDirSnapshotOp.checkSnapshot(iip.getLastINode(), snapshottableDirs);
        filesRemoved = unprotectedDelete(fsd, iip, collectedBlocks,
                                         removedINodes, mtime);
        fsd.getFSNamesystem().removeSnapshottableDirs(snapshottableDirs);
      }
    } finally {
      fsd.writeUnlock();
    }
    return filesRemoved;
  }

  /**
   * Remove a file/directory from the namespace.
   * <p>
   * For large directories, deletion is incremental. The blocks under
   * the directory are collected and deleted a small number at a time holding
   * the {@link FSNamesystem} lock.
   * <p>
   * For small directory or file the deletion is done in one shot.
   *
   */
  static BlocksMapUpdateInfo delete(
      FSNamesystem fsn, String src, boolean recursive, boolean logRetryCache)
      throws IOException {
    FSDirectory fsd = fsn.getFSDirectory();
    FSPermissionChecker pc = fsd.getPermissionChecker();

    final INodesInPath iip = fsd.resolvePathForWrite(pc, src, false);
    src = iip.getPath();
    if (!recursive && fsd.isNonEmptyDirectory(iip)) {
      throw new PathIsNotEmptyDirectoryException(src + " is non empty");
    }
    if (fsd.isPermissionEnabled()) {
      fsd.checkPermission(pc, iip, false, null, FsAction.WRITE, null,
                          FsAction.ALL, true);
    }

    return deleteInternal(fsn, src, iip, logRetryCache);
  }

  /**
   * Delete a path from the name space
   * Update the count at each ancestor directory with quota
   * <br>
   * Note: This is to be used by
   * {@link org.apache.hadoop.hdfs.server.namenode.FSEditLog} only.
   * <br>
   * @param src a string representation of a path to an inode
   * @param mtime the time the inode is removed
   */
  static void deleteForEditLog(FSDirectory fsd, String src, long mtime)
      throws IOException {
    assert fsd.hasWriteLock();
    FSNamesystem fsn = fsd.getFSNamesystem();
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    List<INode> removedINodes = new ChunkedArrayList<>();

    final INodesInPath iip = fsd.getINodesInPath4Write(
        FSDirectory.normalizePath(src), false);
    if (!deleteAllowed(iip, src)) {
      return;
    }
    List<INodeDirectory> snapshottableDirs = new ArrayList<>();
    FSDirSnapshotOp.checkSnapshot(iip.getLastINode(), snapshottableDirs);
    long filesRemoved = unprotectedDelete(
        fsd, iip, collectedBlocks, removedINodes, mtime);
    fsn.removeSnapshottableDirs(snapshottableDirs);

    if (filesRemoved >= 0) {
      fsn.removeLeasesAndINodes(src, removedINodes, false);
      fsn.removeBlocksAndUpdateSafemodeTotal(collectedBlocks);
    }
  }

  /**
   * Remove a file/directory from the namespace.
   * <p>
   * For large directories, deletion is incremental. The blocks under
   * the directory are collected and deleted a small number at a time holding
   * the {@link org.apache.hadoop.hdfs.server.namenode.FSNamesystem} lock.
   * <p>
   * For small directory or file the deletion is done in one shot.
   */
  static BlocksMapUpdateInfo deleteInternal(
      FSNamesystem fsn, String src, INodesInPath iip, boolean logRetryCache)
      throws IOException {
    assert fsn.hasWriteLock();
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.delete: " + src);
    }

    FSDirectory fsd = fsn.getFSDirectory();
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    List<INode> removedINodes = new ChunkedArrayList<>();

    long mtime = now();
    // Unlink the target directory from directory tree
    long filesRemoved = delete(
        fsd, iip, collectedBlocks, removedINodes, mtime);
    if (filesRemoved < 0) {
      return null;
    }
    fsd.getEditLog().logDelete(src, mtime, logRetryCache);
    incrDeletedFileCount(filesRemoved);

    fsn.removeLeasesAndINodes(src, removedINodes, true);

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* Namesystem.delete: "
                                        + src +" is removed");
    }
    return collectedBlocks;
  }

  static void incrDeletedFileCount(long count) {
    NameNode.getNameNodeMetrics().incrFilesDeleted(count);
  }

  private static boolean deleteAllowed(final INodesInPath iip,
      final String src) {
    if (iip.length() < 1 || iip.getLastINode() == null) {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
            "DIR* FSDirectory.unprotectedDelete: failed to remove "
                + src + " because it does not exist");
      }
      return false;
    } else if (iip.length() == 1) { // src is the root
      NameNode.stateChangeLog.warn(
          "DIR* FSDirectory.unprotectedDelete: failed to remove " + src +
              " because the root is not allowed to be deleted");
      return false;
    }
    return true;
  }

  /**
   * Delete a path from the name space
   * Update the count at each ancestor directory with quota
   * @param iip the inodes resolved from the path
   * @param collectedBlocks blocks collected from the deleted path
   * @param removedINodes inodes that should be removed from inodeMap
   * @param mtime the time the inode is removed
   * @return the number of inodes deleted; 0 if no inodes are deleted.
   */
  private static long unprotectedDelete(
      FSDirectory fsd, INodesInPath iip, BlocksMapUpdateInfo collectedBlocks,
      List<INode> removedINodes, long mtime) {
    assert fsd.hasWriteLock();

    // check if target node exists
    INode targetNode = iip.getLastINode();
    if (targetNode == null) {
      return -1;
    }

    // record modification
    final int latestSnapshot = iip.getLatestSnapshotId();
    targetNode.recordModification(latestSnapshot);

    // Remove the node from the namespace
    long removed = fsd.removeLastINode(iip);
    if (removed == -1) {
      return -1;
    }

    // set the parent's modification time
    final INodeDirectory parent = targetNode.getParent();
    parent.updateModificationTime(mtime, latestSnapshot);

    fsd.updateCountForDelete(targetNode, iip);
    if (removed == 0) {
      return 0;
    }

    // collect block and update quota
    if (!targetNode.isInLatestSnapshot(latestSnapshot)) {
      targetNode.destroyAndCollectBlocks(fsd.getBlockStoragePolicySuite(),
        collectedBlocks, removedINodes);
    } else {
      QuotaCounts counts = targetNode.cleanSubtree(
        fsd.getBlockStoragePolicySuite(), CURRENT_STATE_ID,
          latestSnapshot, collectedBlocks, removedINodes);
      removed = counts.getNameSpace();
      fsd.updateCountNoQuotaCheck(iip, iip.length() -1, counts.negation());
    }

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
          + iip.getPath() + " is removed");
    }
    return removed;
  }
}

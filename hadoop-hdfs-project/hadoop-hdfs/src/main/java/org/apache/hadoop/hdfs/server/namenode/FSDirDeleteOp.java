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

import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.INode.ReclaimContext;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.ChunkedArrayList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.CURRENT_STATE_ID;
import static org.apache.hadoop.util.Time.now;

class FSDirDeleteOp {
  /**
   * Delete the target directory and collect the blocks under it
   *
   * @param fsd the FSDirectory instance
   * @param iip the INodesInPath instance containing all the INodes for the path
   * @param collectedBlocks Blocks under the deleted directory
   * @param removedINodes INodes that should be removed from inodeMap
   * @return the number of files that have been removed
   */
  static long delete(FSDirectory fsd, INodesInPath iip,
      BlocksMapUpdateInfo collectedBlocks, List<INode> removedINodes,
      List<Long> removedUCFiles, long mtime) throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.delete: " + iip.getPath());
    }
    long filesRemoved = -1;
    FSNamesystem fsn = fsd.getFSNamesystem();
    fsd.writeLock();
    try {
      if (deleteAllowed(iip, iip.getPath()) ) {
        List<INodeDirectory> snapshottableDirs = new ArrayList<>();
        FSDirSnapshotOp.checkSnapshot(iip.getLastINode(), snapshottableDirs);
        ReclaimContext context = new ReclaimContext(
            fsd.getBlockStoragePolicySuite(), collectedBlocks, removedINodes,
            removedUCFiles);
        if (unprotectedDelete(fsd, iip, context, mtime)) {
          filesRemoved = context.quotaDelta().getNsDelta();
        }
        fsd.updateReplicationFactor(context.collectedBlocks()
                                        .toUpdateReplicationInfo());
        fsn.removeSnapshottableDirs(snapshottableDirs);
        fsd.updateCount(iip, context.quotaDelta(), false);
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
   * @param fsn namespace
   * @param src path name to be deleted
   * @param recursive boolean true to apply to all sub-directories recursively
   * @param logRetryCache whether to record RPC ids in editlog for retry cache
   *          rebuilding
   * @return blocks collected from the deleted path
   * @throws IOException
   */
  static BlocksMapUpdateInfo delete(
      FSNamesystem fsn, String src, boolean recursive, boolean logRetryCache)
      throws IOException {
    FSDirectory fsd = fsn.getFSDirectory();
    FSPermissionChecker pc = fsd.getPermissionChecker();
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);

    src = fsd.resolvePath(pc, src, pathComponents);
    final INodesInPath iip = fsd.getINodesInPath4Write(src, false);
    if (!recursive && fsd.isNonEmptyDirectory(iip)) {
      throw new PathIsNotEmptyDirectoryException(src + " is non empty");
    }
    if (fsd.isPermissionEnabled()) {
      fsd.checkPermission(pc, iip, false, null, FsAction.WRITE, null,
                          FsAction.ALL, true);
    }
    if (recursive && fsd.isNonEmptyDirectory(iip)) {
      checkProtectedDescendants(fsd, fsd.normalizePath(src));
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
   *
   * @param fsd the FSDirectory instance
   * @param src a string representation of a path to an inode
   * @param mtime the time the inode is removed
   */
  static void deleteForEditLog(FSDirectory fsd, String src, long mtime)
      throws IOException {
    assert fsd.hasWriteLock();
    FSNamesystem fsn = fsd.getFSNamesystem();
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    List<INode> removedINodes = new ChunkedArrayList<>();
    List<Long> removedUCFiles = new ChunkedArrayList<>();

    final INodesInPath iip = fsd.getINodesInPath4Write(
        FSDirectory.normalizePath(src), false);
    if (!deleteAllowed(iip, src)) {
      return;
    }
    List<INodeDirectory> snapshottableDirs = new ArrayList<>();
    FSDirSnapshotOp.checkSnapshot(iip.getLastINode(), snapshottableDirs);
    boolean filesRemoved = unprotectedDelete(fsd, iip,
        new ReclaimContext(fsd.getBlockStoragePolicySuite(),
            collectedBlocks, removedINodes, removedUCFiles),
        mtime);
    fsn.removeSnapshottableDirs(snapshottableDirs);

    if (filesRemoved) {
      fsn.removeLeasesAndINodes(removedUCFiles, removedINodes, false);
      fsn.getBlockManager().removeBlocksAndUpdateSafemodeTotal(collectedBlocks);
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
   * @param fsn namespace
   * @param src path name to be deleted
   * @param iip the INodesInPath instance containing all the INodes for the path
   * @param logRetryCache whether to record RPC ids in editlog for retry cache
   *          rebuilding
   * @return blocks collected from the deleted path
   * @throws IOException
   */
  static BlocksMapUpdateInfo deleteInternal(
      FSNamesystem fsn, String src, INodesInPath iip, boolean logRetryCache)
      throws IOException {
    assert fsn.hasWriteLock();
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.delete: " + src);
    }

    if (FSDirectory.isExactReservedName(src)) {
      throw new InvalidPathException(src);
    }

    FSDirectory fsd = fsn.getFSDirectory();
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    List<INode> removedINodes = new ChunkedArrayList<>();
    List<Long> removedUCFiles = new ChunkedArrayList<>();

    long mtime = now();
    // Unlink the target directory from directory tree
    long filesRemoved = delete(
        fsd, iip, collectedBlocks, removedINodes, removedUCFiles, mtime);
    if (filesRemoved < 0) {
      return null;
    }
    fsd.getEditLog().logDelete(src, mtime, logRetryCache);
    incrDeletedFileCount(filesRemoved);

    fsn.removeLeasesAndINodes(removedUCFiles, removedINodes, true);

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
   * @param fsd the FSDirectory instance
   * @param iip the inodes resolved from the path
   * @param reclaimContext used to collect blocks and inodes to be removed
   * @param mtime the time the inode is removed
   * @return true if there are inodes deleted
   */
  private static boolean unprotectedDelete(FSDirectory fsd, INodesInPath iip,
      ReclaimContext reclaimContext, long mtime) {
    assert fsd.hasWriteLock();

    // check if target node exists
    INode targetNode = iip.getLastINode();
    if (targetNode == null) {
      return false;
    }

    // record modification
    final int latestSnapshot = iip.getLatestSnapshotId();
    targetNode.recordModification(latestSnapshot);

    // Remove the node from the namespace
    long removed = fsd.removeLastINode(iip);
    if (removed == -1) {
      return false;
    }

    // set the parent's modification time
    final INodeDirectory parent = targetNode.getParent();
    parent.updateModificationTime(mtime, latestSnapshot);

    // collect block and update quota
    if (!targetNode.isInLatestSnapshot(latestSnapshot)) {
      targetNode.destroyAndCollectBlocks(reclaimContext);
    } else {
      targetNode.cleanSubtree(reclaimContext, CURRENT_STATE_ID, latestSnapshot);
    }

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
          + iip.getPath() + " is removed");
    }
    return true;
  }

  /**
   * Throw if the given directory has any non-empty protected descendants
   * (including itself).
   *
   * @param src directory whose descendants are to be checked. The caller
   *            must ensure src is not terminated with {@link Path#SEPARATOR}.
   * @throws AccessControlException if a non-empty protected descendant
   *                                was found.
   */
  private static void checkProtectedDescendants(FSDirectory fsd, String src)
      throws AccessControlException, UnresolvedLinkException {
    final SortedSet<String> protectedDirs = fsd.getProtectedDirectories();

    // Is src protected? Caller has already checked it is non-empty.
    if (protectedDirs.contains(src)) {
      throw new AccessControlException(
          "Cannot delete non-empty protected directory " + src);
    }

    // Are any descendants of src protected?
    // The subSet call returns only the descendants of src since
    // {@link Path#SEPARATOR} is "/" and '0' is the next ASCII
    // character after '/'.
    for (String descendant :
            protectedDirs.subSet(src + Path.SEPARATOR, src + "0")) {
      if (fsd.isNonEmptyDirectory(fsd.getINodesInPath4Write(
              descendant, false))) {
        throw new AccessControlException(
            "Cannot delete non-empty protected subdirectory " + descendant);
      }
    }
  }
}

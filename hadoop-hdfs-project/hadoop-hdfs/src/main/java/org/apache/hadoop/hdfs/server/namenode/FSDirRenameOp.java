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

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ChunkedArrayList;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.util.Time.now;

class FSDirRenameOp {
  static RenameOldResult renameToInt(
      FSDirectory fsd, final String srcArg, final String dstArg,
      boolean logRetryCache)
      throws IOException {
    String src = srcArg;
    String dst = dstArg;
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: " + src +
          " to " + dst);
    }
    if (!DFSUtil.isValidName(dst)) {
      throw new IOException("Invalid name: " + dst);
    }
    FSPermissionChecker pc = fsd.getPermissionChecker();

    byte[][] srcComponents = FSDirectory.getPathComponentsForReservedPath(src);
    byte[][] dstComponents = FSDirectory.getPathComponentsForReservedPath(dst);
    HdfsFileStatus resultingStat = null;
    src = fsd.resolvePath(pc, src, srcComponents);
    dst = fsd.resolvePath(pc, dst, dstComponents);
    @SuppressWarnings("deprecation")
    final boolean status = renameToInternal(fsd, pc, src, dst, logRetryCache);
    if (status) {
      resultingStat = fsd.getAuditFileInfo(dst, false);
    }
    return new RenameOldResult(status, resultingStat);
  }

  /**
   * Change a path name
   *
   * @param fsd FSDirectory
   * @param src source path
   * @param dst destination path
   * @return true if rename succeeds; false otherwise
   * @deprecated See {@link #renameToInt(FSDirectory, String, String,
   * boolean, Options.Rename...)}
   */
  @Deprecated
  static boolean unprotectedRenameTo(
      FSDirectory fsd, String src, String dst, long timestamp)
      throws IOException {
    assert fsd.hasWriteLock();
    INodesInPath srcIIP = fsd.getINodesInPath4Write(src, false);
    final INode srcInode = srcIIP.getLastINode();
    try {
      validateRenameSource(src, srcIIP);
    } catch (SnapshotException e) {
      throw e;
    } catch (IOException ignored) {
      return false;
    }

    if (fsd.isDir(dst)) {
      dst += Path.SEPARATOR + new Path(src).getName();
    }

    // validate the destination
    if (dst.equals(src)) {
      return true;
    }

    try {
      validateDestination(src, dst, srcInode);
    } catch (IOException ignored) {
      return false;
    }

    INodesInPath dstIIP = fsd.getINodesInPath4Write(dst, false);
    if (dstIIP.getLastINode() != null) {
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
          "failed to rename " + src + " to " + dst + " because destination " +
          "exists");
      return false;
    }
    INode dstParent = dstIIP.getINode(-2);
    if (dstParent == null) {
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
          "failed to rename " + src + " to " + dst + " because destination's " +
          "parent does not exist");
      return false;
    }

    fsd.ezManager.checkMoveValidity(srcIIP, dstIIP, src);
    // Ensure dst has quota to accommodate rename
    fsd.verifyFsLimitsForRename(srcIIP, dstIIP);
    fsd.verifyQuotaForRename(srcIIP.getINodes(), dstIIP.getINodes());

    RenameOperation tx = new RenameOperation(fsd, src, dst, srcIIP, dstIIP);

    boolean added = false;

    try {
      // remove src
      final long removedSrc = fsd.removeLastINode(srcIIP);
      if (removedSrc == -1) {
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
            + "failed to rename " + src + " to " + dst + " because the source" +
            " can not be removed");
        return false;
      }

      added = tx.addSourceToDestination();
      if (added) {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("DIR* FSDirectory" +
              ".unprotectedRenameTo: " + src + " is renamed to " + dst);
        }

        tx.updateMtimeAndLease(timestamp);
        tx.updateQuotasInSourceTree();

        return true;
      }
    } finally {
      if (!added) {
        tx.restoreSource();
      }
    }
    NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
        "failed to rename " + src + " to " + dst);
    return false;
  }

  /**
   * The new rename which has the POSIX semantic.
   */
  static Map.Entry<BlocksMapUpdateInfo, HdfsFileStatus> renameToInt(
      FSDirectory fsd, final String srcArg, final String dstArg,
      boolean logRetryCache, Options.Rename... options)
      throws IOException {
    String src = srcArg;
    String dst = dstArg;
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: with options -" +
          " " + src + " to " + dst);
    }
    if (!DFSUtil.isValidName(dst)) {
      throw new InvalidPathException("Invalid name: " + dst);
    }
    final FSPermissionChecker pc = fsd.getPermissionChecker();

    byte[][] srcComponents = FSDirectory.getPathComponentsForReservedPath(src);
    byte[][] dstComponents = FSDirectory.getPathComponentsForReservedPath(dst);
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    src = fsd.resolvePath(pc, src, srcComponents);
    dst = fsd.resolvePath(pc, dst, dstComponents);
    renameToInternal(fsd, pc, src, dst, logRetryCache, collectedBlocks,
        options);
    HdfsFileStatus resultingStat = fsd.getAuditFileInfo(dst, false);

    return new AbstractMap.SimpleImmutableEntry<BlocksMapUpdateInfo,
        HdfsFileStatus>(collectedBlocks, resultingStat);
  }

  /**
   * @see #unprotectedRenameTo(FSDirectory, String, String, long,
   * org.apache.hadoop.fs.Options.Rename...)
   */
  static void renameTo(
      FSDirectory fsd, String src, String dst, long mtime,
      BlocksMapUpdateInfo collectedBlocks, Options.Rename... options)
      throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.renameTo: " + src + " to "
          + dst);
    }
    fsd.writeLock();
    try {
      if (unprotectedRenameTo(fsd, src, dst, mtime, collectedBlocks, options)) {
        fsd.getFSNamesystem().incrDeletedFileCount(1);
      }
    } finally {
      fsd.writeUnlock();
    }
  }

  /**
   * Rename src to dst.
   * <br>
   * Note: This is to be used by {@link org.apache.hadoop.hdfs.server
   * .namenode.FSEditLog} only.
   * <br>
   *
   * @param fsd       FSDirectory
   * @param src       source path
   * @param dst       destination path
   * @param timestamp modification time
   * @param options   Rename options
   */
  static boolean unprotectedRenameTo(
      FSDirectory fsd, String src, String dst, long timestamp,
      Options.Rename... options)
      throws IOException {
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    boolean ret = unprotectedRenameTo(fsd, src, dst, timestamp,
        collectedBlocks, options);
    if (!collectedBlocks.getToDeleteList().isEmpty()) {
      fsd.getFSNamesystem().removeBlocksAndUpdateSafemodeTotal(collectedBlocks);
    }
    return ret;
  }

  /**
   * Rename src to dst.
   * See {@link DistributedFileSystem#rename(Path, Path, Options.Rename...)}
   * for details related to rename semantics and exceptions.
   *
   * @param fsd             FSDirectory
   * @param src             source path
   * @param dst             destination path
   * @param timestamp       modification time
   * @param collectedBlocks blocks to be removed
   * @param options         Rename options
   */
  static boolean unprotectedRenameTo(
      FSDirectory fsd, String src, String dst, long timestamp,
      BlocksMapUpdateInfo collectedBlocks, Options.Rename... options)
      throws IOException {
    assert fsd.hasWriteLock();
    boolean overwrite = options != null
        && Arrays.asList(options).contains(Options.Rename.OVERWRITE);

    final String error;
    final INodesInPath srcIIP = fsd.getINodesInPath4Write(src, false);
    final INode srcInode = srcIIP.getLastINode();
    validateRenameSource(src, srcIIP);

    // validate the destination
    if (dst.equals(src)) {
      throw new FileAlreadyExistsException("The source " + src +
          " and destination " + dst + " are the same");
    }
    validateDestination(src, dst, srcInode);

    INodesInPath dstIIP = fsd.getINodesInPath4Write(dst, false);
    if (dstIIP.getINodes().length == 1) {
      error = "rename destination cannot be the root";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
          error);
      throw new IOException(error);
    }

    fsd.ezManager.checkMoveValidity(srcIIP, dstIIP, src);
    final INode dstInode = dstIIP.getLastINode();
    List<INodeDirectory> snapshottableDirs = new ArrayList<INodeDirectory>();
    if (dstInode != null) { // Destination exists
      validateOverwrite(src, dst, overwrite, srcInode, dstInode);
      FSDirSnapshotOp.checkSnapshot(dstInode, snapshottableDirs);
    }

    INode dstParent = dstIIP.getINode(-2);
    if (dstParent == null) {
      error = "rename destination parent " + dst + " not found.";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
          error);
      throw new FileNotFoundException(error);
    }
    if (!dstParent.isDirectory()) {
      error = "rename destination parent " + dst + " is a file.";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
          error);
      throw new ParentNotDirectoryException(error);
    }

    // Ensure dst has quota to accommodate rename
    fsd.verifyFsLimitsForRename(srcIIP, dstIIP);
    fsd.verifyQuotaForRename(srcIIP.getINodes(), dstIIP.getINodes());

    RenameOperation tx = new RenameOperation(fsd, src, dst, srcIIP, dstIIP);

    boolean undoRemoveSrc = true;
    final long removedSrc = fsd.removeLastINode(srcIIP);
    if (removedSrc == -1) {
      error = "Failed to rename " + src + " to " + dst +
          " because the source can not be removed";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
          error);
      throw new IOException(error);
    }

    boolean undoRemoveDst = false;
    INode removedDst = null;
    long removedNum = 0;
    try {
      if (dstInode != null) { // dst exists remove it
        if ((removedNum = fsd.removeLastINode(dstIIP)) != -1) {
          removedDst = dstIIP.getLastINode();
          undoRemoveDst = true;
        }
      }

      // add src as dst to complete rename
      if (tx.addSourceToDestination()) {
        undoRemoveSrc = false;
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedRenameTo: "
              + src + " is renamed to " + dst);
        }

        tx.updateMtimeAndLease(timestamp);

        // Collect the blocks and remove the lease for previous dst
        boolean filesDeleted = false;
        if (removedDst != null) {
          undoRemoveDst = false;
          if (removedNum > 0) {
            List<INode> removedINodes = new ChunkedArrayList<INode>();
            if (!removedDst.isInLatestSnapshot(dstIIP.getLatestSnapshotId())) {
              removedDst.destroyAndCollectBlocks(collectedBlocks,
                  removedINodes);
              filesDeleted = true;
            } else {
              filesDeleted = removedDst.cleanSubtree(
                  Snapshot.CURRENT_STATE_ID, dstIIP.getLatestSnapshotId(),
                  collectedBlocks, removedINodes, true)
                  .get(Quota.NAMESPACE) >= 0;
            }
            fsd.getFSNamesystem().removePathAndBlocks(src, null,
                removedINodes, false);
          }
        }

        if (snapshottableDirs.size() > 0) {
          // There are snapshottable directories (without snapshots) to be
          // deleted. Need to update the SnapshotManager.
          fsd.getFSNamesystem().removeSnapshottableDirs(snapshottableDirs);
        }

        tx.updateQuotasInSourceTree();
        return filesDeleted;
      }
    } finally {
      if (undoRemoveSrc) {
        tx.restoreSource();
      }

      if (undoRemoveDst) {
        // Rename failed - restore dst
        if (dstParent.isDirectory() &&
            dstParent.asDirectory().isWithSnapshot()) {
          dstParent.asDirectory().undoRename4DstParent(removedDst,
              dstIIP.getLatestSnapshotId());
        } else {
          fsd.addLastINodeNoQuotaCheck(dstIIP, removedDst);
        }
        assert removedDst != null;
        if (removedDst.isReference()) {
          final INodeReference removedDstRef = removedDst.asReference();
          final INodeReference.WithCount wc = (INodeReference.WithCount)
              removedDstRef.getReferredINode().asReference();
          wc.addReference(removedDstRef);
        }
      }
    }
    NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
        "failed to rename " + src + " to " + dst);
    throw new IOException("rename from " + src + " to " + dst + " failed.");
  }

  /**
   * @see #unprotectedRenameTo(FSDirectory, String, String, long)
   * @deprecated Use {@link #renameToInt(FSDirectory, String, String,
   * boolean, Options.Rename...)}
   */
  @Deprecated
  @SuppressWarnings("deprecation")
  private static boolean renameTo(
      FSDirectory fsd, String src, String dst, long mtime)
      throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.renameTo: " + src + " to "
          + dst);
    }
    boolean stat = false;
    fsd.writeLock();
    try {
      stat = unprotectedRenameTo(fsd, src, dst, mtime);
    } finally {
      fsd.writeUnlock();
    }
    return stat;
  }

  /**
   * @deprecated See {@link #renameTo(FSDirectory, String, String, long)}
   */
  @Deprecated
  private static boolean renameToInternal(
      FSDirectory fsd, FSPermissionChecker pc, String src, String dst,
      boolean logRetryCache)
      throws IOException {
    if (fsd.isPermissionEnabled()) {
      //We should not be doing this.  This is move() not renameTo().
      //but for now,
      //NOTE: yes, this is bad!  it's assuming much lower level behavior
      //      of rewriting the dst
      String actualdst = fsd.isDir(dst) ? dst + Path.SEPARATOR + new Path
          (src).getName() : dst;
      // Rename does not operates on link targets
      // Do not resolveLink when checking permissions of src and dst
      // Check write access to parent of src
      fsd.checkPermission(pc, src, false, null, FsAction.WRITE, null, null,
          false, false);
      // Check write access to ancestor of dst
      fsd.checkPermission(pc, actualdst, false, FsAction.WRITE, null, null,
          null, false, false);
    }

    long mtime = now();
    @SuppressWarnings("deprecation")
    final boolean stat = renameTo(fsd, src, dst, mtime);
    if (stat) {
      fsd.getEditLog().logRename(src, dst, mtime, logRetryCache);
      return true;
    }
    return false;
  }

  private static void renameToInternal(
      FSDirectory fsd, FSPermissionChecker pc, String src, String dst,
      boolean logRetryCache, BlocksMapUpdateInfo collectedBlocks,
      Options.Rename... options)
      throws IOException {
    if (fsd.isPermissionEnabled()) {
      // Rename does not operates on link targets
      // Do not resolveLink when checking permissions of src and dst
      // Check write access to parent of src
      fsd.checkPermission(pc, src, false, null, FsAction.WRITE, null, null,
          false, false);
      // Check write access to ancestor of dst
      fsd.checkPermission(pc, dst, false, FsAction.WRITE, null, null, null,
          false, false);
    }

    long mtime = now();
    renameTo(fsd, src, dst, mtime, collectedBlocks, options);
    fsd.getEditLog().logRename(src, dst, mtime, logRetryCache, options);
  }

  private static void validateDestination(
      String src, String dst, INode srcInode)
      throws IOException {
    String error;
    if (srcInode.isSymlink() &&
        dst.equals(srcInode.asSymlink().getSymlinkString())) {
      throw new FileAlreadyExistsException("Cannot rename symlink " + src
          + " to its target " + dst);
    }
    // dst cannot be a directory or a file under src
    if (dst.startsWith(src)
        && dst.charAt(src.length()) == Path.SEPARATOR_CHAR) {
      error = "Rename destination " + dst
          + " is a directory or file under source " + src;
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new IOException(error);
    }
  }

  private static void validateOverwrite(
      String src, String dst, boolean overwrite, INode srcInode, INode dstInode)
      throws IOException {
    String error;// It's OK to rename a file to a symlink and vice versa
    if (dstInode.isDirectory() != srcInode.isDirectory()) {
      error = "Source " + src + " and destination " + dst
          + " must both be directories";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new IOException(error);
    }
    if (!overwrite) { // If destination exists, overwrite flag must be true
      error = "rename destination " + dst + " already exists";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new FileAlreadyExistsException(error);
    }
    if (dstInode.isDirectory()) {
      final ReadOnlyList<INode> children = dstInode.asDirectory()
          .getChildrenList(Snapshot.CURRENT_STATE_ID);
      if (!children.isEmpty()) {
        error = "rename destination directory is not empty: " + dst;
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
            + error);
        throw new IOException(error);
      }
    }
  }

  private static void validateRenameSource(String src, INodesInPath srcIIP)
      throws IOException {
    String error;
    final INode srcInode = srcIIP.getLastINode();
    // validate source
    if (srcInode == null) {
      error = "rename source " + src + " is not found.";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new FileNotFoundException(error);
    }
    if (srcIIP.getINodes().length == 1) {
      error = "rename source cannot be the root";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new IOException(error);
    }
    // srcInode and its subtree cannot contain snapshottable directories with
    // snapshots
    FSDirSnapshotOp.checkSnapshot(srcInode, null);
  }

  private static class RenameOperation {
    private final FSDirectory fsd;
    private final INodesInPath srcIIP;
    private final INodesInPath dstIIP;
    private final String src;
    private final String dst;
    private final INodeReference.WithCount withCount;
    private final int srcRefDstSnapshot;
    private final INodeDirectory srcParent;
    private final byte[] srcChildName;
    private final boolean isSrcInSnapshot;
    private final boolean srcChildIsReference;
    private final Quota.Counts oldSrcCounts;
    private INode srcChild;

    RenameOperation(FSDirectory fsd, String src, String dst,
                    INodesInPath srcIIP, INodesInPath dstIIP)
        throws QuotaExceededException {
      this.fsd = fsd;
      this.srcIIP = srcIIP;
      this.dstIIP = dstIIP;
      this.src = src;
      this.dst = dst;
      srcChild = srcIIP.getLastINode();
      srcChildName = srcChild.getLocalNameBytes();
      isSrcInSnapshot = srcChild.isInLatestSnapshot(srcIIP
          .getLatestSnapshotId());
      srcChildIsReference = srcChild.isReference();
      srcParent = srcIIP.getINode(-2).asDirectory();

      // Record the snapshot on srcChild. After the rename, before any new
      // snapshot is taken on the dst tree, changes will be recorded in the
      // latest snapshot of the src tree.
      if (isSrcInSnapshot) {
        srcChild.recordModification(srcIIP.getLatestSnapshotId());
      }

      // check srcChild for reference
      srcRefDstSnapshot = srcChildIsReference ?
          srcChild.asReference().getDstSnapshotId() : Snapshot.CURRENT_STATE_ID;
      oldSrcCounts = Quota.Counts.newInstance();
      if (isSrcInSnapshot) {
        final INodeReference.WithName withName =
            srcIIP.getINode(-2).asDirectory().replaceChild4ReferenceWithName(
                srcChild, srcIIP.getLatestSnapshotId());
        withCount = (INodeReference.WithCount) withName.getReferredINode();
        srcChild = withName;
        srcIIP.setLastINode(srcChild);
        // get the counts before rename
        withCount.getReferredINode().computeQuotaUsage(oldSrcCounts, true);
      } else if (srcChildIsReference) {
        // srcChild is reference but srcChild is not in latest snapshot
        withCount = (INodeReference.WithCount) srcChild.asReference()
            .getReferredINode();
      } else {
        withCount = null;
      }
    }

    boolean addSourceToDestination() {
      final INode dstParent = dstIIP.getINode(-2);
      srcChild = srcIIP.getLastINode();
      final byte[] dstChildName = dstIIP.getLastLocalName();
      final INode toDst;
      if (withCount == null) {
        srcChild.setLocalName(dstChildName);
        toDst = srcChild;
      } else {
        withCount.getReferredINode().setLocalName(dstChildName);
        int dstSnapshotId = dstIIP.getLatestSnapshotId();
        toDst = new INodeReference.DstReference(dstParent.asDirectory(),
            withCount, dstSnapshotId);
      }
      return fsd.addLastINodeNoQuotaCheck(dstIIP, toDst);
    }

    void updateMtimeAndLease(long timestamp) throws QuotaExceededException {
      srcParent.updateModificationTime(timestamp, srcIIP.getLatestSnapshotId());
      final INode dstParent = dstIIP.getINode(-2);
      dstParent.updateModificationTime(timestamp, dstIIP.getLatestSnapshotId());
      // update moved lease with new filename
      fsd.getFSNamesystem().unprotectedChangeLease(src, dst);
    }

    void restoreSource() throws QuotaExceededException {
      // Rename failed - restore src
      final INode oldSrcChild = srcChild;
      // put it back
      if (withCount == null) {
        srcChild.setLocalName(srcChildName);
      } else if (!srcChildIsReference) { // src must be in snapshot
        // the withCount node will no longer be used thus no need to update
        // its reference number here
        srcChild = withCount.getReferredINode();
        srcChild.setLocalName(srcChildName);
      } else {
        withCount.removeReference(oldSrcChild.asReference());
        srcChild = new INodeReference.DstReference(srcParent, withCount,
            srcRefDstSnapshot);
        withCount.getReferredINode().setLocalName(srcChildName);
      }

      if (isSrcInSnapshot) {
        srcParent.undoRename4ScrParent(oldSrcChild.asReference(), srcChild);
      } else {
        // srcParent is not an INodeDirectoryWithSnapshot, we only need to add
        // the srcChild back
        fsd.addLastINodeNoQuotaCheck(srcIIP, srcChild);
      }
    }

    void updateQuotasInSourceTree() throws QuotaExceededException {
      // update the quota usage in src tree
      if (isSrcInSnapshot) {
        // get the counts after rename
        Quota.Counts newSrcCounts = srcChild.computeQuotaUsage(
            Quota.Counts.newInstance(), false);
        newSrcCounts.subtract(oldSrcCounts);
        srcParent.addSpaceConsumed(newSrcCounts.get(Quota.NAMESPACE),
            newSrcCounts.get(Quota.DISKSPACE), false);
      }
    }
  }

  static class RenameOldResult {
    final boolean success;
    final HdfsFileStatus auditStat;

    RenameOldResult(boolean success, HdfsFileStatus auditStat) {
      this.success = success;
      this.auditStat = auditStat;
    }
  }
}

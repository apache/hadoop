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

import com.google.common.base.Preconditions;
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
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.util.ChunkedArrayList;
import org.apache.hadoop.util.Time;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdfs.protocol.FSLimitException.MaxDirectoryItemsExceededException;
import static org.apache.hadoop.hdfs.protocol.FSLimitException.PathComponentTooLongException;

class FSDirRenameOp {
  @Deprecated
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
    final boolean status = renameTo(fsd, pc, src, dst, logRetryCache);
    if (status) {
      INodesInPath dstIIP = fsd.getINodesInPath(dst, false);
      resultingStat = fsd.getAuditFileInfo(dstIIP);
    }
    return new RenameOldResult(status, resultingStat);
  }

  /**
   * Verify quota for rename operation where srcInodes[srcInodes.length-1] moves
   * dstInodes[dstInodes.length-1]
   */
  private static void verifyQuotaForRename(FSDirectory fsd, INodesInPath src,
      INodesInPath dst) throws QuotaExceededException {
    if (!fsd.getFSNamesystem().isImageLoaded() || fsd.shouldSkipQuotaChecks()) {
      // Do not check quota if edits log is still being processed
      return;
    }
    int i = 0;
    while(src.getINode(i) == dst.getINode(i)) { i++; }
    // src[i - 1] is the last common ancestor.
    BlockStoragePolicySuite bsps = fsd.getBlockStoragePolicySuite();
    final QuotaCounts delta = src.getLastINode().computeQuotaUsage(bsps);

    // Reduce the required quota by dst that is being removed
    final INode dstINode = dst.getLastINode();
    if (dstINode != null) {
      delta.subtract(dstINode.computeQuotaUsage(bsps));
    }
    FSDirectory.verifyQuota(dst, dst.length() - 1, delta, src.getINode(i - 1));
  }

  /**
   * Checks file system limits (max component length and max directory items)
   * during a rename operation.
   */
  static void verifyFsLimitsForRename(FSDirectory fsd, INodesInPath srcIIP,
      INodesInPath dstIIP)
      throws PathComponentTooLongException, MaxDirectoryItemsExceededException {
    byte[] dstChildName = dstIIP.getLastLocalName();
    final String parentPath = dstIIP.getParentPath();
    fsd.verifyMaxComponentLength(dstChildName, parentPath);
    // Do not enforce max directory items if renaming within same directory.
    if (srcIIP.getINode(-2) != dstIIP.getINode(-2)) {
      fsd.verifyMaxDirItems(dstIIP.getINode(-2).asDirectory(), parentPath);
    }
  }

  /**
   * <br>
   * Note: This is to be used by {@link FSEditLogLoader} only.
   * <br>
   */
  @Deprecated
  @SuppressWarnings("deprecation")
  static boolean renameForEditLog(FSDirectory fsd, String src, String dst,
      long timestamp) throws IOException {
    if (fsd.isDir(dst)) {
      dst += Path.SEPARATOR + new Path(src).getName();
    }
    final INodesInPath srcIIP = fsd.getINodesInPath4Write(src, false);
    final INodesInPath dstIIP = fsd.getINodesInPath4Write(dst, false);
    return unprotectedRenameTo(fsd, src, dst, srcIIP, dstIIP, timestamp);
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
  static boolean unprotectedRenameTo(FSDirectory fsd, String src, String dst,
      final INodesInPath srcIIP, final INodesInPath dstIIP, long timestamp)
      throws IOException {
    assert fsd.hasWriteLock();
    final INode srcInode = srcIIP.getLastINode();
    try {
      validateRenameSource(srcIIP);
    } catch (SnapshotException e) {
      throw e;
    } catch (IOException ignored) {
      return false;
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
    verifyFsLimitsForRename(fsd, srcIIP, dstIIP);
    verifyQuotaForRename(fsd, srcIIP, dstIIP);

    RenameOperation tx = new RenameOperation(fsd, src, dst, srcIIP, dstIIP);

    boolean added = false;

    try {
      // remove src
      if (!tx.removeSrc4OldRename()) {
        return false;
      }

      added = tx.addSourceToDestination();
      if (added) {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("DIR* FSDirectory" +
              ".unprotectedRenameTo: " + src + " is renamed to " + dst);
        }

        tx.updateMtimeAndLease(timestamp);
        tx.updateQuotasInSourceTree(fsd.getBlockStoragePolicySuite());

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
    renameTo(fsd, pc, src, dst, collectedBlocks, logRetryCache, options);
    INodesInPath dstIIP = fsd.getINodesInPath(dst, false);
    HdfsFileStatus resultingStat = fsd.getAuditFileInfo(dstIIP);

    return new AbstractMap.SimpleImmutableEntry<>(
        collectedBlocks, resultingStat);
  }

  /**
   * @see {@link #unprotectedRenameTo(FSDirectory, String, String, INodesInPath,
   * INodesInPath, long, BlocksMapUpdateInfo, Options.Rename...)}
   */
  static void renameTo(FSDirectory fsd, FSPermissionChecker pc, String src,
      String dst, BlocksMapUpdateInfo collectedBlocks, boolean logRetryCache,
      Options.Rename... options) throws IOException {
    final INodesInPath srcIIP = fsd.getINodesInPath4Write(src, false);
    final INodesInPath dstIIP = fsd.getINodesInPath4Write(dst, false);
    if (fsd.isPermissionEnabled()) {
      // Rename does not operate on link targets
      // Do not resolveLink when checking permissions of src and dst
      // Check write access to parent of src
      fsd.checkPermission(pc, srcIIP, false, null, FsAction.WRITE, null, null,
          false);
      // Check write access to ancestor of dst
      fsd.checkPermission(pc, dstIIP, false, FsAction.WRITE, null, null, null,
          false);
    }

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.renameTo: " + src + " to "
          + dst);
    }
    final long mtime = Time.now();
    fsd.writeLock();
    try {
      if (unprotectedRenameTo(fsd, src, dst, srcIIP, dstIIP, mtime,
          collectedBlocks, options)) {
        FSDirDeleteOp.incrDeletedFileCount(1);
      }
    } finally {
      fsd.writeUnlock();
    }
    fsd.getEditLog().logRename(src, dst, mtime, logRetryCache, options);
  }

  /**
   * Rename src to dst.
   * <br>
   * Note: This is to be used by {@link org.apache.hadoop.hdfs.server
   * .namenode.FSEditLogLoader} only.
   * <br>
   *
   * @param fsd       FSDirectory
   * @param src       source path
   * @param dst       destination path
   * @param timestamp modification time
   * @param options   Rename options
   */
  static boolean renameForEditLog(
      FSDirectory fsd, String src, String dst, long timestamp,
      Options.Rename... options)
      throws IOException {
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    final INodesInPath srcIIP = fsd.getINodesInPath4Write(src, false);
    final INodesInPath dstIIP = fsd.getINodesInPath4Write(dst, false);
    boolean ret = unprotectedRenameTo(fsd, src, dst, srcIIP, dstIIP, timestamp,
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
   * @return whether a file/directory gets overwritten in the dst path
   */
  static boolean unprotectedRenameTo(FSDirectory fsd, String src, String dst,
      final INodesInPath srcIIP, final INodesInPath dstIIP, long timestamp,
      BlocksMapUpdateInfo collectedBlocks, Options.Rename... options)
      throws IOException {
    assert fsd.hasWriteLock();
    boolean overwrite = options != null
        && Arrays.asList(options).contains(Options.Rename.OVERWRITE);

    final String error;
    final INode srcInode = srcIIP.getLastINode();
    validateRenameSource(srcIIP);

    // validate the destination
    if (dst.equals(src)) {
      throw new FileAlreadyExistsException("The source " + src +
          " and destination " + dst + " are the same");
    }
    validateDestination(src, dst, srcInode);

    if (dstIIP.length() == 1) {
      error = "rename destination cannot be the root";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
          error);
      throw new IOException(error);
    }

    BlockStoragePolicySuite bsps = fsd.getBlockStoragePolicySuite();
    fsd.ezManager.checkMoveValidity(srcIIP, dstIIP, src);
    final INode dstInode = dstIIP.getLastINode();
    List<INodeDirectory> snapshottableDirs = new ArrayList<>();
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
    verifyFsLimitsForRename(fsd, srcIIP, dstIIP);
    verifyQuotaForRename(fsd, srcIIP, dstIIP);

    RenameOperation tx = new RenameOperation(fsd, src, dst, srcIIP, dstIIP);

    boolean undoRemoveSrc = true;
    tx.removeSrc();

    boolean undoRemoveDst = false;
    long removedNum = 0;
    try {
      if (dstInode != null) { // dst exists, remove it
        removedNum = tx.removeDst();
        if (removedNum != -1) {
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
        if (undoRemoveDst) {
          undoRemoveDst = false;
          if (removedNum > 0) {
            filesDeleted = tx.cleanDst(bsps, collectedBlocks);
          }
        }

        if (snapshottableDirs.size() > 0) {
          // There are snapshottable directories (without snapshots) to be
          // deleted. Need to update the SnapshotManager.
          fsd.getFSNamesystem().removeSnapshottableDirs(snapshottableDirs);
        }

        tx.updateQuotasInSourceTree(bsps);
        return filesDeleted;
      }
    } finally {
      if (undoRemoveSrc) {
        tx.restoreSource();
      }
      if (undoRemoveDst) { // Rename failed - restore dst
        tx.restoreDst(bsps);
      }
    }
    NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
        "failed to rename " + src + " to " + dst);
    throw new IOException("rename from " + src + " to " + dst + " failed.");
  }

  /**
   * @deprecated Use {@link #renameToInt(FSDirectory, String, String,
   * boolean, Options.Rename...)}
   */
  @Deprecated
  @SuppressWarnings("deprecation")
  private static boolean renameTo(FSDirectory fsd, FSPermissionChecker pc,
      String src, String dst, boolean logRetryCache) throws IOException {
    // Rename does not operate on link targets
    // Do not resolveLink when checking permissions of src and dst
    // Check write access to parent of src
    final INodesInPath srcIIP = fsd.getINodesInPath4Write(src, false);
    // Note: We should not be doing this.  This is move() not renameTo().
    final String actualDst = fsd.isDir(dst) ?
        dst + Path.SEPARATOR + new Path(src).getName() : dst;
    final INodesInPath dstIIP = fsd.getINodesInPath4Write(actualDst, false);
    if (fsd.isPermissionEnabled()) {
      fsd.checkPermission(pc, srcIIP, false, null, FsAction.WRITE, null, null,
          false);
      // Check write access to ancestor of dst
      fsd.checkPermission(pc, dstIIP, false, FsAction.WRITE, null, null,
          null, false);
    }

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.renameTo: " + src + " to "
          + dst);
    }
    final long mtime = Time.now();
    boolean stat = false;
    fsd.writeLock();
    try {
      stat = unprotectedRenameTo(fsd, src, actualDst, srcIIP, dstIIP, mtime);
    } finally {
      fsd.writeUnlock();
    }
    if (stat) {
      fsd.getEditLog().logRename(src, actualDst, mtime, logRetryCache);
      return true;
    }
    return false;
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

  private static void validateRenameSource(INodesInPath srcIIP)
      throws IOException {
    String error;
    final INode srcInode = srcIIP.getLastINode();
    // validate source
    if (srcInode == null) {
      error = "rename source " + srcIIP.getPath() + " is not found.";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
          + error);
      throw new FileNotFoundException(error);
    }
    if (srcIIP.length() == 1) {
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
    private INodesInPath srcIIP;
    private final INodesInPath srcParentIIP;
    private INodesInPath dstIIP;
    private final INodesInPath dstParentIIP;
    private final String src;
    private final String dst;
    private final INodeReference.WithCount withCount;
    private final int srcRefDstSnapshot;
    private final INodeDirectory srcParent;
    private final byte[] srcChildName;
    private final boolean isSrcInSnapshot;
    private final boolean srcChildIsReference;
    private final QuotaCounts oldSrcCounts;
    private INode srcChild;
    private INode oldDstChild;

    RenameOperation(FSDirectory fsd, String src, String dst,
                    INodesInPath srcIIP, INodesInPath dstIIP)
        throws QuotaExceededException {
      this.fsd = fsd;
      this.src = src;
      this.dst = dst;
      this.srcIIP = srcIIP;
      this.dstIIP = dstIIP;
      this.srcParentIIP = srcIIP.getParentINodesInPath();
      this.dstParentIIP = dstIIP.getParentINodesInPath();

      BlockStoragePolicySuite bsps = fsd.getBlockStoragePolicySuite();
      srcChild = this.srcIIP.getLastINode();
      srcChildName = srcChild.getLocalNameBytes();
      final int srcLatestSnapshotId = srcIIP.getLatestSnapshotId();
      isSrcInSnapshot = srcChild.isInLatestSnapshot(srcLatestSnapshotId);
      srcChildIsReference = srcChild.isReference();
      srcParent = this.srcIIP.getINode(-2).asDirectory();

      // Record the snapshot on srcChild. After the rename, before any new
      // snapshot is taken on the dst tree, changes will be recorded in the
      // latest snapshot of the src tree.
      if (isSrcInSnapshot) {
        srcChild.recordModification(srcLatestSnapshotId);
      }

      // check srcChild for reference
      srcRefDstSnapshot = srcChildIsReference ?
          srcChild.asReference().getDstSnapshotId() : Snapshot.CURRENT_STATE_ID;
      oldSrcCounts = new QuotaCounts.Builder().build();
      if (isSrcInSnapshot) {
        final INodeReference.WithName withName = srcParent
            .replaceChild4ReferenceWithName(srcChild, srcLatestSnapshotId);
        withCount = (INodeReference.WithCount) withName.getReferredINode();
        srcChild = withName;
        this.srcIIP = INodesInPath.replace(srcIIP, srcIIP.length() - 1,
            srcChild);
        // get the counts before rename
        withCount.getReferredINode().computeQuotaUsage(bsps, oldSrcCounts, true);
      } else if (srcChildIsReference) {
        // srcChild is reference but srcChild is not in latest snapshot
        withCount = (INodeReference.WithCount) srcChild.asReference()
            .getReferredINode();
      } else {
        withCount = null;
      }
    }

    long removeSrc() throws IOException {
      long removedNum = fsd.removeLastINode(srcIIP);
      if (removedNum == -1) {
        String error = "Failed to rename " + src + " to " + dst +
            " because the source can not be removed";
        NameNode.stateChangeLog.warn("DIR* FSDirRenameOp.unprotectedRenameTo:" +
            error);
        throw new IOException(error);
      } else {
        // update the quota count if necessary
        fsd.updateCountForDelete(srcChild, srcIIP);
        srcIIP = INodesInPath.replace(srcIIP, srcIIP.length() - 1, null);
        return removedNum;
      }
    }

    boolean removeSrc4OldRename() {
      final long removedSrc = fsd.removeLastINode(srcIIP);
      if (removedSrc == -1) {
        NameNode.stateChangeLog.warn("DIR* FSDirRenameOp.unprotectedRenameTo: "
            + "failed to rename " + src + " to " + dst + " because the source" +
            " can not be removed");
        return false;
      } else {
        // update the quota count if necessary
        fsd.updateCountForDelete(srcChild, srcIIP);
        srcIIP = INodesInPath.replace(srcIIP, srcIIP.length() - 1, null);
        return true;
      }
    }

    long removeDst() {
      long removedNum = fsd.removeLastINode(dstIIP);
      if (removedNum != -1) {
        oldDstChild = dstIIP.getLastINode();
        // update the quota count if necessary
        fsd.updateCountForDelete(oldDstChild, dstIIP);
        dstIIP = INodesInPath.replace(dstIIP, dstIIP.length() - 1, null);
      }
      return removedNum;
    }

    boolean addSourceToDestination() {
      final INode dstParent = dstParentIIP.getLastINode();
      final byte[] dstChildName = dstIIP.getLastLocalName();
      final INode toDst;
      if (withCount == null) {
        srcChild.setLocalName(dstChildName);
        toDst = srcChild;
      } else {
        withCount.getReferredINode().setLocalName(dstChildName);
        toDst = new INodeReference.DstReference(dstParent.asDirectory(),
            withCount, dstIIP.getLatestSnapshotId());
      }
      return fsd.addLastINodeNoQuotaCheck(dstParentIIP, toDst) != null;
    }

    void updateMtimeAndLease(long timestamp) throws QuotaExceededException {
      srcParent.updateModificationTime(timestamp, srcIIP.getLatestSnapshotId());
      final INode dstParent = dstParentIIP.getLastINode();
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
        fsd.addLastINodeNoQuotaCheck(srcParentIIP, srcChild);
      }
    }

    void restoreDst(BlockStoragePolicySuite bsps) throws QuotaExceededException {
      Preconditions.checkState(oldDstChild != null);
      final INodeDirectory dstParent = dstParentIIP.getLastINode().asDirectory();
      if (dstParent.isWithSnapshot()) {
        dstParent.undoRename4DstParent(bsps, oldDstChild, dstIIP.getLatestSnapshotId());
      } else {
        fsd.addLastINodeNoQuotaCheck(dstParentIIP, oldDstChild);
      }
      if (oldDstChild != null && oldDstChild.isReference()) {
        final INodeReference removedDstRef = oldDstChild.asReference();
        final INodeReference.WithCount wc = (INodeReference.WithCount)
            removedDstRef.getReferredINode().asReference();
        wc.addReference(removedDstRef);
      }
    }

    boolean cleanDst(BlockStoragePolicySuite bsps, BlocksMapUpdateInfo collectedBlocks)
        throws QuotaExceededException {
      Preconditions.checkState(oldDstChild != null);
      List<INode> removedINodes = new ChunkedArrayList<>();
      final boolean filesDeleted;
      if (!oldDstChild.isInLatestSnapshot(dstIIP.getLatestSnapshotId())) {
        oldDstChild.destroyAndCollectBlocks(bsps, collectedBlocks, removedINodes);
        filesDeleted = true;
      } else {
        filesDeleted = oldDstChild.cleanSubtree(bsps, Snapshot.CURRENT_STATE_ID,
            dstIIP.getLatestSnapshotId(), collectedBlocks, removedINodes)
            .getNameSpace() >= 0;
      }
      fsd.getFSNamesystem().removeLeasesAndINodes(src, removedINodes, false);
      return filesDeleted;
    }

    void updateQuotasInSourceTree(BlockStoragePolicySuite bsps) throws QuotaExceededException {
      // update the quota usage in src tree
      if (isSrcInSnapshot) {
        // get the counts after rename
        QuotaCounts newSrcCounts = srcChild.computeQuotaUsage(bsps,
            new QuotaCounts.Builder().build(), false);
        newSrcCounts.subtract(oldSrcCounts);
        srcParent.addSpaceConsumed(newSrcCounts, false);
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

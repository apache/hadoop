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

import com.google.protobuf.ByteString;
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdfs.protocol.FSLimitException.MaxDirectoryItemsExceededException;
import static org.apache.hadoop.hdfs.protocol.FSLimitException.PathComponentTooLongException;
import static org.apache.hadoop.util.Time.now;

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
    HdfsFileStatus resultingStat = null;
    try (RWTransaction tx = fsd.newRWTransaction().begin()) {
      // Rename does not operate on link targets
      // Do not resolveLink when checking permissions of src and dst
      // Check write access to parent of src
      Resolver.Result srcPaths = Resolver.resolveNoSymlink(tx, src);
      Resolver.Result dstPaths = Resolver.resolve(tx, dst);
      @SuppressWarnings("deprecation")
      final boolean status = renameTo(tx,
          fsd, pc, srcPaths, dstPaths, logRetryCache);
      if (status) {
        dstPaths = Resolver.resolve(tx, dst);
        resultingStat = fsd.getAuditFileInfo(dstPaths.inodesInPath());
      }
      tx.commit();
      return new RenameOldResult(status, resultingStat);
    }
  }

  /**
   * Verify quota for rename operation where srcInodes[srcInodes.length-1] moves
   * dstInodes[dstInodes.length-1]
   */
  private static void verifyQuotaForRename(
      FSDirectory fsd, FlatINodesInPath src,
      FlatINodesInPath dst) throws QuotaExceededException {
    // TODO
//    if (!fsd.getFSNamesystem().isImageLoaded() || fsd.shouldSkipQuotaChecks()) {
//      // Do not check quota if edits log is still being processed
//      return;
//    }
//    int i = 0;
//    while(src.getINode(i) == dst.getINode(i)) { i++; }
//    // src[i - 1] is the last common ancestor.
//    BlockStoragePolicySuite bsps = fsd.getBlockStoragePolicySuite();
//    final QuotaCounts delta = src.getLastINode().computeQuotaUsage(bsps);
//
//    // Reduce the required quota by dst that is being removed
//    final INode dstINode = dst.getLastINode();
//    if (dstINode != null) {
//      delta.subtract(dstINode.computeQuotaUsage(bsps));
//    }
//    FSDirectory.verifyQuota(dst, dst.length() - 1, delta, src.getINode(i - 1));
  }

  /**
   * Checks file system limits (max component length and max directory items)
   * during a rename operation.
   */
  static void verifyFsLimitsForRename(
      FSDirectory fsd, FlatINodesInPath src,
      FlatINodesInPath dst)
      throws PathComponentTooLongException, MaxDirectoryItemsExceededException {
    // TODO
//    byte[] dstChildName = dstIIP.getLastLocalName();
//    final String parentPath = dstIIP.getParentPath();
//    fsd.verifyMaxComponentLength(dstChildName, parentPath);
//    // Do not enforce max directory items if renaming within same directory.
//    if (srcIIP.getINode(-2) != dstIIP.getINode(-2)) {
//      fsd.verifyMaxDirItems(dstIIP.getINode(-2).asDirectory(), parentPath);
//    }
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
    try (ReplayTransaction tx = fsd.newReplayTransaction().begin()) {
      Resolver.Result srcPaths = Resolver.resolveNoSymlink(tx, src);
      Resolver.Result dstPaths = Resolver.resolveNoSymlink(tx, dst);
      return unprotectedRenameTo(tx, fsd, srcPaths, dstPaths, timestamp);
    }
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
  static boolean unprotectedRenameTo(RWTransaction ntx,
      FSDirectory fsd, Resolver.Result src, Resolver.Result dst,
      long timestamp)
      throws IOException {
    assert fsd.hasWriteLock();
    try {
      validateRenameSource(src);
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
      validateDestination(src, dst);
    } catch (IOException ignored) {
      return false;
    }

    if (dst.ok()) {
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
          "failed to rename " + src + " to " + dst + " because destination " +
          "exists");
      return false;
    }
    if (FlatNSUtil.hasNextLevelInPath(dst.src, dst.offset)) {
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
          "failed to rename " + src.src + " to " + dst.src + " because " +
          "destination's parent does not exist");
      return false;
    }

    // TODO: Handle encrytpion
    // fsd.ezManager.checkMoveValidity(src, dst, src);
    // Ensure dst has quota to accommodate rename
    verifyFsLimitsForRename(fsd, src.inodesInPath(), dst.inodesInPath());
    verifyQuotaForRename(fsd, src.inodesInPath(), dst.inodesInPath());

    RenameOperation tx = new RenameOperation(ntx, src, dst);

    boolean added;

    // remove src
    if (!tx.removeSrc4OldRename(timestamp)) {
      return false;
    }

    added = tx.addSourceToDestination(timestamp);
    if (added) {
      return true;
    }
    NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
        "failed to rename " + src.src + " to " + dst.src);
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

    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    try (RWTransaction tx = fsd.newRWTransaction().begin()) {
      Resolver.Result srcPaths = Resolver.resolve(tx, src);
      Resolver.Result dstPaths = Resolver.resolve(tx, dst);
      renameTo(tx, fsd, pc, srcPaths, dstPaths, collectedBlocks, logRetryCache,
               options);
      dstPaths = Resolver.resolve(tx, dst);
      HdfsFileStatus resultingStat =
          fsd.getAuditFileInfo(dstPaths.inodesInPath());
      tx.commit();
      return new AbstractMap.SimpleImmutableEntry<>(
          collectedBlocks, resultingStat);
    }
  }

  static void renameTo(RWTransaction tx, FSDirectory fsd, FSPermissionChecker
      pc, Resolver.Result src, Resolver.Result dst,
      BlocksMapUpdateInfo collectedBlocks, boolean logRetryCache,
      Options.Rename... options) throws IOException {
    validateRenameSource(src);
    if (fsd.isPermissionEnabled()) {
      // Rename does not operate on link targets
      // Do not resolveLink when checking permissions of src and dst
      // Check write access to parent of src
      fsd.checkPermission(pc, src.inodesInPath(), false, null, FsAction.WRITE, null,
                          null, false);
      // TODO: Check write access to ancestor of dst
      fsd.checkPermission(pc, dst.inodesInPath(), false, FsAction.WRITE, null,
                          null, null, false);
    }

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
          "DIR* FSDirectory.renameTo: " + src + " to " + dst);
    }
    final long mtime = now();

    if (unprotectedRenameTo(tx, fsd, src, dst, mtime, collectedBlocks,
                            options)) {
      FSDirDeleteOp.incrDeletedFileCount(1);
    }
    tx.logRename(src.src, dst.src, mtime, logRetryCache, options);
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
  static void renameForEditLog(
      FSDirectory fsd, String src, String dst, long timestamp,
      Options.Rename... options)
      throws IOException {
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    boolean ret;
    try (ReplayTransaction tx = fsd.newReplayTransaction().begin()) {
      Resolver.Result srcPaths = Resolver.resolveNoSymlink(tx, src);
      Resolver.Result dstPaths = Resolver.resolveNoSymlink(tx, dst);
      ret = unprotectedRenameTo(tx, fsd, srcPaths, dstPaths,
                                timestamp, collectedBlocks, options);
      if (!collectedBlocks.getToDeleteList().isEmpty()) {
        fsd.getFSNamesystem().removeBlocksAndUpdateSafemodeTotal(collectedBlocks);
      }
    }
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
  static boolean unprotectedRenameTo(
      RWTransaction ntx, FSDirectory fsd, final Resolver.Result src,
      final Resolver.Result dst, long timestamp,
      BlocksMapUpdateInfo collectedBlocks, Options.Rename... options)
      throws IOException {
    boolean overwrite = options != null
        && Arrays.asList(options).contains(Options.Rename.OVERWRITE);

    // validate the destination
    if (dst.equals(src)) {
      throw new FileAlreadyExistsException("The source " + src +
          " and destination " + dst + " are the same");
    }

    validateDestination(src, dst);

    String error;
    if (dst.ok() && dst.inodesInPath().length() == 1) {
      error = "rename destination cannot be the root";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
          error);
      throw new IOException(error);
    }

    // TODO: Handle encrytion zone
    // fsd.ezManager.checkMoveValidity(srcIIP, dstIIP, src);

    if (dst.ok()) { // Destination exists
      // TODO: Validate overwrite
      // validateOverwrite(ntx, src, dst, overwrite, srcInode, dstInode);
      // TODO: Check snapshot
      // FSDirSnapshotOp.checkSnapshot(dstInode, snapshottableDirs);
    }

    if (FlatNSUtil.hasNextLevelInPath(dst.src, dst.offset)) {
      error = "rename destination parent " + dst + " not found.";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
          error);
      throw new FileNotFoundException(error);
    }

    FlatINode dstParent = dst.getLastINode(-2);
    if (!dstParent.isDirectory()) {
      error = "rename destination parent " + dst + " is a file.";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
          error);
      throw new ParentNotDirectoryException(error);
    }

    // TODO: Ensure dst has quota to accommodate rename
    // verifyFsLimitsForRename(fsd, srcIIP, dstIIP);
    // verifyQuotaForRename(fsd, srcIIP, dstIIP);

    RenameOperation tx = new RenameOperation(ntx, src, dst);

    tx.removeSrc(timestamp);

    List<Long> removedINodes = new ArrayList<>();
    List<Long> removedUCFiles = new ArrayList<>();
    if (dst.ok()) { // dst exists, remove it
      tx.removeDst(collectedBlocks, removedINodes, removedUCFiles);
    }

    // add src as dst to complete rename
    if (tx.addSourceToDestination(timestamp)) {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedRenameTo: "
                                          + src + " is renamed to " + dst);
      }

      // Collect the blocks and remove the lease for previous dst
      boolean filesDeleted = false;
      // TODO: Handle snapshots

      FSNamesystem fsn = fsd.getFSNamesystem();
      fsn.removeLeases(removedUCFiles);
      return filesDeleted;
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
  private static boolean renameTo(
      RWTransaction tx, FSDirectory fsd,
      FSPermissionChecker pc, Resolver.Result src, Resolver.Result dst,
      boolean logRetryCache)
      throws IOException {

    Resolver.Result dstArg = dst;
    // Note: We should not be doing this.  This is move() not renameTo().
    if (dst.ok() && dst.inodesInPath().getLastINode().isDirectory()) {
      String actualDst = dst.src + Path.SEPARATOR + new Path(src.src).getName();
      dst = Resolver.resolve(tx, actualDst);
    }

    if (fsd.isPermissionEnabled()) {
      fsd.checkPermission(pc, src.inodesInPath(),
                          false, null, FsAction.WRITE, null, null, false);
      // Check write access to ancestor of dst
      fsd.checkPermission(pc, dst.inodesInPath(), false, FsAction.WRITE,
                          null, null, null, false);
    }

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.renameTo: " + src + " to "
          + dst);
    }
    final long mtime = now();
    boolean stat = unprotectedRenameTo(tx, fsd, src, dst, mtime);
    if (stat) {
      fsd.getEditLog().logRename(src.src, dstArg.src, mtime, logRetryCache);
      return true;
    }
    return false;
  }

  private static void validateDestination(Resolver.Result src, Resolver
      .Result dst) throws IOException {
    String error;
    // TODO: Handle symlink
//    if (srcInode.isSymlink() &&
//        dst.equals(srcInode.asSymlink().getSymlinkString())) {
//      throw new FileAlreadyExistsException("Cannot rename symlink " + src
//                                               + " to its target " + dst);
//    }

    // dst cannot be a directory or a file under src
    if (dst.inodesInPath().length() > src.inodesInPath().length()) {
      List<Map.Entry<ByteString, FlatINode>> srcINodes = src.inodesInPath()
          .inodes();
      List<Map.Entry<ByteString, FlatINode>> dstINodes = dst.inodesInPath()
          .inodes();
      for (int i = 0, e = srcINodes.size(); i < e; ++i) {
        if (srcINodes.get(i).getValue().id() != dstINodes.get(i).getValue()
            .id()) {
          return;
        }
      }
      error = "Rename destination " + dst
          + " is a directory or file under source " + src;
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                                       + error);
      throw new IOException(error);
    }
  }

  private static void validateOverwrite(RWTransaction tx,
      String src, String dst, boolean overwrite,
      FlatINode srcInode, FlatINode dstInode)
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
      boolean hasChildren = !tx.childrenView(dstInode.id()).isEmpty();
      if (hasChildren) {
        error = "rename destination directory is not empty: " + dst;
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                                         + error);
        throw new IOException(error);
      }
    }
  }

  private static void validateRenameSource(Resolver.Result paths)
      throws IOException {
    String error;
    if (paths.invalidPath()) {
      throw new InvalidPathException(paths.src);
    } else if (paths.notFound()) {
      error = "rename source " + paths.src + " is not found.";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                                       + error);
      throw new FileNotFoundException(error);
    } else if (paths.inodesInPath().length() == 1) {
      error = "rename source cannot be the root";
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                                       + error);
      throw new IOException(error);
    }
    // TODO: srcInode and its subtree cannot contain snapshottable
    // directories with snapshots
  }

  private static class RenameOperation {
    private final RWTransaction tx;
    private final Resolver.Result src;
    private final Resolver.Result dst;
    private final ByteString srclocalName;

    RenameOperation(
        RWTransaction tx, Resolver.Result src,
        Resolver.Result dst) {
      this.tx = tx;
      this.src = src;
      this.dst = dst;
      srclocalName = src.inodesInPath().inodes().get(
          src.inodesInPath().length() - 1).getKey();
    }

    long removeSrc(long mtime) throws IOException {
      FlatINode parent = src.inodesInPath().getLastINode(-2);
      ByteString newParent = new FlatINode.Builder()
          .mergeFrom(parent).mtime(mtime).build();
      tx.deleteChild(parent.id(), srclocalName.asReadOnlyByteBuffer());
      tx.putINode(parent.id(), newParent);
      return 1;
      // TODO: Handle quota
    }

    boolean removeSrc4OldRename(long mtime) {
      try {
        removeSrc(mtime);
        return true;
      } catch (IOException ignored) {
        return false;
      }
    }

    long removeDst(BlocksMapUpdateInfo removedBlocks,
        List<Long> removedINodes, List<Long> removedUCFiles) {
      try {
        long deleted = FSDirDeleteOp.delete(
            tx, dst, removedBlocks, removedUCFiles, now());
        return deleted;
      } catch (IOException ignored) {
      }
      return -1;
    }

    boolean addSourceToDestination(long mtime) {
      FlatINode parent = dst.ok()
          ? dst.inodesInPath().getLastINode(-2)
          : dst.inodesInPath().getLastINode();
      ByteString localName = ByteString.copyFromUtf8(
          FlatNSUtil.getNextComponent(dst.src, dst.offset));
      ByteString newParent = new FlatINode.Builder()
          .mergeFrom(parent).mtime(mtime).build();
      tx.putINode(parent.id(), newParent);
      tx.putChild(parent.id(), localName.asReadOnlyByteBuffer(),
                  src.inodesInPath().getLastINode().id());
      return true;
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

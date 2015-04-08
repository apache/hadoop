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
import org.apache.commons.io.Charsets;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.DirectoryListingStartAfterNotFoundException;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.FsPermissionExtension;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectorySnapshottableFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

class FSDirStatAndListingOp {
  static DirectoryListing getListingInt(FSDirectory fsd, final String srcArg,
      byte[] startAfter, boolean needLocation) throws IOException {
    FSPermissionChecker pc = fsd.getPermissionChecker();
    byte[][] pathComponents = FSDirectory
        .getPathComponentsForReservedPath(srcArg);
    final String startAfterString = new String(startAfter, Charsets.UTF_8);
    final String src = fsd.resolvePath(pc, srcArg, pathComponents);
    final INodesInPath iip = fsd.getINodesInPath(src, true);

    // Get file name when startAfter is an INodePath
    if (FSDirectory.isReservedName(startAfterString)) {
      byte[][] startAfterComponents = FSDirectory
          .getPathComponentsForReservedPath(startAfterString);
      try {
        String tmp = FSDirectory.resolvePath(src, startAfterComponents, fsd);
        byte[][] regularPath = INode.getPathComponents(tmp);
        startAfter = regularPath[regularPath.length - 1];
      } catch (IOException e) {
        // Possibly the inode is deleted
        throw new DirectoryListingStartAfterNotFoundException(
            "Can't find startAfter " + startAfterString);
      }
    }

    boolean isSuperUser = true;
    if (fsd.isPermissionEnabled()) {
      if (iip.getLastINode() != null && iip.getLastINode().isDirectory()) {
        fsd.checkPathAccess(pc, iip, FsAction.READ_EXECUTE);
      } else {
        fsd.checkTraverse(pc, iip);
      }
      isSuperUser = pc.isSuperUser();
    }
    return getListing(fsd, iip, src, startAfter, needLocation, isSuperUser);
  }

  /**
   * Get the file info for a specific file.
   *
   * @param srcArg The string representation of the path to the file
   * @param resolveLink whether to throw UnresolvedLinkException
   *        if src refers to a symlink
   *
   * @return object containing information regarding the file
   *         or null if file not found
   */
  static HdfsFileStatus getFileInfo(
      FSDirectory fsd, String srcArg, boolean resolveLink)
      throws IOException {
    String src = srcArg;
    if (!DFSUtil.isValidName(src)) {
      throw new InvalidPathException("Invalid file name: " + src);
    }
    FSPermissionChecker pc = fsd.getPermissionChecker();
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    src = fsd.resolvePath(pc, src, pathComponents);
    final INodesInPath iip = fsd.getINodesInPath(src, resolveLink);
    boolean isSuperUser = true;
    if (fsd.isPermissionEnabled()) {
      fsd.checkPermission(pc, iip, false, null, null, null, null, false);
      isSuperUser = pc.isSuperUser();
    }
    return getFileInfo(fsd, src, resolveLink,
        FSDirectory.isReservedRawName(srcArg), isSuperUser);
  }

  /**
   * Returns true if the file is closed
   */
  static boolean isFileClosed(FSDirectory fsd, String src) throws IOException {
    FSPermissionChecker pc = fsd.getPermissionChecker();
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    src = fsd.resolvePath(pc, src, pathComponents);
    final INodesInPath iip = fsd.getINodesInPath(src, true);
    if (fsd.isPermissionEnabled()) {
      fsd.checkTraverse(pc, iip);
    }
    return !INodeFile.valueOf(iip.getLastINode(), src).isUnderConstruction();
  }

  static ContentSummary getContentSummary(
      FSDirectory fsd, String src) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    FSPermissionChecker pc = fsd.getPermissionChecker();
    src = fsd.resolvePath(pc, src, pathComponents);
    final INodesInPath iip = fsd.getINodesInPath(src, false);
    if (fsd.isPermissionEnabled()) {
      fsd.checkPermission(pc, iip, false, null, null, null,
          FsAction.READ_EXECUTE);
    }
    return getContentSummaryInt(fsd, iip);
  }

  private static byte getStoragePolicyID(byte inodePolicy, byte parentPolicy) {
    return inodePolicy != BlockStoragePolicySuite.ID_UNSPECIFIED ? inodePolicy :
        parentPolicy;
  }

  /**
   * Get a partial listing of the indicated directory
   *
   * We will stop when any of the following conditions is met:
   * 1) this.lsLimit files have been added
   * 2) needLocation is true AND enough files have been added such
   * that at least this.lsLimit block locations are in the response
   *
   * @param fsd FSDirectory
   * @param iip the INodesInPath instance containing all the INodes along the
   *            path
   * @param src the directory name
   * @param startAfter the name to start listing after
   * @param needLocation if block locations are returned
   * @return a partial listing starting after startAfter
   */
  private static DirectoryListing getListing(FSDirectory fsd, INodesInPath iip,
      String src, byte[] startAfter, boolean needLocation, boolean isSuperUser)
      throws IOException {
    String srcs = FSDirectory.normalizePath(src);
    final boolean isRawPath = FSDirectory.isReservedRawName(src);

    fsd.readLock();
    try {
      if (srcs.endsWith(HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR)) {
        return getSnapshotsListing(fsd, srcs, startAfter);
      }
      final int snapshot = iip.getPathSnapshotId();
      final INode targetNode = iip.getLastINode();
      if (targetNode == null)
        return null;
      byte parentStoragePolicy = isSuperUser ?
          targetNode.getStoragePolicyID() : BlockStoragePolicySuite
          .ID_UNSPECIFIED;

      if (!targetNode.isDirectory()) {
        return new DirectoryListing(
            new HdfsFileStatus[]{createFileStatus(fsd, src,
                HdfsFileStatus.EMPTY_NAME, targetNode, needLocation,
                parentStoragePolicy, snapshot, isRawPath, iip)}, 0);
      }

      final INodeDirectory dirInode = targetNode.asDirectory();
      final ReadOnlyList<INode> contents = dirInode.getChildrenList(snapshot);
      int startChild = INodeDirectory.nextChild(contents, startAfter);
      int totalNumChildren = contents.size();
      int numOfListing = Math.min(totalNumChildren - startChild,
          fsd.getLsLimit());
      int locationBudget = fsd.getLsLimit();
      int listingCnt = 0;
      HdfsFileStatus listing[] = new HdfsFileStatus[numOfListing];
      for (int i=0; i<numOfListing && locationBudget>0; i++) {
        INode cur = contents.get(startChild+i);
        byte curPolicy = isSuperUser && !cur.isSymlink()?
            cur.getLocalStoragePolicyID():
            BlockStoragePolicySuite.ID_UNSPECIFIED;
        listing[i] = createFileStatus(fsd, src, cur.getLocalNameBytes(), cur,
            needLocation, getStoragePolicyID(curPolicy,
                parentStoragePolicy), snapshot, isRawPath, iip);
        listingCnt++;
        if (needLocation) {
            // Once we  hit lsLimit locations, stop.
            // This helps to prevent excessively large response payloads.
            // Approximate #locations with locatedBlockCount() * repl_factor
            LocatedBlocks blks =
                ((HdfsLocatedFileStatus)listing[i]).getBlockLocations();
            locationBudget -= (blks == null) ? 0 :
               blks.locatedBlockCount() * listing[i].getReplication();
        }
      }
      // truncate return array if necessary
      if (listingCnt < numOfListing) {
          listing = Arrays.copyOf(listing, listingCnt);
      }
      return new DirectoryListing(
          listing, totalNumChildren-startChild-listingCnt);
    } finally {
      fsd.readUnlock();
    }
  }

  /**
   * Get a listing of all the snapshots of a snapshottable directory
   */
  private static DirectoryListing getSnapshotsListing(
      FSDirectory fsd, String src, byte[] startAfter)
      throws IOException {
    Preconditions.checkState(fsd.hasReadLock());
    Preconditions.checkArgument(
        src.endsWith(HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR),
        "%s does not end with %s", src, HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR);

    final String dirPath = FSDirectory.normalizePath(src.substring(0,
        src.length() - HdfsConstants.DOT_SNAPSHOT_DIR.length()));

    final INode node = fsd.getINode(dirPath);
    final INodeDirectory dirNode = INodeDirectory.valueOf(node, dirPath);
    final DirectorySnapshottableFeature sf = dirNode.getDirectorySnapshottableFeature();
    if (sf == null) {
      throw new SnapshotException(
          "Directory is not a snapshottable directory: " + dirPath);
    }
    final ReadOnlyList<Snapshot> snapshots = sf.getSnapshotList();
    int skipSize = ReadOnlyList.Util.binarySearch(snapshots, startAfter);
    skipSize = skipSize < 0 ? -skipSize - 1 : skipSize + 1;
    int numOfListing = Math.min(snapshots.size() - skipSize, fsd.getLsLimit());
    final HdfsFileStatus listing[] = new HdfsFileStatus[numOfListing];
    for (int i = 0; i < numOfListing; i++) {
      Snapshot.Root sRoot = snapshots.get(i + skipSize).getRoot();
      listing[i] = createFileStatus(fsd, src, sRoot.getLocalNameBytes(), sRoot,
          BlockStoragePolicySuite.ID_UNSPECIFIED, Snapshot.CURRENT_STATE_ID,
          false, INodesInPath.fromINode(sRoot));
    }
    return new DirectoryListing(
        listing, snapshots.size() - skipSize - numOfListing);
  }

  /** Get the file info for a specific file.
   * @param fsd FSDirectory
   * @param src The string representation of the path to the file
   * @param isRawPath true if a /.reserved/raw pathname was passed by the user
   * @param includeStoragePolicy whether to include storage policy
   * @return object containing information regarding the file
   *         or null if file not found
   */
  static HdfsFileStatus getFileInfo(
      FSDirectory fsd, String path, INodesInPath src, boolean isRawPath,
      boolean includeStoragePolicy)
      throws IOException {
    fsd.readLock();
    try {
      final INode i = src.getLastINode();
      byte policyId = includeStoragePolicy && i != null && !i.isSymlink() ?
          i.getStoragePolicyID() : BlockStoragePolicySuite.ID_UNSPECIFIED;
      return i == null ? null : createFileStatus(
          fsd, path, HdfsFileStatus.EMPTY_NAME, i, policyId,
          src.getPathSnapshotId(), isRawPath, src);
    } finally {
      fsd.readUnlock();
    }
  }

  static HdfsFileStatus getFileInfo(
      FSDirectory fsd, String src, boolean resolveLink, boolean isRawPath,
      boolean includeStoragePolicy)
    throws IOException {
    String srcs = FSDirectory.normalizePath(src);
    if (srcs.endsWith(HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR)) {
      if (fsd.getINode4DotSnapshot(srcs) != null) {
        return new HdfsFileStatus(0, true, 0, 0, 0, 0, null, null, null, null,
            HdfsFileStatus.EMPTY_NAME, -1L, 0, null,
            BlockStoragePolicySuite.ID_UNSPECIFIED);
      }
      return null;
    }

    fsd.readLock();
    try {
      final INodesInPath iip = fsd.getINodesInPath(srcs, resolveLink);
      return getFileInfo(fsd, src, iip, isRawPath, includeStoragePolicy);
    } finally {
      fsd.readUnlock();
    }
  }

  /**
   * Currently we only support "ls /xxx/.snapshot" which will return all the
   * snapshots of a directory. The FSCommand Ls will first call getFileInfo to
   * make sure the file/directory exists (before the real getListing call).
   * Since we do not have a real INode for ".snapshot", we return an empty
   * non-null HdfsFileStatus here.
   */
  private static HdfsFileStatus getFileInfo4DotSnapshot(
      FSDirectory fsd, String src)
      throws UnresolvedLinkException {
    if (fsd.getINode4DotSnapshot(src) != null) {
      return new HdfsFileStatus(0, true, 0, 0, 0, 0, null, null, null, null,
          HdfsFileStatus.EMPTY_NAME, -1L, 0, null,
          BlockStoragePolicySuite.ID_UNSPECIFIED);
    }
    return null;
  }

  /**
   * create an hdfs file status from an inode
   *
   * @param fsd FSDirectory
   * @param path the local name
   * @param node inode
   * @param needLocation if block locations need to be included or not
   * @param isRawPath true if this is being called on behalf of a path in
   *                  /.reserved/raw
   * @return a file status
   * @throws java.io.IOException if any error occurs
   */
  static HdfsFileStatus createFileStatus(
      FSDirectory fsd, String fullPath, byte[] path, INode node,
      boolean needLocation, byte storagePolicy, int snapshot, boolean isRawPath,
      INodesInPath iip)
      throws IOException {
    if (needLocation) {
      return createLocatedFileStatus(fsd, fullPath, path, node, storagePolicy,
          snapshot, isRawPath, iip);
    } else {
      return createFileStatus(fsd, fullPath, path, node, storagePolicy, snapshot,
          isRawPath, iip);
    }
  }

  /**
   * Create FileStatus by file INode
   */
  static HdfsFileStatus createFileStatus(
      FSDirectory fsd, String fullPath, byte[] path, INode node,
      byte storagePolicy, int snapshot, boolean isRawPath,
      INodesInPath iip) throws IOException {
     long size = 0;     // length is zero for directories
     short replication = 0;
     long blocksize = 0;
     final boolean isEncrypted;

     final FileEncryptionInfo feInfo = isRawPath ? null :
         fsd.getFileEncryptionInfo(node, snapshot, iip);

     if (node.isFile()) {
       final INodeFile fileNode = node.asFile();
       size = fileNode.computeFileSize(snapshot);
       replication = fileNode.getFileReplication(snapshot);
       blocksize = fileNode.getPreferredBlockSize();
       isEncrypted = (feInfo != null) ||
           (isRawPath && fsd.isInAnEZ(INodesInPath.fromINode(node)));
     } else {
       isEncrypted = fsd.isInAnEZ(INodesInPath.fromINode(node));
     }

     int childrenNum = node.isDirectory() ?
         node.asDirectory().getChildrenNum(snapshot) : 0;

     INodeAttributes nodeAttrs =
         fsd.getAttributes(fullPath, path, node, snapshot);
     return new HdfsFileStatus(
        size,
        node.isDirectory(),
        replication,
        blocksize,
        node.getModificationTime(snapshot),
        node.getAccessTime(snapshot),
        getPermissionForFileStatus(nodeAttrs, isEncrypted),
        nodeAttrs.getUserName(),
        nodeAttrs.getGroupName(),
        node.isSymlink() ? node.asSymlink().getSymlink() : null,
        path,
        node.getId(),
        childrenNum,
        feInfo,
        storagePolicy);
  }

  /**
   * Create FileStatus with location info by file INode
   */
  private static HdfsLocatedFileStatus createLocatedFileStatus(
      FSDirectory fsd, String fullPath, byte[] path, INode node,
      byte storagePolicy, int snapshot, boolean isRawPath,
      INodesInPath iip) throws IOException {
    assert fsd.hasReadLock();
    long size = 0; // length is zero for directories
    short replication = 0;
    long blocksize = 0;
    LocatedBlocks loc = null;
    final boolean isEncrypted;
    final FileEncryptionInfo feInfo = isRawPath ? null :
        fsd.getFileEncryptionInfo(node, snapshot, iip);
    if (node.isFile()) {
      final INodeFile fileNode = node.asFile();
      size = fileNode.computeFileSize(snapshot);
      replication = fileNode.getFileReplication(snapshot);
      blocksize = fileNode.getPreferredBlockSize();

      final boolean inSnapshot = snapshot != Snapshot.CURRENT_STATE_ID;
      final boolean isUc = !inSnapshot && fileNode.isUnderConstruction();
      final long fileSize = !inSnapshot && isUc ?
          fileNode.computeFileSizeNotIncludingLastUcBlock() : size;

      loc = fsd.getFSNamesystem().getBlockManager().createLocatedBlocks(
          fileNode.getBlocks(snapshot), fileSize, isUc, 0L, size, false,
          inSnapshot, feInfo);
      if (loc == null) {
        loc = new LocatedBlocks();
      }
      isEncrypted = (feInfo != null) ||
          (isRawPath && fsd.isInAnEZ(INodesInPath.fromINode(node)));
    } else {
      isEncrypted = fsd.isInAnEZ(INodesInPath.fromINode(node));
    }
    int childrenNum = node.isDirectory() ?
        node.asDirectory().getChildrenNum(snapshot) : 0;

    INodeAttributes nodeAttrs =
        fsd.getAttributes(fullPath, path, node, snapshot);
    HdfsLocatedFileStatus status =
        new HdfsLocatedFileStatus(size, node.isDirectory(), replication,
          blocksize, node.getModificationTime(snapshot),
          node.getAccessTime(snapshot),
          getPermissionForFileStatus(nodeAttrs, isEncrypted),
          nodeAttrs.getUserName(), nodeAttrs.getGroupName(),
          node.isSymlink() ? node.asSymlink().getSymlink() : null, path,
          node.getId(), loc, childrenNum, feInfo, storagePolicy);
    // Set caching information for the located blocks.
    if (loc != null) {
      CacheManager cacheManager = fsd.getFSNamesystem().getCacheManager();
      for (LocatedBlock lb: loc.getLocatedBlocks()) {
        cacheManager.setCachedLocations(lb);
      }
    }
    return status;
  }

  /**
   * Returns an inode's FsPermission for use in an outbound FileStatus.  If the
   * inode has an ACL or is for an encrypted file/dir, then this method will
   * return an FsPermissionExtension.
   *
   * @param node INode to check
   * @param snapshot int snapshot ID
   * @param isEncrypted boolean true if the file/dir is encrypted
   * @return FsPermission from inode, with ACL bit on if the inode has an ACL
   * and encrypted bit on if it represents an encrypted file/dir.
   */
  private static FsPermission getPermissionForFileStatus(
      INodeAttributes node, boolean isEncrypted) {
    FsPermission perm = node.getFsPermission();
    boolean hasAcl = node.getAclFeature() != null;
    if (hasAcl || isEncrypted) {
      perm = new FsPermissionExtension(perm, hasAcl, isEncrypted);
    }
    return perm;
  }

  private static ContentSummary getContentSummaryInt(FSDirectory fsd,
      INodesInPath iip) throws IOException {
    fsd.readLock();
    try {
      INode targetNode = iip.getLastINode();
      if (targetNode == null) {
        throw new FileNotFoundException("File does not exist: " + iip.getPath());
      }
      else {
        // Make it relinquish locks everytime contentCountLimit entries are
        // processed. 0 means disabled. I.e. blocking for the entire duration.
        ContentSummaryComputationContext cscc =
            new ContentSummaryComputationContext(fsd, fsd.getFSNamesystem(),
                fsd.getContentCountLimit(), fsd.getContentSleepMicroSec());
        ContentSummary cs = targetNode.computeAndConvertContentSummary(cscc);
        fsd.addYieldCount(cscc.getYieldCount());
        return cs;
      }
    } finally {
      fsd.readUnlock();
    }
  }
}

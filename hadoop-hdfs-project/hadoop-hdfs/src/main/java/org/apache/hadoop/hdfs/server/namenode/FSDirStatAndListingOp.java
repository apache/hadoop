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

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.DirectoryListingStartAfterNotFoundException;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectorySnapshottableFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.security.AccessControlException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;

import static org.apache.hadoop.util.Time.now;

class FSDirStatAndListingOp {
  static DirectoryListing getListingInt(FSDirectory fsd, final String srcArg,
      byte[] startAfter, boolean needLocation) throws IOException {
    final FSPermissionChecker pc = fsd.getPermissionChecker();
    final INodesInPath iip = fsd.resolvePath(pc, srcArg, DirOp.READ);

    // Get file name when startAfter is an INodePath.  This is not the
    // common case so avoid any unnecessary processing unless required.
    if (startAfter.length > 0 && startAfter[0] == Path.SEPARATOR_CHAR) {
      final String startAfterString = DFSUtil.bytes2String(startAfter);
      if (FSDirectory.isReservedName(startAfterString)) {
        try {
          byte[][] components = INode.getPathComponents(startAfterString);
          components = FSDirectory.resolveComponents(components, fsd);
          startAfter = components[components.length - 1];
        } catch (IOException e) {
          // Possibly the inode is deleted
          throw new DirectoryListingStartAfterNotFoundException(
              "Can't find startAfter " + startAfterString);
        }
      }
    }

    boolean isSuperUser = true;
    if (fsd.isPermissionEnabled()) {
      if (iip.getLastINode() != null && iip.getLastINode().isDirectory()) {
        fsd.checkPathAccess(pc, iip, FsAction.READ_EXECUTE);
      }
      isSuperUser = pc.isSuperUser();
    }
    return getListing(fsd, iip, startAfter, needLocation, isSuperUser);
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
    DirOp dirOp = resolveLink ? DirOp.READ : DirOp.READ_LINK;
    FSPermissionChecker pc = fsd.getPermissionChecker();
    final INodesInPath iip;
    if (pc.isSuperUser()) {
      // superuser can only get an ACE if an existing ancestor is a file.
      // right or (almost certainly) wrong, current fs contracts expect
      // superuser to receive null instead.
      try {
        iip = fsd.resolvePath(pc, srcArg, dirOp);
      } catch (AccessControlException ace) {
        return null;
      }
    } else {
      iip = fsd.resolvePath(pc, srcArg, dirOp);
    }
    return getFileInfo(fsd, iip);
  }

  /**
   * Returns true if the file is closed
   */
  static boolean isFileClosed(FSDirectory fsd, String src) throws IOException {
    FSPermissionChecker pc = fsd.getPermissionChecker();
    final INodesInPath iip = fsd.resolvePath(pc, src, DirOp.READ);
    return !INodeFile.valueOf(iip.getLastINode(), src).isUnderConstruction();
  }

  static ContentSummary getContentSummary(
      FSDirectory fsd, String src) throws IOException {
    FSPermissionChecker pc = fsd.getPermissionChecker();
    final INodesInPath iip = fsd.resolvePath(pc, src, DirOp.READ_LINK);
    // getContentSummaryInt() call will check access (if enabled) when
    // traversing all sub directories.
    return getContentSummaryInt(fsd, iip);
  }

  /**
   * Get block locations within the specified range.
   * @see ClientProtocol#getBlockLocations(String, long, long)
   * @throws IOException
   */
  static GetBlockLocationsResult getBlockLocations(
      FSDirectory fsd, FSPermissionChecker pc, String src, long offset,
      long length, boolean needBlockToken) throws IOException {
    Preconditions.checkArgument(offset >= 0,
        "Negative offset is not supported. File: " + src);
    Preconditions.checkArgument(length >= 0,
        "Negative length is not supported. File: " + src);
    BlockManager bm = fsd.getBlockManager();
    fsd.readLock();
    try {
      final INodesInPath iip = fsd.resolvePath(pc, src, DirOp.READ);
      src = iip.getPath();
      final INodeFile inode = INodeFile.valueOf(iip.getLastINode(), src);
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.READ);
        fsd.checkUnreadableBySuperuser(pc, iip);
      }

      final long fileSize = iip.isSnapshot()
          ? inode.computeFileSize(iip.getPathSnapshotId())
          : inode.computeFileSizeNotIncludingLastUcBlock();

      boolean isUc = inode.isUnderConstruction();
      if (iip.isSnapshot()) {
        // if src indicates a snapshot file, we need to make sure the returned
        // blocks do not exceed the size of the snapshot file.
        length = Math.min(length, fileSize - offset);
        isUc = false;
      }

      final FileEncryptionInfo feInfo =
          FSDirEncryptionZoneOp.getFileEncryptionInfo(fsd, iip);
      final ErasureCodingPolicy ecPolicy = FSDirErasureCodingOp.
          unprotectedGetErasureCodingPolicy(fsd.getFSNamesystem(), iip);

      final LocatedBlocks blocks = bm.createLocatedBlocks(
          inode.getBlocks(iip.getPathSnapshotId()), fileSize, isUc, offset,
          length, needBlockToken, iip.isSnapshot(), feInfo, ecPolicy);

      final long now = now();
      boolean updateAccessTime = fsd.isAccessTimeSupported()
          && !iip.isSnapshot()
          && now > inode.getAccessTime() + fsd.getAccessTimePrecision();
      return new GetBlockLocationsResult(updateAccessTime, blocks);
    } finally {
      fsd.readUnlock();
    }
  }

  private static byte getStoragePolicyID(byte inodePolicy, byte parentPolicy) {
    return inodePolicy != HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED
        ? inodePolicy : parentPolicy;
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
   * @param startAfter the name to start listing after
   * @param needLocation if block locations are returned
   * @param includeStoragePolicy if storage policy is returned
   * @return a partial listing starting after startAfter
   */
  private static DirectoryListing getListing(FSDirectory fsd, INodesInPath iip,
      byte[] startAfter, boolean needLocation, boolean includeStoragePolicy)
      throws IOException {
    if (FSDirectory.isExactReservedName(iip.getPathComponents())) {
      return getReservedListing(fsd);
    }

    fsd.readLock();
    try {
      if (iip.isDotSnapshotDir()) {
        return getSnapshotsListing(fsd, iip, startAfter);
      }
      final int snapshot = iip.getPathSnapshotId();
      final INode targetNode = iip.getLastINode();
      if (targetNode == null) {
        return null;
      }

      byte parentStoragePolicy = includeStoragePolicy
          ? targetNode.getStoragePolicyID()
          : HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;

      if (!targetNode.isDirectory()) {
        // return the file's status. note that the iip already includes the
        // target INode
        return new DirectoryListing(
            new HdfsFileStatus[]{ createFileStatus(
                fsd, iip, null, parentStoragePolicy, needLocation)
            }, 0);
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
      for (int i = 0; i < numOfListing && locationBudget > 0; i++) {
        INode child = contents.get(startChild+i);
        byte childStoragePolicy = (includeStoragePolicy && !child.isSymlink())
            ? getStoragePolicyID(child.getLocalStoragePolicyID(),
                                 parentStoragePolicy)
            : parentStoragePolicy;
        listing[i] =
            createFileStatus(fsd, iip, child, childStoragePolicy, needLocation);
        listingCnt++;
        if (listing[i] instanceof HdfsLocatedFileStatus) {
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
      FSDirectory fsd, INodesInPath iip, byte[] startAfter)
      throws IOException {
    Preconditions.checkState(fsd.hasReadLock());
    Preconditions.checkArgument(iip.isDotSnapshotDir(),
        "%s does not end with %s",
        iip.getPath(), HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR);
    // drop off the null .snapshot component
    iip = iip.getParentINodesInPath();
    final String dirPath = iip.getPath();
    final INode node = iip.getLastINode();
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
      listing[i] = createFileStatus(fsd, iip, sRoot,
          HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED, false);
    }
    return new DirectoryListing(
        listing, snapshots.size() - skipSize - numOfListing);
  }

  /**
   * Get a listing of the /.reserved directory.
   * @param fsd FSDirectory
   * @return listing containing child directories of /.reserved
   */
  private static DirectoryListing getReservedListing(FSDirectory fsd) {
    return new DirectoryListing(fsd.getReservedStatuses(), 0);
  }

  /** Get the file info for a specific file.
   * @param fsd FSDirectory
   * @param iip The path to the file, the file is included
   * @param includeStoragePolicy whether to include storage policy
   * @return object containing information regarding the file
   *         or null if file not found
   */
  static HdfsFileStatus getFileInfo(FSDirectory fsd,
      INodesInPath iip, boolean includeStoragePolicy) throws IOException {
    fsd.readLock();
    try {
      final INode node = iip.getLastINode();
      if (node == null) {
        return null;
      }
      byte policy = (includeStoragePolicy && !node.isSymlink())
          ? node.getStoragePolicyID()
          : HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
      return createFileStatus(fsd, iip, null, policy, false);
    } finally {
      fsd.readUnlock();
    }
  }

  static HdfsFileStatus getFileInfo(FSDirectory fsd, INodesInPath iip)
    throws IOException {
    fsd.readLock();
    try {
      HdfsFileStatus status = null;
      if (FSDirectory.isExactReservedName(iip.getPathComponents())) {
        status = FSDirectory.DOT_RESERVED_STATUS;
      } else if (iip.isDotSnapshotDir()) {
        if (fsd.getINode4DotSnapshot(iip) != null) {
          status = FSDirectory.DOT_SNAPSHOT_DIR_STATUS;
        }
      } else {
        status = getFileInfo(fsd, iip, true);
      }
      return status;
    } finally {
      fsd.readUnlock();
    }
  }

  /**
   * create a hdfs file status from an iip.
   * @param fsd FSDirectory
   * @param iip The INodesInPath containing the INodeFile and its ancestors
   * @return HdfsFileStatus without locations or storage policy
   */
  static HdfsFileStatus createFileStatusForEditLog(
      FSDirectory fsd, INodesInPath iip) throws IOException {
    return createFileStatus(fsd, iip,
        null, HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED, false);
  }

  /**
   * create a hdfs file status from an iip.
   *
   * @param fsd FSDirectory
   * @param iip The INodesInPath containing the INodeFile and its ancestors.
   * @param child for a directory listing of the iip, else null
   * @param storagePolicy for the path or closest ancestor
   * @param needLocation if block locations need to be included or not
   * @return a file status
   * @throws java.io.IOException if any error occurs
   */
  private static HdfsFileStatus createFileStatus(
      FSDirectory fsd, INodesInPath iip, INode child, byte storagePolicy,
      boolean needLocation) throws IOException {
    assert fsd.hasReadLock();
    // only directory listing sets the status name.
    byte[] name = HdfsFileStatus.EMPTY_NAME;
    if (child != null) {
      name = child.getLocalNameBytes();
      // have to do this for EC and EZ lookups...
      iip = INodesInPath.append(iip, child, name);
    }

    long size = 0;     // length is zero for directories
    short replication = 0;
    long blocksize = 0;
    final INode node = iip.getLastINode();
    final int snapshot = iip.getPathSnapshotId();
    LocatedBlocks loc = null;

    final boolean isEncrypted = FSDirEncryptionZoneOp.isInAnEZ(fsd, iip);
    FileEncryptionInfo feInfo = null;

    final ErasureCodingPolicy ecPolicy = FSDirErasureCodingOp
        .unprotectedGetErasureCodingPolicy(fsd.getFSNamesystem(), iip);
    final boolean isErasureCoded = (ecPolicy != null);

    boolean isSnapShottable = false;

    if (node.isFile()) {
      final INodeFile fileNode = node.asFile();
      size = fileNode.computeFileSize(snapshot);
      replication = fileNode.getFileReplication(snapshot);
      blocksize = fileNode.getPreferredBlockSize();
      if (isEncrypted) {
        feInfo = FSDirEncryptionZoneOp.getFileEncryptionInfo(fsd, iip);
      }
      if (needLocation) {
        final boolean inSnapshot = snapshot != Snapshot.CURRENT_STATE_ID;
        final boolean isUc = !inSnapshot && fileNode.isUnderConstruction();
        final long fileSize = !inSnapshot && isUc
            ? fileNode.computeFileSizeNotIncludingLastUcBlock() : size;
        loc = fsd.getBlockManager().createLocatedBlocks(
            fileNode.getBlocks(snapshot), fileSize, isUc, 0L, size, false,
            inSnapshot, feInfo, ecPolicy);
        if (loc == null) {
          loc = new LocatedBlocks();
        }
      }
    } else if (node.isDirectory()) {
      isSnapShottable = node.asDirectory().isSnapshottable();
    }

    int childrenNum = node.isDirectory() ?
        node.asDirectory().getChildrenNum(snapshot) : 0;

    EnumSet<HdfsFileStatus.Flags> flags =
        EnumSet.noneOf(HdfsFileStatus.Flags.class);
    INodeAttributes nodeAttrs = fsd.getAttributes(iip);
    boolean hasAcl = nodeAttrs.getAclFeature() != null;
    if (hasAcl) {
      flags.add(HdfsFileStatus.Flags.HAS_ACL);
    }
    if (isEncrypted) {
      flags.add(HdfsFileStatus.Flags.HAS_CRYPT);
    }
    if (isErasureCoded) {
      flags.add(HdfsFileStatus.Flags.HAS_EC);
    }
    if(isSnapShottable){
      flags.add(HdfsFileStatus.Flags.SNAPSHOT_ENABLED);
    }
    return createFileStatus(
        size,
        node.isDirectory(),
        replication,
        blocksize,
        node.getModificationTime(snapshot),
        node.getAccessTime(snapshot),
        nodeAttrs.getFsPermission(),
        flags,
        nodeAttrs.getUserName(),
        nodeAttrs.getGroupName(),
        node.isSymlink() ? node.asSymlink().getSymlink() : null,
        name,
        node.getId(),
        childrenNum,
        feInfo,
        storagePolicy,
        ecPolicy,
        loc);
  }

  private static HdfsFileStatus createFileStatus(
      long length, boolean isdir,
      int replication, long blocksize, long mtime, long atime,
      FsPermission permission, EnumSet<HdfsFileStatus.Flags> flags,
      String owner, String group, byte[] symlink, byte[] path, long fileId,
      int childrenNum, FileEncryptionInfo feInfo, byte storagePolicy,
      ErasureCodingPolicy ecPolicy, LocatedBlocks locations) {
    if (locations == null) {
      return new HdfsFileStatus.Builder()
          .length(length)
          .isdir(isdir)
          .replication(replication)
          .blocksize(blocksize)
          .mtime(mtime)
          .atime(atime)
          .perm(permission)
          .flags(flags)
          .owner(owner)
          .group(group)
          .symlink(symlink)
          .path(path)
          .fileId(fileId)
          .children(childrenNum)
          .feInfo(feInfo)
          .storagePolicy(storagePolicy)
          .ecPolicy(ecPolicy)
          .build();
    } else {
      return new HdfsLocatedFileStatus(length, isdir, replication, blocksize,
          mtime, atime, permission, flags, owner, group, symlink, path,
          fileId, locations, childrenNum, feInfo, storagePolicy, ecPolicy);
    }
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
                fsd.getContentCountLimit(), fsd.getContentSleepMicroSec(),
                fsd.getPermissionChecker());
        ContentSummary cs = targetNode.computeAndConvertContentSummary(
            iip.getPathSnapshotId(), cscc);
        fsd.addYieldCount(cscc.getYieldCount());
        return cs;
      }
    } finally {
      fsd.readUnlock();
    }
  }

  static QuotaUsage getQuotaUsage(
      FSDirectory fsd, String src) throws IOException {
    FSPermissionChecker pc = fsd.getPermissionChecker();
    final INodesInPath iip;
    fsd.readLock();
    try {
      iip = fsd.resolvePath(pc, src, DirOp.READ_LINK);
      if (fsd.isPermissionEnabled()) {
        fsd.checkPermission(pc, iip, false, null, null, null,
            FsAction.READ_EXECUTE);
      }
    } finally {
      fsd.readUnlock();
    }
    QuotaUsage usage = getQuotaUsageInt(fsd, iip);
    if (usage != null) {
      return usage;
    } else {
      //If quota isn't set, fall back to getContentSummary.
      return getContentSummaryInt(fsd, iip);
    }
  }

  private static QuotaUsage getQuotaUsageInt(FSDirectory fsd, INodesInPath iip)
    throws IOException {
    fsd.readLock();
    try {
      INode targetNode = iip.getLastINode();
      QuotaUsage usage = null;
      if (targetNode.isDirectory()) {
        DirectoryWithQuotaFeature feature =
            targetNode.asDirectory().getDirectoryWithQuotaFeature();
        if (feature != null) {
          QuotaCounts counts = feature.getSpaceConsumed();
          QuotaCounts quotas = feature.getQuota();
          usage = new QuotaUsage.Builder().
              fileAndDirectoryCount(counts.getNameSpace()).
              quota(quotas.getNameSpace()).
              spaceConsumed(counts.getStorageSpace()).
              spaceQuota(quotas.getStorageSpace()).
              typeConsumed(counts.getTypeSpaces().asArray()).
              typeQuota(quotas.getTypeSpaces().asArray()).build();
        }
      }
      return usage;
    } finally {
      fsd.readUnlock();
    }
  }

  static class GetBlockLocationsResult {
    final boolean updateAccessTime;
    final LocatedBlocks blocks;
    boolean updateAccessTime() {
      return updateAccessTime;
    }
    private GetBlockLocationsResult(
        boolean updateAccessTime, LocatedBlocks blocks) {
      this.updateAccessTime = updateAccessTime;
      this.blocks = blocks;
    }
  }
}

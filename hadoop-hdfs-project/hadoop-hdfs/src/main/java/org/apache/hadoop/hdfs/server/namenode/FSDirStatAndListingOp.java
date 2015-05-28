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
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.FsPermissionExtension;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import static org.apache.hadoop.util.Time.now;

class FSDirStatAndListingOp {
  static DirectoryListing getListingInt(FSDirectory fsd, final String srcArg,
      byte[] startAfter, boolean needLocation) throws IOException {
    FSPermissionChecker pc = fsd.getPermissionChecker();
    String startAfterComponent = startAfter.length == 0 ? "" : new String(startAfter, Charsets.UTF_8);
    try (ROTransaction tx = fsd.newROTransaction().begin()) {
      Resolver.Result paths = Resolver.resolve(tx, srcArg);
      if (paths.invalidPath()) {
        throw new InvalidPathException(srcArg);
      } else if (paths.notFound()) {
        return null;
      }
      final FlatINodesInPath iip = paths.inodesInPath();

      boolean isSuperUser = true;
      if (fsd.isPermissionEnabled()) {
        if (iip.getLastINode() != null && iip.getLastINode().isDirectory()) {
          fsd.checkPathAccess(pc, iip, FsAction.READ_EXECUTE);
        } else {
          fsd.checkTraverse(pc, paths);
        }
        isSuperUser = pc.isSuperUser();
      }
      return getListing(tx, fsd, iip, srcArg, startAfterComponent, needLocation,
                        isSuperUser);
    }
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
    try (ROTransaction tx = fsd.newROTransaction().begin()) {
      Resolver.Result paths = Resolver.resolve(tx, src);
      if (!paths.ok()) {
        return null;
      }

      // TODO: Handle storage policy ID
      if (fsd.isPermissionEnabled()) {
        fsd.checkTraverse(pc, paths);
      }

//      byte policyId = includeStoragePolicy && i != null && !i.isSymlink() ?
//          i.getStoragePolicyID() : BlockStoragePolicySuite.ID_UNSPECIFIED;

      byte policyId = HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
      return createFileStatus(tx, fsd, paths.inodesInPath().getLastINode(),
                              HdfsFileStatus.EMPTY_NAME,
                              policyId);
    }
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

  /**
   * Get block locations within the specified range.
   * @see ClientProtocol#getBlockLocations(String, long, long)
   * @throws IOException
   */
  static GetBlockLocationsResult getBlockLocations(
      FSDirectory fsd, FSPermissionChecker pc, String src, long offset,
      long length, boolean needBlockToken) throws IOException {
    Preconditions.checkArgument(offset >= 0,
        "Negative offset is not supported.");
    Preconditions.checkArgument(length >= 0,
        "Negative length is not supported.");
    CacheManager cm = fsd.getFSNamesystem().getCacheManager();
    BlockManager bm = fsd.getBlockManager();
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    src = fsd.resolvePath(pc, src, pathComponents);
    try (ROTransaction tx = fsd.newROTransaction().begin()){
      Resolver.Result paths = Resolver.resolve(tx, src);
      if (paths.invalidPath()) {
        throw new InvalidPathException(src);
      } else if (paths.notFound()) {
        throw new FileNotFoundException(src);
      }

      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, paths.inodesInPath(), FsAction.READ);
        fsd.checkUnreadableBySuperuser(pc, paths.inodesInPath());
      }

      FlatINode inode = paths.inodesInPath().getLastINode();
      FlatINodeFileFeature file = inode.feature(FlatINodeFileFeature.class);
      if (file == null) {
        throw new FileNotFoundException(src);
      }

      final long fileSize = file.fileSize();
      final FileEncryptionInfo feInfo = null;
//      final FileEncryptionInfo feInfo = FSDirectory.isReservedRawName(src)
//          ? null
//          : fsd.getFileEncryptionInfo(inode, iip.getPathSnapshotId(), iip);

      LocatedBlocks blocks = bm.createLocatedBlocks(
          file.blocks(), offset, length, fileSize, needBlockToken);
      // TODO: Fix snapshot
      blocks = FSDirStatAndListingOp.attachFileInfo(
          blocks, fileSize, file.inConstruction(), false, feInfo);

      // Set caching information for the located blocks.
      for (LocatedBlock lb : blocks.getLocatedBlocks()) {
        cm.setCachedLocations(lb);
      }

      final long now = now();
      boolean updateAccessTime = fsd.isAccessTimeSupported()
          && now > inode.atime() + fsd.getAccessTimePrecision();
      return new GetBlockLocationsResult(updateAccessTime, blocks);
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
   * @param src the directory name
   * @param startAfter the name to start listing after
   * @param needLocation if block locations are returned
   * @return a partial listing starting after startAfter
   */
  private static DirectoryListing getListing(
      ROTransaction tx,
      FSDirectory fsd, FlatINodesInPath iip,
      String src, String startAfter, boolean needLocation, boolean isSuperUser)
      throws IOException {
    String srcs = FSDirectory.normalizePath(src);
    final boolean isRawPath = FSDirectory.isReservedRawName(src);
    final FlatINode targetNode = iip.getLastINode();

      // TODO: Handle storage policy
//      byte parentStoragePolicy = isSuperUser ?
//          targetNode.getStoragePolicyID() : HdfsConstants
//          .BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
    byte parentStoragePolicy = HdfsConstants
        .BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;

    if (!targetNode.isDirectory()) {
      return new DirectoryListing(
          new HdfsFileStatus[]{ createFileStatus(
              tx, fsd, targetNode, HdfsFileStatus.EMPTY_NAME,
              parentStoragePolicy)
          }, 0);
    }

    try (DBChildrenView children = tx.childrenView(targetNode.id())) {
      children.seekTo(ByteBuffer.wrap(startAfter.getBytes(Charsets.UTF_8)));
      int numOfListing = fsd.getLsLimit();
      int locationBudget = fsd.getLsLimit();
      int listingCnt = 0;
      int i = 0;
      HdfsFileStatus listing[] = new HdfsFileStatus[numOfListing];

      for (Map.Entry<ByteBuffer, Long> e : children) {
        if (locationBudget < 0 && i >= listing.length) {
          break;
        }

        FlatINode cur = tx.getINode(e.getValue());
        // TODO: Handle Storage policy
//      byte curPolicy = isSuperUser && !cur.isSymlink()?
//          cur.getLocalStoragePolicyID():
//          HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
        byte curPolicy = HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
        ByteBuffer b = e.getKey().duplicate();
        byte[] localName = new byte[b.remaining()];
        b.get(localName);
        listing[i] = createFileStatus(tx, fsd, cur, localName, needLocation,
                                      getStoragePolicyID(curPolicy,
                                                         parentStoragePolicy));
        if (needLocation) {
          // Once we  hit lsLimit locations, stop.
          // This helps to prevent excessively large response payloads.
          // Approximate #locations with locatedBlockCount() * repl_factor
          LocatedBlocks blks = ((HdfsLocatedFileStatus) listing[i]).getBlockLocations();
          locationBudget -= (blks == null) ? 0 : blks.locatedBlockCount() * listing[i].getReplication();
        }
        ++i;
        ++listingCnt;
      }

      // truncate return array if necessary
      if (listingCnt < numOfListing) {
        listing = Arrays.copyOf(listing, listingCnt);
      }

      return new DirectoryListing(
          listing,
          listingCnt < numOfListing ? 0 : children.size() - listingCnt);
    }
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
    throw new IllegalArgumentException("Unimplemented");
  }

  static HdfsFileStatus getFileInfo(
      FSDirectory fsd, String src, boolean resolveLink, boolean isRawPath,
      boolean includeStoragePolicy)
    throws IOException {
    throw new IllegalArgumentException("Unimplemented");
  }

  /**
   * create an hdfs file status from an inode
   *
   * @param fsd FSDirectory
   * @param node inode
   * @param needLocation if block locations need to be included or not
   * @return a file status
   * @throws java.io.IOException if any error occurs
   */
  private static HdfsFileStatus createFileStatus(
      ROTransaction tx, FSDirectory fsd, FlatINode node,
      byte[] localName, boolean needLocation, byte storagePolicy)
      throws IOException {
    if (needLocation) {
      return createLocatedFileStatus(tx, fsd, node, localName, storagePolicy);
    } else {
      return createFileStatus(tx, fsd, node, localName, storagePolicy);
    }
  }

  /**
   * Create FileStatus by file INode
   */
  static HdfsFileStatus createFileStatusForEditLog(
      FSDirectory fsd, String fullPath, byte[] path, INode node,
      byte storagePolicy, int snapshot, boolean isRawPath,
      INodesInPath iip) throws IOException {
    INodeAttributes nodeAttrs = getINodeAttributes(fsd, fullPath, path, node,
                                                   snapshot);
    return createFileStatus(fsd, path, node, nodeAttrs,
                            storagePolicy, snapshot, isRawPath, iip);
  }

  /**
   * Create FileStatus by file INode
   */
  static HdfsFileStatus createFileStatus(
      FSDirectory fsd, byte[] path, INode node,
      INodeAttributes nodeAttrs, byte storagePolicy, int snapshot,
      boolean isRawPath, INodesInPath iip) throws IOException {
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

  private static INodeAttributes getINodeAttributes(
      FSDirectory fsd, String fullPath, byte[] path, INode node, int snapshot) {
    return fsd.getAttributes(fullPath, path, node, snapshot);
  }

  /**
   * Create FileStatus by file INode
   */
  static HdfsFileStatus createFileStatus(
      Transaction tx, FSDirectory fsd, FlatINode node,
      byte[] localName, byte storagePolicy)
      throws IOException {
    long size = 0;     // length is zero for directories
    short replication = 0;
    long blocksize = 0;
    final boolean isEncrypted;

    // TODO: Handle FileEncryptionInfo
    final FileEncryptionInfo feInfo = null;
//    final FileEncryptionInfo feInfo = isRawPath ? null :
//        fsd.getFileEncryptionInfo(node, snapshot, iip);

    if (node.isFile()) {
      FlatINodeFileFeature f = node.feature(FlatINodeFileFeature.class);
      size =
          f.fileSize();
      replication = f.replication();
      blocksize = f.blockSize();
      isEncrypted = false;
    } else {
      isEncrypted = false;
    }

    int childrenNum = 0;
    if (node.isDirectory()) {
      try(DBChildrenView children = tx.childrenView(node.id())) {
        childrenNum = children.size();
      }
    }

    PermissionStatus perm = node.permissionStatus(fsd.ugid());

    // TODO:
    // <ul>
    // <li>If the INode has ACL / encryption, the function returns
    // FsPermissionExtension instead of FsPermisson</li>
    // <li>Handle symlink</li>
    // </ul>
    //
    return new HdfsFileStatus(size,
        node.isDirectory(), replication, blocksize,
        node.mtime(),
        node.atime(),
        perm.getPermission(),
        perm.getUserName(),
        perm.getGroupName(),
        null,
        localName,
        node.id(),
        childrenNum,
        feInfo,
        storagePolicy);
  }

  /**
   * Create FileStatus with location info by file INode
   */
  private static HdfsLocatedFileStatus createLocatedFileStatus(
      ROTransaction tx, FSDirectory fsd, FlatINode node, byte[] path,
      byte storagePolicy) throws IOException {
    assert fsd.hasReadLock();
    long size = 0; // length is zero for directories
    short replication = 0;
    long blocksize = 0;
    LocatedBlocks loc = null;

    // TODO
    final FileEncryptionInfo feInfo = null;
//    final FileEncryptionInfo feInfo = isRawPath ? null :
//        fsd.getFileEncryptionInfo(node, snapshot, iip);

    if (node.isFile()) {
      FlatINodeFileFeature file = node.feature(FlatINodeFileFeature.class);
      size = file.fileSize();
      replication = file.replication();
      blocksize = file.blockSize();

      final boolean isUc = file.inConstruction();
      final long fileSize = file.inConstruction()
          ? size - file.lastBlock().getNumBytes()
          : size;

      loc = fsd.getBlockManager().createLocatedBlocks(
          file.blocks(), 0L, size, fileSize, false);
      // TODO: Snapsho
      loc = attachFileInfo(loc, fileSize, isUc, false, feInfo);
    }

    int childrenNum = 0;
    if (node.isDirectory()) {
      try(DBChildrenView children = tx.childrenView(node.id())) {
        childrenNum = children.size();
      }
    }

    PermissionStatus perm = node.permissionStatus(fsd.ugid());
    HdfsLocatedFileStatus status = new HdfsLocatedFileStatus(
        size, node.isDirectory(), replication, blocksize, node.mtime(), node.atime(),
        perm.getPermission(), perm.getUserName(), perm.getGroupName(),
        null, path, node.id(), loc, childrenNum, feInfo, storagePolicy);
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

  static LocatedBlocks attachFileInfo(
      LocatedBlocks from, long flength, boolean isUnderConstruction,
      boolean inSnapshot, FileEncryptionInfo feInfo) {
    return new LocatedBlocks(flength, isUnderConstruction,
                             from.getLocatedBlocks(),
                             from.getLastLocatedBlock(),
                             from.isLastBlockComplete() || inSnapshot,
                             feInfo);
  }
}

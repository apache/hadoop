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

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.security.AccessControlException;

import com.google.common.collect.Lists;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_QUOTA_BY_STORAGETYPE_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY;

public class FSDirAttrOp {
  static FileStatus setPermission(
      FSDirectory fsd, final String src, FsPermission permission)
      throws IOException {
    if (FSDirectory.isExactReservedName(src)) {
      throw new InvalidPathException(src);
    }
    FSPermissionChecker pc = fsd.getPermissionChecker();
    INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      fsd.checkOwner(pc, iip);
      unprotectedSetPermission(fsd, iip, permission);
    } finally {
      fsd.writeUnlock();
    }
    fsd.getEditLog().logSetPermissions(iip.getPath(), permission);
    return fsd.getAuditFileInfo(iip);
  }

  static FileStatus setOwner(
      FSDirectory fsd, String src, String username, String group)
      throws IOException {
    if (FSDirectory.isExactReservedName(src)) {
      throw new InvalidPathException(src);
    }
    FSPermissionChecker pc = fsd.getPermissionChecker();
    INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      fsd.checkOwner(pc, iip);
      if (!pc.isSuperUser()) {
        if (username != null && !pc.getUser().equals(username)) {
          throw new AccessControlException("User " + username
              + " is not a super user (non-super user cannot change owner).");
        }
        if (group != null && !pc.isMemberOfGroup(group)) {
          throw new AccessControlException(
              "User " + username + " does not belong to " + group);
        }
      }
      unprotectedSetOwner(fsd, iip, username, group);
    } finally {
      fsd.writeUnlock();
    }
    fsd.getEditLog().logSetOwner(iip.getPath(), username, group);
    return fsd.getAuditFileInfo(iip);
  }

  static FileStatus setTimes(
      FSDirectory fsd, String src, long mtime, long atime)
      throws IOException {
    FSPermissionChecker pc = fsd.getPermissionChecker();

    INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      // Write access is required to set access and modification times
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }
      final INode inode = iip.getLastINode();
      if (inode == null) {
        throw new FileNotFoundException("File/Directory " + iip.getPath() +
                                            " does not exist.");
      }
      boolean changed = unprotectedSetTimes(fsd, iip, mtime, atime, true);
      if (changed) {
        fsd.getEditLog().logTimes(iip.getPath(), mtime, atime);
      }
    } finally {
      fsd.writeUnlock();
    }
    return fsd.getAuditFileInfo(iip);
  }

  static boolean setReplication(
      FSDirectory fsd, BlockManager bm, String src, final short replication)
      throws IOException {
    bm.verifyReplication(src, replication, null);
    final boolean isFile;
    FSPermissionChecker pc = fsd.getPermissionChecker();
    fsd.writeLock();
    try {
      final INodesInPath iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }

      final BlockInfo[] blocks = unprotectedSetReplication(fsd, iip,
                                                           replication);
      isFile = blocks != null;
      if (isFile) {
        fsd.getEditLog().logSetReplication(iip.getPath(), replication);
      }
    } finally {
      fsd.writeUnlock();
    }
    return isFile;
  }

  static FileStatus unsetStoragePolicy(FSDirectory fsd, BlockManager bm,
      String src) throws IOException {
    return setStoragePolicy(fsd, bm, src,
        HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED, "unset");
  }

  static FileStatus setStoragePolicy(FSDirectory fsd, BlockManager bm,
      String src, final String policyName) throws IOException {
    // get the corresponding policy and make sure the policy name is valid
    BlockStoragePolicy policy = bm.getStoragePolicy(policyName);
    if (policy == null) {
      throw new HadoopIllegalArgumentException(
          "Cannot find a block policy with the name " + policyName);
    }

    return setStoragePolicy(fsd, bm, src, policy.getId(), "set");
  }

  static FileStatus setStoragePolicy(FSDirectory fsd, BlockManager bm,
      String src, final byte policyId, final String operation)
      throws IOException {
    if (!fsd.isStoragePolicyEnabled()) {
      throw new IOException(String.format(
          "Failed to %s storage policy since %s is set to false.", operation,
          DFS_STORAGE_POLICY_ENABLED_KEY));
    }
    FSPermissionChecker pc = fsd.getPermissionChecker();
    INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, src, DirOp.WRITE);

      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }

      unprotectedSetStoragePolicy(fsd, bm, iip, policyId);
      fsd.getEditLog().logSetStoragePolicy(iip.getPath(), policyId);
    } finally {
      fsd.writeUnlock();
    }
    return fsd.getAuditFileInfo(iip);
  }

  static BlockStoragePolicy[] getStoragePolicies(BlockManager bm)
      throws IOException {
    return bm.getStoragePolicies();
  }

  static BlockStoragePolicy getStoragePolicy(FSDirectory fsd, BlockManager bm,
      String path) throws IOException {
    FSPermissionChecker pc = fsd.getPermissionChecker();
    fsd.readLock();
    try {
      final INodesInPath iip = fsd.resolvePath(pc, path, DirOp.READ_LINK);
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.READ);
      }
      INode inode = iip.getLastINode();
      if (inode == null) {
        throw new FileNotFoundException("File/Directory does not exist: "
            + iip.getPath());
      }
      return bm.getStoragePolicy(inode.getStoragePolicyID());
    } finally {
      fsd.readUnlock();
    }
  }

  static long getPreferredBlockSize(FSDirectory fsd, String src)
      throws IOException {
    FSPermissionChecker pc = fsd.getPermissionChecker();
    fsd.readLock();
    try {
      final INodesInPath iip = fsd.resolvePath(pc, src, DirOp.READ_LINK);
      return INodeFile.valueOf(iip.getLastINode(), iip.getPath())
          .getPreferredBlockSize();
    } finally {
      fsd.readUnlock();
    }
  }

  /**
   * Set the namespace, storagespace and typespace quota for a directory.
   *
   * Note: This does not support ".inodes" relative path.
   */
  static void setQuota(FSDirectory fsd, String src, long nsQuota, long ssQuota,
      StorageType type) throws IOException {
    FSPermissionChecker pc = fsd.getPermissionChecker();
    if (fsd.isPermissionEnabled()) {
      pc.checkSuperuserPrivilege();
    }

    fsd.writeLock();
    try {
      INodesInPath iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      INodeDirectory changed =
          unprotectedSetQuota(fsd, iip, nsQuota, ssQuota, type);
      if (changed != null) {
        final QuotaCounts q = changed.getQuotaCounts();
        if (type == null) {
          fsd.getEditLog().logSetQuota(src, q.getNameSpace(), q.getStorageSpace());
        } else {
          fsd.getEditLog().logSetQuotaByStorageType(
              src, q.getTypeSpaces().get(type), type);
        }
      }
    } finally {
      fsd.writeUnlock();
    }
  }

  static void unprotectedSetPermission(
      FSDirectory fsd, INodesInPath iip, FsPermission permissions)
      throws FileNotFoundException, UnresolvedLinkException,
             QuotaExceededException, SnapshotAccessControlException {
    assert fsd.hasWriteLock();
    final INode inode = FSDirectory.resolveLastINode(iip);
    int snapshotId = iip.getLatestSnapshotId();
    inode.setPermission(permissions, snapshotId);
  }

  static void unprotectedSetOwner(
      FSDirectory fsd, INodesInPath iip, String username, String groupname)
      throws FileNotFoundException, UnresolvedLinkException,
      QuotaExceededException, SnapshotAccessControlException {
    assert fsd.hasWriteLock();
    final INode inode = FSDirectory.resolveLastINode(iip);
    if (username != null) {
      inode.setUser(username, iip.getLatestSnapshotId());
    }
    if (groupname != null) {
      inode.setGroup(groupname, iip.getLatestSnapshotId());
    }
  }

  static boolean setTimes(
      FSDirectory fsd, INodesInPath iip, long mtime, long atime, boolean force)
          throws QuotaExceededException {
    fsd.writeLock();
    try {
      return unprotectedSetTimes(fsd, iip, mtime, atime, force);
    } finally {
      fsd.writeUnlock();
    }
  }

  /**
   * See {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#setQuota(String,
   *     long, long, StorageType)}
   * for the contract.
   * Sets quota for for a directory.
   * @return INodeDirectory if any of the quotas have changed. null otherwise.
   * @throws FileNotFoundException if the path does not exist.
   * @throws PathIsNotDirectoryException if the path is not a directory.
   * @throws QuotaExceededException if the directory tree size is
   *                                greater than the given quota
   * @throws UnresolvedLinkException if a symlink is encountered in src.
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  static INodeDirectory unprotectedSetQuota(
      FSDirectory fsd, INodesInPath iip, long nsQuota,
      long ssQuota, StorageType type)
      throws FileNotFoundException, PathIsNotDirectoryException,
      QuotaExceededException, UnresolvedLinkException,
      SnapshotAccessControlException, UnsupportedActionException {
    assert fsd.hasWriteLock();
    // sanity check
    if ((nsQuota < 0 && nsQuota != HdfsConstants.QUOTA_DONT_SET &&
         nsQuota != HdfsConstants.QUOTA_RESET) ||
        (ssQuota < 0 && ssQuota != HdfsConstants.QUOTA_DONT_SET &&
          ssQuota != HdfsConstants.QUOTA_RESET)) {
      throw new IllegalArgumentException("Illegal value for nsQuota or " +
                                         "ssQuota : " + nsQuota + " and " +
                                         ssQuota);
    }
    // sanity check for quota by storage type
    if ((type != null) && (!fsd.isQuotaByStorageTypeEnabled() ||
        nsQuota != HdfsConstants.QUOTA_DONT_SET)) {
      throw new UnsupportedActionException(
          "Failed to set quota by storage type because either" +
          DFS_QUOTA_BY_STORAGETYPE_ENABLED_KEY + " is set to " +
          fsd.isQuotaByStorageTypeEnabled() + " or nsQuota value is illegal " +
          nsQuota);
    }

    INodeDirectory dirNode =
        INodeDirectory.valueOf(iip.getLastINode(), iip.getPath());
    if (dirNode.isRoot() && nsQuota == HdfsConstants.QUOTA_RESET) {
      throw new IllegalArgumentException("Cannot clear namespace quota on root.");
    } else { // a directory inode
      final QuotaCounts oldQuota = dirNode.getQuotaCounts();
      final long oldNsQuota = oldQuota.getNameSpace();
      final long oldSsQuota = oldQuota.getStorageSpace();

      if (nsQuota == HdfsConstants.QUOTA_DONT_SET) {
        nsQuota = oldNsQuota;
      }
      if (ssQuota == HdfsConstants.QUOTA_DONT_SET) {
        ssQuota = oldSsQuota;
      }

      // unchanged space/namespace quota
      if (type == null && oldNsQuota == nsQuota && oldSsQuota == ssQuota) {
        return null;
      }

      // unchanged type quota
      if (type != null) {
          EnumCounters<StorageType> oldTypeQuotas = oldQuota.getTypeSpaces();
          if (oldTypeQuotas != null && oldTypeQuotas.get(type) == ssQuota) {
              return null;
          }
      }

      final int latest = iip.getLatestSnapshotId();
      dirNode.recordModification(latest);
      dirNode.setQuota(fsd.getBlockStoragePolicySuite(), nsQuota, ssQuota, type);
      return dirNode;
    }
  }

  static BlockInfo[] unprotectedSetReplication(
      FSDirectory fsd, INodesInPath iip, short replication)
      throws QuotaExceededException, UnresolvedLinkException,
      SnapshotAccessControlException, UnsupportedActionException {
    assert fsd.hasWriteLock();

    final BlockManager bm = fsd.getBlockManager();
    final INode inode = iip.getLastINode();
    if (inode == null || !inode.isFile() || inode.asFile().isStriped()) {
      // TODO we do not support replication on stripe layout files yet
      return null;
    }

    INodeFile file = inode.asFile();
    // Make sure the directory has sufficient quotas
    short oldBR = file.getPreferredBlockReplication();

    long size = file.computeFileSize(true, true);
    // Ensure the quota does not exceed
    if (oldBR < replication) {
      fsd.updateCount(iip, 0L, size, oldBR, replication, true);
    }

    file.setFileReplication(replication, iip.getLatestSnapshotId());
    short targetReplication = (short) Math.max(
        replication, file.getPreferredBlockReplication());

    if (oldBR > replication) {
      fsd.updateCount(iip, 0L, size, oldBR, targetReplication, true);
    }
    for (BlockInfo b : file.getBlocks()) {
      bm.setReplication(oldBR, targetReplication, b);
    }

    if (oldBR != -1) {
      if (oldBR > targetReplication) {
        FSDirectory.LOG.info("Decreasing replication from {} to {} for {}",
                             oldBR, targetReplication, iip.getPath());
      } else {
        FSDirectory.LOG.info("Increasing replication from {} to {} for {}",
                             oldBR, targetReplication, iip.getPath());
      }
    }
    return file.getBlocks();
  }

  static void unprotectedSetStoragePolicy(FSDirectory fsd, BlockManager bm,
      INodesInPath iip, final byte policyId)
      throws IOException {
    assert fsd.hasWriteLock();
    final INode inode = iip.getLastINode();
    if (inode == null) {
      throw new FileNotFoundException("File/Directory does not exist: "
          + iip.getPath());
    }
    final int snapshotId = iip.getLatestSnapshotId();
    if (inode.isFile()) {
      if (policyId != HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
        BlockStoragePolicy newPolicy = bm.getStoragePolicy(policyId);
        if (newPolicy.isCopyOnCreateFile()) {
          throw new HadoopIllegalArgumentException("Policy " + newPolicy
              + " cannot be set after file creation.");
        }
      }

      BlockStoragePolicy currentPolicy =
          bm.getStoragePolicy(inode.getLocalStoragePolicyID());

      if (currentPolicy != null && currentPolicy.isCopyOnCreateFile()) {
        throw new HadoopIllegalArgumentException(
            "Existing policy " + currentPolicy.getName() +
                " cannot be changed after file creation.");
      }
      inode.asFile().setStoragePolicyID(policyId, snapshotId);
    } else if (inode.isDirectory()) {
      setDirStoragePolicy(fsd, iip, policyId);
    } else {
      throw new FileNotFoundException(iip.getPath()
          + " is not a file or directory");
    }
  }

  private static void setDirStoragePolicy(
      FSDirectory fsd, INodesInPath iip, byte policyId) throws IOException {
    INode inode = FSDirectory.resolveLastINode(iip);
    List<XAttr> existingXAttrs = XAttrStorage.readINodeXAttrs(inode);
    XAttr xAttr = BlockStoragePolicySuite.buildXAttr(policyId);
    List<XAttr> newXAttrs = null;
    if (policyId == HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
      List<XAttr> toRemove = Lists.newArrayList();
      toRemove.add(xAttr);
      List<XAttr> removed = Lists.newArrayList();
      newXAttrs = FSDirXAttrOp.filterINodeXAttrs(existingXAttrs, toRemove,
          removed);
    } else {
      newXAttrs = FSDirXAttrOp.setINodeXAttrs(fsd, existingXAttrs,
          Arrays.asList(xAttr),
          EnumSet.of(XAttrSetFlag.CREATE, XAttrSetFlag.REPLACE));
    }
    XAttrStorage.updateINodeXAttrs(inode, newXAttrs, iip.getLatestSnapshotId());
  }

  static boolean unprotectedSetTimes(
      FSDirectory fsd, INodesInPath iip, long mtime, long atime, boolean force)
          throws QuotaExceededException {
    assert fsd.hasWriteLock();
    boolean status = false;
    INode inode = iip.getLastINode();
    int latest = iip.getLatestSnapshotId();
    if (mtime != -1) {
      inode = inode.setModificationTime(mtime, latest);
      status = true;
    }

    // if the last access time update was within the last precision interval,
    // then no need to store access time
    if (atime != -1 && (status || force
        || atime > inode.getAccessTime() + fsd.getAccessTimePrecision())) {
      inode.setAccessTime(atime, latest,
          fsd.getFSNamesystem().getSnapshotManager().
          getSkipCaptureAccessTimeOnlyChange());
      status = true;
    }
    return status;
  }
}

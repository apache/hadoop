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
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.security.AccessControlException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_QUOTA_BY_STORAGETYPE_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY;

public class FSDirAttrOp {
  static HdfsFileStatus setPermission(
      FSDirectory fsd, final String srcArg, FsPermission permission)
      throws IOException {
    String src = srcArg;
    FSPermissionChecker pc = fsd.getPermissionChecker();
    INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePathForWrite(pc, src);
      src = iip.getPath();
      fsd.checkOwner(pc, iip);
      unprotectedSetPermission(fsd, src, permission);
    } finally {
      fsd.writeUnlock();
    }
    fsd.getEditLog().logSetPermissions(src, permission);
    return fsd.getAuditFileInfo(iip);
  }

  static HdfsFileStatus setOwner(
      FSDirectory fsd, String src, String username, String group)
      throws IOException {
    FSPermissionChecker pc = fsd.getPermissionChecker();
    INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePathForWrite(pc, src);
      src = iip.getPath();
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
      unprotectedSetOwner(fsd, src, username, group);
    } finally {
      fsd.writeUnlock();
    }
    fsd.getEditLog().logSetOwner(src, username, group);
    return fsd.getAuditFileInfo(iip);
  }

  static HdfsFileStatus setTimes(
      FSDirectory fsd, String src, long mtime, long atime)
      throws IOException {
    if (!fsd.isAccessTimeSupported() && atime != -1) {
      throw new IOException(
          "Access time for hdfs is not configured. " +
              " Please set " + DFS_NAMENODE_ACCESSTIME_PRECISION_KEY
              + " configuration parameter.");
    }

    FSPermissionChecker pc = fsd.getPermissionChecker();

    INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePathForWrite(pc, src);
      src = iip.getPath();
      // Write access is required to set access and modification times
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }
      final INode inode = iip.getLastINode();
      if (inode == null) {
        throw new FileNotFoundException("File/Directory " + src +
                                            " does not exist.");
      }
      boolean changed = unprotectedSetTimes(fsd, inode, mtime, atime, true,
                                            iip.getLatestSnapshotId());
      if (changed) {
        fsd.getEditLog().logTimes(src, mtime, atime);
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
      final INodesInPath iip = fsd.resolvePathForWrite(pc, src);
      src = iip.getPath();
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }

      final short[] blockRepls = new short[2]; // 0: old, 1: new
      final Block[] blocks = unprotectedSetReplication(fsd, src, replication,
                                                       blockRepls);
      isFile = blocks != null;
      if (isFile) {
        fsd.getEditLog().logSetReplication(src, replication);
        bm.setReplication(blockRepls[0], blockRepls[1], src, blocks);
      }
    } finally {
      fsd.writeUnlock();
    }
    return isFile;
  }

  static HdfsFileStatus setStoragePolicy(
      FSDirectory fsd, BlockManager bm, String src, final String policyName)
      throws IOException {
    if (!fsd.isStoragePolicyEnabled()) {
      throw new IOException(
          "Failed to set storage policy since "
              + DFS_STORAGE_POLICY_ENABLED_KEY + " is set to false.");
    }
    FSPermissionChecker pc = fsd.getPermissionChecker();
    INodesInPath iip;
    fsd.writeLock();
    try {
      src = FSDirectory.resolvePath(src, fsd);
      iip = fsd.getINodesInPath4Write(src);

      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }

      // get the corresponding policy and make sure the policy name is valid
      BlockStoragePolicy policy = bm.getStoragePolicy(policyName);
      if (policy == null) {
        throw new HadoopIllegalArgumentException(
            "Cannot find a block policy with the name " + policyName);
      }
      unprotectedSetStoragePolicy(fsd, bm, iip, policy.getId());
      fsd.getEditLog().logSetStoragePolicy(src, policy.getId());
    } finally {
      fsd.writeUnlock();
    }
    return fsd.getAuditFileInfo(iip);
  }

  static BlockStoragePolicy[] getStoragePolicies(BlockManager bm)
      throws IOException {
    return bm.getStoragePolicies();
  }

  static long getPreferredBlockSize(FSDirectory fsd, String src)
      throws IOException {
    FSPermissionChecker pc = fsd.getPermissionChecker();
    fsd.readLock();
    try {
      final INodesInPath iip = fsd.resolvePath(pc, src, false);
      src = iip.getPath();
      if (fsd.isPermissionEnabled()) {
        fsd.checkTraverse(pc, iip);
      }
      return INodeFile.valueOf(iip.getLastINode(), src)
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
    if (fsd.isPermissionEnabled()) {
      FSPermissionChecker pc = fsd.getPermissionChecker();
      pc.checkSuperuserPrivilege();
    }

    fsd.writeLock();
    try {
      INodeDirectory changed = unprotectedSetQuota(fsd, src, nsQuota, ssQuota, type);
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
      FSDirectory fsd, String src, FsPermission permissions)
      throws FileNotFoundException, UnresolvedLinkException,
             QuotaExceededException, SnapshotAccessControlException {
    assert fsd.hasWriteLock();
    final INodesInPath inodesInPath = fsd.getINodesInPath4Write(src, true);
    final INode inode = inodesInPath.getLastINode();
    if (inode == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    int snapshotId = inodesInPath.getLatestSnapshotId();
    inode.setPermission(permissions, snapshotId);
  }

  static void unprotectedSetOwner(
      FSDirectory fsd, String src, String username, String groupname)
      throws FileNotFoundException, UnresolvedLinkException,
      QuotaExceededException, SnapshotAccessControlException {
    assert fsd.hasWriteLock();
    final INodesInPath inodesInPath = fsd.getINodesInPath4Write(src, true);
    INode inode = inodesInPath.getLastINode();
    if (inode == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    if (username != null) {
      inode = inode.setUser(username, inodesInPath.getLatestSnapshotId());
    }
    if (groupname != null) {
      inode.setGroup(groupname, inodesInPath.getLatestSnapshotId());
    }
  }

  static boolean setTimes(
      FSDirectory fsd, INode inode, long mtime, long atime, boolean force,
      int latestSnapshotId) throws QuotaExceededException {
    fsd.writeLock();
    try {
      return unprotectedSetTimes(fsd, inode, mtime, atime, force,
                                 latestSnapshotId);
    } finally {
      fsd.writeUnlock();
    }
  }

  static boolean unprotectedSetTimes(
      FSDirectory fsd, String src, long mtime, long atime, boolean force)
      throws UnresolvedLinkException, QuotaExceededException {
    assert fsd.hasWriteLock();
    final INodesInPath i = fsd.getINodesInPath(src, true);
    return unprotectedSetTimes(fsd, i.getLastINode(), mtime, atime,
                               force, i.getLatestSnapshotId());
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
      FSDirectory fsd, String src, long nsQuota, long ssQuota, StorageType type)
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

    String srcs = FSDirectory.normalizePath(src);
    final INodesInPath iip = fsd.getINodesInPath4Write(srcs, true);
    INodeDirectory dirNode = INodeDirectory.valueOf(iip.getLastINode(), srcs);
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

  static Block[] unprotectedSetReplication(
      FSDirectory fsd, String src, short replication, short[] blockRepls)
      throws QuotaExceededException, UnresolvedLinkException,
             SnapshotAccessControlException {
    assert fsd.hasWriteLock();

    final INodesInPath iip = fsd.getINodesInPath4Write(src, true);
    final INode inode = iip.getLastINode();
    if (inode == null || !inode.isFile()) {
      return null;
    }
    INodeFile file = inode.asFile();
    final short oldBR = file.getBlockReplication();
    long size = file.computeFileSize(true, true);

    // before setFileReplication, check for increasing block replication.
    // if replication > oldBR, then newBR == replication.
    // if replication < oldBR, we don't know newBR yet.
    if (replication > oldBR) {
      fsd.updateCount(iip, 0L, size, oldBR, replication, true);
    }

    file.setFileReplication(replication, iip.getLatestSnapshotId());

    final short newBR = file.getBlockReplication();
    // check newBR < oldBR case.
    if (newBR < oldBR) {
      fsd.updateCount(iip, 0L, size, oldBR, newBR, true);
    }

    if (blockRepls != null) {
      blockRepls[0] = oldBR;
      blockRepls[1] = newBR;
    }
    return file.getBlocks();
  }

  static void unprotectedSetStoragePolicy(
      FSDirectory fsd, BlockManager bm, INodesInPath iip, byte policyId)
      throws IOException {
    assert fsd.hasWriteLock();
    final INode inode = iip.getLastINode();
    if (inode == null) {
      throw new FileNotFoundException("File/Directory does not exist: "
          + iip.getPath());
    }
    final int snapshotId = iip.getLatestSnapshotId();
    if (inode.isFile()) {
      BlockStoragePolicy newPolicy = bm.getStoragePolicy(policyId);
      if (newPolicy.isCopyOnCreateFile()) {
        throw new HadoopIllegalArgumentException(
            "Policy " + newPolicy + " cannot be set after file creation.");
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
      setDirStoragePolicy(fsd, inode.asDirectory(), policyId, snapshotId);
    } else {
      throw new FileNotFoundException(iip.getPath()
          + " is not a file or directory");
    }
  }

  private static void setDirStoragePolicy(
      FSDirectory fsd, INodeDirectory inode, byte policyId,
      int latestSnapshotId) throws IOException {
    List<XAttr> existingXAttrs = XAttrStorage.readINodeXAttrs(inode);
    XAttr xAttr = BlockStoragePolicySuite.buildXAttr(policyId);
    List<XAttr> newXAttrs = FSDirXAttrOp.setINodeXAttrs(fsd, existingXAttrs,
                                                        Arrays.asList(xAttr),
                                                        EnumSet.of(
                                                            XAttrSetFlag.CREATE,
                                                            XAttrSetFlag.REPLACE));
    XAttrStorage.updateINodeXAttrs(inode, newXAttrs, latestSnapshotId);
  }

  private static boolean unprotectedSetTimes(
      FSDirectory fsd, INode inode, long mtime, long atime, boolean force,
      int latest) throws QuotaExceededException {
    assert fsd.hasWriteLock();
    boolean status = false;
    if (mtime != -1) {
      inode = inode.setModificationTime(mtime, latest);
      status = true;
    }
    // if the last access time update was within the last precision interval,
    // then no need to store access time
    if (atime != -1 && (status || force || atime > inode.getAccessTime() +
        fsd.getFSNamesystem().getAccessTimePrecision())) {
      inode.setAccessTime(atime, latest);
      status = true;
    }
    return status;
  }
}

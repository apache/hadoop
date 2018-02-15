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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

class FSDirAclOp {
  static FileStatus modifyAclEntries(
      FSDirectory fsd, FSPermissionChecker pc, final String srcArg,
      List<AclEntry> aclSpec) throws IOException {
    String src = srcArg;
    checkAclsConfigFlag(fsd);
    INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      src = iip.getPath();
      fsd.checkOwner(pc, iip);
      INode inode = FSDirectory.resolveLastINode(iip);
      int snapshotId = iip.getLatestSnapshotId();
      List<AclEntry> existingAcl = AclStorage.readINodeLogicalAcl(inode);
      List<AclEntry> newAcl = AclTransformation.mergeAclEntries(
          existingAcl, aclSpec);
      AclStorage.updateINodeAcl(inode, newAcl, snapshotId);
      fsd.getEditLog().logSetAcl(src, newAcl);
    } finally {
      fsd.writeUnlock();
    }
    return fsd.getAuditFileInfo(iip);
  }

  static FileStatus removeAclEntries(
      FSDirectory fsd, FSPermissionChecker pc, final String srcArg,
      List<AclEntry> aclSpec) throws IOException {
    String src = srcArg;
    checkAclsConfigFlag(fsd);
    INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      src = iip.getPath();
      fsd.checkOwner(pc, iip);
      INode inode = FSDirectory.resolveLastINode(iip);
      int snapshotId = iip.getLatestSnapshotId();
      List<AclEntry> existingAcl = AclStorage.readINodeLogicalAcl(inode);
      List<AclEntry> newAcl = AclTransformation.filterAclEntriesByAclSpec(
        existingAcl, aclSpec);
      AclStorage.updateINodeAcl(inode, newAcl, snapshotId);
      fsd.getEditLog().logSetAcl(src, newAcl);
    } finally {
      fsd.writeUnlock();
    }
    return fsd.getAuditFileInfo(iip);
  }

  static FileStatus removeDefaultAcl(FSDirectory fsd, FSPermissionChecker pc,
      final String srcArg) throws IOException {
    String src = srcArg;
    checkAclsConfigFlag(fsd);
    INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      src = iip.getPath();
      fsd.checkOwner(pc, iip);
      INode inode = FSDirectory.resolveLastINode(iip);
      int snapshotId = iip.getLatestSnapshotId();
      List<AclEntry> existingAcl = AclStorage.readINodeLogicalAcl(inode);
      List<AclEntry> newAcl = AclTransformation.filterDefaultAclEntries(
        existingAcl);
      AclStorage.updateINodeAcl(inode, newAcl, snapshotId);
      fsd.getEditLog().logSetAcl(src, newAcl);
    } finally {
      fsd.writeUnlock();
    }
    return fsd.getAuditFileInfo(iip);
  }

  static FileStatus removeAcl(FSDirectory fsd, FSPermissionChecker pc,
      final String srcArg) throws IOException {
    String src = srcArg;
    checkAclsConfigFlag(fsd);
    INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      src = iip.getPath();
      fsd.checkOwner(pc, iip);
      unprotectedRemoveAcl(fsd, iip);
    } finally {
      fsd.writeUnlock();
    }
    fsd.getEditLog().logSetAcl(src, AclFeature.EMPTY_ENTRY_LIST);
    return fsd.getAuditFileInfo(iip);
  }

  static FileStatus setAcl(
      FSDirectory fsd, FSPermissionChecker pc, final String srcArg,
      List<AclEntry> aclSpec) throws IOException {
    String src = srcArg;
    checkAclsConfigFlag(fsd);
    INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      fsd.checkOwner(pc, iip);
      List<AclEntry> newAcl = unprotectedSetAcl(fsd, iip, aclSpec, false);
      fsd.getEditLog().logSetAcl(iip.getPath(), newAcl);
    } finally {
      fsd.writeUnlock();
    }
    return fsd.getAuditFileInfo(iip);
  }

  static AclStatus getAclStatus(
      FSDirectory fsd, FSPermissionChecker pc, String src) throws IOException {
    checkAclsConfigFlag(fsd);
    fsd.readLock();
    try {
      INodesInPath iip = fsd.resolvePath(pc, src, DirOp.READ);
      // There is no real inode for the path ending in ".snapshot", so return a
      // non-null, unpopulated AclStatus.  This is similar to getFileInfo.
      if (iip.isDotSnapshotDir() && fsd.getINode4DotSnapshot(iip) != null) {
        return new AclStatus.Builder().owner("").group("").build();
      }
      INode inode = FSDirectory.resolveLastINode(iip);
      int snapshotId = iip.getPathSnapshotId();
      List<AclEntry> acl = AclStorage.readINodeAcl(fsd.getAttributes(iip));
      FsPermission fsPermission = inode.getFsPermission(snapshotId);
      return new AclStatus.Builder()
          .owner(inode.getUserName()).group(inode.getGroupName())
          .stickyBit(fsPermission.getStickyBit())
          .setPermission(fsPermission)
          .addEntries(acl).build();
    } finally {
      fsd.readUnlock();
    }
  }

  static List<AclEntry> unprotectedSetAcl(FSDirectory fsd, INodesInPath iip,
      List<AclEntry> aclSpec, boolean fromEdits) throws IOException {
    assert fsd.hasWriteLock();

    // ACL removal is logged to edits as OP_SET_ACL with an empty list.
    if (aclSpec.isEmpty()) {
      unprotectedRemoveAcl(fsd, iip);
      return AclFeature.EMPTY_ENTRY_LIST;
    }

    INode inode = FSDirectory.resolveLastINode(iip);
    int snapshotId = iip.getLatestSnapshotId();
    List<AclEntry> newAcl = aclSpec;
    if (!fromEdits) {
      List<AclEntry> existingAcl = AclStorage.readINodeLogicalAcl(inode);
      newAcl = AclTransformation.replaceAclEntries(existingAcl, aclSpec);
    }
    AclStorage.updateINodeAcl(inode, newAcl, snapshotId);
    return newAcl;
  }

  private static void checkAclsConfigFlag(FSDirectory fsd) throws AclException {
    if (!fsd.isAclsEnabled()) {
      throw new AclException(String.format(
          "The ACL operation has been rejected.  "
              + "Support for ACLs has been disabled by setting %s to false.",
          DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY));
    }
  }

  private static void unprotectedRemoveAcl(FSDirectory fsd, INodesInPath iip)
      throws IOException {
    assert fsd.hasWriteLock();
    INode inode = FSDirectory.resolveLastINode(iip);
    int snapshotId = iip.getLatestSnapshotId();
    AclFeature f = inode.getAclFeature();
    if (f == null) {
      return;
    }

    FsPermission perm = inode.getFsPermission();
    List<AclEntry> featureEntries = AclStorage.getEntriesFromAclFeature(f);
    if (featureEntries.get(0).getScope() == AclEntryScope.ACCESS) {
      // Restore group permissions from the feature's entry to permission
      // bits, overwriting the mask, which is not part of a minimal ACL.
      AclEntry groupEntryKey = new AclEntry.Builder()
          .setScope(AclEntryScope.ACCESS).setType(AclEntryType.GROUP).build();
      int groupEntryIndex = Collections.binarySearch(
          featureEntries, groupEntryKey,
          AclTransformation.ACL_ENTRY_COMPARATOR);
      Preconditions.checkPositionIndex(groupEntryIndex, featureEntries.size(),
          "Invalid group entry index after binary-searching inode: " +
              inode.getFullPathName() + "(" + inode.getId() + ") "
              + "with featureEntries:" + featureEntries);
      FsAction groupPerm = featureEntries.get(groupEntryIndex).getPermission();
      FsPermission newPerm = new FsPermission(perm.getUserAction(), groupPerm,
          perm.getOtherAction(), perm.getStickyBit());
      inode.setPermission(newPerm, snapshotId);
    }

    inode.removeAclFeature(snapshotId);
  }
}

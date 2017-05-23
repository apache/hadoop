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

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.XATTR_SATISFY_STORAGE_POLICY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;

import com.google.common.collect.Lists;

/**
 * Helper class to perform storage policy satisfier related operations.
 */
final class FSDirSatisfyStoragePolicyOp {

  /**
   * Private constructor for preventing FSDirSatisfyStoragePolicyOp object
   * creation. Static-only class.
   */
  private FSDirSatisfyStoragePolicyOp() {
  }

  static FileStatus satisfyStoragePolicy(FSDirectory fsd, BlockManager bm,
      String src, boolean logRetryCache) throws IOException {

    assert fsd.getFSNamesystem().hasWriteLock();
    FSPermissionChecker pc = fsd.getPermissionChecker();
    List<XAttr> xAttrs = Lists.newArrayListWithCapacity(1);
    INodesInPath iip;
    fsd.writeLock();
    try {

      // check operation permission.
      iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }
      XAttr satisfyXAttr = unprotectedSatisfyStoragePolicy(iip, bm, fsd);
      xAttrs.add(satisfyXAttr);
      fsd.getEditLog().logSetXAttrs(src, xAttrs, logRetryCache);
    } finally {
      fsd.writeUnlock();
    }
    return fsd.getAuditFileInfo(iip);
  }

  static XAttr unprotectedSatisfyStoragePolicy(INodesInPath iip,
      BlockManager bm, FSDirectory fsd) throws IOException {

    final INode inode = FSDirectory.resolveLastINode(iip);
    final int snapshotId = iip.getLatestSnapshotId();
    final List<INode> candidateNodes = new ArrayList<>();

    // TODO: think about optimization here, label the dir instead
    // of the sub-files of the dir.
    if (inode.isFile()) {
      candidateNodes.add(inode);
    } else if (inode.isDirectory()) {
      for (INode node : inode.asDirectory().getChildrenList(snapshotId)) {
        if (node.isFile()) {
          candidateNodes.add(node);
        }
      }
    }

    // If node has satisfy xattr, then stop adding it
    // to satisfy movement queue.
    if (inodeHasSatisfyXAttr(candidateNodes)) {
      throw new IOException(
          "Cannot request to call satisfy storage policy on path "
              + iip.getPath()
              + ", as this file/dir was already called for satisfying "
              + "storage policy.");
    }

    final List<XAttr> xattrs = Lists.newArrayListWithCapacity(1);
    final XAttr satisfyXAttr = XAttrHelper
        .buildXAttr(XATTR_SATISFY_STORAGE_POLICY);
    xattrs.add(satisfyXAttr);

    for (INode node : candidateNodes) {
      bm.satisfyStoragePolicy(node.getId());
      List<XAttr> existingXAttrs = XAttrStorage.readINodeXAttrs(node);
      List<XAttr> newXAttrs = FSDirXAttrOp.setINodeXAttrs(fsd, existingXAttrs,
          xattrs, EnumSet.of(XAttrSetFlag.CREATE));
      XAttrStorage.updateINodeXAttrs(node, newXAttrs, snapshotId);
    }
    return satisfyXAttr;
  }

  private static boolean inodeHasSatisfyXAttr(List<INode> candidateNodes) {
    // If the node is a directory and one of the child files
    // has satisfy xattr, then return true for this directory.
    for (INode inode : candidateNodes) {
      final XAttrFeature f = inode.getXAttrFeature();
      if (inode.isFile() && f != null
          && f.getXAttr(XATTR_SATISFY_STORAGE_POLICY) != null) {
        return true;
      }
    }
    return false;
  }

  static void removeSPSXattr(FSDirectory fsd, INode inode, XAttr spsXAttr)
      throws IOException {
    try {
      fsd.writeLock();
      List<XAttr> existingXAttrs = XAttrStorage.readINodeXAttrs(inode);
      existingXAttrs.remove(spsXAttr);
      XAttrStorage.updateINodeXAttrs(inode, existingXAttrs, INodesInPath
          .fromINode(inode).getLatestSnapshotId());
      List<XAttr> xAttrs = Lists.newArrayListWithCapacity(1);
      xAttrs.add(spsXAttr);
      fsd.getEditLog().logRemoveXAttrs(inode.getFullPathName(), xAttrs, false);
    } finally {
      fsd.writeUnlock();
    }
  }
}

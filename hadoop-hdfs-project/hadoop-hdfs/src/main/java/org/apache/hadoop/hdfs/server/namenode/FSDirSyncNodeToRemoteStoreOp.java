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


import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfyManager;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.XATTR_SYNC_TO_EXTERNAL;

final class FSDirSyncNodeToRemoteStoreOp {
  /**
   * Private constructor for preventing FSDirSyncNodeToExternalStoreOp object
   * creation. Static-only class.
   */
  private FSDirSyncNodeToRemoteStoreOp() {
  }

  /**
   * Satisfy storage policy function which will add the entry to SPS call queue
   * and will perform satisfaction async way.
   *
   * @param fsd
   *          fs directory
   * @param bm
   *          block manager
   * @param src
   *          source path
   * @param logRetryCache
   *          whether to record RPC ids in editlog for retry cache rebuilding
   * @return file status info
   * @throws IOException
   */
  static FileStatus syncCreateToRemoteStore(FSDirectory fsd, BlockManager bm,
      String src, boolean logRetryCache) throws IOException {
    FSPermissionChecker pc = fsd.getPermissionChecker();
    INodesInPath iip;
    fsd.readLock();
    try {
      // check operation permission.
      iip = fsd.resolvePath(pc, src, DirOp.READ);
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.READ);
      }
      INode inode = FSDirectory.resolveLastINode(iip);
      if (inode.isFile() && inode.asFile().numBlocks() == 0) {
        if (NameNode.LOG.isInfoEnabled()) {
          NameNode.LOG.info(
              "Skipping sync node to external store on path:{} as "
                  + "this file doesn't have any blocks!",
              inode.getFullPathName());
        }
      } else if (inodeHasSyncToExternalXAttr(inode)) {
        NameNode.LOG
            .warn("Cannot request sync node to external store on path: "
                + inode.getFullPathName()
                + ", as this file/dir was already called for sync.");
      } else {
        XAttr syncToExternalXAttr = XAttrHelper
            .buildXAttr(XATTR_SYNC_TO_EXTERNAL);
        List<XAttr> xAttrs = Arrays.asList(syncToExternalXAttr);
        List<XAttr> existingXAttrs = XAttrStorage.readINodeXAttrs(inode);
        List<XAttr> newXAttrs = FSDirXAttrOp.setINodeXAttrs(fsd, existingXAttrs,
            xAttrs, EnumSet.of(XAttrSetFlag.CREATE));
        XAttrStorage.updateINodeXAttrs(inode, newXAttrs,
            iip.getLatestSnapshotId());
        fsd.getEditLog().logSetXAttrs(src, xAttrs, logRetryCache);

        // Adding directory in the pending queue, so FileInodeIdCollector
        // process directory child in batch and recursively
        StoragePolicySatisfyManager spsManager =
            fsd.getBlockManager().getSPSManager();
        if (spsManager != null) {
          spsManager.addPathId(inode.getId());
        }
      }
    } finally {
      fsd.readUnlock();
    }
    return fsd.getAuditFileInfo(iip);
  }

  static FileStatus syncRenameToRemoteStore(FSDirectory fsd, BlockManager bm,
      String src, String dest, boolean logRetryCache) throws IOException {
    FSPermissionChecker pc = fsd.getPermissionChecker();
    INodesInPath iip;
    fsd.readLock();
    try {
      // check operation permission.
      iip = fsd.resolvePath(pc, src, DirOp.READ);
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.READ);
      }
      INode inode = FSDirectory.resolveLastINode(iip);
      if (inode.isFile() && inode.asFile().numBlocks() == 0) {
        if (NameNode.LOG.isInfoEnabled()) {
          NameNode.LOG.info(
              "Skipping sync node to external store on path:{} as "
                  + "this file doesn't have any blocks!",
              inode.getFullPathName());
        }
      } else if (inodeHasSyncToExternalXAttr(inode)) {
        NameNode.LOG
            .warn("Cannot request sync node to external store on path: "
                + inode.getFullPathName()
                + ", as this file/dir was already called for sync.");
      } else {
        XAttr syncToExternalXAttr = XAttrHelper
            .buildXAttr(XATTR_SYNC_TO_EXTERNAL);
        List<XAttr> xAttrs = Arrays.asList(syncToExternalXAttr);
        List<XAttr> existingXAttrs = XAttrStorage.readINodeXAttrs(inode);
        List<XAttr> newXAttrs = FSDirXAttrOp.setINodeXAttrs(fsd, existingXAttrs,
            xAttrs, EnumSet.of(XAttrSetFlag.CREATE));
        XAttrStorage.updateINodeXAttrs(inode, newXAttrs,
            iip.getLatestSnapshotId());
        fsd.getEditLog().logSetXAttrs(src, xAttrs, logRetryCache);

        // Adding directory in the pending queue, so FileInodeIdCollector
        // process directory child in batch and recursively
        StoragePolicySatisfyManager spsManager =
            fsd.getBlockManager().getSPSManager();
        if (spsManager != null) {
          spsManager.addPathId(inode.getId());
        }
      }
    } finally {
      fsd.readUnlock();
    }
    return fsd.getAuditFileInfo(iip);
  }
  static FileStatus syncDeleteToRemoteStore(FSDirectory fsd, BlockManager bm,
      String src, boolean logRetryCache) throws IOException {
    FSPermissionChecker pc = fsd.getPermissionChecker();
    INodesInPath iip;
    fsd.readLock();
    try {
      // check operation permission.
      iip = fsd.resolvePath(pc, src, DirOp.READ);
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.READ);
      }
      INode inode = FSDirectory.resolveLastINode(iip);
      if (inode.isFile() && inode.asFile().numBlocks() == 0) {
        if (NameNode.LOG.isInfoEnabled()) {
          NameNode.LOG.info(
              "Skipping sync node to external store on path:{} as "
                  + "this file doesn't have any blocks!",
              inode.getFullPathName());
        }
      } else if (inodeHasSyncToExternalXAttr(inode)) {
        NameNode.LOG
            .warn("Cannot request sync node to external store on path: "
                + inode.getFullPathName()
                + ", as this file/dir was already called for sync.");
      } else {
        XAttr syncToExternalXAttr = XAttrHelper
            .buildXAttr(XATTR_SYNC_TO_EXTERNAL);
        List<XAttr> xAttrs = Arrays.asList(syncToExternalXAttr);
        List<XAttr> existingXAttrs = XAttrStorage.readINodeXAttrs(inode);
        List<XAttr> newXAttrs = FSDirXAttrOp.setINodeXAttrs(fsd, existingXAttrs,
            xAttrs, EnumSet.of(XAttrSetFlag.CREATE));
        XAttrStorage.updateINodeXAttrs(inode, newXAttrs,
            iip.getLatestSnapshotId());
        fsd.getEditLog().logSetXAttrs(src, xAttrs, logRetryCache);

        // Adding directory in the pending queue, so FileInodeIdCollector
        // process directory child in batch and recursively
        StoragePolicySatisfyManager spsManager =
            fsd.getBlockManager().getSPSManager();
        if (spsManager != null) {
          spsManager.addPathId(inode.getId());
        }
      }
    } finally {
      fsd.readUnlock();
    }
    return fsd.getAuditFileInfo(iip);
  }

  private static boolean inodeHasSyncToExternalXAttr(INode inode) {
    final XAttrFeature f = inode.getXAttrFeature();
    if (inode.isFile() && f != null
        && f.getXAttr(XATTR_SYNC_TO_EXTERNAL) != null) {
      return true;
    }
    return false;
  }
}

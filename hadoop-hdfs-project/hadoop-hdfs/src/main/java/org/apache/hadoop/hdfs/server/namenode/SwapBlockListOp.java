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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.INodeFile.HeaderFormat;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.util.Time;

/**
 * Class to carry out the operation of swapping blocks from one file to another.
 * Along with swapping blocks, we can also optionally swap the block layout
 * of a file header, which is useful for client operations like converting
 * replicated to EC file.
 */
public final class SwapBlockListOp {

  private SwapBlockListOp() {
  }

  static SwapBlockListResult swapBlocks(FSDirectory fsd, FSPermissionChecker pc,
                          String src, String dst, long genTimestamp)
      throws IOException {

    final INodesInPath srcIIP = fsd.resolvePath(pc, src, DirOp.WRITE);
    final INodesInPath dstIIP = fsd.resolvePath(pc, dst, DirOp.WRITE);
    if (fsd.isPermissionEnabled()) {
      fsd.checkAncestorAccess(pc, srcIIP, FsAction.WRITE);
      fsd.checkAncestorAccess(pc, dstIIP, FsAction.WRITE);
    }
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.swapBlockList: "
          + srcIIP.getPath() + " and " + dstIIP.getPath());
    }
    SwapBlockListResult result = null;
    fsd.writeLock();
    try {
      result = swapBlockList(fsd, srcIIP, dstIIP, genTimestamp);
    } finally {
      fsd.writeUnlock();
    }
    return result;
  }

  private static SwapBlockListResult swapBlockList(FSDirectory fsd,
                                    final INodesInPath srcIIP,
                                    final INodesInPath dstIIP,
                                    long genTimestamp)
      throws IOException {

    assert fsd.hasWriteLock();
    validateInode(srcIIP);
    validateInode(dstIIP);
    fsd.ezManager.checkMoveValidity(srcIIP, dstIIP);

    final String src = srcIIP.getPath();
    final String dst = dstIIP.getPath();
    if (dst.equals(src)) {
      throw new FileAlreadyExistsException("The source " + src +
          " and destination " + dst + " are the same");
    }

    INodeFile srcINodeFile = (INodeFile) srcIIP.getLastINode();
    INodeFile dstINodeFile = (INodeFile) dstIIP.getLastINode();

    String errorPrefix = "DIR* FSDirectory.swapBlockList: ";
    String error = "Swap Block List destination file ";
    BlockInfo lastBlock = dstINodeFile.getLastBlock();
    if (lastBlock != null && lastBlock.getGenerationStamp() != genTimestamp) {
      error  += dstIIP.getPath() +
          " has last block with different gen timestamp.";
      NameNode.stateChangeLog.warn(errorPrefix + error);
      throw new IOException(error);
    }

    long mtime = Time.now();
    BlockInfo[] dstINodeFileBlocks = dstINodeFile.getBlocks();
    dstINodeFile.replaceBlocks(srcINodeFile.getBlocks());
    srcINodeFile.replaceBlocks(dstINodeFileBlocks);

    long srcHeader = srcINodeFile.getHeaderLong();
    long dstHeader = dstINodeFile.getHeaderLong();

    byte dstBlockLayoutPolicy =
        HeaderFormat.getBlockLayoutPolicy(dstHeader);
    byte srcBlockLayoutPolicy =
        HeaderFormat.getBlockLayoutPolicy(srcHeader);

    byte dstStoragePolicyID = HeaderFormat.getStoragePolicyID(dstHeader);
    byte srcStoragePolicyID = HeaderFormat.getStoragePolicyID(srcHeader);

    dstINodeFile.updateHeaderWithNewPolicy(srcBlockLayoutPolicy,
        srcStoragePolicyID);
    dstINodeFile.setModificationTime(mtime);

    srcINodeFile.updateHeaderWithNewPolicy(dstBlockLayoutPolicy,
        dstStoragePolicyID);
    srcINodeFile.setModificationTime(mtime);

    return new SwapBlockListResult(true,
        fsd.getAuditFileInfo(srcIIP),
        fsd.getAuditFileInfo(dstIIP));
  }

  private static void validateInode(INodesInPath srcIIP)
      throws IOException {

    String errorPrefix = "DIR* FSDirectory.swapBlockList: ";
    String error = "Swap Block List input ";
    final INode srcInode = srcIIP.getLastINode();

    // Check if INode is null.
    if (srcInode == null) {
      error  += srcIIP.getPath() + " is not found.";
      NameNode.stateChangeLog.warn(errorPrefix + error);
      throw new FileNotFoundException(error);
    }

    // Check if INode is a file and NOT a directory.
    if (!srcInode.isFile()) {
      error  += srcIIP.getPath() + " is not a file.";
      NameNode.stateChangeLog.warn(errorPrefix + error);
      throw new IOException(error);
    }

    // Check if file is under construction.
    INodeFile iNodeFile = (INodeFile) srcIIP.getLastINode();
    if (iNodeFile.isUnderConstruction()) {
      error  += srcIIP.getPath() + " is under construction.";
      NameNode.stateChangeLog.warn(errorPrefix + error);
      throw new IOException(error);
    }

    // Check if any parent directory is in a snapshot.
    if (srcIIP.getLatestSnapshotId() != Snapshot.CURRENT_STATE_ID) {
      error  += srcIIP.getPath() + " is in a snapshot directory.";
      NameNode.stateChangeLog.warn(errorPrefix + error);
      throw new IOException(error);
    }
  }

  static class SwapBlockListResult {
    private final boolean success;
    private final FileStatus srcFileAuditStat;
    private final FileStatus dstFileAuditStat;

    SwapBlockListResult(boolean success,
                        FileStatus srcFileAuditStat,
                        FileStatus dstFileAuditStat) {
      this.success = success;
      this.srcFileAuditStat = srcFileAuditStat;
      this.dstFileAuditStat = dstFileAuditStat;
    }

    public boolean isSuccess() {
      return success;
    }

    public FileStatus getDstFileAuditStat() {
      return dstFileAuditStat;
    }

    public FileStatus getSrcFileAuditStat() {
      return srcFileAuditStat;
    }
  }
}

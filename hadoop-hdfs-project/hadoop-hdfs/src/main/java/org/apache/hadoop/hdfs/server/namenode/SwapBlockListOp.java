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
import java.util.Arrays;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Options;
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
public class SwapBlockListOp {

  static SwapBlockListResult swapBlocks(FSDirectory fsd, FSPermissionChecker pc,
                          String src, String dst,
                          Options.SwapBlockList... options) throws IOException {

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
      result = swapBlockList(fsd, srcIIP, dstIIP, options);
    } finally {
      fsd.writeUnlock();
    }
    return result;
  }

  private static SwapBlockListResult swapBlockList(FSDirectory fsd,
                                    final INodesInPath srcIIP,
                                    final INodesInPath dstIIP,
                                    Options.SwapBlockList... options)
      throws IOException {

    assert fsd.hasWriteLock();
    validateInode(fsd, srcIIP);
    validateInode(fsd, dstIIP);
    fsd.ezManager.checkMoveValidity(srcIIP, dstIIP);

    final String src = srcIIP.getPath();
    final String dst = dstIIP.getPath();
    if (dst.equals(src)) {
      throw new FileAlreadyExistsException("The source " + src +
          " and destination " + dst + " are the same");
    }

    INodeFile srcINodeFile = (INodeFile) srcIIP.getLastINode();
    INodeFile dstINodeFile = (INodeFile) dstIIP.getLastINode();

    long mtime = Time.now();
    BlockInfo[] dstINodeFileBlocks = dstINodeFile.getBlocks();
    dstINodeFile.replaceBlocks(srcINodeFile.getBlocks());

    boolean overwrite = options != null
        && Arrays.asList(options).contains(
            Options.SwapBlockList.ONE_WAY_BLOCK_SWAP);
    if (!overwrite) {
      srcINodeFile.replaceBlocks(dstINodeFileBlocks);
    }

    boolean excludeHeader = options != null &&
        Arrays.asList(options).contains(
            Options.SwapBlockList.EXCLUDE_BLOCK_LAYOUT_HEADER_SWAP);
    if (!excludeHeader) {
      long srcHeader = srcINodeFile.getHeaderLong();
      long dstHeader = dstINodeFile.getHeaderLong();

      byte srcBlockLayoutPolicy =
          HeaderFormat.getBlockLayoutPolicy(srcHeader);
      dstINodeFile.updateHeaderWithNewBlockLayoutPolicy(srcBlockLayoutPolicy);

      if (!overwrite) {
        byte dstBlockLayoutPolicy =
            HeaderFormat.getBlockLayoutPolicy(dstHeader);
        srcINodeFile.updateHeaderWithNewBlockLayoutPolicy(dstBlockLayoutPolicy);
        srcINodeFile.setModificationTime(mtime);
      }
    }
    // Update modification time.
    dstINodeFile.setModificationTime(mtime);

    return new SwapBlockListResult(true,
        fsd.getAuditFileInfo(srcIIP),
        fsd.getAuditFileInfo(dstIIP));
  }

  private static void validateInode(FSDirectory fsd, INodesInPath srcIIP)
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
    final boolean success;
    final FileStatus srcFileAuditStat;
    final FileStatus dstFileAuditStat;

    SwapBlockListResult(boolean success,
                        FileStatus srcFileAuditStat,
                        FileStatus dstFileAuditStat) {
      this.success = success;
      this.srcFileAuditStat = srcFileAuditStat;
      this.dstFileAuditStat = dstFileAuditStat;
    }
  }
}

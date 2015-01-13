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
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.hadoop.util.Time.now;

class FSDirConcatOp {
  static HdfsFileStatus concat(
    FSDirectory fsd, String target, String[] srcs,
    boolean logRetryCache) throws IOException {
    Preconditions.checkArgument(!target.isEmpty(), "Target file name is empty");
    Preconditions.checkArgument(srcs != null && srcs.length > 0,
      "No sources given");
    assert srcs != null;

    FSDirectory.LOG.debug("concat {} to {}", Arrays.toString(srcs), target);
    // We require all files be in the same directory
    String trgParent =
      target.substring(0, target.lastIndexOf(Path.SEPARATOR_CHAR));
    for (String s : srcs) {
      String srcParent = s.substring(0, s.lastIndexOf(Path.SEPARATOR_CHAR));
      if (!srcParent.equals(trgParent)) {
        throw new IllegalArgumentException(
           "Sources and target are not in the same directory");
      }
    }

    final INodesInPath trgIip = fsd.getINodesInPath4Write(target);
    // write permission for the target
    if (fsd.isPermissionEnabled()) {
      FSPermissionChecker pc = fsd.getPermissionChecker();
      fsd.checkPathAccess(pc, trgIip, FsAction.WRITE);

      // and srcs
      for(String aSrc: srcs) {
        final INodesInPath srcIip = fsd.getINodesInPath4Write(aSrc);
        fsd.checkPathAccess(pc, srcIip, FsAction.READ); // read the file
        fsd.checkParentAccess(pc, srcIip, FsAction.WRITE); // for delete
      }
    }

    // to make sure no two files are the same
    Set<INode> si = new HashSet<INode>();

    // we put the following prerequisite for the operation
    // replication and blocks sizes should be the same for ALL the blocks

    // check the target
    if (fsd.getEZForPath(trgIip) != null) {
      throw new HadoopIllegalArgumentException(
          "concat can not be called for files in an encryption zone.");
    }
    final INodeFile trgInode = INodeFile.valueOf(trgIip.getLastINode(), target);
    if(trgInode.isUnderConstruction()) {
      throw new HadoopIllegalArgumentException("concat: target file "
          + target + " is under construction");
    }
    // per design target shouldn't be empty and all the blocks same size
    if(trgInode.numBlocks() == 0) {
      throw new HadoopIllegalArgumentException("concat: target file "
          + target + " is empty");
    }
    if (trgInode.isWithSnapshot()) {
      throw new HadoopIllegalArgumentException("concat: target file "
          + target + " is in a snapshot");
    }

    long blockSize = trgInode.getPreferredBlockSize();

    // check the end block to be full
    final BlockInfo last = trgInode.getLastBlock();
    if(blockSize != last.getNumBytes()) {
      throw new HadoopIllegalArgumentException("The last block in " + target
          + " is not full; last block size = " + last.getNumBytes()
          + " but file block size = " + blockSize);
    }

    si.add(trgInode);
    final short repl = trgInode.getFileReplication();

    // now check the srcs
    boolean endSrc = false; // final src file doesn't have to have full end block
    for(int i=0; i< srcs.length; i++) {
      String src = srcs[i];
      if(i== srcs.length-1)
        endSrc=true;

      final INodeFile srcInode = INodeFile.valueOf(fsd.getINode4Write(src), src);
      if(src.isEmpty()
          || srcInode.isUnderConstruction()
          || srcInode.numBlocks() == 0) {
        throw new HadoopIllegalArgumentException("concat: source file " + src
            + " is invalid or empty or underConstruction");
      }

      // check replication and blocks size
      if(repl != srcInode.getBlockReplication()) {
        throw new HadoopIllegalArgumentException("concat: the source file "
            + src + " and the target file " + target
            + " should have the same replication: source replication is "
            + srcInode.getBlockReplication()
            + " but target replication is " + repl);
      }

      //boolean endBlock=false;
      // verify that all the blocks are of the same length as target
      // should be enough to check the end blocks
      final BlockInfo[] srcBlocks = srcInode.getBlocks();
      int idx = srcBlocks.length-1;
      if(endSrc)
        idx = srcBlocks.length-2; // end block of endSrc is OK not to be full
      if(idx >= 0 && srcBlocks[idx].getNumBytes() != blockSize) {
        throw new HadoopIllegalArgumentException("concat: the source file "
            + src + " and the target file " + target
            + " should have the same blocks sizes: target block size is "
            + blockSize + " but the size of source block " + idx + " is "
            + srcBlocks[idx].getNumBytes());
      }

      si.add(srcInode);
    }

    // make sure no two files are the same
    if(si.size() < srcs.length+1) { // trg + srcs
      // it means at least two files are the same
      throw new HadoopIllegalArgumentException(
          "concat: at least two of the source files are the same");
    }

    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.concat: " +
          Arrays.toString(srcs) + " to " + target);
    }

    long timestamp = now();
    fsd.writeLock();
    try {
      unprotectedConcat(fsd, target, srcs, timestamp);
    } finally {
      fsd.writeUnlock();
    }
    fsd.getEditLog().logConcat(target, srcs, timestamp, logRetryCache);
    return fsd.getAuditFileInfo(trgIip);
  }

  /**
   * Concat all the blocks from srcs to trg and delete the srcs files
   * @param fsd FSDirectory
   * @param target target file to move the blocks to
   * @param srcs list of file to move the blocks from
   */
  static void unprotectedConcat(
    FSDirectory fsd, String target, String[] srcs, long timestamp)
    throws IOException {
    assert fsd.hasWriteLock();
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSNamesystem.concat to "+target);
    }
    // do the move

    final INodesInPath trgIIP = fsd.getINodesInPath4Write(target, true);
    final INodeFile trgInode = trgIIP.getLastINode().asFile();
    INodeDirectory trgParent = trgIIP.getINode(-2).asDirectory();
    final int trgLatestSnapshot = trgIIP.getLatestSnapshotId();

    final INodeFile [] allSrcInodes = new INodeFile[srcs.length];
    for(int i = 0; i < srcs.length; i++) {
      final INodesInPath iip = fsd.getINodesInPath4Write(srcs[i]);
      final int latest = iip.getLatestSnapshotId();
      final INode inode = iip.getLastINode();

      // check if the file in the latest snapshot
      if (inode.isInLatestSnapshot(latest)) {
        throw new SnapshotException("Concat: the source file " + srcs[i]
            + " is in snapshot " + latest);
      }

      // check if the file has other references.
      if (inode.isReference() && ((INodeReference.WithCount)
          inode.asReference().getReferredINode()).getReferenceCount() > 1) {
        throw new SnapshotException("Concat: the source file " + srcs[i]
            + " is referred by some other reference in some snapshot.");
      }

      allSrcInodes[i] = inode.asFile();
    }
    trgInode.concatBlocks(allSrcInodes);

    // since we are in the same dir - we can use same parent to remove files
    int count = 0;
    for(INodeFile nodeToRemove: allSrcInodes) {
      if(nodeToRemove == null) continue;

      nodeToRemove.setBlocks(null);
      trgParent.removeChild(nodeToRemove, trgLatestSnapshot);
      fsd.getINodeMap().remove(nodeToRemove);
      count++;
    }

    trgInode.setModificationTime(timestamp, trgLatestSnapshot);
    trgParent.updateModificationTime(timestamp, trgLatestSnapshot);
    // update quota on the parent directory ('count' files removed, 0 space)
    FSDirectory.unprotectedUpdateCount(trgIIP, trgIIP.length() - 1, -count, 0);
  }
}

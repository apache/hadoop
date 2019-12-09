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
import java.util.List;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.RecoverLeaseOp;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion.Feature;
import org.apache.hadoop.ipc.RetriableException;

import com.google.common.base.Preconditions;

/**
 * Helper class to perform append operation.
 */
final class FSDirAppendOp {

  /**
   * Private constructor for preventing FSDirAppendOp object creation.
   * Static-only class.
   */
  private FSDirAppendOp() {}

  /**
   * Append to an existing file.
   * <p>
   *
   * The method returns the last block of the file if this is a partial block,
   * which can still be used for writing more data. The client uses the
   * returned block locations to form the data pipeline for this block.<br>
   * The {@link LocatedBlock} will be null if the last block is full.
   * The client then allocates a new block with the next call using
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#addBlock}.
   * <p>
   *
   * For description of parameters and exceptions thrown see
   * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#append}
   *
   * @param fsn namespace
   * @param srcArg path name
   * @param pc permission checker to check fs permission
   * @param holder client name
   * @param clientMachine client machine info
   * @param newBlock if the data is appended to a new block
   * @param logRetryCache whether to record RPC ids in editlog for retry cache
   *                      rebuilding
   * @return the last block with status
   */
  static LastBlockWithStatus appendFile(final FSNamesystem fsn,
      final String srcArg, final FSPermissionChecker pc, final String holder,
      final String clientMachine, final boolean newBlock,
      final boolean logRetryCache) throws IOException {
    assert fsn.hasWriteLock();

    final LocatedBlock lb;
    final FSDirectory fsd = fsn.getFSDirectory();
    final INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, srcArg, DirOp.WRITE);
      // Verify that the destination does not exist as a directory already
      final INode inode = iip.getLastINode();
      final String path = iip.getPath();
      if (inode != null && inode.isDirectory()) {
        throw new FileAlreadyExistsException("Cannot append to directory "
            + path + "; already exists as a directory.");
      }
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }

      if (inode == null) {
        throw new FileNotFoundException(
            "Failed to append to non-existent file " + path + " for client "
                + clientMachine);
      }
      final INodeFile file = INodeFile.valueOf(inode, path, true);

      // not support appending file with striped blocks
      if (file.isStriped()) {
        throw new UnsupportedOperationException(
            "Cannot append to files with striped block " + path);
      }

      BlockManager blockManager = fsd.getBlockManager();
      final BlockStoragePolicy lpPolicy = blockManager
          .getStoragePolicy("LAZY_PERSIST");
      if (lpPolicy != null && lpPolicy.getId() == file.getStoragePolicyID()) {
        throw new UnsupportedOperationException(
            "Cannot append to lazy persist file " + path);
      }
      // Opening an existing file for append - may need to recover lease.
      fsn.recoverLeaseInternal(RecoverLeaseOp.APPEND_FILE, iip, path, holder,
          clientMachine, false);

      final BlockInfo lastBlock = file.getLastBlock();
      // Check that the block has at least minimum replication.
      if (lastBlock != null) {
        if (lastBlock.getBlockUCState() == BlockUCState.COMMITTED) {
          throw new RetriableException(
              new NotReplicatedYetException("append: lastBlock="
                  + lastBlock + " of src=" + path
                  + " is COMMITTED but not yet COMPLETE."));
        } else if (lastBlock.isComplete()
          && !blockManager.isSufficientlyReplicated(lastBlock)) {
          throw new IOException("append: lastBlock=" + lastBlock + " of src="
              + path + " is not sufficiently replicated yet.");
        }
      }
      lb = prepareFileForAppend(fsn, iip, holder, clientMachine, newBlock,
          true, logRetryCache);
    } catch (IOException ie) {
      NameNode.stateChangeLog
          .warn("DIR* NameSystem.append: " + ie.getMessage());
      throw ie;
    } finally {
      fsd.writeUnlock();
    }

    HdfsFileStatus stat =
        FSDirStatAndListingOp.getFileInfo(fsd, iip, false, false);
    if (lb != null) {
      NameNode.stateChangeLog.debug(
          "DIR* NameSystem.appendFile: file {} for {} at {} block {} block"
              + " size {}", srcArg, holder, clientMachine, lb.getBlock(), lb
              .getBlock().getNumBytes());
    }
    return new LastBlockWithStatus(lb, stat);
  }

  /**
   * Convert current node to under construction.
   * Recreate in-memory lease record.
   *
   * @param fsn namespace
   * @param iip inodes in the path containing the file
   * @param leaseHolder identifier of the lease holder on this file
   * @param clientMachine identifier of the client machine
   * @param newBlock if the data is appended to a new block
   * @param writeToEditLog whether to persist this change to the edit log
   * @param logRetryCache whether to record RPC ids in editlog for retry cache
   *                      rebuilding
   * @return the last block locations if the block is partial or null otherwise
   * @throws IOException
   */
  static LocatedBlock prepareFileForAppend(final FSNamesystem fsn,
      final INodesInPath iip, final String leaseHolder,
      final String clientMachine, final boolean newBlock,
      final boolean writeToEditLog, final boolean logRetryCache)
      throws IOException {
    assert fsn.hasWriteLock();

    final INodeFile file = iip.getLastINode().asFile();
    final QuotaCounts delta = verifyQuotaForUCBlock(fsn, file, iip);

    file.recordModification(iip.getLatestSnapshotId());
    file.toUnderConstruction(leaseHolder, clientMachine);

    fsn.getLeaseManager().addLease(
        file.getFileUnderConstructionFeature().getClientName(), file.getId());

    LocatedBlock ret = null;
    if (!newBlock) {
      FSDirectory fsd = fsn.getFSDirectory();
      ret = fsd.getBlockManager().convertLastBlockToUnderConstruction(file, 0);
      if (ret != null && delta != null) {
        Preconditions.checkState(delta.getStorageSpace() >= 0, "appending to"
            + " a block with size larger than the preferred block size");
        fsd.writeLock();
        try {
          fsd.updateCountNoQuotaCheck(iip, iip.length() - 1, delta);
        } finally {
          fsd.writeUnlock();
        }
      }
    } else {
      BlockInfo lastBlock = file.getLastBlock();
      if (lastBlock != null) {
        ExtendedBlock blk = new ExtendedBlock(fsn.getBlockPoolId(), lastBlock);
        ret = new LocatedBlock(blk, new DatanodeInfo[0]);
      }
    }

    if (writeToEditLog) {
      final String path = iip.getPath();
      if (NameNodeLayoutVersion.supports(Feature.APPEND_NEW_BLOCK,
          fsn.getEffectiveLayoutVersion())) {
        fsn.getEditLog().logAppendFile(path, file, newBlock, logRetryCache);
      } else {
        fsn.getEditLog().logOpenFile(path, file, false, logRetryCache);
      }
    }
    return ret;
  }

  /**
   * Verify quota when using the preferred block size for UC block. This is
   * usually used by append and truncate.
   *
   * @throws QuotaExceededException when violating the storage quota
   * @return expected quota usage update. null means no change or no need to
   *         update quota usage later
   */
  private static QuotaCounts verifyQuotaForUCBlock(FSNamesystem fsn,
      INodeFile file, INodesInPath iip) throws QuotaExceededException {
    FSDirectory fsd = fsn.getFSDirectory();
    if (!fsn.isImageLoaded() || fsd.shouldSkipQuotaChecks()) {
      // Do not check quota if editlog is still being processed
      return null;
    }
    if (file.getLastBlock() != null) {
      final QuotaCounts delta = computeQuotaDeltaForUCBlock(fsn, file);
      fsd.readLock();
      try {
        FSDirectory.verifyQuota(iip, iip.length() - 1, delta, null);
        return delta;
      } finally {
        fsd.readUnlock();
      }
    }
    return null;
  }

  /** Compute quota change for converting a complete block to a UC block. */
  private static QuotaCounts computeQuotaDeltaForUCBlock(FSNamesystem fsn,
      INodeFile file) {
    final QuotaCounts delta = new QuotaCounts.Builder().build();
    final BlockInfo lastBlock = file.getLastBlock();
    if (lastBlock != null) {
      final long diff = file.getPreferredBlockSize() - lastBlock.getNumBytes();
      final short repl = lastBlock.getReplication();
      delta.addStorageSpace(diff * repl);
      final BlockStoragePolicy policy = fsn.getFSDirectory()
          .getBlockStoragePolicySuite().getPolicy(file.getStoragePolicyID());
      List<StorageType> types = policy.chooseStorageTypes(repl);
      for (StorageType t : types) {
        if (t.supportTypeQuota()) {
          delta.addTypeSpace(t, diff);
        }
      }
    }
    return delta;
  }
}

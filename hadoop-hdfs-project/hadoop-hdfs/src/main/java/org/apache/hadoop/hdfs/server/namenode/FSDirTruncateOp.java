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

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockUnderConstructionFeature;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.RecoverLeaseOp;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;

import com.google.common.annotations.VisibleForTesting;

/**
 * Helper class to perform truncate operation.
 */
final class FSDirTruncateOp {

  /**
   * Private constructor for preventing FSDirTruncateOp object creation.
   * Static-only class.
   */
  private FSDirTruncateOp() {}

  /**
   * Truncate a file to a given size.
   *
   * @param fsn namespace
   * @param srcArg path name
   * @param newLength the target file size
   * @param clientName client name
   * @param clientMachine client machine info
   * @param mtime modified time
   * @param toRemoveBlocks to be removed blocks
   * @param pc permission checker to check fs permission
   * @return tuncate result
   * @throws IOException
   */
  static TruncateResult truncate(final FSNamesystem fsn, final String srcArg,
      final long newLength, final String clientName,
      final String clientMachine, final long mtime,
      final BlocksMapUpdateInfo toRemoveBlocks, final FSPermissionChecker pc)
      throws IOException, UnresolvedLinkException {
    assert fsn.hasWriteLock();

    FSDirectory fsd = fsn.getFSDirectory();
    final String src;
    final INodesInPath iip;
    final boolean onBlockBoundary;
    Block truncateBlock = null;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, srcArg, DirOp.WRITE);
      src = iip.getPath();
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }
      INodeFile file = INodeFile.valueOf(iip.getLastINode(), src);

      // not support truncating file with striped blocks
      if (file.isStriped()) {
        throw new UnsupportedOperationException(
            "Cannot truncate file with striped block " + src);
      }

      final BlockStoragePolicy lpPolicy = fsd.getBlockManager()
          .getStoragePolicy("LAZY_PERSIST");

      if (lpPolicy != null && lpPolicy.getId() == file.getStoragePolicyID()) {
        throw new UnsupportedOperationException(
            "Cannot truncate lazy persist file " + src);
      }

      // Check if the file is already being truncated with the same length
      final BlockInfo last = file.getLastBlock();
      if (last != null && last.getBlockUCState()
          == BlockUCState.UNDER_RECOVERY) {
        final BlockInfo truncatedBlock = last.getUnderConstructionFeature()
            .getTruncateBlock();
        if (truncatedBlock != null) {
          final long truncateLength = file.computeFileSize(false, false)
              + truncatedBlock.getNumBytes();
          if (newLength == truncateLength) {
            return new TruncateResult(false, fsd.getAuditFileInfo(iip));
          }
        }
      }

      // Opening an existing file for truncate. May need lease recovery.
      fsn.recoverLeaseInternal(RecoverLeaseOp.TRUNCATE_FILE, iip, src,
          clientName, clientMachine, false);
      // Truncate length check.
      long oldLength = file.computeFileSize();
      if (oldLength == newLength) {
        return new TruncateResult(true, fsd.getAuditFileInfo(iip));
      }
      if (oldLength < newLength) {
        throw new HadoopIllegalArgumentException(
            "Cannot truncate to a larger file size. Current size: " + oldLength
                + ", truncate size: " + newLength + ".");
      }
      // Perform INodeFile truncation.
      final QuotaCounts delta = new QuotaCounts.Builder().build();
      onBlockBoundary = unprotectedTruncate(fsn, iip, newLength,
          toRemoveBlocks, mtime, delta);
      if (!onBlockBoundary) {
        // Open file for write, but don't log into edits
        long lastBlockDelta = file.computeFileSize() - newLength;
        assert lastBlockDelta > 0 : "delta is 0 only if on block bounday";
        truncateBlock = prepareFileForTruncate(fsn, iip, clientName,
            clientMachine, lastBlockDelta, null);
      }

      // update the quota: use the preferred block size for UC block
      fsd.updateCountNoQuotaCheck(iip, iip.length() - 1, delta);
    } finally {
      fsd.writeUnlock();
    }

    fsn.getEditLog().logTruncate(src, clientName, clientMachine, newLength,
        mtime, truncateBlock);
    return new TruncateResult(onBlockBoundary, fsd.getAuditFileInfo(iip));
  }

  /**
   * Unprotected truncate implementation. Unlike
   * {@link FSDirTruncateOp#truncate}, this will not schedule block recovery.
   *
   * @param fsn namespace
   * @param iip path name
   * @param clientName client name
   * @param clientMachine client machine info
   * @param newLength the target file size
   * @param mtime modified time
   * @param truncateBlock truncate block
   * @throws IOException
   */
  static void unprotectedTruncate(final FSNamesystem fsn,
      final INodesInPath iip,
      final String clientName, final String clientMachine,
      final long newLength, final long mtime, final Block truncateBlock)
      throws UnresolvedLinkException, QuotaExceededException,
      SnapshotAccessControlException, IOException {
    assert fsn.hasWriteLock();

    FSDirectory fsd = fsn.getFSDirectory();
    INodeFile file = iip.getLastINode().asFile();
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    boolean onBlockBoundary = unprotectedTruncate(fsn, iip, newLength,
        collectedBlocks, mtime, null);

    if (!onBlockBoundary) {
      BlockInfo oldBlock = file.getLastBlock();
      Block tBlk = prepareFileForTruncate(fsn, iip, clientName, clientMachine,
          file.computeFileSize() - newLength, truncateBlock);
      assert Block.matchingIdAndGenStamp(tBlk, truncateBlock) &&
          tBlk.getNumBytes() == truncateBlock.getNumBytes() :
          "Should be the same block.";
      if (oldBlock.getBlockId() != tBlk.getBlockId()
          && !file.isBlockInLatestSnapshot(oldBlock)) {
        oldBlock.delete();
        fsd.getBlockManager().removeBlockFromMap(oldBlock);
      }
    }
    assert onBlockBoundary == (truncateBlock == null) :
      "truncateBlock is null iff on block boundary: " + truncateBlock;
    fsn.getBlockManager().removeBlocksAndUpdateSafemodeTotal(collectedBlocks);
  }

  /**
   * Convert current INode to UnderConstruction. Recreate lease. Create new
   * block for the truncated copy. Schedule truncation of the replicas.
   *
   * @param fsn namespace
   * @param iip inodes in the path containing the file
   * @param leaseHolder lease holder
   * @param clientMachine client machine info
   * @param lastBlockDelta last block delta size
   * @param newBlock new block
   * @return the returned block will be written to editLog and passed back
   *         into this method upon loading.
   * @throws IOException
   */
  @VisibleForTesting
  static Block prepareFileForTruncate(FSNamesystem fsn, INodesInPath iip,
      String leaseHolder, String clientMachine, long lastBlockDelta,
      Block newBlock) throws IOException {
    assert fsn.hasWriteLock();

    INodeFile file = iip.getLastINode().asFile();
    assert !file.isStriped();
    file.recordModification(iip.getLatestSnapshotId());
    file.toUnderConstruction(leaseHolder, clientMachine);
    assert file.isUnderConstruction() : "inode should be under construction.";
    fsn.getLeaseManager().addLease(
        file.getFileUnderConstructionFeature().getClientName(), file.getId());
    boolean shouldRecoverNow = (newBlock == null);
    BlockInfo oldBlock = file.getLastBlock();

    boolean shouldCopyOnTruncate = shouldCopyOnTruncate(fsn, file, oldBlock);
    if (newBlock == null) {
      newBlock = (shouldCopyOnTruncate) ?
          fsn.createNewBlock(BlockType.CONTIGUOUS)
          : new Block(oldBlock.getBlockId(), oldBlock.getNumBytes(),
          fsn.nextGenerationStamp(fsn.getBlockManager().isLegacyBlock(
              oldBlock)));
    }

    final BlockInfo truncatedBlockUC;
    BlockManager blockManager = fsn.getFSDirectory().getBlockManager();
    if (shouldCopyOnTruncate) {
      // Add new truncateBlock into blocksMap and
      // use oldBlock as a source for copy-on-truncate recovery
      truncatedBlockUC = new BlockInfoContiguous(newBlock,
          file.getPreferredBlockReplication());
      truncatedBlockUC.convertToBlockUnderConstruction(
          BlockUCState.UNDER_CONSTRUCTION, blockManager.getStorages(oldBlock));
      truncatedBlockUC.setNumBytes(oldBlock.getNumBytes() - lastBlockDelta);
      truncatedBlockUC.getUnderConstructionFeature().setTruncateBlock(oldBlock);
      file.setLastBlock(truncatedBlockUC);
      blockManager.addBlockCollection(truncatedBlockUC, file);

      NameNode.stateChangeLog.debug(
          "BLOCK* prepareFileForTruncate: Scheduling copy-on-truncate to new"
              + " size {}  new block {} old block {}",
          truncatedBlockUC.getNumBytes(), newBlock, oldBlock);
    } else {
      // Use new generation stamp for in-place truncate recovery
      blockManager.convertLastBlockToUnderConstruction(file, lastBlockDelta);
      oldBlock = file.getLastBlock();
      assert !oldBlock.isComplete() : "oldBlock should be under construction";
      BlockUnderConstructionFeature uc = oldBlock.getUnderConstructionFeature();
      uc.setTruncateBlock(new BlockInfoContiguous(oldBlock,
          oldBlock.getReplication()));
      uc.getTruncateBlock().setNumBytes(oldBlock.getNumBytes() - lastBlockDelta);
      uc.getTruncateBlock().setGenerationStamp(newBlock.getGenerationStamp());
      truncatedBlockUC = oldBlock;

      NameNode.stateChangeLog.debug("BLOCK* prepareFileForTruncate: " +
          "{} Scheduling in-place block truncate to new size {}",
          uc, uc.getTruncateBlock().getNumBytes());
    }
    if (shouldRecoverNow) {
      truncatedBlockUC.getUnderConstructionFeature().initializeBlockRecovery(
          truncatedBlockUC, newBlock.getGenerationStamp(), true);
    }

    return newBlock;
  }

  /**
   * Truncate has the following properties:
   * 1.) Any block deletions occur now.
   * 2.) INode length is truncated now - new clients can only read up to
   *     the truncated length.
   * 3.) INode will be set to UC and lastBlock set to UNDER_RECOVERY.
   * 4.) NN will trigger DN truncation recovery and waits for DNs to report.
   * 5.) File is considered UNDER_RECOVERY until truncation recovery
   *     completes.
   * 6.) Soft and hard Lease expiration require truncation recovery to
   *     complete.
   *
   * @return true if on the block boundary or false if recovery is need
   */
  private static boolean unprotectedTruncate(FSNamesystem fsn,
      INodesInPath iip, long newLength, BlocksMapUpdateInfo collectedBlocks,
      long mtime, QuotaCounts delta) throws IOException {
    assert fsn.hasWriteLock();

    INodeFile file = iip.getLastINode().asFile();
    int latestSnapshot = iip.getLatestSnapshotId();
    file.recordModification(latestSnapshot, true);

    verifyQuotaForTruncate(fsn, iip, file, newLength, delta);

    Set<BlockInfo> toRetain = file.getSnapshotBlocksToRetain(latestSnapshot);
    long remainingLength = file.collectBlocksBeyondMax(newLength,
        collectedBlocks, toRetain);
    file.setModificationTime(mtime);
    // return whether on a block boundary
    return (remainingLength - newLength) == 0;
  }

  private static void verifyQuotaForTruncate(FSNamesystem fsn,
      INodesInPath iip, INodeFile file, long newLength, QuotaCounts delta)
      throws QuotaExceededException {
    FSDirectory fsd = fsn.getFSDirectory();
    if (!fsn.isImageLoaded() || fsd.shouldSkipQuotaChecks()) {
      // Do not check quota if edit log is still being processed
      return;
    }
    final BlockStoragePolicy policy = fsd.getBlockStoragePolicySuite()
        .getPolicy(file.getStoragePolicyID());
    file.computeQuotaDeltaForTruncate(newLength, policy, delta);
    fsd.readLock();
    try {
      FSDirectory.verifyQuota(iip, iip.length() - 1, delta, null);
    } finally {
      fsd.readUnlock();
    }
  }

  /**
   * Defines if a replica needs to be copied on truncate or
   * can be truncated in place.
   */
  private static boolean shouldCopyOnTruncate(FSNamesystem fsn, INodeFile file,
      BlockInfo blk) {
    if (!fsn.isUpgradeFinalized()) {
      return true;
    }
    if (fsn.isRollingUpgrade()) {
      return true;
    }
    return file.isBlockInLatestSnapshot(blk);
  }

  /**
   * Result of truncate operation.
   */
  static class TruncateResult {
    private final boolean result;
    private final FileStatus stat;

    public TruncateResult(boolean result, FileStatus stat) {
      this.result = result;
      this.stat = stat;
    }

    /**
     * @return true if client does not need to wait for block recovery,
     *          false if client needs to wait for block recovery.
     */
    boolean getResult() {
      return result;
    }

    /**
     * @return file information.
     */
    FileStatus getFileStatus() {
      return stat;
    }
  }
}

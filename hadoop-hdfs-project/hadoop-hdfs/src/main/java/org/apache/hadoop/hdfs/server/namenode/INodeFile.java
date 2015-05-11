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

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.CURRENT_STATE_ID;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.NO_SNAPSHOT_ID;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStripedUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileDiffList;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileWithSnapshotFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.LongBitFormat;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/** I-node for closed file. */
@InterfaceAudience.Private
public class INodeFile extends INodeWithAdditionalFields
    implements INodeFileAttributes, BlockCollection {

  /** The same as valueOf(inode, path, false). */
  public static INodeFile valueOf(INode inode, String path
      ) throws FileNotFoundException {
    return valueOf(inode, path, false);
  }

  /** Cast INode to INodeFile. */
  public static INodeFile valueOf(INode inode, String path, boolean acceptNull)
      throws FileNotFoundException {
    if (inode == null) {
      if (acceptNull) {
        return null;
      } else {
        throw new FileNotFoundException("File does not exist: " + path);
      }
    }
    if (!inode.isFile()) {
      throw new FileNotFoundException("Path is not a file: " + path);
    }
    return inode.asFile();
  }

  /** 
   * Bit format:
   * [4-bit storagePolicyID][12-bit replication][48-bit preferredBlockSize]
   */
  static enum HeaderFormat {
    PREFERRED_BLOCK_SIZE(null, 48, 1),
    REPLICATION(PREFERRED_BLOCK_SIZE.BITS, 12, 0),
    STORAGE_POLICY_ID(REPLICATION.BITS, BlockStoragePolicySuite.ID_BIT_LENGTH,
        0);

    private final LongBitFormat BITS;

    private HeaderFormat(LongBitFormat previous, int length, long min) {
      BITS = new LongBitFormat(name(), previous, length, min);
    }

    static short getReplication(long header) {
      return (short)REPLICATION.BITS.retrieve(header);
    }

    static long getPreferredBlockSize(long header) {
      return PREFERRED_BLOCK_SIZE.BITS.retrieve(header);
    }

    static byte getStoragePolicyID(long header) {
      return (byte)STORAGE_POLICY_ID.BITS.retrieve(header);
    }

    static long toLong(long preferredBlockSize, short replication,
        byte storagePolicyID) {
      long h = 0;
      if (preferredBlockSize == 0) {
        preferredBlockSize = PREFERRED_BLOCK_SIZE.BITS.getMin();
      }
      h = PREFERRED_BLOCK_SIZE.BITS.combine(preferredBlockSize, h);
      h = REPLICATION.BITS.combine(replication, h);
      h = STORAGE_POLICY_ID.BITS.combine(storagePolicyID, h);
      return h;
    }

  }

  private long header = 0L;

  private BlockInfoContiguous[] blocks;

  INodeFile(long id, byte[] name, PermissionStatus permissions, long mtime,
            long atime, BlockInfoContiguous[] blklist, short replication,
            long preferredBlockSize) {
    this(id, name, permissions, mtime, atime, blklist, replication,
         preferredBlockSize, (byte) 0);
  }

  INodeFile(long id, byte[] name, PermissionStatus permissions, long mtime,
      long atime, BlockInfoContiguous[] blklist, short replication,
      long preferredBlockSize, byte storagePolicyID) {
    super(id, name, permissions, mtime, atime);
    header = HeaderFormat.toLong(preferredBlockSize, replication, storagePolicyID);
    this.blocks = blklist;
  }
  
  public INodeFile(INodeFile that) {
    super(that);
    this.header = that.header;
    this.blocks = that.blocks;
    this.features = that.features;
  }
  
  public INodeFile(INodeFile that, FileDiffList diffs) {
    this(that);
    Preconditions.checkArgument(!that.isWithSnapshot());
    this.addSnapshotFeature(diffs);
  }

  /** @return true unconditionally. */
  @Override
  public final boolean isFile() {
    return true;
  }

  /** @return this object. */
  @Override
  public final INodeFile asFile() {
    return this;
  }

  @Override
  public boolean metadataEquals(INodeFileAttributes other) {
    return other != null
        && getHeaderLong()== other.getHeaderLong()
        && getPermissionLong() == other.getPermissionLong()
        && getAclFeature() == other.getAclFeature()
        && getXAttrFeature() == other.getXAttrFeature();
  }

  /* Start of StripedBlock Feature */

  public final FileWithStripedBlocksFeature getStripedBlocksFeature() {
    return getFeature(FileWithStripedBlocksFeature.class);
  }

  public FileWithStripedBlocksFeature addStripedBlocksFeature() {
    assert blocks == null || blocks.length == 0:
        "The file contains contiguous blocks";
    assert !isStriped();
    this.setFileReplication((short) 0);
    FileWithStripedBlocksFeature sb = new FileWithStripedBlocksFeature();
    addFeature(sb);
    return sb;
  }

  /** Used to make sure there is no contiguous block related info */
  private boolean hasNoContiguousBlock() {
    return (blocks == null || blocks.length == 0) && getFileReplication() == 0;
  }

  /* Start of Under-Construction Feature */

  /**
   * If the inode contains a {@link FileUnderConstructionFeature}, return it;
   * otherwise, return null.
   */
  public final FileUnderConstructionFeature getFileUnderConstructionFeature() {
    return getFeature(FileUnderConstructionFeature.class);
  }

  /** Is this file under construction? */
  @Override // BlockCollection
  public boolean isUnderConstruction() {
    return getFileUnderConstructionFeature() != null;
  }

  INodeFile toUnderConstruction(String clientName, String clientMachine) {
    Preconditions.checkState(!isUnderConstruction(),
        "file is already under construction");
    FileUnderConstructionFeature uc = new FileUnderConstructionFeature(
        clientName, clientMachine);
    addFeature(uc);
    return this;
  }

  /**
   * Convert the file to a complete file, i.e., to remove the Under-Construction
   * feature.
   */
  public INodeFile toCompleteFile(long mtime) {
    Preconditions.checkState(isUnderConstruction(),
        "file is no longer under construction");
    FileUnderConstructionFeature uc = getFileUnderConstructionFeature();
    if (uc != null) {
      assertAllBlocksComplete(getBlocks());
      removeFeature(uc);
      this.setModificationTime(mtime);
    }
    return this;
  }

  /** Assert all blocks are complete. */
  private void assertAllBlocksComplete(BlockInfo[] blks) {
    if (blks == null) {
      return;
    }
    for (int i = 0; i < blks.length; i++) {
      Preconditions.checkState(blks[i].isComplete(), "Failed to finalize"
          + " %s %s since blocks[%s] is non-complete, where blocks=%s.",
          getClass().getSimpleName(), this, i, Arrays.asList(blks));
    }
  }

  /**
   * Instead of adding a new block, this function is usually used while loading
   * fsimage or converting the last block to UC/Complete.
   */
  @Override // BlockCollection
  public void setBlock(int index, BlockInfo blk) {
    FileWithStripedBlocksFeature sb = getStripedBlocksFeature();
    if (sb == null) {
      assert !blk.isStriped();
      this.blocks[index] = (BlockInfoContiguous) blk;
    } else {
      assert blk.isStriped();
      assert hasNoContiguousBlock();
      sb.setBlock(index, (BlockInfoStriped) blk);
    }
  }

  @Override // BlockCollection, the file should be under construction
  public void convertLastBlockToUC(BlockInfo lastBlock,
      DatanodeStorageInfo[] locations) throws IOException {
    Preconditions.checkState(isUnderConstruction(),
        "file is no longer under construction");
    if (numBlocks() == 0) {
      throw new IOException("Failed to set last block: File is empty.");
    }

    final BlockInfo ucBlock;
    FileWithStripedBlocksFeature sb = getStripedBlocksFeature();
    if (sb == null) {
      assert !lastBlock.isStriped();
      ucBlock = ((BlockInfoContiguous) lastBlock)
          .convertToBlockUnderConstruction(UNDER_CONSTRUCTION, locations);
    } else {
      assert hasNoContiguousBlock();
      assert lastBlock.isStriped();
      ucBlock = ((BlockInfoStriped) lastBlock)
          .convertToBlockUnderConstruction(UNDER_CONSTRUCTION, locations);
    }
    setBlock(numBlocks() - 1, ucBlock);
  }

  /**
   * Remove a block from the block list. This block should be
   * the last one on the list.
   */
  BlockInfoContiguousUnderConstruction removeLastBlock(Block oldblock) {
    Preconditions.checkState(isUnderConstruction(),
        "file is no longer under construction");
    FileWithStripedBlocksFeature sb = getStripedBlocksFeature();
    if (sb == null) {
      if (blocks == null || blocks.length == 0) {
        return null;
      }
      int size_1 = blocks.length - 1;
      if (!blocks[size_1].equals(oldblock)) {
        return null;
      }

      BlockInfoContiguousUnderConstruction uc =
          (BlockInfoContiguousUnderConstruction)blocks[size_1];
      //copy to a new list
      BlockInfoContiguous[] newlist = new BlockInfoContiguous[size_1];
      System.arraycopy(blocks, 0, newlist, 0, size_1);
      setContiguousBlocks(newlist);
      return uc;
    } else {
      assert hasNoContiguousBlock();
      return null;
    }
  }

  /* End of Under-Construction Feature */
  
  /* Start of Snapshot Feature */

  public FileWithSnapshotFeature addSnapshotFeature(FileDiffList diffs) {
    Preconditions.checkState(!isWithSnapshot(), 
        "File is already with snapshot");
    FileWithSnapshotFeature sf = new FileWithSnapshotFeature(diffs);
    this.addFeature(sf);
    return sf;
  }
  
  /**
   * If feature list contains a {@link FileWithSnapshotFeature}, return it;
   * otherwise, return null.
   */
  public final FileWithSnapshotFeature getFileWithSnapshotFeature() {
    return getFeature(FileWithSnapshotFeature.class);
  }

  /** Is this file has the snapshot feature? */
  public final boolean isWithSnapshot() {
    return getFileWithSnapshotFeature() != null;
  }
    
  @Override
  public String toDetailString() {
    FileWithSnapshotFeature sf = this.getFileWithSnapshotFeature();
    return super.toDetailString() + (sf == null ? "" : sf.getDetailedString()); 
  }

  @Override
  public INodeFileAttributes getSnapshotINode(final int snapshotId) {
    FileWithSnapshotFeature sf = this.getFileWithSnapshotFeature();
    if (sf != null) {
      return sf.getDiffs().getSnapshotINode(snapshotId, this);
    } else {
      return this;
    }
  }

  @Override
  public void recordModification(final int latestSnapshotId) {
    recordModification(latestSnapshotId, false);
  }

  public void recordModification(final int latestSnapshotId, boolean withBlocks) {
    if (isInLatestSnapshot(latestSnapshotId)
        && !shouldRecordInSrcSnapshot(latestSnapshotId)) {
      // the file is in snapshot, create a snapshot feature if it does not have
      FileWithSnapshotFeature sf = this.getFileWithSnapshotFeature();
      if (sf == null) {
        sf = addSnapshotFeature(null);
      }
      // record self in the diff list if necessary
      sf.getDiffs().saveSelf2Snapshot(latestSnapshotId, this, null, withBlocks);
    }
  }

  public FileDiffList getDiffs() {
    FileWithSnapshotFeature sf = this.getFileWithSnapshotFeature();
    if (sf != null) {
      return sf.getDiffs();
    }
    return null;
  }
  
  /* End of Snapshot Feature */

  /** @return the replication factor of the file. */
  public final short getFileReplication(int snapshot) {
    if (snapshot != CURRENT_STATE_ID) {
      return getSnapshotINode(snapshot).getFileReplication();
    }
    return HeaderFormat.getReplication(header);
  }

  /** The same as getFileReplication(null). */
  @Override // INodeFileAttributes
  // TODO striped
  public final short getFileReplication() {
    return getFileReplication(CURRENT_STATE_ID);
  }

  @Override // BlockCollection
  public short getPreferredBlockReplication() {
    short max = getFileReplication(CURRENT_STATE_ID);
    FileWithSnapshotFeature sf = this.getFileWithSnapshotFeature();
    if (sf != null) {
      short maxInSnapshot = sf.getMaxBlockRepInDiffs();
      if (sf.isCurrentFileDeleted()) {
        return maxInSnapshot;
      }
      max = maxInSnapshot > max ? maxInSnapshot : max;
    }
    return isStriped() ?
        HdfsConstants.NUM_DATA_BLOCKS + HdfsConstants.NUM_PARITY_BLOCKS : max;
  }

  /** Set the replication factor of this file. */
  private void setFileReplication(short replication) {
    header = HeaderFormat.REPLICATION.BITS.combine(replication, header);
  }

  /** Set the replication factor of this file. */
  public final INodeFile setFileReplication(short replication,
      int latestSnapshotId) throws QuotaExceededException {
    Preconditions.checkState(!isStriped(),
        "Cannot set replication to a file with striped blocks");
    recordModification(latestSnapshotId);
    setFileReplication(replication);
    return this;
  }

  /** @return preferred block size (in bytes) of the file. */
  @Override
  public long getPreferredBlockSize() {
    return HeaderFormat.getPreferredBlockSize(header);
  }

  @Override
  public byte getLocalStoragePolicyID() {
    return HeaderFormat.getStoragePolicyID(header);
  }

  @Override
  public byte getStoragePolicyID() {
    byte id = getLocalStoragePolicyID();
    if (id == BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
      return this.getParent() != null ?
          this.getParent().getStoragePolicyID() : id;
    }
    return id;
  }

  private void setStoragePolicyID(byte storagePolicyId) {
    header = HeaderFormat.STORAGE_POLICY_ID.BITS.combine(storagePolicyId,
        header);
  }

  public final void setStoragePolicyID(byte storagePolicyId,
      int latestSnapshotId) throws QuotaExceededException {
    recordModification(latestSnapshotId);
    setStoragePolicyID(storagePolicyId);
  }

  @Override // INodeFileAttributes
  public long getHeaderLong() {
    return header;
  }

  /** @return the blocks of the file. */
  @Override // BlockCollection
  public BlockInfo[] getBlocks() {
    FileWithStripedBlocksFeature sb = getStripedBlocksFeature();
    if (sb != null) {
      assert hasNoContiguousBlock();
      return sb.getBlocks();
    } else {
      return this.blocks;
    }
  }

  /** Used by snapshot diff */
  public BlockInfoContiguous[] getContiguousBlocks() {
    return this.blocks;
  }

  /** @return blocks of the file corresponding to the snapshot. */
  public BlockInfo[] getBlocks(int snapshot) {
    if (snapshot == CURRENT_STATE_ID || getDiffs() == null) {
      return getBlocks();
    }
    // find blocks stored in snapshot diffs (for truncate)
    FileDiff diff = getDiffs().getDiffById(snapshot);
    // note that currently FileDiff can only store contiguous blocks
    BlockInfo[] snapshotBlocks = diff == null ? getBlocks() : diff.getBlocks();
    if (snapshotBlocks != null) {
      return snapshotBlocks;
    }
    // Blocks are not in the current snapshot
    // Find next snapshot with blocks present or return current file blocks
    snapshotBlocks = getDiffs().findLaterSnapshotBlocks(snapshot);
    return (snapshotBlocks == null) ? getBlocks() : snapshotBlocks;
  }

  /** Used during concat to update the BlockCollection for each block */
  private void updateBlockCollection() {
    if (blocks != null && blocks.length > 0) {
      for(BlockInfoContiguous b : blocks) {
        b.setBlockCollection(this);
      }
    } else {
      FileWithStripedBlocksFeature sb = getStripedBlocksFeature();
      if (sb != null) {
        sb.updateBlockCollection(this);
      }
    }
  }

  /**
   * append array of blocks to this.blocks
   */
  void concatBlocks(INodeFile[] inodes) {
    int size = this.blocks.length;
    int totalAddedBlocks = 0;
    for(INodeFile f : inodes) {
      totalAddedBlocks += f.blocks.length;
    }
    
    BlockInfoContiguous[] newlist =
        new BlockInfoContiguous[size + totalAddedBlocks];
    System.arraycopy(this.blocks, 0, newlist, 0, size);
    
    for(INodeFile in: inodes) {
      System.arraycopy(in.blocks, 0, newlist, size, in.blocks.length);
      size += in.blocks.length;
    }

    setContiguousBlocks(newlist);
    updateBlockCollection();
  }
  
  /**
   * add a contiguous block to the block list
   */
  private void addContiguousBlock(BlockInfoContiguous newblock) {
    if (this.blocks == null) {
      this.setContiguousBlocks(new BlockInfoContiguous[]{newblock});
    } else {
      int size = this.blocks.length;
      BlockInfoContiguous[] newlist = new BlockInfoContiguous[size + 1];
      System.arraycopy(this.blocks, 0, newlist, 0, size);
      newlist[size] = newblock;
      this.setContiguousBlocks(newlist);
    }
  }

  /** add a striped or contiguous block */
  void addBlock(BlockInfo newblock) {
    FileWithStripedBlocksFeature sb = getStripedBlocksFeature();
    if (sb == null) {
      assert !newblock.isStriped();
      addContiguousBlock((BlockInfoContiguous) newblock);
    } else {
      assert newblock.isStriped();
      assert hasNoContiguousBlock();
      sb.addBlock((BlockInfoStriped) newblock);
    }
  }

  /** Set the blocks. */
  public void setContiguousBlocks(BlockInfoContiguous[] blocks) {
    this.blocks = blocks;
  }

  @Override
  public void cleanSubtree(ReclaimContext reclaimContext,
      final int snapshot, int priorSnapshotId) {
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf != null) {
      // TODO: avoid calling getStoragePolicyID
      sf.cleanFile(reclaimContext, this, snapshot, priorSnapshotId,
          getStoragePolicyID());
    } else {
      if (snapshot == CURRENT_STATE_ID) {
        if (priorSnapshotId == NO_SNAPSHOT_ID) {
          // this only happens when deleting the current file and it is not
          // in any snapshot
          destroyAndCollectBlocks(reclaimContext);
        } else {
          FileUnderConstructionFeature uc = getFileUnderConstructionFeature();
          // when deleting the current file and it is in snapshot, we should
          // clean the 0-sized block if the file is UC
          if (uc != null) {
            uc.cleanZeroSizeBlock(this, reclaimContext.collectedBlocks);
            if (reclaimContext.removedUCFiles != null) {
              reclaimContext.removedUCFiles.add(getId());
            }
          }
        }
      }
    }
  }

  @Override
  public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
    // TODO pass in the storage policy
    reclaimContext.quotaDelta().add(computeQuotaUsage(reclaimContext.bsps,
        false));
    clearFile(reclaimContext);
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf != null) {
      sf.getDiffs().destroyAndCollectSnapshotBlocks(
          reclaimContext.collectedBlocks);
      sf.clearDiffs();
    }
    if (isUnderConstruction() && reclaimContext.removedUCFiles != null) {
      reclaimContext.removedUCFiles.add(getId());
    }
  }

  public void clearFile(ReclaimContext reclaimContext) {
    BlockInfo[] blks = getBlocks();
    if (blks != null && reclaimContext.collectedBlocks != null) {
      for (BlockInfo blk : blks) {
        reclaimContext.collectedBlocks.addDeleteBlock(blk);
        blk.setBlockCollection(null);
      }
    }
    setContiguousBlocks(null);

    FileWithStripedBlocksFeature sb = getStripedBlocksFeature();
    if (sb != null) {
      sb.clear();
    }
    if (getAclFeature() != null) {
      AclStorage.removeAclFeature(getAclFeature());
    }
    clear();
    reclaimContext.removedINodes.add(this);
  }

  @Override
  public String getName() {
    // Get the full path name of this inode.
    return getFullPathName();
  }

  // This is the only place that needs to use the BlockStoragePolicySuite to
  // derive the intended storage type usage for quota by storage type
  @Override
  public final QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps,
      byte blockStoragePolicyId, boolean useCache, int lastSnapshotId) {
    final QuotaCounts counts = new QuotaCounts.Builder().nameSpace(1).build();

    final BlockStoragePolicy bsp = bsps.getPolicy(blockStoragePolicyId);
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf == null) {
      counts.add(storagespaceConsumed(bsp));
      return counts;
    }

    FileDiffList fileDiffList = sf.getDiffs();
    int last = fileDiffList.getLastSnapshotId();

    if (lastSnapshotId == Snapshot.CURRENT_STATE_ID
        || last == Snapshot.CURRENT_STATE_ID) {
      counts.add(storagespaceConsumed(bsp));
      return counts;
    }

    final long ssDeltaNoReplication;
    short replication;
    if (isStriped()) {
      return computeQuotaUsageWithStriped(bsps, counts);
    }
    
    if (last < lastSnapshotId) {
      ssDeltaNoReplication = computeFileSize(true, false);
      replication = getFileReplication();
    } else {
      int sid = fileDiffList.getSnapshotById(lastSnapshotId);
      ssDeltaNoReplication = computeFileSize(sid);
      replication = getFileReplication(sid);
    }

    counts.addStorageSpace(ssDeltaNoReplication * replication);
    if (bsp != null) {
      List<StorageType> storageTypes = bsp.chooseStorageTypes(replication);
      for (StorageType t : storageTypes) {
        if (!t.supportTypeQuota()) {
          continue;
        }
        counts.addTypeSpace(t, ssDeltaNoReplication);
      }
    }
    return counts;
  }

  /**
   * Compute quota of striped file
   */
  public final QuotaCounts computeQuotaUsageWithStriped(
      BlockStoragePolicySuite bsps, QuotaCounts counts) {
    return null;
  }

  @Override
  public final ContentSummaryComputationContext computeContentSummary(
      final ContentSummaryComputationContext summary) {
    final ContentCounts counts = summary.getCounts();
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    final long fileLen;
    if (sf == null) {
      fileLen = computeFileSize();
      counts.addContent(Content.FILE, 1);
    } else {
      final FileDiffList diffs = sf.getDiffs();
      final int n = diffs.asList().size();
      counts.addContent(Content.FILE, n);
      if (n > 0 && sf.isCurrentFileDeleted()) {
        fileLen =  diffs.getLast().getFileSize();
      } else {
        fileLen = computeFileSize();
      }
    }
    counts.addContent(Content.LENGTH, fileLen);
    counts.addContent(Content.DISKSPACE, storagespaceConsumed(null)
        .getStorageSpace());

    if (getStoragePolicyID() != BLOCK_STORAGE_POLICY_ID_UNSPECIFIED){
      BlockStoragePolicy bsp = summary.getBlockStoragePolicySuite().
          getPolicy(getStoragePolicyID());
      List<StorageType> storageTypes = bsp.chooseStorageTypes(getFileReplication());
      for (StorageType t : storageTypes) {
        if (!t.supportTypeQuota()) {
          continue;
        }
        counts.addTypeSpace(t, fileLen);
      }
    }
    return summary;
  }

  /** The same as computeFileSize(null). */
  public final long computeFileSize() {
    return computeFileSize(CURRENT_STATE_ID);
  }

  /**
   * Compute file size of the current file if the given snapshot is null;
   * otherwise, get the file size from the given snapshot.
   */
  public final long computeFileSize(int snapshotId) {
    FileWithSnapshotFeature sf = this.getFileWithSnapshotFeature();
    if (snapshotId != CURRENT_STATE_ID && sf != null) {
      final FileDiff d = sf.getDiffs().getDiffById(snapshotId);
      if (d != null) {
        return d.getFileSize();
      }
    }
    return computeFileSize(true, false);
  }

  /**
   * Compute file size of the current file size
   * but not including the last block if it is under construction.
   */
  public final long computeFileSizeNotIncludingLastUcBlock() {
    return computeFileSize(false, false);
  }

  /**
   * Compute file size of the current file.
   * 
   * @param includesLastUcBlock
   *          If the last block is under construction, should it be included?
   * @param usePreferredBlockSize4LastUcBlock
   *          If the last block is under construction, should we use actual
   *          block size or preferred block size?
   *          Note that usePreferredBlockSize4LastUcBlock is ignored
   *          if includesLastUcBlock == false.
   * @return file size
   */
  public final long computeFileSize(boolean includesLastUcBlock,
                                    boolean usePreferredBlockSize4LastUcBlock) {
    BlockInfo[] blockInfos = getBlocks();
    // In case of contiguous blocks
    if (blockInfos == null || blockInfos.length == 0) {
      return 0;
    }
    final int last = blockInfos.length - 1;
    //check if the last block is BlockInfoUnderConstruction
    long size = blockInfos[last].getNumBytes();
    if (blockInfos[last] instanceof BlockInfoContiguousUnderConstruction) {
      if (!includesLastUcBlock) {
        size = 0;
      } else if (usePreferredBlockSize4LastUcBlock) {
        size = getPreferredBlockSize();
      }
    } else if (blockInfos[last] instanceof BlockInfoStripedUnderConstruction) {
      if (!includesLastUcBlock) {
        size = 0;
      } else if (usePreferredBlockSize4LastUcBlock) {
        // Striped blocks keeps block group which counts
        // (data blocks num + parity blocks num). When you
        // count actual used size by BlockInfoStripedUC must
        // be multiplied by these blocks number.
        BlockInfoStripedUnderConstruction blockInfoStripedUC
            = (BlockInfoStripedUnderConstruction) blockInfos[last];
        size = getPreferredBlockSize() * blockInfoStripedUC.getTotalBlockNum();
      }
    }
    //sum other blocks
    for (int i = 0; i < last; i++) {
      size += blockInfos[i].getNumBytes();
    }
    return size;
  }

  /**
   * Compute size consumed by all blocks of the current file,
   * including blocks in its snapshots.
   * Use preferred block size for the last block if it is under construction.
   */
  public final QuotaCounts storagespaceConsumed(BlockStoragePolicy bsp) {
    QuotaCounts counts = new QuotaCounts.Builder().build();
    if (isStriped()) {
      return storagespaceConsumedWithStriped(bsp);
    } else {
      return storagespaceConsumedWithReplication(bsp);
    }
  }

  public final QuotaCounts storagespaceConsumedWithStriped(
      BlockStoragePolicy bsp) {
    return  null;
  }

  public final QuotaCounts storagespaceConsumedWithReplication(
      BlockStoragePolicy bsp) {    QuotaCounts counts = new QuotaCounts.Builder().build();
    final Iterable<BlockInfo> blocks;
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf == null) {
      blocks = Arrays.asList(getBlocks());
    } else {
      // Collect all distinct blocks
      Set<BlockInfo> allBlocks = new HashSet<>(Arrays.asList(getBlocks()));
      List<FileDiff> diffs = sf.getDiffs().asList();
      for(FileDiff diff : diffs) {
        BlockInfoContiguous[] diffBlocks = diff.getBlocks();
        if (diffBlocks != null) {
          allBlocks.addAll(Arrays.asList(diffBlocks));
        }
      }
      blocks = allBlocks;
    }

    final short replication = getPreferredBlockReplication();
    for (BlockInfo b : blocks) {
      long blockSize = b.isComplete() ? b.getNumBytes() :
          getPreferredBlockSize();
      counts.addStorageSpace(blockSize * replication);
      if (bsp != null) {
        List<StorageType> types = bsp.chooseStorageTypes(replication);
        for (StorageType t : types) {
          if (t.supportTypeQuota()) {
            counts.addTypeSpace(t, blockSize);
          }
        }
      }
    }
    return counts;
  }

  public final short getReplication(int lastSnapshotId) {
    if (lastSnapshotId != CURRENT_STATE_ID) {
      return getFileReplication(lastSnapshotId);
    } else {
      return getBlockReplication();
    }
  }

  /**
   * Return the penultimate allocated block for this file.
   */
  BlockInfo getPenultimateBlock() {
    BlockInfo[] blks = getBlocks();
    return (blks == null || blks.length <= 1) ?
        null : blks[blks.length - 2];
  }

  @Override
  public BlockInfo getLastBlock() {
    FileWithStripedBlocksFeature sb = getStripedBlocksFeature();
    if (sb == null) {
      return blocks == null || blocks.length == 0 ?
          null : blocks[blocks.length - 1];
    } else {
      assert hasNoContiguousBlock();
      return sb.getLastBlock();
    }
  }

  @Override
  public int numBlocks() {
    FileWithStripedBlocksFeature sb = getStripedBlocksFeature();
    if (sb == null) {
      return blocks == null ? 0 : blocks.length;
    } else {
      assert hasNoContiguousBlock();
      return sb.numBlocks();
    }
  }

  @VisibleForTesting
  @Override
  public void dumpTreeRecursively(PrintWriter out, StringBuilder prefix,
      final int snapshotId) {
    super.dumpTreeRecursively(out, prefix, snapshotId);
    out.print(", fileSize=" + computeFileSize(snapshotId));
    // only compare the first block
    out.print(", blocks=");
    BlockInfo[] blks = getBlocks();
    out.print(blks == null || blks.length == 0? null: blks[0]);
    out.println();
  }

  /**
   * Remove full blocks at the end file up to newLength
   * @return sum of sizes of the remained blocks
   */
  public long collectBlocksBeyondMax(final long max,
      final BlocksMapUpdateInfo collectedBlocks) {
    final BlockInfo[] oldBlocks = getBlocks();
    if (oldBlocks == null) {
      return 0;
    }
    // find the minimum n such that the size of the first n blocks > max
    int n = 0;
    long size = 0;
    for(; n < oldBlocks.length && max > size; n++) {
      size += oldBlocks[n].getNumBytes();
    }
    if (n >= oldBlocks.length)
      return size;

    // starting from block n, the data is beyond max.
    // resize the array.
    truncateBlocksTo(n);

    // collect the blocks beyond max
    if (collectedBlocks != null) {
      for(; n < oldBlocks.length; n++) {
        collectedBlocks.addDeleteBlock(oldBlocks[n]);
      }
    }
    return size;
  }

  /**
   * compute the quota usage change for a truncate op
   * @param newLength the length for truncation
   **/
  void computeQuotaDeltaForTruncate(
      long newLength, BlockStoragePolicy bsps,
      QuotaCounts delta) {
    final BlockInfo[] blocks = getBlocks();
    if (blocks == null || blocks.length == 0) {
      return;
    }

    long size = 0;
    for (BlockInfo b : blocks) {
      size += b.getNumBytes();
    }

    BlockInfo[] sblocks = null;
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf != null) {
      FileDiff diff = sf.getDiffs().getLast();
      sblocks = diff != null ? diff.getBlocks() : null;
    }

    for (int i = blocks.length - 1; i >= 0 && size > newLength;
         size -= blocks[i].getNumBytes(), --i) {
      BlockInfo bi = blocks[i];
      long truncatedBytes;
      if (size - newLength < bi.getNumBytes()) {
        // Record a full block as the last block will be copied during
        // recovery
        truncatedBytes = bi.getNumBytes() - getPreferredBlockSize();
      } else {
        truncatedBytes = bi.getNumBytes();
      }

      // The block exist in snapshot, adding back the truncated bytes in the
      // existing files
      if (sblocks != null && i < sblocks.length && bi.equals(sblocks[i])) {
        truncatedBytes -= bi.getNumBytes();
      }

      delta.addStorageSpace(-truncatedBytes * getPreferredBlockReplication());
      if (bsps != null) {
        List<StorageType> types = bsps.chooseStorageTypes(
            getPreferredBlockReplication());
        for (StorageType t : types) {
          if (t.supportTypeQuota()) {
            delta.addTypeSpace(t, -truncatedBytes);
          }
        }
      }
    }
  }

  void truncateBlocksTo(int n) {
    FileWithStripedBlocksFeature sb = getStripedBlocksFeature();
    if (sb == null) {
      truncateContiguousBlocks(n);
    } else {
      sb.truncateStripedBlocks(n);
    }
  }

  private void truncateContiguousBlocks(int n) {
    final BlockInfoContiguous[] newBlocks;
    if (n == 0) {
      newBlocks = BlockInfoContiguous.EMPTY_ARRAY;
    } else {
      newBlocks = new BlockInfoContiguous[n];
      System.arraycopy(blocks, 0, newBlocks, 0, n);
    }
    // set new blocks
    setContiguousBlocks(newBlocks);
  }

  /**
   * This function is only called when block list is stored in snapshot
   * diffs. Note that this can only happen when truncation happens with
   * snapshots. Since we do not support truncation with striped blocks,
   * we only need to handle contiguous blocks here.
   */
  public void collectBlocksBeyondSnapshot(BlockInfoContiguous[] snapshotBlocks,
                                          BlocksMapUpdateInfo collectedBlocks) {
    BlockInfoContiguous[] oldBlocks = this.blocks;
    if (snapshotBlocks == null || oldBlocks == null)
      return;
    // Skip blocks in common between the file and the snapshot
    int n = 0;
    while(n < oldBlocks.length && n < snapshotBlocks.length &&
          oldBlocks[n] == snapshotBlocks[n]) {
      n++;
    }
    truncateContiguousBlocks(n);
    // Collect the remaining blocks of the file
    while(n < oldBlocks.length) {
      collectedBlocks.addDeleteBlock(oldBlocks[n++]);
    }
  }

  /** Exclude blocks collected for deletion that belong to a snapshot. */
  void excludeSnapshotBlocks(int snapshotId,
                             BlocksMapUpdateInfo collectedBlocks) {
    if(collectedBlocks == null || collectedBlocks.getToDeleteList().isEmpty())
      return;
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if(sf == null)
      return;
    BlockInfoContiguous[] snapshotBlocks =
        getDiffs().findEarlierSnapshotBlocks(snapshotId);
    if(snapshotBlocks == null)
      return;
    List<BlockInfo> toDelete = collectedBlocks.getToDeleteList();
    for(BlockInfo blk : snapshotBlocks) {
      if(toDelete.contains(blk))
        collectedBlocks.removeDeleteBlock(blk);
    }
  }

  /**
   * @return true if the block is contained in a snapshot or false otherwise.
   */
  boolean isBlockInLatestSnapshot(BlockInfoContiguous block) {
    FileWithSnapshotFeature sf = this.getFileWithSnapshotFeature();
    if (sf == null || sf.getDiffs() == null) {
      return false;
    }
    BlockInfoContiguous[] snapshotBlocks = getDiffs()
        .findEarlierSnapshotBlocks(getDiffs().getLastSnapshotId());
    return snapshotBlocks != null &&
        Arrays.asList(snapshotBlocks).contains(block);
  }

  /**
   * @return true if the file is in the striping layout.
   */
  @VisibleForTesting
  @Override
  public boolean isStriped() {
    return getStripedBlocksFeature() != null;
  }
}

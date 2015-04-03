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

import static org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite.ID_UNSPECIFIED;
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
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
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
    REPLICATION(PREFERRED_BLOCK_SIZE.BITS, 12, 1),
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
    header = HeaderFormat.toLong(preferredBlockSize, replication,
        storagePolicyID);
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
      assertAllBlocksComplete();
      removeFeature(uc);
      this.setModificationTime(mtime);
    }
    return this;
  }

  /** Assert all blocks are complete. */
  private void assertAllBlocksComplete() {
    if (blocks == null) {
      return;
    }
    for (int i = 0; i < blocks.length; i++) {
      Preconditions.checkState(blocks[i].isComplete(), "Failed to finalize"
          + " %s %s since blocks[%s] is non-complete, where blocks=%s.",
          getClass().getSimpleName(), this, i, Arrays.asList(blocks));
    }
  }

  @Override // BlockCollection
  public void setBlock(int index, BlockInfoContiguous blk) {
    this.blocks[index] = blk;
  }

  @Override // BlockCollection, the file should be under construction
  public BlockInfoContiguousUnderConstruction setLastBlock(
      BlockInfoContiguous lastBlock, DatanodeStorageInfo[] locations)
      throws IOException {
    Preconditions.checkState(isUnderConstruction(),
        "file is no longer under construction");

    if (numBlocks() == 0) {
      throw new IOException("Failed to set last block: File is empty.");
    }
    BlockInfoContiguousUnderConstruction ucBlock =
      lastBlock.convertToBlockUnderConstruction(
          BlockUCState.UNDER_CONSTRUCTION, locations);
    setBlock(numBlocks() - 1, ucBlock);
    return ucBlock;
  }

  /**
   * Remove a block from the block list. This block should be
   * the last one on the list.
   */
  boolean removeLastBlock(Block oldblock) {
    Preconditions.checkState(isUnderConstruction(),
        "file is no longer under construction");
    if (blocks == null || blocks.length == 0) {
      return false;
    }
    int size_1 = blocks.length - 1;
    if (!blocks[size_1].equals(oldblock)) {
      return false;
    }

    //copy to a new list
    BlockInfoContiguous[] newlist = new BlockInfoContiguous[size_1];
    System.arraycopy(blocks, 0, newlist, 0, size_1);
    setBlocks(newlist);
    return true;
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
  public final short getFileReplication() {
    return getFileReplication(CURRENT_STATE_ID);
  }

  @Override // BlockCollection
  public short getBlockReplication() {
    short max = getFileReplication(CURRENT_STATE_ID);
    FileWithSnapshotFeature sf = this.getFileWithSnapshotFeature();
    if (sf != null) {
      short maxInSnapshot = sf.getMaxBlockRepInDiffs();
      if (sf.isCurrentFileDeleted()) {
        return maxInSnapshot;
      }
      max = maxInSnapshot > max ? maxInSnapshot : max;
    }
    return max;
  }

  /** Set the replication factor of this file. */
  public final void setFileReplication(short replication) {
    header = HeaderFormat.REPLICATION.BITS.combine(replication, header);
  }

  /** Set the replication factor of this file. */
  public final INodeFile setFileReplication(short replication,
      int latestSnapshotId) throws QuotaExceededException {
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
    if (id == ID_UNSPECIFIED) {
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

  @Override
  public long getHeaderLong() {
    return header;
  }

  /** @return the storagespace required for a full block. */
  final long getPreferredBlockStoragespace() {
    return getPreferredBlockSize() * getBlockReplication();
  }

  /** @return the blocks of the file. */
  @Override
  public BlockInfoContiguous[] getBlocks() {
    return this.blocks;
  }

  /** @return blocks of the file corresponding to the snapshot. */
  public BlockInfoContiguous[] getBlocks(int snapshot) {
    if(snapshot == CURRENT_STATE_ID || getDiffs() == null)
      return getBlocks();
    FileDiff diff = getDiffs().getDiffById(snapshot);
    BlockInfoContiguous[] snapshotBlocks =
        diff == null ? getBlocks() : diff.getBlocks();
    if(snapshotBlocks != null)
      return snapshotBlocks;
    // Blocks are not in the current snapshot
    // Find next snapshot with blocks present or return current file blocks
    snapshotBlocks = getDiffs().findLaterSnapshotBlocks(snapshot);
    return (snapshotBlocks == null) ? getBlocks() : snapshotBlocks;
  }

  void updateBlockCollection() {
    if (blocks != null) {
      for(BlockInfoContiguous b : blocks) {
        b.setBlockCollection(this);
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

    setBlocks(newlist);
    updateBlockCollection();
  }
  
  /**
   * add a block to the block list
   */
  void addBlock(BlockInfoContiguous newblock) {
    if (this.blocks == null) {
      this.setBlocks(new BlockInfoContiguous[]{newblock});
    } else {
      int size = this.blocks.length;
      BlockInfoContiguous[] newlist = new BlockInfoContiguous[size + 1];
      System.arraycopy(this.blocks, 0, newlist, 0, size);
      newlist[size] = newblock;
      this.setBlocks(newlist);
    }
  }

  /** Set the blocks. */
  public void setBlocks(BlockInfoContiguous[] blocks) {
    this.blocks = blocks;
  }

  @Override
  public QuotaCounts cleanSubtree(BlockStoragePolicySuite bsps, final int snapshot,
                                  int priorSnapshotId,
      final BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes) {
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf != null) {
      return sf.cleanFile(bsps, this, snapshot, priorSnapshotId, collectedBlocks,
          removedINodes);
    }
    QuotaCounts counts = new QuotaCounts.Builder().build();
    if (snapshot == CURRENT_STATE_ID) {
      if (priorSnapshotId == NO_SNAPSHOT_ID) {
        // this only happens when deleting the current file and the file is not
        // in any snapshot
        computeQuotaUsage(bsps, counts, false);
        destroyAndCollectBlocks(bsps, collectedBlocks, removedINodes);
      } else {
        // when deleting the current file and the file is in snapshot, we should
        // clean the 0-sized block if the file is UC
        FileUnderConstructionFeature uc = getFileUnderConstructionFeature();
        if (uc != null) {
          uc.cleanZeroSizeBlock(this, collectedBlocks);
        }
      }
    }
    return counts;
  }

  @Override
  public void destroyAndCollectBlocks(BlockStoragePolicySuite bsps,
      BlocksMapUpdateInfo collectedBlocks, final List<INode> removedINodes) {
    if (blocks != null && collectedBlocks != null) {
      for (BlockInfoContiguous blk : blocks) {
        collectedBlocks.addDeleteBlock(blk);
        blk.setBlockCollection(null);
      }
    }
    setBlocks(null);
    if (getAclFeature() != null) {
      AclStorage.removeAclFeature(getAclFeature());
    }
    clear();
    removedINodes.add(this);
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf != null) {
      sf.getDiffs().destroyAndCollectSnapshotBlocks(collectedBlocks);
      sf.clearDiffs();
    }
  }

  @Override
  public String getName() {
    // Get the full path name of this inode.
    return getFullPathName();
  }

  // This is the only place that needs to use the BlockStoragePolicySuite to
  // derive the intended storage type usage for quota by storage type
  @Override
  public final QuotaCounts computeQuotaUsage(
      BlockStoragePolicySuite bsps, byte blockStoragePolicyId,
      QuotaCounts counts, boolean useCache,
      int lastSnapshotId) {
    long nsDelta = 1;
    final long ssDeltaNoReplication;
    short replication;
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf != null) {
      FileDiffList fileDiffList = sf.getDiffs();
      int last = fileDiffList.getLastSnapshotId();

      if (lastSnapshotId == Snapshot.CURRENT_STATE_ID
          || last == Snapshot.CURRENT_STATE_ID) {
        ssDeltaNoReplication = storagespaceConsumedNoReplication();
        replication = getBlockReplication();
      } else if (last < lastSnapshotId) {
        ssDeltaNoReplication = computeFileSize(true, false);
        replication = getFileReplication();
      } else {
        int sid = fileDiffList.getSnapshotById(lastSnapshotId);
        ssDeltaNoReplication = storagespaceConsumedNoReplication(sid);
        replication = getReplication(sid);
      }
    } else {
      ssDeltaNoReplication = storagespaceConsumedNoReplication();
      replication = getBlockReplication();
    }
    counts.addNameSpace(nsDelta);
    counts.addStorageSpace(ssDeltaNoReplication * replication);

    if (blockStoragePolicyId != ID_UNSPECIFIED){
      BlockStoragePolicy bsp = bsps.getPolicy(blockStoragePolicyId);
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

  @Override
  public final ContentSummaryComputationContext computeContentSummary(
      final ContentSummaryComputationContext summary) {
    final ContentCounts counts = summary.getCounts();
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    long fileLen = 0;
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
    counts.addContent(Content.DISKSPACE, storagespaceConsumed());

    if (getStoragePolicyID() != ID_UNSPECIFIED){
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
    if (blocks == null || blocks.length == 0) {
      return 0;
    }
    final int last = blocks.length - 1;
    //check if the last block is BlockInfoUnderConstruction
    long size = blocks[last].getNumBytes();
    if (blocks[last] instanceof BlockInfoContiguousUnderConstruction) {
       if (!includesLastUcBlock) {
         size = 0;
       } else if (usePreferredBlockSize4LastUcBlock) {
         size = getPreferredBlockSize();
       }
    }
    //sum other blocks
    for(int i = 0; i < last; i++) {
      size += blocks[i].getNumBytes();
    }
    return size;
  }

  /**
   * Compute size consumed by all blocks of the current file,
   * including blocks in its snapshots.
   * Use preferred block size for the last block if it is under construction.
   */
  public final long storagespaceConsumed() {
    return storagespaceConsumedNoReplication() * getBlockReplication();
  }

  public final long storagespaceConsumedNoReplication() {
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if(sf == null) {
      return computeFileSize(true, true);
    }

    // Collect all distinct blocks
    long size = 0;
    Set<Block> allBlocks = new HashSet<Block>(Arrays.asList(getBlocks()));
    List<FileDiff> diffs = sf.getDiffs().asList();
    for(FileDiff diff : diffs) {
      BlockInfoContiguous[] diffBlocks = diff.getBlocks();
      if (diffBlocks != null) {
        allBlocks.addAll(Arrays.asList(diffBlocks));
      }
    }
    for(Block block : allBlocks) {
      size += block.getNumBytes();
    }
    // check if the last block is under construction
    BlockInfoContiguous lastBlock = getLastBlock();
    if(lastBlock != null &&
        lastBlock instanceof BlockInfoContiguousUnderConstruction) {
      size += getPreferredBlockSize() - lastBlock.getNumBytes();
    }
    return size;
  }

  public final long storagespaceConsumed(int lastSnapshotId) {
    if (lastSnapshotId != CURRENT_STATE_ID) {
      return computeFileSize(lastSnapshotId)
        * getFileReplication(lastSnapshotId);
    } else {
      return storagespaceConsumed();
    }
  }

  public final short getReplication(int lastSnapshotId) {
    if (lastSnapshotId != CURRENT_STATE_ID) {
      return getFileReplication(lastSnapshotId);
    } else {
      return getBlockReplication();
    }
  }

  public final long storagespaceConsumedNoReplication(int lastSnapshotId) {
    if (lastSnapshotId != CURRENT_STATE_ID) {
      return computeFileSize(lastSnapshotId);
    } else {
      return storagespaceConsumedNoReplication();
    }
  }

  /**
   * Return the penultimate allocated block for this file.
   */
  BlockInfoContiguous getPenultimateBlock() {
    if (blocks == null || blocks.length <= 1) {
      return null;
    }
    return blocks[blocks.length - 2];
  }

  @Override
  public BlockInfoContiguous getLastBlock() {
    return blocks == null || blocks.length == 0? null: blocks[blocks.length-1];
  }

  @Override
  public int numBlocks() {
    return blocks == null ? 0 : blocks.length;
  }

  @VisibleForTesting
  @Override
  public void dumpTreeRecursively(PrintWriter out, StringBuilder prefix,
      final int snapshotId) {
    super.dumpTreeRecursively(out, prefix, snapshotId);
    out.print(", fileSize=" + computeFileSize(snapshotId));
    // only compare the first block
    out.print(", blocks=");
    out.print(blocks == null || blocks.length == 0? null: blocks[0]);
    out.println();
  }

  /**
   * Remove full blocks at the end file up to newLength
   * @return sum of sizes of the remained blocks
   */
  public long collectBlocksBeyondMax(final long max,
      final BlocksMapUpdateInfo collectedBlocks) {
    final BlockInfoContiguous[] oldBlocks = getBlocks();
    if (oldBlocks == null)
      return 0;
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
   * @return the quota usage delta (not considering replication factor)
   */
  long computeQuotaDeltaForTruncate(final long newLength) {
    final BlockInfoContiguous[] blocks = getBlocks();
    if (blocks == null || blocks.length == 0) {
      return 0;
    }

    int n = 0;
    long size = 0;
    for (; n < blocks.length && newLength > size; n++) {
      size += blocks[n].getNumBytes();
    }
    final boolean onBoundary = size == newLength;

    long truncateSize = 0;
    for (int i = (onBoundary ? n : n - 1); i < blocks.length; i++) {
      truncateSize += blocks[i].getNumBytes();
    }

    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf != null) {
      FileDiff diff = sf.getDiffs().getLast();
      BlockInfoContiguous[] sblocks = diff != null ? diff.getBlocks() : null;
      if (sblocks != null) {
        for (int i = (onBoundary ? n : n-1); i < blocks.length
            && i < sblocks.length && blocks[i].equals(sblocks[i]); i++) {
          truncateSize -= blocks[i].getNumBytes();
        }
      }
    }
    return onBoundary ? -truncateSize : (getPreferredBlockSize() - truncateSize);
  }

  void truncateBlocksTo(int n) {
    final BlockInfoContiguous[] newBlocks;
    if (n == 0) {
      newBlocks = BlockInfoContiguous.EMPTY_ARRAY;
    } else {
      newBlocks = new BlockInfoContiguous[n];
      System.arraycopy(getBlocks(), 0, newBlocks, 0, n);
    }
    // set new blocks
    setBlocks(newBlocks);
  }

  public void collectBlocksBeyondSnapshot(BlockInfoContiguous[] snapshotBlocks,
                                          BlocksMapUpdateInfo collectedBlocks) {
    BlockInfoContiguous[] oldBlocks = getBlocks();
    if(snapshotBlocks == null || oldBlocks == null)
      return;
    // Skip blocks in common between the file and the snapshot
    int n = 0;
    while(n < oldBlocks.length && n < snapshotBlocks.length &&
          oldBlocks[n] == snapshotBlocks[n]) {
      n++;
    }
    truncateBlocksTo(n);
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
    List<Block> toDelete = collectedBlocks.getToDeleteList();
    for(Block blk : snapshotBlocks) {
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
}

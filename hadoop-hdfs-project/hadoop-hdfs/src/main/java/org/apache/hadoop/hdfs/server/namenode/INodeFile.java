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

import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.CURRENT_STATE_ID;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.NO_SNAPSHOT_ID;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
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

  /** Format: [16 bits for replication][48 bits for PreferredBlockSize] */
  static enum HeaderFormat {
    PREFERRED_BLOCK_SIZE(null, 48, 1),
    REPLICATION(PREFERRED_BLOCK_SIZE.BITS, 16, 1);

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

    static long toLong(long preferredBlockSize, short replication) {
      long h = 0;
      h = PREFERRED_BLOCK_SIZE.BITS.combine(preferredBlockSize, h);
      h = REPLICATION.BITS.combine(replication, h);
      return h;
    }
  }

  private long header = 0L;

  private BlockInfo[] blocks;

  INodeFile(long id, byte[] name, PermissionStatus permissions, long mtime,
      long atime, BlockInfo[] blklist, short replication,
      long preferredBlockSize) {
    super(id, name, permissions, mtime, atime);
    header = HeaderFormat.toLong(preferredBlockSize, replication);
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

  /** Convert this file to an {@link INodeFileUnderConstruction}. */
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
  public void setBlock(int index, BlockInfo blk) {
    this.blocks[index] = blk;
  }

  @Override // BlockCollection, the file should be under construction
  public BlockInfoUnderConstruction setLastBlock(BlockInfo lastBlock,
      DatanodeStorageInfo[] locations) throws IOException {
    Preconditions.checkState(isUnderConstruction(),
        "file is no longer under construction");

    if (numBlocks() == 0) {
      throw new IOException("Failed to set last block: File is empty.");
    }
    BlockInfoUnderConstruction ucBlock =
      lastBlock.convertToBlockUnderConstruction(
          BlockUCState.UNDER_CONSTRUCTION, locations);
    ucBlock.setBlockCollection(this);
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
    BlockInfo[] newlist = new BlockInfo[size_1];
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
  public void recordModification(final int latestSnapshotId)
      throws QuotaExceededException {
    if (isInLatestSnapshot(latestSnapshotId)
        && !shouldRecordInSrcSnapshot(latestSnapshotId)) {
      // the file is in snapshot, create a snapshot feature if it does not have
      FileWithSnapshotFeature sf = this.getFileWithSnapshotFeature();
      if (sf == null) {
        sf = addSnapshotFeature(null);
      }
      // record self in the diff list if necessary
      sf.getDiffs().saveSelf2Snapshot(latestSnapshotId, this, null);
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
  public long getHeaderLong() {
    return header;
  }

  /** @return the diskspace required for a full block. */
  final long getBlockDiskspace() {
    return getPreferredBlockSize() * getBlockReplication();
  }

  /** @return the blocks of the file. */
  @Override
  public BlockInfo[] getBlocks() {
    return this.blocks;
  }

  void updateBlockCollection() {
    if (blocks != null) {
      for(BlockInfo b : blocks) {
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
    
    BlockInfo[] newlist = new BlockInfo[size + totalAddedBlocks];
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
  void addBlock(BlockInfo newblock) {
    if (this.blocks == null) {
      this.setBlocks(new BlockInfo[]{newblock});
    } else {
      int size = this.blocks.length;
      BlockInfo[] newlist = new BlockInfo[size + 1];
      System.arraycopy(this.blocks, 0, newlist, 0, size);
      newlist[size] = newblock;
      this.setBlocks(newlist);
    }
  }

  /** Set the blocks. */
  public void setBlocks(BlockInfo[] blocks) {
    this.blocks = blocks;
  }

  @Override
  public Quota.Counts cleanSubtree(final int snapshot, int priorSnapshotId,
      final BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes, final boolean countDiffChange)
      throws QuotaExceededException {
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf != null) {
      return sf.cleanFile(this, snapshot, priorSnapshotId, collectedBlocks,
          removedINodes, countDiffChange);
    }
    Quota.Counts counts = Quota.Counts.newInstance();
    if (snapshot == CURRENT_STATE_ID) {
      if (priorSnapshotId == NO_SNAPSHOT_ID) {
        // this only happens when deleting the current file and the file is not
        // in any snapshot
        computeQuotaUsage(counts, false);
        destroyAndCollectBlocks(collectedBlocks, removedINodes);
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
  public void destroyAndCollectBlocks(BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes) {
    if (blocks != null && collectedBlocks != null) {
      for (BlockInfo blk : blocks) {
        collectedBlocks.addDeleteBlock(blk);
        blk.setBlockCollection(null);
      }
    }
    setBlocks(null);
    clear();
    removedINodes.add(this);
    
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf != null) {
      sf.clearDiffs();
    }
  }
  
  @Override
  public String getName() {
    // Get the full path name of this inode.
    return getFullPathName();
  }

  @Override
  public final Quota.Counts computeQuotaUsage(Quota.Counts counts,
      boolean useCache, int lastSnapshotId) {
    long nsDelta = 1;
    final long dsDelta;
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf != null) {
      FileDiffList fileDiffList = sf.getDiffs();
      int last = fileDiffList.getLastSnapshotId();
      List<FileDiff> diffs = fileDiffList.asList();

      if (lastSnapshotId == Snapshot.CURRENT_STATE_ID
          || last == Snapshot.CURRENT_STATE_ID) {
        nsDelta += diffs.size();
        dsDelta = diskspaceConsumed();
      } else if (last < lastSnapshotId) {
        dsDelta = computeFileSize(true, false) * getFileReplication();
      } else {      
        int sid = fileDiffList.getSnapshotById(lastSnapshotId);
        dsDelta = diskspaceConsumed(sid);
      }
    } else {
      dsDelta = diskspaceConsumed();
    }
    counts.add(Quota.NAMESPACE, nsDelta);
    counts.add(Quota.DISKSPACE, dsDelta);
    return counts;
  }

  @Override
  public final ContentSummaryComputationContext computeContentSummary(
      final ContentSummaryComputationContext summary) {
    computeContentSummary4Snapshot(summary.getCounts());
    computeContentSummary4Current(summary.getCounts());
    return summary;
  }

  private void computeContentSummary4Snapshot(final Content.Counts counts) {
    // file length and diskspace only counted for the latest state of the file
    // i.e. either the current state or the last snapshot
    FileWithSnapshotFeature sf = getFileWithSnapshotFeature();
    if (sf != null) {
      final FileDiffList diffs = sf.getDiffs();
      final int n = diffs.asList().size();
      counts.add(Content.FILE, n);
      if (n > 0 && sf.isCurrentFileDeleted()) {
        counts.add(Content.LENGTH, diffs.getLast().getFileSize());
      }

      if (sf.isCurrentFileDeleted()) {
        final long lastFileSize = diffs.getLast().getFileSize();
        counts.add(Content.DISKSPACE, lastFileSize * getBlockReplication());
      }
    }
  }

  private void computeContentSummary4Current(final Content.Counts counts) {
    FileWithSnapshotFeature sf = this.getFileWithSnapshotFeature();
    if (sf != null && sf.isCurrentFileDeleted()) {
      return;
    }

    counts.add(Content.LENGTH, computeFileSize());
    counts.add(Content.FILE, 1);
    counts.add(Content.DISKSPACE, diskspaceConsumed());
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
    if (blocks[last] instanceof BlockInfoUnderConstruction) {
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

  public final long diskspaceConsumed() {
    // use preferred block size for the last block if it is under construction
    return computeFileSize(true, true) * getBlockReplication();
  }

  public final long diskspaceConsumed(int lastSnapshotId) {
    if (lastSnapshotId != CURRENT_STATE_ID) {
      return computeFileSize(lastSnapshotId)
          * getFileReplication(lastSnapshotId);
    } else {
      return diskspaceConsumed();
    }
  }
  
  /**
   * Return the penultimate allocated block for this file.
   */
  BlockInfo getPenultimateBlock() {
    if (blocks == null || blocks.length <= 1) {
      return null;
    }
    return blocks[blocks.length - 2];
  }

  @Override
  public BlockInfo getLastBlock() {
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
}

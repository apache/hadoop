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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;

/**
 * Subclass of {@link BlockInfo}, presenting a block group in erasure coding.
 *
 * We still use triplets to store DatanodeStorageInfo for each block in the
 * block group, as well as the previous/next block in the corresponding
 * DatanodeStorageInfo. For a (m+k) block group, the first (m+k) triplet units
 * are sorted and strictly mapped to the corresponding block.
 *
 * Normally each block belonging to group is stored in only one DataNode.
 * However, it is possible that some block is over-replicated. Thus the triplet
 * array's size can be larger than (m+k). Thus currently we use an extra byte
 * array to record the block index for each triplet.
 */
public class BlockInfoStriped extends BlockInfo {
  private final short dataBlockNum;
  private final short parityBlockNum;
  /**
   * Always the same size with triplets. Record the block index for each triplet
   * TODO: actually this is only necessary for over-replicated block. Thus can
   * be further optimized to save memory usage.
   */
  private byte[] indices;

  public BlockInfoStriped(Block blk, short dataBlockNum, short parityBlockNum) {
    super(blk, (short) (dataBlockNum + parityBlockNum));
    indices = new byte[dataBlockNum + parityBlockNum];
    initIndices();
    this.dataBlockNum = dataBlockNum;
    this.parityBlockNum = parityBlockNum;
  }

  BlockInfoStriped(BlockInfoStriped b) {
    this(b, b.dataBlockNum, b.parityBlockNum);
    this.setBlockCollection(b.getBlockCollection());
  }

  short getTotalBlockNum() {
    return (short) (dataBlockNum + parityBlockNum);
  }

  public short getDataBlockNum() {
    return dataBlockNum;
  }

  public short getParityBlockNum() {
    return parityBlockNum;
  }

  private void initIndices() {
    for (int i = 0; i < indices.length; i++) {
      indices[i] = -1;
    }
  }

  private int findSlot() {
    int i = getTotalBlockNum();
    for (; i < getCapacity(); i++) {
      if (getStorageInfo(i) == null) {
        return i;
      }
    }
    // need to expand the triplet size
    ensureCapacity(i + 1, true);
    return i;
  }

  @Override
  boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock) {
    int blockIndex = BlockIdManager.getBlockIndex(reportedBlock);
    int index = blockIndex;
    DatanodeStorageInfo old = getStorageInfo(index);
    if (old != null && !old.equals(storage)) { // over replicated
      // check if the storage has been stored
      int i = findStorageInfo(storage);
      if (i == -1) {
        index = findSlot();
      } else {
        return true;
      }
    }
    addStorage(storage, index, blockIndex);
    return true;
  }

  private void addStorage(DatanodeStorageInfo storage, int index,
      int blockIndex) {
    setStorageInfo(index, storage);
    setNext(index, null);
    setPrevious(index, null);
    indices[index] = (byte) blockIndex;
  }

  private int findStorageInfoFromEnd(DatanodeStorageInfo storage) {
    final int len = getCapacity();
    for(int idx = len - 1; idx >= 0; idx--) {
      DatanodeStorageInfo cur = getStorageInfo(idx);
      if (storage.equals(cur)) {
        return idx;
      }
    }
    return -1;
  }

  @Override
  boolean removeStorage(DatanodeStorageInfo storage) {
    int dnIndex = findStorageInfoFromEnd(storage);
    if (dnIndex < 0) { // the node is not found
      return false;
    }
    assert getPrevious(dnIndex) == null && getNext(dnIndex) == null :
        "Block is still in the list and must be removed first.";
    // set the triplet to null
    setStorageInfo(dnIndex, null);
    setNext(dnIndex, null);
    setPrevious(dnIndex, null);
    indices[dnIndex] = -1;
    return true;
  }

  private void ensureCapacity(int totalSize, boolean keepOld) {
    if (getCapacity() < totalSize) {
      Object[] old = triplets;
      byte[] oldIndices = indices;
      triplets = new Object[totalSize * 3];
      indices = new byte[totalSize];
      initIndices();

      if (keepOld) {
        System.arraycopy(old, 0, triplets, 0, old.length);
        System.arraycopy(oldIndices, 0, indices, 0, oldIndices.length);
      }
    }
  }

  @Override
  void replaceBlock(BlockInfo newBlock) {
    assert newBlock instanceof BlockInfoStriped;
    BlockInfoStriped newBlockGroup = (BlockInfoStriped) newBlock;
    final int size = getCapacity();
    newBlockGroup.ensureCapacity(size, false);
    for (int i = 0; i < size; i++) {
      final DatanodeStorageInfo storage = this.getStorageInfo(i);
      if (storage != null) {
        final int blockIndex = indices[i];
        final boolean removed = storage.removeBlock(this);
        assert removed : "currentBlock not found.";

        newBlockGroup.addStorage(storage, i, blockIndex);
        storage.insertToList(newBlockGroup);
      }
    }
  }

  @Override
  public final boolean isStriped() {
    return true;
  }

  @Override
  public int numNodes() {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert triplets.length % 3 == 0 : "Malformed BlockInfo";
    int num = 0;
    for (int idx = getCapacity()-1; idx >= 0; idx--) {
      if (getStorageInfo(idx) != null) {
        num++;
      }
    }
    return num;
  }

  /**
   * Convert a complete block to an under construction block.
   * @return BlockInfoUnderConstruction -  an under construction block.
   */
  public BlockInfoStripedUnderConstruction convertToBlockUnderConstruction(
      BlockUCState s, DatanodeStorageInfo[] targets) {
    final BlockInfoStripedUnderConstruction ucBlock;
    if(isComplete()) {
      ucBlock = new BlockInfoStripedUnderConstruction(this, getDataBlockNum(),
              getParityBlockNum(),  s, targets);
      ucBlock.setBlockCollection(getBlockCollection());
    } else {
      // the block is already under construction
      ucBlock = (BlockInfoStripedUnderConstruction) this;
      ucBlock.setBlockUCState(s);
      ucBlock.setExpectedLocations(targets);
      ucBlock.setBlockCollection(getBlockCollection());
    }
    return ucBlock;
  }
}

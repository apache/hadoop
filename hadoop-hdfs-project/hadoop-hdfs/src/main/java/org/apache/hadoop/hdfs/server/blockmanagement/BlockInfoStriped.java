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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;

import java.util.Iterator;
import java.util.NoSuchElementException;

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
@InterfaceAudience.Private
public class BlockInfoStriped extends BlockInfo {
  private final ErasureCodingPolicy ecPolicy;
  /**
   * Always the same size with triplets. Record the block index for each triplet
   * TODO: actually this is only necessary for over-replicated block. Thus can
   * be further optimized to save memory usage.
   */
  private byte[] indices;

  public BlockInfoStriped(Block blk, ErasureCodingPolicy ecPolicy) {
    super(blk, (short) (ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits()));
    indices = new byte[ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits()];
    initIndices();
    this.ecPolicy = ecPolicy;
  }

  public short getTotalBlockNum() {
    return (short) (ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits());
  }

  public short getDataBlockNum() {
    return (short) ecPolicy.getNumDataUnits();
  }

  public short getParityBlockNum() {
    return (short) ecPolicy.getNumParityUnits();
  }

  public int getCellSize() {
    return ecPolicy.getCellSize();
  }

  /**
   * If the block is committed/completed and its length is less than a full
   * stripe, it returns the the number of actual data blocks.
   * Otherwise it returns the number of data units specified by erasure coding policy.
   */
  public short getRealDataBlockNum() {
    if (isComplete() || getBlockUCState() == BlockUCState.COMMITTED) {
      return (short) Math.min(getDataBlockNum(),
          (getNumBytes() - 1) / ecPolicy.getCellSize() + 1);
    } else {
      return getDataBlockNum();
    }
  }

  public short getRealTotalBlockNum() {
    return (short) (getRealDataBlockNum() + getParityBlockNum());
  }

  public ErasureCodingPolicy getErasureCodingPolicy() {
    return ecPolicy;
  }

  private void initIndices() {
    for (int i = 0; i < indices.length; i++) {
      indices[i] = -1;
    }
  }

  private int findSlot() {
    int i = getTotalBlockNum();
    int capacity = getCapacity();
    for (; i < capacity; i++) {
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
    Preconditions.checkArgument(BlockIdManager.isStripedBlockID(
        reportedBlock.getBlockId()), "reportedBlock is not striped");
    Preconditions.checkArgument(BlockIdManager.convertToStripedID(
        reportedBlock.getBlockId()) == this.getBlockId(),
        "reported blk_%s does not belong to the group of stored blk_%s",
        reportedBlock.getBlockId(), this.getBlockId());
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

  @VisibleForTesting
  public byte getStorageBlockIndex(DatanodeStorageInfo storage) {
    int i = this.findStorageInfo(storage);
    return i == -1 ? -1 : indices[i];
  }

  /**
   * Identify the block stored in the given datanode storage. Note that
   * the returned block has the same block Id with the one seen/reported by the
   * DataNode.
   */
  Block getBlockOnStorage(DatanodeStorageInfo storage) {
    int index = getStorageBlockIndex(storage);
    if (index < 0) {
      return null;
    } else {
      Block block = new Block(this);
      block.setBlockId(this.getBlockId() + index);
      return block;
    }
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

  public long spaceConsumed() {
    // In case striped blocks, total usage by this striped blocks should
    // be the total of data blocks and parity blocks because
    // `getNumBytes` is the total of actual data block size.
    return StripedBlockUtil.spaceConsumedByStripedBlock(getNumBytes(),
        ecPolicy.getNumDataUnits(), ecPolicy.getNumParityUnits(),
        ecPolicy.getCellSize());
    }

  @Override
  public final boolean isStriped() {
    return true;
  }

  @Override
  public BlockType getBlockType() {
    return BlockType.STRIPED;
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

  @Override
  final boolean hasNoStorage() {
    final int len = getCapacity();
    for(int idx = 0; idx < len; idx++) {
      if (getStorageInfo(idx) != null) {
        return false;
      }
    }
    return true;
  }

  /**
   * Striped blocks on Provided Storage is not supported. All blocks on
   * Provided storage are assumed to be "contiguous".
   */
  @Override
  boolean isProvided() {
    return false;
  }

  /**
   * This class contains datanode storage information and block index in the
   * block group.
   */
  public static class StorageAndBlockIndex {
    private final DatanodeStorageInfo storage;
    private final byte blockIndex;

    StorageAndBlockIndex(DatanodeStorageInfo storage, byte blockIndex) {
      this.storage = storage;
      this.blockIndex = blockIndex;
    }

    /**
     * @return storage in the datanode.
     */
    public DatanodeStorageInfo getStorage() {
      return storage;
    }

    /**
     * @return block index in the block group.
     */
    public byte getBlockIndex() {
      return blockIndex;
    }
  }

  public Iterable<StorageAndBlockIndex> getStorageAndIndexInfos() {
    return new Iterable<StorageAndBlockIndex>() {
      @Override
      public Iterator<StorageAndBlockIndex> iterator() {
        return new Iterator<StorageAndBlockIndex>() {
          private int index = 0;

          @Override
          public boolean hasNext() {
            while (index < getCapacity() && getStorageInfo(index) == null) {
              index++;
            }
            return index < getCapacity();
          }

          @Override
          public StorageAndBlockIndex next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            int i = index++;
            return new StorageAndBlockIndex(
                (DatanodeStorageInfo) triplets[i * 3], indices[i]);
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException("Remove is not supported");
          }
        };
      }
    };
  }
}

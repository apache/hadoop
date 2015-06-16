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

import java.util.Iterator;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo.AddBlockResult;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.LightWeightGSet;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

/**
 * This class maintains the map from a block to its metadata.
 * block's metadata currently includes blockCollection it belongs to and
 * the datanodes that store the block.
 */
class BlocksMap {
  private static class StorageIterator implements Iterator<DatanodeStorageInfo> {
    private final BlockInfoContiguous blockInfo;
    private int nextIdx = 0;
      
    StorageIterator(BlockInfoContiguous blkInfo) {
      this.blockInfo = blkInfo;
    }

    @Override
    public boolean hasNext() {
      return blockInfo != null && nextIdx < blockInfo.getCapacity()
              && blockInfo.getDatanode(nextIdx) != null;
    }

    @Override
    public DatanodeStorageInfo next() {
      return blockInfo.getStorageInfo(nextIdx++);
    }

    @Override
    public void remove()  {
      throw new UnsupportedOperationException("Sorry. can't remove.");
    }
  }

  /** Constant {@link LightWeightGSet} capacity. */
  private final int capacity;
  
  private GSet<Block, BlockInfoContiguous> blocks;

  BlocksMap(int capacity) {
    // Use 2% of total memory to size the GSet capacity
    this.capacity = capacity;
    this.blocks = new LightWeightGSet<Block, BlockInfoContiguous>(capacity) {
      @Override
      public Iterator<BlockInfoContiguous> iterator() {
        SetIterator iterator = new SetIterator();
        /*
         * Not tracking any modifications to set. As this set will be used
         * always under FSNameSystem lock, modifications will not cause any
         * ConcurrentModificationExceptions. But there is a chance of missing
         * newly added elements during iteration.
         */
        iterator.setTrackModification(false);
        return iterator;
      }
    };
  }


  void close() {
    clear();
    blocks = null;
  }
  
  void clear() {
    if (blocks != null) {
      blocks.clear();
    }
  }

  BlockCollection getBlockCollection(Block b) {
    BlockInfoContiguous info = blocks.get(b);
    return (info != null) ? info.getBlockCollection() : null;
  }

  /**
   * Add block b belonging to the specified block collection to the map.
   */
  BlockInfoContiguous addBlockCollection(BlockInfoContiguous b, BlockCollection bc) {
    BlockInfoContiguous info = blocks.get(b);
    if (info != b) {
      info = b;
      blocks.put(info);
    }
    info.setBlockCollection(bc);
    return info;
  }

  /**
   * Remove the block from the block map;
   * remove it from all data-node lists it belongs to;
   * and remove all data-node locations associated with the block.
   */
  void removeBlock(Block block) {
    BlockInfoContiguous blockInfo = blocks.remove(block);
    if (blockInfo == null)
      return;

    blockInfo.setBlockCollection(null);
    for(int idx = blockInfo.numNodes()-1; idx >= 0; idx--) {
      DatanodeDescriptor dn = blockInfo.getDatanode(idx);
      dn.removeBlock(blockInfo); // remove from the list and wipe the location
    }
  }
  
  /** Returns the block object it it exists in the map. */
  BlockInfoContiguous getStoredBlock(Block b) {
    return blocks.get(b);
  }

  /**
   * Searches for the block in the BlocksMap and 
   * returns {@link Iterable} of the storages the block belongs to.
   */
  Iterable<DatanodeStorageInfo> getStorages(Block b) {
    return getStorages(blocks.get(b));
  }

  /**
   * Searches for the block in the BlocksMap and 
   * returns {@link Iterable} of the storages the block belongs to
   * <i>that are of the given {@link DatanodeStorage.State state}</i>.
   * 
   * @param state DatanodeStorage state by which to filter the returned Iterable
   */
  Iterable<DatanodeStorageInfo> getStorages(Block b, final DatanodeStorage.State state) {
    return Iterables.filter(getStorages(blocks.get(b)), new Predicate<DatanodeStorageInfo>() {
      @Override
      public boolean apply(DatanodeStorageInfo storage) {
        return storage.getState() == state;
      }
    });
  }

  /**
   * For a block that has already been retrieved from the BlocksMap
   * returns {@link Iterable} of the storages the block belongs to.
   */
  Iterable<DatanodeStorageInfo> getStorages(final BlockInfoContiguous storedBlock) {
    return new Iterable<DatanodeStorageInfo>() {
      @Override
      public Iterator<DatanodeStorageInfo> iterator() {
        return new StorageIterator(storedBlock);
      }
    };
  }

  /** counts number of containing nodes. Better than using iterator. */
  int numNodes(Block b) {
    BlockInfoContiguous info = blocks.get(b);
    return info == null ? 0 : info.numNodes();
  }

  /**
   * Remove data-node reference from the block.
   * Remove the block from the block map
   * only if it does not belong to any file and data-nodes.
   */
  boolean removeNode(Block b, DatanodeDescriptor node) {
    BlockInfoContiguous info = blocks.get(b);
    if (info == null)
      return false;

    // remove block from the data-node list and the node from the block info
    boolean removed = node.removeBlock(info);

    if (info.getDatanode(0) == null     // no datanodes left
              && info.getBlockCollection() == null) {  // does not belong to a file
      blocks.remove(b);  // remove block from the map
    }
    return removed;
  }

  int size() {
    if (blocks != null) {
      return blocks.size();
    } else {
      return 0;
    }
  }

  Iterable<BlockInfoContiguous> getBlocks() {
    return blocks;
  }
  
  /** Get the capacity of the HashMap that stores blocks */
  int getCapacity() {
    return capacity;
  }

  /**
   * Replace a block in the block map by a new block.
   * The new block and the old one have the same key.
   * @param newBlock - block for replacement
   * @return new block
   */
  BlockInfoContiguous replaceBlock(BlockInfoContiguous newBlock) {
    BlockInfoContiguous currentBlock = blocks.get(newBlock);
    assert currentBlock != null : "the block if not in blocksMap";
    // replace block in data-node lists
    for (int i = currentBlock.numNodes() - 1; i >= 0; i--) {
      final DatanodeDescriptor dn = currentBlock.getDatanode(i);
      final DatanodeStorageInfo storage = currentBlock.findStorageInfo(dn);
      final boolean removed = storage.removeBlock(currentBlock);
      Preconditions.checkState(removed, "currentBlock not found.");

      final AddBlockResult result = storage.addBlock(newBlock);
      Preconditions.checkState(result == AddBlockResult.ADDED,
          "newBlock already exists.");
    }
    // replace block in the map itself
    blocks.put(newBlock);
    return newBlock;
  }
}

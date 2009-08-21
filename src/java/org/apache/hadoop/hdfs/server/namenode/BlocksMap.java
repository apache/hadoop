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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hdfs.protocol.Block;

/**
 * This class maintains the map from a block to its metadata.
 * block's metadata currently includes INode it belongs to and
 * the datanodes that store the block.
 */
class BlocksMap {
  private static class NodeIterator implements Iterator<DatanodeDescriptor> {
    private BlockInfo blockInfo;
    private int nextIdx = 0;
      
    NodeIterator(BlockInfo blkInfo) {
      this.blockInfo = blkInfo;
    }

    public boolean hasNext() {
      return blockInfo != null && nextIdx < blockInfo.getCapacity()
              && blockInfo.getDatanode(nextIdx) != null;
    }

    public DatanodeDescriptor next() {
      return blockInfo.getDatanode(nextIdx++);
    }

    public void remove()  {
      throw new UnsupportedOperationException("Sorry. can't remove.");
    }
  }

  // Used for tracking HashMap capacity growth
  private int capacity;
  private final float loadFactor;
  
  private Map<BlockInfo, BlockInfo> map;

  BlocksMap(int initialCapacity, float loadFactor) {
    this.capacity = 1;
    // Capacity is initialized to the next multiple of 2 of initialCapacity
    while (this.capacity < initialCapacity)
      this.capacity <<= 1;
    this.loadFactor = loadFactor;
    this.map = new HashMap<BlockInfo, BlockInfo>(initialCapacity, loadFactor);
  }

  INodeFile getINode(Block b) {
    BlockInfo info = map.get(b);
    return (info != null) ? info.getINode() : null;
  }

  /**
   * Add block b belonging to the specified file inode to the map.
   */
  BlockInfo addINode(BlockInfo b, INodeFile iNode) {
    BlockInfo info = map.get(b);
    if (info != b) {
      info = b;
      map.put(info, info);
    }
    info.setINode(iNode);
    return info;
  }

  /**
   * Remove the block from the block map;
   * remove it from all data-node lists it belongs to;
   * and remove all data-node locations associated with the block.
   */
  void removeBlock(Block block) {
    BlockInfo blockInfo = map.remove(block);
    if (blockInfo == null)
      return;

    blockInfo.setINode(null);
    for(int idx = blockInfo.numNodes()-1; idx >= 0; idx--) {
      DatanodeDescriptor dn = blockInfo.getDatanode(idx);
      dn.removeBlock(blockInfo); // remove from the list and wipe the location
    }
  }
  
  /** Returns the block object it it exists in the map. */
  BlockInfo getStoredBlock(Block b) {
    return map.get(b);
  }

  /**
   * Searches for the block in the BlocksMap and 
   * returns Iterator that iterates through the nodes the block belongs to.
   */
  Iterator<DatanodeDescriptor> nodeIterator(Block b) {
    return nodeIterator(map.get(b));
  }

  /**
   * For a block that has already been retrieved from the BlocksMap
   * returns Iterator that iterates through the nodes the block belongs to.
   */
  Iterator<DatanodeDescriptor> nodeIterator(BlockInfo storedBlock) {
    return new NodeIterator(storedBlock);
  }

  /** counts number of containing nodes. Better than using iterator. */
  int numNodes(Block b) {
    BlockInfo info = map.get(b);
    return info == null ? 0 : info.numNodes();
  }

  /**
   * Remove data-node reference from the block.
   * Remove the block from the block map
   * only if it does not belong to any file and data-nodes.
   */
  boolean removeNode(Block b, DatanodeDescriptor node) {
    BlockInfo info = map.get(b);
    if (info == null)
      return false;

    // remove block from the data-node list and the node from the block info
    boolean removed = node.removeBlock(info);

    if (info.getDatanode(0) == null     // no datanodes left
              && info.getINode() == null) {  // does not belong to a file
      map.remove(b);  // remove block from the map
    }
    return removed;
  }

  int size() {
    return map.size();
  }

  Collection<BlockInfo> getBlocks() {
    return map.values();
  }
  /**
   * Check if the block exists in map
   */
  boolean contains(Block block) {
    return map.containsKey(block);
  }
  
  /**
   * Check if the replica at the given datanode exists in map
   */
  boolean contains(Block block, DatanodeDescriptor datanode) {
    BlockInfo info = map.get(block);
    if (info == null)
      return false;
    
    if (-1 == info.findDatanode(datanode))
      return false;
    
    return true;
  }
  
  /** Get the capacity of the HashMap that stores blocks */
  int getCapacity() {
    // Capacity doubles every time the map size reaches the threshold
    while (map.size() > (int)(capacity * loadFactor)) {
      capacity <<= 1;
    }
    return capacity;
  }
  
  /** Get the load factor of the map */
  float getLoadFactor() {
    return loadFactor;
  }

  /**
   * Replace a block in the block map by a new block.
   * The new block and the old one have the same key.
   * @param newBlock - block for replacement
   * @return new block
   */
  BlockInfo replaceBlock(BlockInfo newBlock) {
    BlockInfo currentBlock = map.get(newBlock);
    assert currentBlock != null : "the block if not in blocksMap";
    // replace block in data-node lists
    for(int idx = currentBlock.numNodes()-1; idx >= 0; idx--) {
      DatanodeDescriptor dn = currentBlock.getDatanode(idx);
      dn.replaceBlock(currentBlock, newBlock);
    }
    // replace block in the map itself
    map.put(newBlock, newBlock);
    return newBlock;
  }
}

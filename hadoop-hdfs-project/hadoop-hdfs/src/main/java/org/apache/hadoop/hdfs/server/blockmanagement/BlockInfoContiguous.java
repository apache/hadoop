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

import java.util.LinkedList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.util.LightWeightGSet;

/**
 * BlockInfo class maintains for a given block
 * the {@link INodeFile} it is part of and datanodes where the replicas of 
 * the block are stored.
 * BlockInfo class maintains for a given block
 * the {@link BlockCollection} it is part of and datanodes where the replicas of 
 * the block are stored.
 */
@InterfaceAudience.Private
public class BlockInfoContiguous extends Block
    implements LightWeightGSet.LinkedElement {
  public static final BlockInfoContiguous[] EMPTY_ARRAY = {};

  private BlockCollection bc;

  /** For implementing {@link LightWeightGSet.LinkedElement} interface */
  private LightWeightGSet.LinkedElement nextLinkedElement;

  /**
   * This array contains triplets of references. For each i-th storage, the
   * block belongs to triplets[3*i] is the reference to the
   * {@link DatanodeStorageInfo} and triplets[3*i+1] and triplets[3*i+2] are
   * references to the previous and the next blocks, respectively, in the list
   * of blocks belonging to this storage.
   * 
   * Using previous and next in Object triplets is done instead of a
   * {@link LinkedList} list to efficiently use memory. With LinkedList the cost
   * per replica is 42 bytes (LinkedList#Entry object per replica) versus 16
   * bytes using the triplets.
   */
  private Object[] triplets;

  /**
   * Construct an entry for blocksmap
   * @param replication the block's replication factor
   */
  public BlockInfoContiguous(short replication) {
    this.triplets = new Object[3*replication];
    this.bc = null;
  }
  
  public BlockInfoContiguous(Block blk, short replication) {
    super(blk);
    this.triplets = new Object[3*replication];
    this.bc = null;
  }

  /**
   * Copy construction.
   * This is used to convert BlockInfoUnderConstruction
   * @param from BlockInfo to copy from.
   */
  protected BlockInfoContiguous(BlockInfoContiguous from) {
    this(from, from.bc.getBlockReplication());
    this.bc = from.bc;
  }

  public BlockCollection getBlockCollection() {
    return bc;
  }

  public void setBlockCollection(BlockCollection bc) {
    this.bc = bc;
  }

  public DatanodeDescriptor getDatanode(int index) {
    DatanodeStorageInfo storage = getStorageInfo(index);
    return storage == null ? null : storage.getDatanodeDescriptor();
  }

  DatanodeStorageInfo getStorageInfo(int index) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3 < triplets.length : "Index is out of bound";
    return (DatanodeStorageInfo)triplets[index*3];
  }

  private BlockInfoContiguous getPrevious(int index) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3+1 < triplets.length : "Index is out of bound";
    BlockInfoContiguous info = (BlockInfoContiguous)triplets[index*3+1];
    assert info == null || 
        info.getClass().getName().startsWith(BlockInfoContiguous.class.getName()) :
              "BlockInfo is expected at " + index*3;
    return info;
  }

  BlockInfoContiguous getNext(int index) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3+2 < triplets.length : "Index is out of bound";
    BlockInfoContiguous info = (BlockInfoContiguous)triplets[index*3+2];
    assert info == null || info.getClass().getName().startsWith(
        BlockInfoContiguous.class.getName()) :
        "BlockInfo is expected at " + index*3;
    return info;
  }

  private void setStorageInfo(int index, DatanodeStorageInfo storage) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3 < triplets.length : "Index is out of bound";
    triplets[index*3] = storage;
  }

  /**
   * Return the previous block on the block list for the datanode at
   * position index. Set the previous block on the list to "to".
   *
   * @param index - the datanode index
   * @param to - block to be set to previous on the list of blocks
   * @return current previous block on the list of blocks
   */
  private BlockInfoContiguous setPrevious(int index, BlockInfoContiguous to) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3+1 < triplets.length : "Index is out of bound";
    BlockInfoContiguous info = (BlockInfoContiguous)triplets[index*3+1];
    triplets[index*3+1] = to;
    return info;
  }

  /**
   * Return the next block on the block list for the datanode at
   * position index. Set the next block on the list to "to".
   *
   * @param index - the datanode index
   * @param to - block to be set to next on the list of blocks
   *    * @return current next block on the list of blocks
   */
  private BlockInfoContiguous setNext(int index, BlockInfoContiguous to) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3+2 < triplets.length : "Index is out of bound";
    BlockInfoContiguous info = (BlockInfoContiguous)triplets[index*3+2];
    triplets[index*3+2] = to;
    return info;
  }

  public int getCapacity() {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert triplets.length % 3 == 0 : "Malformed BlockInfo";
    return triplets.length / 3;
  }

  /**
   * Ensure that there is enough  space to include num more triplets.
   * @return first free triplet index.
   */
  private int ensureCapacity(int num) {
    assert this.triplets != null : "BlockInfo is not initialized";
    int last = numNodes();
    if(triplets.length >= (last+num)*3)
      return last;
    /* Not enough space left. Create a new array. Should normally 
     * happen only when replication is manually increased by the user. */
    Object[] old = triplets;
    triplets = new Object[(last+num)*3];
    System.arraycopy(old, 0, triplets, 0, last*3);
    return last;
  }

  /**
   * Count the number of data-nodes the block belongs to.
   */
  public int numNodes() {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert triplets.length % 3 == 0 : "Malformed BlockInfo";
    for(int idx = getCapacity()-1; idx >= 0; idx--) {
      if(getDatanode(idx) != null)
        return idx+1;
    }
    return 0;
  }

  /**
   * Add a {@link DatanodeStorageInfo} location for a block
   */
  boolean addStorage(DatanodeStorageInfo storage) {
    // find the last null node
    int lastNode = ensureCapacity(1);
    setStorageInfo(lastNode, storage);
    setNext(lastNode, null);
    setPrevious(lastNode, null);
    return true;
  }

  /**
   * Remove {@link DatanodeStorageInfo} location for a block
   */
  boolean removeStorage(DatanodeStorageInfo storage) {
    int dnIndex = findStorageInfo(storage);
    if(dnIndex < 0) // the node is not found
      return false;
    assert getPrevious(dnIndex) == null && getNext(dnIndex) == null : 
      "Block is still in the list and must be removed first.";
    // find the last not null node
    int lastNode = numNodes()-1; 
    // replace current node triplet by the lastNode one 
    setStorageInfo(dnIndex, getStorageInfo(lastNode));
    setNext(dnIndex, getNext(lastNode)); 
    setPrevious(dnIndex, getPrevious(lastNode)); 
    // set the last triplet to null
    setStorageInfo(lastNode, null);
    setNext(lastNode, null); 
    setPrevious(lastNode, null); 
    return true;
  }

  /**
   * Find specified DatanodeDescriptor.
   * @return index or -1 if not found.
   */
  boolean findDatanode(DatanodeDescriptor dn) {
    int len = getCapacity();
    for(int idx = 0; idx < len; idx++) {
      DatanodeDescriptor cur = getDatanode(idx);
      if(cur == dn) {
        return true;
      }
      if(cur == null) {
        break;
      }
    }
    return false;
  }

  /**
   * Find specified DatanodeStorageInfo.
   * @return DatanodeStorageInfo or null if not found.
   */
  DatanodeStorageInfo findStorageInfo(DatanodeDescriptor dn) {
    int len = getCapacity();
    for(int idx = 0; idx < len; idx++) {
      DatanodeStorageInfo cur = getStorageInfo(idx);
      if(cur == null)
        break;
      if(cur.getDatanodeDescriptor() == dn)
        return cur;
    }
    return null;
  }
  
  /**
   * Find specified DatanodeStorageInfo.
   * @return index or -1 if not found.
   */
  int findStorageInfo(DatanodeStorageInfo storageInfo) {
    int len = getCapacity();
    for(int idx = 0; idx < len; idx++) {
      DatanodeStorageInfo cur = getStorageInfo(idx);
      if (cur == storageInfo) {
        return idx;
      }
      if (cur == null) {
        break;
      }
    }
    return -1;
  }

  /**
   * Insert this block into the head of the list of blocks 
   * related to the specified DatanodeStorageInfo.
   * If the head is null then form a new list.
   * @return current block as the new head of the list.
   */
  BlockInfoContiguous listInsert(BlockInfoContiguous head,
      DatanodeStorageInfo storage) {
    int dnIndex = this.findStorageInfo(storage);
    assert dnIndex >= 0 : "Data node is not found: current";
    assert getPrevious(dnIndex) == null && getNext(dnIndex) == null : 
            "Block is already in the list and cannot be inserted.";
    this.setPrevious(dnIndex, null);
    this.setNext(dnIndex, head);
    if(head != null)
      head.setPrevious(head.findStorageInfo(storage), this);
    return this;
  }

  /**
   * Remove this block from the list of blocks 
   * related to the specified DatanodeStorageInfo.
   * If this block is the head of the list then return the next block as 
   * the new head.
   * @return the new head of the list or null if the list becomes
   * empy after deletion.
   */
  BlockInfoContiguous listRemove(BlockInfoContiguous head,
      DatanodeStorageInfo storage) {
    if(head == null)
      return null;
    int dnIndex = this.findStorageInfo(storage);
    if(dnIndex < 0) // this block is not on the data-node list
      return head;

    BlockInfoContiguous next = this.getNext(dnIndex);
    BlockInfoContiguous prev = this.getPrevious(dnIndex);
    this.setNext(dnIndex, null);
    this.setPrevious(dnIndex, null);
    if(prev != null)
      prev.setNext(prev.findStorageInfo(storage), next);
    if(next != null)
      next.setPrevious(next.findStorageInfo(storage), prev);
    if(this == head)  // removing the head
      head = next;
    return head;
  }

  /**
   * Remove this block from the list of blocks related to the specified
   * DatanodeDescriptor. Insert it into the head of the list of blocks.
   *
   * @return the new head of the list.
   */
  public BlockInfoContiguous moveBlockToHead(BlockInfoContiguous head,
      DatanodeStorageInfo storage, int curIndex, int headIndex) {
    if (head == this) {
      return this;
    }
    BlockInfoContiguous next = this.setNext(curIndex, head);
    BlockInfoContiguous prev = this.setPrevious(curIndex, null);

    head.setPrevious(headIndex, this);
    prev.setNext(prev.findStorageInfo(storage), next);
    if (next != null) {
      next.setPrevious(next.findStorageInfo(storage), prev);
    }
    return this;
  }

  /**
   * BlockInfo represents a block that is not being constructed.
   * In order to start modifying the block, the BlockInfo should be converted
   * to {@link BlockInfoContiguousUnderConstruction}.
   * @return {@link BlockUCState#COMPLETE}
   */
  public BlockUCState getBlockUCState() {
    return BlockUCState.COMPLETE;
  }

  /**
   * Is this block complete?
   * 
   * @return true if the state of the block is {@link BlockUCState#COMPLETE}
   */
  public boolean isComplete() {
    return getBlockUCState().equals(BlockUCState.COMPLETE);
  }

  /**
   * Convert a complete block to an under construction block.
   * @return BlockInfoUnderConstruction -  an under construction block.
   */
  public BlockInfoContiguousUnderConstruction convertToBlockUnderConstruction(
      BlockUCState s, DatanodeStorageInfo[] targets) {
    if(isComplete()) {
      BlockInfoContiguousUnderConstruction ucBlock =
          new BlockInfoContiguousUnderConstruction(this,
          getBlockCollection().getBlockReplication(), s, targets);
      ucBlock.setBlockCollection(getBlockCollection());
      return ucBlock;
    }
    // the block is already under construction
    BlockInfoContiguousUnderConstruction ucBlock =
        (BlockInfoContiguousUnderConstruction)this;
    ucBlock.setBlockUCState(s);
    ucBlock.setExpectedLocations(targets);
    ucBlock.setBlockCollection(getBlockCollection());
    return ucBlock;
  }

  @Override
  public int hashCode() {
    // Super implementation is sufficient
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    // Sufficient to rely on super's implementation
    return (this == obj) || super.equals(obj);
  }

  @Override
  public LightWeightGSet.LinkedElement getNext() {
    return nextLinkedElement;
  }

  @Override
  public void setNext(LightWeightGSet.LinkedElement next) {
    this.nextLinkedElement = next;
  }
}

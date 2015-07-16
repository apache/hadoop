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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;

/**
 * Subclass of {@link BlockInfo}, used for a block with replication scheme.
 */
@InterfaceAudience.Private
public class BlockInfoContiguous extends BlockInfo {

  public BlockInfoContiguous(short size) {
    super(size);
  }

  public BlockInfoContiguous(Block blk, short size) {
    super(blk, size);
  }

  /**
   * Copy construction. This is used to convert
   * BlockReplicationInfoUnderConstruction
   *
   * @param from BlockReplicationInfo to copy from.
   */
  protected BlockInfoContiguous(BlockInfoContiguous from) {
    this(from, (short) (from.triplets.length / 3));
    this.setBlockCollection(from.getBlockCollection());
  }

  /**
   * Ensure that there is enough  space to include num more triplets.
   * @return first free triplet index.
   */
  private int ensureCapacity(int num) {
    assert this.triplets != null : "BlockInfo is not initialized";
    int last = numNodes();
    if (triplets.length >= (last+num)*3) {
      return last;
    }
    /* Not enough space left. Create a new array. Should normally
     * happen only when replication is manually increased by the user. */
    Object[] old = triplets;
    triplets = new Object[(last+num)*3];
    System.arraycopy(old, 0, triplets, 0, last * 3);
    return last;
  }

  @Override
  boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock) {
    // find the last null node
    int lastNode = ensureCapacity(1);
    setStorageInfo(lastNode, storage);
    setNext(lastNode, null);
    setPrevious(lastNode, null);
    return true;
  }

  @Override
  boolean removeStorage(DatanodeStorageInfo storage) {
    int dnIndex = findStorageInfo(storage);
    if (dnIndex < 0) { // the node is not found
      return false;
    }
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

  @Override
  public int numNodes() {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert triplets.length % 3 == 0 : "Malformed BlockInfo";

    for (int idx = getCapacity()-1; idx >= 0; idx--) {
      if (getDatanode(idx) != null) {
        return idx + 1;
      }
    }
    return 0;
  }

  @Override
  void replaceBlock(BlockInfo newBlock) {
    assert newBlock instanceof BlockInfoContiguous;
    for (int i = this.numNodes() - 1; i >= 0; i--) {
      final DatanodeStorageInfo storage = this.getStorageInfo(i);
      final boolean removed = storage.removeBlock(this);
      assert removed : "currentBlock not found.";

      final DatanodeStorageInfo.AddBlockResult result = storage.addBlock(
          newBlock, newBlock);
      assert result == DatanodeStorageInfo.AddBlockResult.ADDED :
          "newBlock already exists.";
    }
  }

  /**
   * Convert a complete block to an under construction block.
   * @return BlockInfoUnderConstruction -  an under construction block.
   */
  public BlockInfoUnderConstructionContiguous convertToBlockUnderConstruction(
      BlockUCState s, DatanodeStorageInfo[] targets) {
    if(isComplete()) {
      BlockInfoUnderConstructionContiguous ucBlock =
          new BlockInfoUnderConstructionContiguous(this,
          getBlockCollection().getPreferredBlockReplication(), s, targets);
      ucBlock.setBlockCollection(getBlockCollection());
      return ucBlock;
    }
    // the block is already under construction
    BlockInfoUnderConstructionContiguous ucBlock =
        (BlockInfoUnderConstructionContiguous) this;
    ucBlock.setBlockUCState(s);
    ucBlock.setExpectedLocations(targets);
    ucBlock.setBlockCollection(getBlockCollection());
    return ucBlock;
  }

  @Override
  public final boolean isStriped() {
    return false;
  }

  @Override
  final boolean hasNoStorage() {
    return getStorageInfo(0) == null;
  }
}

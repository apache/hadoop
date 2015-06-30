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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

import java.util.ArrayList;

/**
 * Subclass of {@link BlockInfoUnderConstruction}, representing a block under
 * the contiguous (instead of striped) layout.
 */
public class BlockInfoUnderConstructionContiguous extends
    BlockInfoUnderConstruction {
  /**
   * Create block and set its state to
   * {@link HdfsServerConstants.BlockUCState#UNDER_CONSTRUCTION}.
   */
  public BlockInfoUnderConstructionContiguous(Block blk, short replication) {
    this(blk, replication, HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION,
        null);
  }

  /**
   * Create a block that is currently being constructed.
   */
  public BlockInfoUnderConstructionContiguous(Block blk, short replication,
      HdfsServerConstants.BlockUCState state, DatanodeStorageInfo[] targets) {
    super(blk, replication);
    Preconditions.checkState(getBlockUCState() !=
        HdfsServerConstants.BlockUCState.COMPLETE,
        "BlockInfoUnderConstructionContiguous cannot be in COMPLETE state");
    this.blockUCState = state;
    setExpectedLocations(targets);
  }

  /**
   * Convert an under construction block to a complete block.
   *
   * @return BlockInfo - a complete block.
   * @throws IOException if the state of the block
   * (the generation stamp and the length) has not been committed by
   * the client or it does not have at least a minimal number of replicas
   * reported from data-nodes.
   */
  @Override
  public BlockInfoContiguous convertToCompleteBlock() {
    Preconditions.checkState(getBlockUCState() !=
        HdfsServerConstants.BlockUCState.COMPLETE,
        "Trying to convert a COMPLETE block");
    return new BlockInfoContiguous(this);
  }

  @Override
  boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock) {
    return ContiguousBlockStorageOp.addStorage(this, storage);
  }

  @Override
  boolean removeStorage(DatanodeStorageInfo storage) {
    return ContiguousBlockStorageOp.removeStorage(this, storage);
  }

  @Override
  public int numNodes() {
    return ContiguousBlockStorageOp.numNodes(this);
  }

  @Override
  void replaceBlock(BlockInfo newBlock) {
    ContiguousBlockStorageOp.replaceBlock(this, newBlock);
  }

  @Override
  public void setExpectedLocations(DatanodeStorageInfo[] targets) {
    int numLocations = targets == null ? 0 : targets.length;
    this.replicas = new ArrayList<>(numLocations);
    for(int i = 0; i < numLocations; i++) {
      replicas.add(
          new ReplicaUnderConstruction(this, targets[i], HdfsServerConstants.ReplicaState.RBW));
    }
  }

  @Override
  public Block getTruncateBlock() {
    return truncateBlock;
  }

  @Override
  public void setTruncateBlock(Block recoveryBlock) {
    this.truncateBlock = recoveryBlock;
  }
}

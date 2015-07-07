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
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

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
   * Copy construction.
   * This is used to convert BlockReplicationInfoUnderConstruction
   * @param from BlockReplicationInfo to copy from.
   */
  protected BlockInfoContiguous(BlockInfo from) {
    super(from);
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
  BlockInfoUnderConstruction convertCompleteBlockToUC(
      HdfsServerConstants.BlockUCState s, DatanodeStorageInfo[] targets) {
    BlockInfoUnderConstructionContiguous ucBlock =
        new BlockInfoUnderConstructionContiguous(this,
            getBlockCollection().getPreferredBlockReplication(), s, targets);
    ucBlock.setBlockCollection(getBlockCollection());
    return ucBlock;
  }
}

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
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.net.Node;

import java.util.List;
import java.util.Set;

class ErasureCodingWork extends BlockReconstructionWork {
  private final byte[] liveBlockIndices;
  private final byte[] excludeReconstructedIndices;
  private final String blockPoolId;

  public ErasureCodingWork(String blockPoolId, BlockInfo block,
      BlockCollection bc,
      DatanodeDescriptor[] srcNodes,
      List<DatanodeDescriptor> containingNodes,
      List<DatanodeStorageInfo> liveReplicaStorages,
      int additionalReplRequired, int priority,
      byte[] liveBlockIndices,
      byte[] excludeReconstrutedIndices) {
    super(block, bc, srcNodes, containingNodes,
        liveReplicaStorages, additionalReplRequired, priority);
    this.blockPoolId = blockPoolId;
    this.liveBlockIndices = liveBlockIndices;
    this.excludeReconstructedIndices = excludeReconstrutedIndices;
    LOG.debug("Creating an ErasureCodingWork to {} reconstruct ",
        block);
  }

  byte[] getLiveBlockIndices() {
    return liveBlockIndices;
  }

  @Override
  void chooseTargets(BlockPlacementPolicy blockplacement,
      BlockStoragePolicySuite storagePolicySuite,
      Set<Node> excludedNodes) {
    // TODO: new placement policy for EC considering multiple writers
    DatanodeStorageInfo[] chosenTargets = null;
    // HDFS-14720. If the block is deleted, the block size will become
    // BlockCommand.NO_ACK (LONG.MAX_VALUE) . This kind of block we don't need
    // to send for replication or reconstruction
    if (!getBlock().isDeleted()) {
      chosenTargets = blockplacement.chooseTarget(
          getSrcPath(), getAdditionalReplRequired(), getSrcNodes()[0],
          getLiveReplicaStorages(), false, excludedNodes, getBlockSize(),
          storagePolicySuite.getPolicy(getStoragePolicyID()), null);
    } else {
      LOG.warn("ErasureCodingWork could not need choose targets for {}", getBlock());
    }
    setTargets(chosenTargets);
  }

  @Override
  boolean addTaskToDatanode(NumberReplicas numberReplicas) {
    final DatanodeStorageInfo[] targets = getTargets();
    assert targets.length > 0;
    BlockInfoStriped stripedBlk = (BlockInfoStriped) getBlock();
    targets[0].getDatanodeDescriptor().addBlockToBeErasureCoded(
        new ExtendedBlock(blockPoolId, stripedBlk), getSrcNodes(), targets,
        liveBlockIndices, excludeReconstructedIndices, stripedBlk.getErasureCodingPolicy());
    return true;
  }

  @VisibleForTesting
  public byte[] getExcludeReconstructedIndices() {
    return excludeReconstructedIndices;
  }
}

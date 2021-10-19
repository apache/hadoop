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

import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.net.Node;

import java.util.List;
import java.util.Set;

class ReplicationWork extends BlockReconstructionWork {
  public ReplicationWork(BlockInfo block, BlockCollection bc,
      DatanodeDescriptor[] srcNodes, List<DatanodeDescriptor> containingNodes,
      List<DatanodeStorageInfo> liveReplicaStorages, int additionalReplRequired,
      int priority) {
    super(block, bc, srcNodes, containingNodes,
        liveReplicaStorages, additionalReplRequired, priority);
    assert getSrcNodes().length == 1 :
        "There should be exactly 1 source node that have been selected";
    getSrcNodes()[0].incrementPendingReplicationWithoutTargets();
    LOG.debug("Creating a ReplicationWork to reconstruct " + block);
  }

  @Override
  void chooseTargets(BlockPlacementPolicy blockplacement,
      BlockStoragePolicySuite storagePolicySuite,
      Set<Node> excludedNodes) {
    assert getSrcNodes().length > 0
        : "At least 1 source node should have been selected";
    try {
      DatanodeStorageInfo[] chosenTargets = null;
      // HDFS-14720 If the block is deleted, the block size will become
      // BlockCommand.NO_ACK (LONG.MAX_VALUE) . This kind of block we don't need
      // to send for replication or reconstruction
      if (getBlock().getNumBytes() != BlockCommand.NO_ACK) {
        chosenTargets = blockplacement.chooseTarget(getSrcPath(),
            getAdditionalReplRequired(), getSrcNodes()[0],
            getLiveReplicaStorages(), false, excludedNodes, getBlockSize(),
            storagePolicySuite.getPolicy(getStoragePolicyID()), null);
      }
      setTargets(chosenTargets);
    } finally {
      getSrcNodes()[0].decrementPendingReplicationWithoutTargets();
    }
  }

  @Override
  void addTaskToDatanode(NumberReplicas numberReplicas) {
    getSrcNodes()[0].addBlockToBeReplicated(getBlock(), getTargets());
  }
}

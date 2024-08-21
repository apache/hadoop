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
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.DECOMMISSIONING;
import static org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState.LIVE;

public class ErasureCodingReplicationWork extends BlockReconstructionWork {

  private final Byte[] srcIndices;

  public ErasureCodingReplicationWork(BlockInfo block, BlockCollection bc,
      DatanodeDescriptor[] srcNodes, List<DatanodeDescriptor> containingNodes,
      List<DatanodeStorageInfo> liveReplicaStorages, int additionalReplRequired, Byte[] srcIndices,
      int priority) {
    super(block, bc, srcNodes, containingNodes, liveReplicaStorages, additionalReplRequired,
        priority);
    this.srcIndices = srcIndices;
    BlockManager.LOG.debug("Creating an ErasureCodingReplicationWork to {} reconstruct ", block);
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
    for (int i = 0; i < getSrcNodes().length && i < getTargets().length; i++) {
      BlockInfoStriped stripedBlk = (BlockInfoStriped) getBlock();
      final byte blockIndex = srcIndices[i];
      final DatanodeDescriptor source = getSrcNodes()[i];
      final DatanodeStorageInfo target = getTargets()[i];
      final long internBlkLen = StripedBlockUtil.getInternalBlockLength(
          stripedBlk.getNumBytes(), stripedBlk.getCellSize(),
          stripedBlk.getDataBlockNum(), blockIndex);
      final Block targetBlk = new Block(stripedBlk.getBlockId() + blockIndex,
          internBlkLen, stripedBlk.getGenerationStamp());
      source.addECBlockToBeReplicated(targetBlk, new DatanodeStorageInfo[] {target});
      LOG.debug("Add replication task from source {} to "
          + "target {} for EC block {}", source, target, targetBlk);
    }
    return true;
  }

  @VisibleForTesting
  public Byte[] getSrcIndices() {
    return srcIndices;
  }

  static byte chooseSource4SimpleReplication(NumberReplicasStriped numberReplicas) {
    Map<String, List<Byte>> map = new HashMap<>();
    for (int i = 0; i < numberReplicas.getSize(); i++) {
      if (numberReplicas.exist(i)) {
        DatanodeStorageInfo storage = numberReplicas.getStorage(i);
        StoredReplicaState state = numberReplicas.getState(i);
        if ((state == LIVE || state == DECOMMISSIONING) && !numberReplicas.isBusy(i)) {
          final String rack = storage.getDatanodeDescriptor().getNetworkLocation();
          List<Byte> dnList = map.get(rack);
          if (dnList == null) {
            dnList = new ArrayList<>();
            map.put(rack, dnList);
          }
          dnList.add((byte) i);
        }
      }
    }
    List<Byte> max = null;
    for (Map.Entry<String, List<Byte>> entry : map.entrySet()) {
      if (max == null || entry.getValue().size() > max.size()) {
        max = entry.getValue();
      }
    }
    return max != null ? max.get(0) : (byte) -1;
  }
}

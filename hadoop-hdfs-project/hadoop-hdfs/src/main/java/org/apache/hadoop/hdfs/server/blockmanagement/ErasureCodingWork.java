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

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.net.Node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class ErasureCodingWork extends BlockReconstructionWork {
  private final byte[] liveBlockIndices;
  private final byte[] liveBusyBlockIndices;
  private final byte[] excludeReconstructedIndices;
  private final String blockPoolId;

  public ErasureCodingWork(String blockPoolId, BlockInfo block,
      BlockCollection bc,
      DatanodeDescriptor[] srcNodes,
      List<DatanodeDescriptor> containingNodes,
      List<DatanodeStorageInfo> liveReplicaStorages,
      int additionalReplRequired, int priority,
      byte[] liveBlockIndices, byte[] liveBusyBlockIndices,
      byte[] excludeReconstrutedIndices) {
    super(block, bc, srcNodes, containingNodes,
        liveReplicaStorages, additionalReplRequired, priority);
    this.blockPoolId = blockPoolId;
    this.liveBlockIndices = liveBlockIndices;
    this.liveBusyBlockIndices = liveBusyBlockIndices;
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

  /**
   * @return true if the current source nodes cover all the internal blocks.
   * I.e., we only need to have more racks.
   */
  private boolean hasAllInternalBlocks() {
    final BlockInfoStriped block = (BlockInfoStriped) getBlock();
    if (liveBlockIndices.length
        + liveBusyBlockIndices.length < block.getRealTotalBlockNum()) {
      return false;
    }
    BitSet bitSet = new BitSet(block.getTotalBlockNum());
    for (byte index : liveBlockIndices) {
      bitSet.set(index);
    }
    for (byte busyIndex: liveBusyBlockIndices) {
      bitSet.set(busyIndex);
    }
    for (int i = 0; i < block.getRealDataBlockNum(); i++) {
      if (!bitSet.get(i)) {
        return false;
      }
    }
    for (int i = block.getDataBlockNum(); i < block.getTotalBlockNum(); i++) {
      if (!bitSet.get(i)) {
        return false;
      }
    }
    return true;
  }

  /**
   * We have all the internal blocks but not enough racks. Thus we do not need
   * to do decoding but only simply make an extra copy of an internal block. In
   * this scenario, use this method to choose the source datanode for simple
   * replication.
   * @return The index of the source datanode.
   */
  private int chooseSource4SimpleReplication() {
    Map<String, List<Integer>> map = new HashMap<>();
    for (int i = 0; i < getSrcNodes().length; i++) {
      final String rack = getSrcNodes()[i].getNetworkLocation();
      List<Integer> dnList = map.get(rack);
      if (dnList == null) {
        dnList = new ArrayList<>();
        map.put(rack, dnList);
      }
      dnList.add(i);
    }
    List<Integer> max = null;
    for (Map.Entry<String, List<Integer>> entry : map.entrySet()) {
      if (max == null || entry.getValue().size() > max.size()) {
        max = entry.getValue();
      }
    }
    assert max != null;
    return max.get(0);
  }

  @Override
  boolean addTaskToDatanode(NumberReplicas numberReplicas) {
    final DatanodeStorageInfo[] targets = getTargets();
    assert targets.length > 0;
    BlockInfoStriped stripedBlk = (BlockInfoStriped) getBlock();
    boolean flag = true;
    if (hasNotEnoughRack()) {
      // if we already have all the internal blocks, but not enough racks,
      // we only need to replicate one internal block to a new rack
      int sourceIndex = chooseSource4SimpleReplication();

      // Try to find a target on a new rack
      Set<String> racks = Arrays.stream(getSrcNodes())
          .map(DatanodeDescriptor::getNetworkLocation)
          .collect(Collectors.toSet());
      DatanodeStorageInfo targetOnNewRack = Arrays.stream(targets).filter(target ->
          target.getDatanodeDescriptor() != null &&
              !racks.contains(target.getDatanodeDescriptor().getNetworkLocation())
      ).findFirst().orElse(targets[0]);

      createReplicationWork(sourceIndex, targetOnNewRack);
    } else if ((numberReplicas.decommissioning() > 0 ||
        numberReplicas.liveEnteringMaintenanceReplicas() > 0) &&
        hasAllInternalBlocks()) {
      List<Integer> leavingServiceSources = findLeavingServiceSources();
      // decommissioningSources.size() should be >= targets.length
      final int num = Math.min(leavingServiceSources.size(), targets.length);
      if (num == 0) {
        flag = false;
      }
      for (int i = 0; i < num; i++) {
        createReplicationWork(leavingServiceSources.get(i), targets[i]);
      }
    } else {
      targets[0].getDatanodeDescriptor().addBlockToBeErasureCoded(
          new ExtendedBlock(blockPoolId, stripedBlk), getSrcNodes(), targets,
          liveBlockIndices, excludeReconstructedIndices, stripedBlk.getErasureCodingPolicy());
    }
    return flag;
  }

  private void createReplicationWork(int sourceIndex,
      DatanodeStorageInfo target) {
    BlockInfoStriped stripedBlk = (BlockInfoStriped) getBlock();
    final byte blockIndex = liveBlockIndices[sourceIndex];
    final DatanodeDescriptor source = getSrcNodes()[sourceIndex];
    final long internBlkLen = StripedBlockUtil.getInternalBlockLength(
        stripedBlk.getNumBytes(), stripedBlk.getCellSize(),
        stripedBlk.getDataBlockNum(), blockIndex);
    final Block targetBlk = new Block(stripedBlk.getBlockId() + blockIndex,
        internBlkLen, stripedBlk.getGenerationStamp());
    source.addECBlockToBeReplicated(targetBlk,
        new DatanodeStorageInfo[] {target});
    LOG.debug("Add replication task from source {} to "
        + "target {} for EC block {}", source, target, targetBlk);
  }

  private List<Integer> findLeavingServiceSources() {
    // Mark the block in normal node.
    BlockInfoStriped block = (BlockInfoStriped)getBlock();
    BitSet bitSet = new BitSet(block.getRealTotalBlockNum());
    for (int i = 0; i < getSrcNodes().length; i++) {
      if (getSrcNodes()[i].isInService()) {
        bitSet.set(liveBlockIndices[i]);
      }
    }
    // If the block is on the node which is decommissioning or
    // entering_maintenance, and it doesn't exist on other normal nodes,
    // we just add the node into source list.
    List<Integer> srcIndices = new ArrayList<>();
    for (int i = 0; i < getSrcNodes().length; i++) {
      if ((getSrcNodes()[i].isDecommissionInProgress() ||
          (getSrcNodes()[i].isEnteringMaintenance() &&
          getSrcNodes()[i].isAlive())) &&
          !bitSet.get(liveBlockIndices[i])) {
        srcIndices.add(i);
      }
    }
    return srcIndices;
  }
}

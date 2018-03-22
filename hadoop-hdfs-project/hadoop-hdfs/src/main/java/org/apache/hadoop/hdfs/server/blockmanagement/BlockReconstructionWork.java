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

import org.apache.hadoop.net.Node;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * This class is used internally by
 * {@link BlockManager#computeReconstructionWorkForBlocks} to represent a
 * task to reconstruct a block through replication or erasure coding.
 * Reconstruction is done by transferring data from srcNodes to targets
 */
abstract class BlockReconstructionWork {
  private final BlockInfo block;

  private final String srcPath;
  private final long blockSize;
  private final byte storagePolicyID;

  /**
   * An erasure coding reconstruction task has multiple source nodes.
   * A replication task only has 1 source node, stored on top of the array
   */
  private final DatanodeDescriptor[] srcNodes;
  /** Nodes containing the block; avoid them in choosing new targets */
  private final List<DatanodeDescriptor> containingNodes;
  /** Required by {@link BlockPlacementPolicy#chooseTarget} */
  private  final List<DatanodeStorageInfo> liveReplicaStorages;
  private final int additionalReplRequired;

  private DatanodeStorageInfo[] targets;
  private final int priority;
  private boolean notEnoughRack = false;

  public BlockReconstructionWork(BlockInfo block,
      BlockCollection bc,
      DatanodeDescriptor[] srcNodes,
      List<DatanodeDescriptor> containingNodes,
      List<DatanodeStorageInfo> liveReplicaStorages,
      int additionalReplRequired,
      int priority) {
    this.block = block;
    this.srcPath = bc.getName();
    this.blockSize = block.getNumBytes();
    this.storagePolicyID = bc.getStoragePolicyID();
    this.srcNodes = srcNodes;
    this.containingNodes = containingNodes;
    this.liveReplicaStorages = liveReplicaStorages;
    this.additionalReplRequired = additionalReplRequired;
    this.priority = priority;
    this.targets = null;
  }

  DatanodeStorageInfo[] getTargets() {
    return targets;
  }

  void resetTargets() {
    this.targets = null;
  }

  void setTargets(DatanodeStorageInfo[] targets) {
    this.targets = targets;
  }

  List<DatanodeDescriptor> getContainingNodes() {
    return Collections.unmodifiableList(containingNodes);
  }

  public int getPriority() {
    return priority;
  }

  public BlockInfo getBlock() {
    return block;
  }

  public DatanodeDescriptor[] getSrcNodes() {
    return srcNodes;
  }

  public String getSrcPath() {
    return srcPath;
  }

  public long getBlockSize() {
    return blockSize;
  }

  public byte getStoragePolicyID() {
    return storagePolicyID;
  }

  List<DatanodeStorageInfo> getLiveReplicaStorages() {
    return liveReplicaStorages;
  }

  public int getAdditionalReplRequired() {
    return additionalReplRequired;
  }

  /**
   * Mark that the reconstruction work is to replicate internal block to a new
   * rack.
   */
  void setNotEnoughRack() {
    notEnoughRack = true;
  }

  boolean hasNotEnoughRack() {
    return notEnoughRack;
  }

  abstract void chooseTargets(BlockPlacementPolicy blockplacement,
      BlockStoragePolicySuite storagePolicySuite,
      Set<Node> excludedNodes);

  /**
   * Add reconstruction task into a source datanode.
   *
   * @param numberReplicas replica details
   */
  abstract void addTaskToDatanode(NumberReplicas numberReplicas);
}

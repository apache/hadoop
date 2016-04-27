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

class ReplicationWork {
  private final BlockInfo block;
  private final BlockCollection bc;
  private final DatanodeDescriptor srcNode;
  private final int additionalReplRequired;
  private final int priority;
  private final List<DatanodeDescriptor> containingNodes;
  private final List<DatanodeStorageInfo> liveReplicaStorages;
  private DatanodeStorageInfo[] targets;

  public ReplicationWork(BlockInfo block, BlockCollection bc,
      DatanodeDescriptor srcNode, List<DatanodeDescriptor> containingNodes,
      List<DatanodeStorageInfo> liveReplicaStorages, int additionalReplRequired,
      int priority) {
    this.block = block;
    this.bc = bc;
    this.srcNode = srcNode;
    this.srcNode.incrementPendingReplicationWithoutTargets();
    this.containingNodes = containingNodes;
    this.liveReplicaStorages = liveReplicaStorages;
    this.additionalReplRequired = additionalReplRequired;
    this.priority = priority;
    this.targets = null;
  }

  void chooseTargets(BlockPlacementPolicy blockplacement,
      BlockStoragePolicySuite storagePolicySuite,
      Set<Node> excludedNodes) {
    try {
      targets = blockplacement.chooseTarget(bc.getName(),
          additionalReplRequired, srcNode, liveReplicaStorages, false,
          excludedNodes, block.getNumBytes(),
          storagePolicySuite.getPolicy(bc.getStoragePolicyID()), null);
    } finally {
      srcNode.decrementPendingReplicationWithoutTargets();
    }
  }

  DatanodeStorageInfo[] getTargets() {
    return targets;
  }

  void resetTargets() {
    this.targets = null;
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

  public DatanodeDescriptor getSrcNode() {
    return srcNode;
  }
}

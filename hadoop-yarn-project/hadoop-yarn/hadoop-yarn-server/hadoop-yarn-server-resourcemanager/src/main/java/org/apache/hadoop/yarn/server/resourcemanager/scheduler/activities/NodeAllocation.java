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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/*
 * It contains allocation information for one allocation in a node heartbeat.
 * Detailed allocation activities are first stored in "AllocationActivity"
 * as operations, then transformed to a tree structure.
 * Tree structure starts from root queue and ends in leaf queue,
 * application or container allocation.
 */
public class NodeAllocation {
  private NodeId nodeId;
  private long timestamp;
  private ContainerId containerId = null;
  private AllocationState containerState = AllocationState.DEFAULT;
  private List<AllocationActivity> allocationOperations;
  private String partition;

  private ActivityNode root = null;

  private static final Logger LOG =
      LoggerFactory.getLogger(NodeAllocation.class);

  public NodeAllocation(NodeId nodeId) {
    this.nodeId = nodeId;
    this.allocationOperations = new ArrayList<>();
  }

  public void addAllocationActivity(String parentName, String childName,
      Integer priority, ActivityState state, String diagnostic,
      ActivityLevel level, NodeId nId, Long allocationRequestId) {
    AllocationActivity allocate = new AllocationActivity(parentName, childName,
        priority, state, diagnostic, level, nId, allocationRequestId);
    this.allocationOperations.add(allocate);
  }

  public void updateContainerState(ContainerId containerId,
      AllocationState containerState) {
    this.containerId = containerId;
    this.containerState = containerState;
  }

  // In node allocation, transform each activity to a tree-like structure
  // for frontend activity display.
  // eg:    root
  //         / \
  //        a   b
  //       / \
  //    app1 app2
  //    / \
  //  CA1 CA2
  // CA means Container Attempt
  public void transformToTree() {
    List<ActivityNode> allocationTree = new ArrayList<>();

    if (root == null) {
      Set<String> names = Collections.newSetFromMap(new ConcurrentHashMap<>());
      ListIterator<AllocationActivity> ite = allocationOperations.listIterator(
          allocationOperations.size());
      while (ite.hasPrevious()) {
        String name = ite.previous().getName();
        if (name != null) {
          if (!names.contains(name)) {
            names.add(name);
          } else {
            ite.remove();
          }
        }
      }

      for (AllocationActivity allocationOperation : allocationOperations) {
        ActivityNode node = allocationOperation.createTreeNode();
        String name = node.getName();
        for (int i = allocationTree.size() - 1; i > -1; i--) {
          if (allocationTree.get(i).getParentName().equals(name)) {
            node.addChild(allocationTree.get(i));
            allocationTree.remove(i);
          } else {
            break;
          }
        }
        allocationTree.add(node);
      }
      root = allocationTree.get(0);
    }
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public AllocationState getFinalAllocationState() {
    return containerState;
  }

  public String getContainerId() {
    if (containerId == null)
      return null;
    return containerId.toString();
  }

  public ActivityNode getRoot() {
    return root;
  }

  public NodeId getNodeId() {
    return nodeId;
  }

  public String getPartition() {
    return partition;
  }

  public void setPartition(String partition) {
    this.partition = partition;
  }
}

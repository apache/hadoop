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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
  private long timeStamp;
  private ContainerId containerId = null;
  private AllocationState containerState = AllocationState.DEFAULT;
  private List<AllocationActivity> allocationOperations;

  private ActivityNode root = null;

  private static final Log LOG = LogFactory.getLog(NodeAllocation.class);

  public NodeAllocation(NodeId nodeId) {
    this.nodeId = nodeId;
    this.allocationOperations = new ArrayList<>();
  }

  public void addAllocationActivity(String parentName, String childName,
      String priority, ActivityState state, String diagnostic, String type) {
    AllocationActivity allocate = new AllocationActivity(parentName, childName,
        priority, state, diagnostic, type);
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

  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }

  public long getTimeStamp() {
    return this.timeStamp;
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

  public String getNodeId() {
    return nodeId.toString();
  }
}

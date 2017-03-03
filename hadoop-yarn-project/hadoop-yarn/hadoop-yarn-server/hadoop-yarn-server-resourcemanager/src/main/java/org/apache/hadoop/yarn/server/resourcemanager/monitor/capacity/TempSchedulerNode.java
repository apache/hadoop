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

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.List;

/**
 * This class will save necessary information which copied from
 * FiCaSchedulerNode. This is added majorly for performance consideration, this
 * can be cached to avoid hitting scheduler again and again. In addition,
 * we can add some preemption-required fields to the class.
 */
public class TempSchedulerNode {
  private List<RMContainer> runningContainers;
  private RMContainer reservedContainer;
  private Resource totalResource;

  // excluded reserved resource
  private Resource allocatedResource;

  // total - allocated
  private Resource availableResource;

  // just a shortcut of reservedContainer.getResource.
  private Resource reservedResource;

  private NodeId nodeId;

  public static TempSchedulerNode fromSchedulerNode(
      FiCaSchedulerNode schedulerNode) {
    TempSchedulerNode n = new TempSchedulerNode();
    n.totalResource = Resources.clone(schedulerNode.getTotalResource());
    n.allocatedResource = Resources.clone(schedulerNode.getAllocatedResource());
    n.runningContainers = schedulerNode.getCopiedListOfRunningContainers();
    n.reservedContainer = schedulerNode.getReservedContainer();
    if (n.reservedContainer != null) {
      n.reservedResource = n.reservedContainer.getReservedResource();
    } else {
      n.reservedResource = Resources.none();
    }
    n.availableResource = Resources.subtract(n.totalResource,
        n.allocatedResource);
    n.nodeId = schedulerNode.getNodeID();
    return n;
  }

  public NodeId getNodeId() {
    return nodeId;
  }

  public List<RMContainer> getRunningContainers() {
    return runningContainers;
  }

  public void setRunningContainers(List<RMContainer> runningContainers) {
    this.runningContainers = runningContainers;
  }

  public RMContainer getReservedContainer() {
    return reservedContainer;
  }

  public void setReservedContainer(RMContainer reservedContainer) {
    this.reservedContainer = reservedContainer;
  }

  public Resource getTotalResource() {
    return totalResource;
  }

  public void setTotalResource(Resource totalResource) {
    this.totalResource = totalResource;
  }

  public Resource getAllocatedResource() {
    return allocatedResource;
  }

  public void setAllocatedResource(Resource allocatedResource) {
    this.allocatedResource = allocatedResource;
  }

  public Resource getAvailableResource() {
    return availableResource;
  }

  public void setAvailableResource(Resource availableResource) {
    this.availableResource = availableResource;
  }

  public Resource getReservedResource() {
    return reservedResource;
  }

  public void setReservedResource(Resource reservedResource) {
    this.reservedResource = reservedResource;
  }
}

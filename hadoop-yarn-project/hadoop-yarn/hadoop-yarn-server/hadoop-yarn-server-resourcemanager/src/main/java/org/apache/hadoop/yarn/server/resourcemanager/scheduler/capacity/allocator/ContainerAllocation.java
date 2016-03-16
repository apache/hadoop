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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.List;

public class ContainerAllocation {
  /**
   * Skip the locality (e.g. node-local, rack-local, any), and look at other
   * localities of the same priority
   */
  public static final ContainerAllocation LOCALITY_SKIPPED =
      new ContainerAllocation(null, null, AllocationState.LOCALITY_SKIPPED);

  /**
   * Skip the priority, and look at other priorities of the same application
   */
  public static final ContainerAllocation PRIORITY_SKIPPED =
      new ContainerAllocation(null, null, AllocationState.PRIORITY_SKIPPED);

  /**
   * Skip the application, and look at other applications of the same queue
   */
  public static final ContainerAllocation APP_SKIPPED =
      new ContainerAllocation(null, null, AllocationState.APP_SKIPPED);

  /**
   * Skip the leaf-queue, and look at other queues of the same parent queue
   */
  public static final ContainerAllocation QUEUE_SKIPPED =
      new ContainerAllocation(null, null, AllocationState.QUEUE_SKIPPED);

  RMContainer containerToBeUnreserved;
  private Resource resourceToBeAllocated = Resources.none();
  AllocationState state;
  NodeType containerNodeType = NodeType.NODE_LOCAL;
  NodeType requestNodeType = NodeType.NODE_LOCAL;
  Container updatedContainer;
  private List<RMContainer> toKillContainers;

  public ContainerAllocation(RMContainer containerToBeUnreserved,
      Resource resourceToBeAllocated, AllocationState state) {
    this.containerToBeUnreserved = containerToBeUnreserved;
    this.resourceToBeAllocated = resourceToBeAllocated;
    this.state = state;
  }

  public RMContainer getContainerToBeUnreserved() {
    return containerToBeUnreserved;
  }

  public Resource getResourceToBeAllocated() {
    if (resourceToBeAllocated == null) {
      return Resources.none();
    }
    return resourceToBeAllocated;
  }

  public AllocationState getAllocationState() {
    return state;
  }

  public NodeType getContainerNodeType() {
    return containerNodeType;
  }

  public Container getUpdatedContainer() {
    return updatedContainer;
  }

  public void setToKillContainers(List<RMContainer> toKillContainers) {
    this.toKillContainers = toKillContainers;
  }

  public List<RMContainer> getToKillContainers() {
    return toKillContainers;
  }
}

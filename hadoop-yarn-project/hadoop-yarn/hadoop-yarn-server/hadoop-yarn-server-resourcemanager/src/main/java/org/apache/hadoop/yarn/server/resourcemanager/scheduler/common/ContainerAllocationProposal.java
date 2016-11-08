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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;

import java.util.Collections;
import java.util.List;

/**
 * Proposal to allocate/reserve a new container
 */
public class ContainerAllocationProposal<A extends SchedulerApplicationAttempt,
    N extends SchedulerNode> {
  // Container we allocated or reserved
  private SchedulerContainer<A, N> allocatedOrReservedContainer;

  // Containers we need to release before allocating or reserving the
  // new container
  private List<SchedulerContainer<A, N>> toRelease = Collections.emptyList();

  // When trying to allocate from a reserved container, set this, and this will
  // not be included by toRelease list
  private SchedulerContainer<A, N> allocateFromReservedContainer;

  private boolean isIncreasedAllocation;

  private NodeType allocationLocalityType;

  private NodeType requestLocalityType;

  private SchedulingMode schedulingMode;

  private Resource allocatedResource; // newly allocated resource

  public ContainerAllocationProposal(
      SchedulerContainer<A, N> allocatedOrReservedContainer,
      List<SchedulerContainer<A, N>> toRelease,
      SchedulerContainer<A, N> allocateFromReservedContainer,
      boolean isIncreasedAllocation, NodeType allocationLocalityType,
      NodeType requestLocalityType, SchedulingMode schedulingMode,
      Resource allocatedResource) {
    this.allocatedOrReservedContainer = allocatedOrReservedContainer;
    if (null != toRelease) {
      this.toRelease = toRelease;
    }
    this.allocateFromReservedContainer = allocateFromReservedContainer;
    this.isIncreasedAllocation = isIncreasedAllocation;
    this.allocationLocalityType = allocationLocalityType;
    this.requestLocalityType = requestLocalityType;
    this.schedulingMode = schedulingMode;
    this.allocatedResource = allocatedResource;
  }

  public SchedulingMode getSchedulingMode() {
    return schedulingMode;
  }

  public Resource getAllocatedOrReservedResource() {
    return allocatedResource;
  }

  public NodeType getAllocationLocalityType() {
    return allocationLocalityType;
  }

  public boolean isIncreasedAllocation() {
    return isIncreasedAllocation;
  }

  public SchedulerContainer<A, N> getAllocateFromReservedContainer() {
    return allocateFromReservedContainer;
  }

  public SchedulerContainer<A, N> getAllocatedOrReservedContainer() {
    return allocatedOrReservedContainer;
  }

  public List<SchedulerContainer<A, N>> getToRelease() {
    return toRelease;
  }

  @Override
  public String toString() {
    return allocatedOrReservedContainer.toString();
  }

  public NodeType getRequestLocalityType() {
    return requestLocalityType;
  }
}
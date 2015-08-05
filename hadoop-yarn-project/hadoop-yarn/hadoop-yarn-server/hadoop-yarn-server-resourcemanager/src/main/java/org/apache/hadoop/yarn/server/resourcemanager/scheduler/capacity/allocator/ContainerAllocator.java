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

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * For an application, resource limits and resource requests, decide how to
 * allocate container. This is to make application resource allocation logic
 * extensible.
 */
public abstract class ContainerAllocator {
  FiCaSchedulerApp application;
  final ResourceCalculator rc;
  final RMContext rmContext;
  
  public ContainerAllocator(FiCaSchedulerApp application,
      ResourceCalculator rc, RMContext rmContext) {
    this.application = application;
    this.rc = rc;
    this.rmContext = rmContext;
  }
  
  /**
   * preAllocation is to perform checks, etc. to see if we can/cannot allocate
   * container. It will put necessary information to returned
   * {@link ContainerAllocation}. 
   */
  abstract ContainerAllocation preAllocation(
      Resource clusterResource, FiCaSchedulerNode node,
      SchedulingMode schedulingMode, ResourceLimits resourceLimits,
      Priority priority, RMContainer reservedContainer);
  
  /**
   * doAllocation is to update application metrics, create containers, etc.
   * According to allocating conclusion decided by preAllocation.
   */
  abstract ContainerAllocation doAllocation(
      ContainerAllocation allocationResult, Resource clusterResource,
      FiCaSchedulerNode node, SchedulingMode schedulingMode, Priority priority,
      RMContainer reservedContainer);
  
  boolean checkHeadroom(Resource clusterResource,
      ResourceLimits currentResourceLimits, Resource required,
      FiCaSchedulerNode node) {
    // If headroom + currentReservation < required, we cannot allocate this
    // require
    Resource resourceCouldBeUnReserved = application.getCurrentReservation();
    if (!application.getCSLeafQueue().getReservationContinueLooking()
        || !node.getPartition().equals(RMNodeLabelsManager.NO_LABEL)) {
      // If we don't allow reservation continuous looking, OR we're looking at
      // non-default node partition, we won't allow to unreserve before
      // allocation.
      resourceCouldBeUnReserved = Resources.none();
    }
    return Resources.greaterThanOrEqual(rc, clusterResource, Resources.add(
        currentResourceLimits.getHeadroom(), resourceCouldBeUnReserved),
        required);
  }
  
  /**
   * allocate needs to handle following stuffs:
   * 
   * <ul>
   * <li>Select request: Select a request to allocate. E.g. select a resource
   * request based on requirement/priority/locality.</li>
   * <li>Check if a given resource can be allocated based on resource
   * availability</li>
   * <li>Do allocation: this will decide/create allocated/reserved
   * container, this will also update metrics</li>
   * </ul>
   */
  public ContainerAllocation allocate(Resource clusterResource,
      FiCaSchedulerNode node, SchedulingMode schedulingMode,
      ResourceLimits resourceLimits, Priority priority,
      RMContainer reservedContainer) {
    ContainerAllocation result =
        preAllocation(clusterResource, node, schedulingMode,
            resourceLimits, priority, reservedContainer);
    
    if (AllocationState.ALLOCATED == result.state
        || AllocationState.RESERVED == result.state) {
      result = doAllocation(result, clusterResource, node,
          schedulingMode, priority, reservedContainer);
    }
    
    return result;
  }
}
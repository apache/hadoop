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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSAssignment;
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
public abstract class AbstractContainerAllocator {
  private static final Log LOG = LogFactory.getLog(AbstractContainerAllocator.class);

  FiCaSchedulerApp application;
  final ResourceCalculator rc;
  final RMContext rmContext;
  
  public AbstractContainerAllocator(FiCaSchedulerApp application,
      ResourceCalculator rc, RMContext rmContext) {
    this.application = application;
    this.rc = rc;
    this.rmContext = rmContext;
  }

  protected CSAssignment getCSAssignmentFromAllocateResult(
      Resource clusterResource, ContainerAllocation result,
      RMContainer rmContainer) {
    // Handle skipped
    boolean skipped =
        (result.getAllocationState() == AllocationState.APP_SKIPPED);
    CSAssignment assignment = new CSAssignment(skipped);
    assignment.setApplication(application);
    
    // Handle excess reservation
    assignment.setExcessReservation(result.getContainerToBeUnreserved());

    // If we allocated something
    if (Resources.greaterThan(rc, clusterResource,
        result.getResourceToBeAllocated(), Resources.none())) {
      Resource allocatedResource = result.getResourceToBeAllocated();
      Container updatedContainer = result.getUpdatedContainer();

      assignment.setResource(allocatedResource);
      assignment.setType(result.getContainerNodeType());

      if (result.getAllocationState() == AllocationState.RESERVED) {
        // This is a reserved container
        LOG.info("Reserved container " + " application="
            + application.getApplicationId() + " resource=" + allocatedResource
            + " queue=" + this.toString() + " cluster=" + clusterResource);
        assignment.getAssignmentInformation().addReservationDetails(
            updatedContainer.getId(),
            application.getCSLeafQueue().getQueuePath());
        assignment.getAssignmentInformation().incrReservations();
        Resources.addTo(assignment.getAssignmentInformation().getReserved(),
            allocatedResource);
      } else if (result.getAllocationState() == AllocationState.ALLOCATED){
        // This is a new container
        // Inform the ordering policy
        LOG.info("assignedContainer" + " application attempt="
            + application.getApplicationAttemptId() + " container="
            + updatedContainer.getId() + " queue=" + this + " clusterResource="
            + clusterResource + " type=" + assignment.getType());

        application
            .getCSLeafQueue()
            .getOrderingPolicy()
            .containerAllocated(application,
                application.getRMContainer(updatedContainer.getId()));

        assignment.getAssignmentInformation().addAllocationDetails(
            updatedContainer.getId(),
            application.getCSLeafQueue().getQueuePath());
        assignment.getAssignmentInformation().incrAllocations();
        Resources.addTo(assignment.getAssignmentInformation().getAllocated(),
            allocatedResource);
        
        if (rmContainer != null) {
          assignment.setFulfilledReservation(true);
        }
      }

      assignment.setContainersToKill(result.getToKillContainers());
    }
    
    return assignment;
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
  public abstract CSAssignment assignContainers(Resource clusterResource,
      FiCaSchedulerNode node, SchedulingMode schedulingMode,
      ResourceLimits resourceLimits, RMContainer reservedContainer);
}
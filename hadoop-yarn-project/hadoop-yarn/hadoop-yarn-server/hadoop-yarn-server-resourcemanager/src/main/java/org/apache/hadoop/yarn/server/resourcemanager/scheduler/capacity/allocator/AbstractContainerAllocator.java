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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesLogger;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityDiagnosticConstant;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSAssignment;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSet;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * For an application, resource limits and resource requests, decide how to
 * allocate container. This is to make application resource allocation logic
 * extensible.
 */
public abstract class AbstractContainerAllocator {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractContainerAllocator.class);

  FiCaSchedulerApp application;
  AppSchedulingInfo appInfo;
  final ResourceCalculator rc;
  final RMContext rmContext;
  ActivitiesManager activitiesManager;

  public AbstractContainerAllocator(FiCaSchedulerApp application,
      ResourceCalculator rc, RMContext rmContext) {
    this(application, rc, rmContext, null);
  }

  public AbstractContainerAllocator(FiCaSchedulerApp application,
      ResourceCalculator rc, RMContext rmContext,
      ActivitiesManager activitiesManager) {
    this.application = application;
    this.appInfo =
        application == null ? null : application.getAppSchedulingInfo();
    this.rc = rc;
    this.rmContext = rmContext;
    this.activitiesManager = activitiesManager;
  }

  protected CSAssignment getCSAssignmentFromAllocateResult(
      Resource clusterResource, ContainerAllocation result,
      RMContainer rmContainer, FiCaSchedulerNode node) {
    // Handle skipped
    CSAssignment.SkippedType skipped =
        (result.getAllocationState() == AllocationState.APP_SKIPPED) ?
        CSAssignment.SkippedType.OTHER :
        CSAssignment.SkippedType.NONE;
    CSAssignment assignment = new CSAssignment(skipped);
    assignment.setApplication(application);

    // Handle excess reservation
    assignment.setExcessReservation(result.getContainerToBeUnreserved());

    assignment.setRequestLocalityType(result.requestLocalityType);

    // If we allocated something
    if (Resources.greaterThan(rc, clusterResource,
        result.getResourceToBeAllocated(), Resources.none())) {
      Resource allocatedResource = result.getResourceToBeAllocated();
      RMContainer updatedContainer = result.getUpdatedContainer();

      assignment.setResource(allocatedResource);
      assignment.setType(result.getContainerNodeType());

      if (result.getAllocationState() == AllocationState.RESERVED) {
        if (LOG.isDebugEnabled()) {
          // This is a reserved container
          // Since re-reservation could happen again and again for already
          // reserved containers. only do this in debug log.
          LOG.debug("Reserved container " + " application=" + application
              .getApplicationId() + " resource=" + allocatedResource + " queue="
              + appInfo.getQueueName() + " cluster=" + clusterResource);
        }
        assignment.getAssignmentInformation().addReservationDetails(
            updatedContainer, application.getCSLeafQueue().getQueuePath());
        assignment.getAssignmentInformation().incrReservations();
        Resources.addTo(assignment.getAssignmentInformation().getReserved(),
            allocatedResource);

        if (rmContainer != null) {
          ActivitiesLogger.APP.recordAppActivityWithAllocation(
              activitiesManager, node, application, updatedContainer,
              ActivityState.RE_RESERVED);
          ActivitiesLogger.APP.finishSkippedAppAllocationRecording(
              activitiesManager, application.getApplicationId(),
              ActivityState.SKIPPED, ActivityDiagnosticConstant.EMPTY);
        } else {
          ActivitiesLogger.APP.recordAppActivityWithAllocation(
              activitiesManager, node, application, updatedContainer,
              ActivityState.RESERVED);
          ActivitiesLogger.APP.finishAllocatedAppAllocationRecording(
              activitiesManager, application.getApplicationId(),
              updatedContainer.getContainerId(), ActivityState.RESERVED,
              ActivityDiagnosticConstant.EMPTY);
        }
      } else if (result.getAllocationState() == AllocationState.ALLOCATED){
        // This is a new container
        // Inform the ordering policy
        LOG.info("assignedContainer" + " application attempt=" + application
            .getApplicationAttemptId() + " container=" + updatedContainer
            .getContainerId() + " queue=" + appInfo.getQueueName()
            + " clusterResource=" + clusterResource
            + " type=" + assignment.getType() + " requestedPartition="
            + updatedContainer.getNodeLabelExpression());

        assignment.getAssignmentInformation().addAllocationDetails(
            updatedContainer, application.getCSLeafQueue().getQueuePath());
        assignment.getAssignmentInformation().incrAllocations();
        Resources.addTo(assignment.getAssignmentInformation().getAllocated(),
            allocatedResource);

        if (rmContainer != null) {
          assignment.setFulfilledReservation(true);
          assignment.setFulfilledReservedContainer(rmContainer);
        }

        ActivitiesLogger.APP.recordAppActivityWithAllocation(activitiesManager,
            node, application, updatedContainer, ActivityState.ALLOCATED);
        ActivitiesLogger.APP.finishAllocatedAppAllocationRecording(
            activitiesManager, application.getApplicationId(),
            updatedContainer.getContainerId(), ActivityState.ACCEPTED,
            ActivityDiagnosticConstant.EMPTY);

        // Update unformed resource
        application.incUnconfirmedRes(allocatedResource);
      }

      assignment.setContainersToKill(result.getToKillContainers());
    } else {
      if (result.getAllocationState() == AllocationState.QUEUE_SKIPPED) {
        assignment.setSkippedType(
            CSAssignment.SkippedType.QUEUE_LIMIT);
      }
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
   *
   * @param clusterResource clusterResource
   * @param candidates CandidateNodeSet
   * @param schedulingMode scheduling mode (exclusive or nonexclusive)
   * @param resourceLimits resourceLimits
   * @param reservedContainer reservedContainer
   * @return CSAssignemnt proposal
   */
  public abstract CSAssignment assignContainers(Resource clusterResource,
      CandidateNodeSet<FiCaSchedulerNode> candidates,
      SchedulingMode schedulingMode, ResourceLimits resourceLimits,
      RMContainer reservedContainer);
}

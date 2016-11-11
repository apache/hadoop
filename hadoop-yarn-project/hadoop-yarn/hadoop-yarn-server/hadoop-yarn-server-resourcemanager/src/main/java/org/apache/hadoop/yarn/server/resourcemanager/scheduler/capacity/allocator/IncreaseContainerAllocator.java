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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedContainerChangeRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSAssignment;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.PlacementSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.PlacementSetUtils;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class IncreaseContainerAllocator extends AbstractContainerAllocator {
  private static final Log LOG =
      LogFactory.getLog(IncreaseContainerAllocator.class);

  public IncreaseContainerAllocator(FiCaSchedulerApp application,
      ResourceCalculator rc, RMContext rmContext) {
    super(application, rc, rmContext);
  }
  
  /**
   * Quick check if we can allocate anything here:
   * We will not continue if: 
   * - Headroom doesn't support allocate minimumAllocation
   * - 
   */
  private boolean checkHeadroom(Resource clusterResource,
      ResourceLimits currentResourceLimits, Resource required) {
    return Resources.greaterThanOrEqual(rc, clusterResource,
        currentResourceLimits.getHeadroom(), required);
  }
  
  private CSAssignment createReservedIncreasedCSAssignment(
      SchedContainerChangeRequest request) {
    CSAssignment assignment =
        new CSAssignment(request.getDeltaCapacity(), NodeType.NODE_LOCAL, null,
            application, CSAssignment.SkippedType.NONE, false);
    Resources.addTo(assignment.getAssignmentInformation().getReserved(),
        request.getDeltaCapacity());
    assignment.getAssignmentInformation().incrReservations();
    assignment.getAssignmentInformation().addReservationDetails(
        request.getRMContainer(), application.getCSLeafQueue().getQueuePath());
    assignment.setIncreasedAllocation(true);
    
    LOG.info("Reserved increase container request:" + request.toString());
    
    return assignment;
  }
  
  private CSAssignment createSuccessfullyIncreasedCSAssignment(
      SchedContainerChangeRequest request, boolean fromReservation) {
    CSAssignment assignment =
        new CSAssignment(request.getDeltaCapacity(), NodeType.NODE_LOCAL, null,
            application, CSAssignment.SkippedType.NONE, fromReservation);
    Resources.addTo(assignment.getAssignmentInformation().getAllocated(),
        request.getDeltaCapacity());
    assignment.getAssignmentInformation().incrAllocations();
    assignment.getAssignmentInformation().addAllocationDetails(
        request.getRMContainer(), application.getCSLeafQueue().getQueuePath());
    assignment.setIncreasedAllocation(true);

    if (fromReservation) {
      assignment.setFulfilledReservedContainer(request.getRMContainer());
    }
    
    // notify application
    application
        .getCSLeafQueue()
        .getOrderingPolicy()
        .containerAllocated(application,
            application.getRMContainer(request.getContainerId()));

    LOG.info("Approved increase container request:" + request.toString()
        + " fromReservation=" + fromReservation);    
    
    return assignment;
  }
  
  private CSAssignment allocateIncreaseRequestFromReservedContainer(
      SchedulerNode node, Resource cluster,
      SchedContainerChangeRequest increaseRequest) {
    if (Resources.fitsIn(rc, cluster, increaseRequest.getDeltaCapacity(),
        node.getUnallocatedResource())) {
      return createSuccessfullyIncreasedCSAssignment(increaseRequest, true);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to allocate reserved increase request:"
            + increaseRequest.toString()
            + ". There's no enough available resource");
      }
      
      // We still cannot allocate this container, will wait for next turn
      return CSAssignment.SKIP_ASSIGNMENT;
    }
  }
  
  private CSAssignment allocateIncreaseRequest(FiCaSchedulerNode node,
      Resource cluster, SchedContainerChangeRequest increaseRequest) {
    if (Resources.fitsIn(rc, cluster, increaseRequest.getDeltaCapacity(),
        node.getUnallocatedResource())) {
      return createSuccessfullyIncreasedCSAssignment(increaseRequest, false);
    } else{
      // We cannot allocate this container, but since queue capacity /
      // user-limit matches, we can reserve this container on this node.
      return createReservedIncreasedCSAssignment(increaseRequest);
    }
  }

  @Override
  public CSAssignment assignContainers(Resource clusterResource,
      PlacementSet<FiCaSchedulerNode> ps, SchedulingMode schedulingMode,
      ResourceLimits resourceLimits, RMContainer reservedContainer) {
    AppSchedulingInfo sinfo = application.getAppSchedulingInfo();
    FiCaSchedulerNode node = PlacementSetUtils.getSingleNode(ps);

    if (null == node) {
      // This is global scheduling enabled
      // FIXME, support container increase when global scheduling enabled
      return CSAssignment.SKIP_ASSIGNMENT;
    }
    NodeId nodeId = node.getNodeID();

    if (reservedContainer == null) {
      // Do we have increase request on this node?
      if (!sinfo.hasIncreaseRequest(nodeId)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skip allocating increase request since we don't have any"
              + " increase request on this node=" + node.getNodeID());
        }
        
        return CSAssignment.SKIP_ASSIGNMENT;
      }
      
      // Check if we need to unreserve something, note that we don't support
      // continuousReservationLooking now. TODO, need think more about how to
      // support it.
      boolean shouldUnreserve =
          Resources.greaterThan(rc, clusterResource,
              resourceLimits.getAmountNeededUnreserve(), Resources.none());
      
      // Check if we can allocate minimum resource according to headroom
      boolean cannotAllocateAnything =
          !checkHeadroom(clusterResource, resourceLimits, rmContext
              .getScheduler().getMinimumResourceCapability());
      
      // Skip the app if we failed either of above check
      if (cannotAllocateAnything || shouldUnreserve) {
        if (LOG.isDebugEnabled()) {
          if (shouldUnreserve) {
            LOG.debug("Cannot continue since we have to unreserve some resource"
                + ", now increase container allocation doesn't "
                + "support continuous reservation looking..");
          }
          if (cannotAllocateAnything) {
            LOG.debug("We cannot allocate anything because of low headroom, "
                + "headroom=" + resourceLimits.getHeadroom());
          }
        }
        
        return CSAssignment.SKIP_ASSIGNMENT;
      }
      
      CSAssignment assigned = null;

      /*
       * Loop each priority, and containerId. Container priority is not
       * equivalent to request priority, application master can run an important
       * task on a less prioritized container.
       * 
       * So behavior here is, we still try to increase container with higher
       * priority, but will skip increase request and move to next increase
       * request if queue-limit or user-limit aren't satisfied 
       */
      for (SchedulerRequestKey schedulerKey : application.getSchedulerKeys()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Looking at increase request for application="
              + application.getApplicationAttemptId() + " priority="
              + schedulerKey.getPriority());
        }

        /*
         * If we have multiple to-be-increased containers under same priority on
         * a same host, we will try to increase earlier launched container
         * first. And again - we will skip a request and move to next if it
         * cannot be allocated.
         */
        Map<ContainerId, SchedContainerChangeRequest> increaseRequestMap =
            sinfo.getIncreaseRequests(nodeId, schedulerKey);

        // We don't have more increase request on this priority, skip..
        if (null == increaseRequestMap) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("There's no increase request for "
                + application.getApplicationAttemptId() + " priority="
                + schedulerKey.getPriority());
          }
          continue;
        }
        Iterator<Entry<ContainerId, SchedContainerChangeRequest>> iter =
            increaseRequestMap.entrySet().iterator();

        while (iter.hasNext()) {
          Entry<ContainerId, SchedContainerChangeRequest> entry =
              iter.next();
          SchedContainerChangeRequest increaseRequest =
              entry.getValue();
          
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "Looking at increase request=" + increaseRequest.toString());
          }

          boolean headroomSatisifed = checkHeadroom(clusterResource,
              resourceLimits, increaseRequest.getDeltaCapacity());
          if (!headroomSatisifed) {
            // skip if doesn't satisfy headroom limit
            if (LOG.isDebugEnabled()) {
              LOG.debug(" Headroom is not satisfied, skip..");
            }
            continue;
          }

          RMContainer rmContainer = increaseRequest.getRMContainer();
          if (rmContainer.getContainerState() != ContainerState.RUNNING) {
            // if the container is not running, we should remove the
            // increaseRequest and continue;
            if (LOG.isDebugEnabled()) {
              LOG.debug("  Container is not running any more, skip...");
            }
            application.addToBeRemovedIncreaseRequest(increaseRequest);
            continue;
          }

          if (!Resources.fitsIn(rc, clusterResource,
              increaseRequest.getTargetCapacity(), node.getTotalResource())) {
            // if the target capacity is more than what the node can offer, we
            // will simply remove and skip it.
            // The reason of doing check here instead of adding increase request
            // to scheduler because node's resource could be updated after
            // request added.
            if (LOG.isDebugEnabled()) {
              LOG.debug("  Target capacity is more than what node can offer,"
                  + " node.resource=" + node.getTotalResource());
            }
            application.addToBeRemovedIncreaseRequest(increaseRequest);
            continue;
          }

          // Try to allocate the increase request
          assigned =
              allocateIncreaseRequest(node, clusterResource, increaseRequest);
          if (assigned.getSkippedType()
              == CSAssignment.SkippedType.NONE) {
            // When we don't skip this request, which means we either allocated
            // OR reserved this request. We will break
            break;
          }
        }

        // We may have allocated something
        if (assigned != null && assigned.getSkippedType()
            == CSAssignment.SkippedType.NONE) {
          break;
        }
      }
      
      return assigned == null ? CSAssignment.SKIP_ASSIGNMENT : assigned;
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Trying to allocate reserved increase container request..");
      }
      
      // We already reserved this increase container
      SchedContainerChangeRequest request =
          sinfo.getIncreaseRequest(nodeId,
              reservedContainer.getAllocatedSchedulerKey(),
              reservedContainer.getContainerId());
      
      // We will cancel the reservation any of following happens
      // - Container finished
      // - No increase request needed
      // - Target resource updated
      if (null == request
          || reservedContainer.getContainerState() != ContainerState.RUNNING
          || (!Resources.equals(reservedContainer.getReservedResource(),
              request.getDeltaCapacity()))) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("We don't need reserved increase container request "
              + "for container=" + reservedContainer.getContainerId()
              + ". Unreserving and return...");
        }
        
        // We don't need this container now, just return excessive reservation
        return new CSAssignment(application, reservedContainer);
      }
      
      return allocateIncreaseRequestFromReservedContainer(node, clusterResource,
          request);
    }
  }
}

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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityLevel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.DiagnosticsCollector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.AppPlacementAllocator;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityDiagnosticConstant;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesLogger;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityState;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSAMContainerLaunchDiagnosticsConstants;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSAssignment;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSetUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.AM_ALLOW_NON_EXCLUSIVE_ALLOCATION;

/**
 * Allocate normal (new) containers, considers locality/label, etc. Using
 * delayed scheduling mechanism to get better locality allocation.
 */
public class RegularContainerAllocator extends AbstractContainerAllocator {
  private static final Logger LOG =
      LoggerFactory.getLogger(RegularContainerAllocator.class);

  public RegularContainerAllocator(FiCaSchedulerApp application,
      ResourceCalculator rc, RMContext rmContext,
      ActivitiesManager activitiesManager) {
    super(application, rc, rmContext, activitiesManager);
  }

  private boolean checkHeadroom(ResourceLimits currentResourceLimits,
                                Resource required, String nodePartition) {
    // If headroom + currentReservation < required, we cannot allocate this
    // require
    Resource resourceCouldBeUnReserved =
        application.getAppAttemptResourceUsage().getReserved(nodePartition);
    if (!application.getCSLeafQueue().isReservationsContinueLooking()) {
      // If we don't allow reservation continuous looking,
      // we won't allow to unreserve before allocation.
      resourceCouldBeUnReserved = Resources.none();
    }
    return Resources.fitsIn(rc, required,
        Resources.add(currentResourceLimits.getHeadroom(), resourceCouldBeUnReserved));
  }

  /*
   * Pre-check if we can allocate a pending resource request
   * (given schedulerKey) to a given CandidateNodeSet.
   * We will consider stuffs like exclusivity, pending resource, node partition,
   * headroom, etc.
   */
  private ContainerAllocation preCheckForNodeCandidateSet(FiCaSchedulerNode node,
      SchedulingMode schedulingMode, ResourceLimits resourceLimits,
      SchedulerRequestKey schedulerKey) {
    PendingAsk offswitchPendingAsk = application.getPendingAsk(schedulerKey,
        ResourceRequest.ANY);

    if (offswitchPendingAsk.getCount() <= 0) {
      ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
          activitiesManager, node, application, schedulerKey,
          ActivityDiagnosticConstant.REQUEST_DO_NOT_NEED_RESOURCE,
          ActivityLevel.REQUEST);
      return ContainerAllocation.PRIORITY_SKIPPED;
    }

    // Required resource
    Resource required = offswitchPendingAsk.getPerAllocationResource();

    // Do we need containers at this 'priority'?
    if (application.getOutstandingAsksCount(schedulerKey) <= 0) {
      ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
          activitiesManager, node, application, schedulerKey,
          ActivityDiagnosticConstant.REQUEST_DO_NOT_NEED_RESOURCE,
          ActivityLevel.REQUEST);
      return ContainerAllocation.PRIORITY_SKIPPED;
    }

    if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
      if (application.isWaitingForAMContainer() && !rmContext.getYarnConfiguration()
          .getBoolean(AM_ALLOW_NON_EXCLUSIVE_ALLOCATION, false)) {
        LOG.debug("Skip allocating AM container to app_attempt={},"
            + " don't allow to allocate AM container in non-exclusive mode",
            application.getApplicationAttemptId());
        application.updateAppSkipNodeDiagnostics(
            "Skipping assigning to Node in Ignore Exclusivity mode. ");
        ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
            activitiesManager, node, application, schedulerKey,
            ActivityDiagnosticConstant.
                REQUEST_SKIPPED_IN_IGNORE_EXCLUSIVITY_MODE,
            ActivityLevel.REQUEST);
        return ContainerAllocation.APP_SKIPPED;
      }
    }

    // Is the nodePartition of pending request matches the node's partition
    // If not match, jump to next priority.
    Optional<DiagnosticsCollector> dcOpt = activitiesManager == null ?
        Optional.empty() :
        activitiesManager.getOptionalDiagnosticsCollector();
    if (!appInfo.precheckNode(schedulerKey, node, schedulingMode, dcOpt)) {
      ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
          activitiesManager, node, application, schedulerKey,
          ActivityDiagnosticConstant.
              NODE_DO_NOT_MATCH_PARTITION_OR_PLACEMENT_CONSTRAINTS
              + ActivitiesManager.getDiagnostics(dcOpt),
          ActivityLevel.NODE);
      return ContainerAllocation.PRIORITY_SKIPPED;
    }

    if (!application.getCSLeafQueue().isReservationsContinueLooking()) {
      if (!shouldAllocOrReserveNewContainer(schedulerKey, required)) {
        LOG.debug("doesn't need containers based on reservation algo!");
        ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
            activitiesManager, node, application, schedulerKey,
            ActivityDiagnosticConstant.REQUEST_SKIPPED_BECAUSE_OF_RESERVATION,
            ActivityLevel.REQUEST);
        return ContainerAllocation.PRIORITY_SKIPPED;
      }
    }

    if (!checkHeadroom(resourceLimits, required, node.getPartition())) {
      LOG.debug("cannot allocate required resource={} because of headroom",
          required);
      ActivitiesLogger.APP.recordAppActivityWithoutAllocation(
          activitiesManager, node, application, schedulerKey,
          ActivityDiagnosticConstant.QUEUE_DO_NOT_HAVE_ENOUGH_HEADROOM,
          ActivityState.REJECTED,
          ActivityLevel.REQUEST);
      return ContainerAllocation.QUEUE_SKIPPED;
    }

    // Increase missed-non-partitioned-resource-request-opportunity.
    // This is to make sure non-partitioned-resource-request will prefer
    // to be allocated to non-partitioned nodes
    int missedNonPartitionedRequestSchedulingOpportunity = 0;
    AppPlacementAllocator appPlacementAllocator =
        appInfo.getAppPlacementAllocator(schedulerKey);
    if (null == appPlacementAllocator){
      // This is possible when #pending resource decreased by a different
      // thread.
      ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
          activitiesManager, node, application, schedulerKey,
          ActivityDiagnosticConstant.REQUEST_SKIPPED_BECAUSE_NULL_ANY_REQUEST,
          ActivityLevel.REQUEST);
      return ContainerAllocation.PRIORITY_SKIPPED;
    }
    String requestPartition =
        appPlacementAllocator.getPrimaryRequestedNodePartition();

    // Only do this when request associated with given scheduler key accepts
    // NO_LABEL under RESPECT_EXCLUSIVITY mode
    if (StringUtils.equals(RMNodeLabelsManager.NO_LABEL, requestPartition)) {
      missedNonPartitionedRequestSchedulingOpportunity =
          application.addMissedNonPartitionedRequestSchedulingOpportunity(
              schedulerKey);
    }

    if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
      // Before doing allocation, we need to check scheduling opportunity to
      // make sure : non-partitioned resource request should be scheduled to
      // non-partitioned partition first.
      if (missedNonPartitionedRequestSchedulingOpportunity < rmContext
          .getScheduler().getNumClusterNodes()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skip app_attempt=" + application.getApplicationAttemptId()
              + " priority=" + schedulerKey.getPriority()
              + " because missed-non-partitioned-resource-request"
              + " opportunity under required:" + " Now="
              + missedNonPartitionedRequestSchedulingOpportunity + " required="
              + rmContext.getScheduler().getNumClusterNodes());
        }
        ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
            activitiesManager, node, application, schedulerKey,
            ActivityDiagnosticConstant.
                REQUEST_SKIPPED_BECAUSE_NON_PARTITIONED_PARTITION_FIRST,
            ActivityLevel.REQUEST);
        return ContainerAllocation.APP_SKIPPED;
      }
    }

    return null;
  }

  private ContainerAllocation checkIfNodeBlackListed(FiCaSchedulerNode node,
      SchedulerRequestKey schedulerKey) {
    if (SchedulerAppUtils.isPlaceBlacklisted(application, node, LOG)) {
      application.updateAppSkipNodeDiagnostics(
          CSAMContainerLaunchDiagnosticsConstants.SKIP_AM_ALLOCATION_IN_BLACK_LISTED_NODE);
      ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
          activitiesManager, node, application, schedulerKey,
          ActivityDiagnosticConstant.NODE_IS_BLACKLISTED,
          ActivityLevel.NODE);
      return ContainerAllocation.APP_SKIPPED;
    }

    return null;
  }

  ContainerAllocation tryAllocateOnNode(Resource clusterResource,
      FiCaSchedulerNode node, SchedulingMode schedulingMode,
      ResourceLimits resourceLimits, SchedulerRequestKey schedulerKey,
      RMContainer reservedContainer) {
    ContainerAllocation result;

    // Sanity checks before assigning to this node
    result = checkIfNodeBlackListed(node, schedulerKey);
    if (null != result) {
      return result;
    }

    // Inform the application it is about to get a scheduling opportunity
    // TODO, we may need to revisit here to see if we should add scheduling
    // opportunity here
    application.addSchedulingOpportunity(schedulerKey);

    // Try to allocate containers on node
    result =
        assignContainersOnNode(clusterResource, node, schedulerKey,
            reservedContainer, schedulingMode, resourceLimits);
    
    if (null == reservedContainer) {
      if (result.getAllocationState() == AllocationState.PRIORITY_SKIPPED) {
        // Don't count 'skipped nodes' as a scheduling opportunity!
        application.subtractSchedulingOpportunity(schedulerKey);
      }
    }
    
    return result;
  }
  
  public float getLocalityWaitFactor(int uniqAsks, int clusterNodes) {
    // Estimate: Required unique resources (i.e. hosts + racks)
    int requiredResources = Math.max(uniqAsks - 1, 0);
    
    // waitFactor can't be more than '1' 
    // i.e. no point skipping more than clustersize opportunities
    return Math.min(((float)requiredResources / clusterNodes), 1.0f);
  }
  
  private int getActualNodeLocalityDelay() {
    return Math.min(rmContext.getScheduler().getNumClusterNodes(), application
        .getCSLeafQueue().getNodeLocalityDelay());
  }

  private int getActualRackLocalityDelay() {
    return Math.min(rmContext.getScheduler().getNumClusterNodes(),
        application.getCSLeafQueue().getNodeLocalityDelay()
        + application.getCSLeafQueue().getRackLocalityAdditionalDelay());
  }

  private boolean canAssign(SchedulerRequestKey schedulerKey,
      FiCaSchedulerNode node, NodeType type, RMContainer reservedContainer) {

    // Clearly we need containers for this application...
    if (type == NodeType.OFF_SWITCH) {
      if (reservedContainer != null) {
        return true;
      }
      // If there are no nodes in the cluster, return false.
      if (rmContext.getScheduler().getNumClusterNodes() == 0) {
        return false;
      }

      int uniqLocationAsks = 0;
      AppPlacementAllocator appPlacementAllocator =
          application.getAppPlacementAllocator(schedulerKey);
      if (appPlacementAllocator != null) {
        uniqLocationAsks = appPlacementAllocator.getUniqueLocationAsks();
      }
      // If we have only ANY requests for this schedulerKey, we should not
      // delay its scheduling.
      if (uniqLocationAsks == 1) {
        return true;
      }

      // 'Delay' off-switch
      long missedOpportunities =
          application.getSchedulingOpportunities(schedulerKey);

      // If rack locality additional delay parameter is enabled.
      if (application.getCSLeafQueue().getRackLocalityAdditionalDelay() > -1) {
        return missedOpportunities > getActualRackLocalityDelay();
      } else {
        long requiredContainers =
            application.getOutstandingAsksCount(schedulerKey);
        float localityWaitFactor = getLocalityWaitFactor(uniqLocationAsks,
            rmContext.getScheduler().getNumClusterNodes());
        // Cap the delay by the number of nodes in the cluster.
        return (Math.min(rmContext.getScheduler().getNumClusterNodes(),
            (requiredContainers * localityWaitFactor)) < missedOpportunities);
      }
    }

    // Check if we need containers on this rack
    if (application.getOutstandingAsksCount(schedulerKey,
        node.getRackName()) <= 0) {
      return false;
    }

    // If we are here, we do need containers on this rack for RACK_LOCAL req
    if (type == NodeType.RACK_LOCAL) {
      // 'Delay' rack-local just a little bit...
      long missedOpportunities =
          application.getSchedulingOpportunities(schedulerKey);
      return getActualNodeLocalityDelay() < missedOpportunities;
    }

    // Check if we need containers on this host
    if (type == NodeType.NODE_LOCAL) {
      // Now check if we need containers on this host...
      return application.getOutstandingAsksCount(schedulerKey,
          node.getNodeName()) > 0;
    }

    return false;
  }

  private ContainerAllocation assignNodeLocalContainers(
      Resource clusterResource, PendingAsk nodeLocalAsk,
      FiCaSchedulerNode node, SchedulerRequestKey schedulerKey,
      RMContainer reservedContainer, SchedulingMode schedulingMode,
      ResourceLimits currentResourceLimits) {
    if (canAssign(schedulerKey, node, NodeType.NODE_LOCAL, reservedContainer)) {
      return assignContainer(clusterResource, node, schedulerKey,
          nodeLocalAsk, NodeType.NODE_LOCAL, reservedContainer,
          schedulingMode, currentResourceLimits);
    }

    // Skip node-local request, go to rack-local request
    return ContainerAllocation.LOCALITY_SKIPPED;
  }

  private ContainerAllocation assignRackLocalContainers(
      Resource clusterResource, PendingAsk rackLocalAsk,
      FiCaSchedulerNode node, SchedulerRequestKey schedulerKey,
      RMContainer reservedContainer, SchedulingMode schedulingMode,
      ResourceLimits currentResourceLimits) {
    if (canAssign(schedulerKey, node, NodeType.RACK_LOCAL, reservedContainer)) {
      return assignContainer(clusterResource, node, schedulerKey,
          rackLocalAsk, NodeType.RACK_LOCAL, reservedContainer,
          schedulingMode, currentResourceLimits);
    }

    // Skip rack-local request, go to off-switch request
    return ContainerAllocation.LOCALITY_SKIPPED;
  }

  private ContainerAllocation assignOffSwitchContainers(
      Resource clusterResource, PendingAsk offSwitchAsk,
      FiCaSchedulerNode node, SchedulerRequestKey schedulerKey,
      RMContainer reservedContainer, SchedulingMode schedulingMode,
      ResourceLimits currentResourceLimits) {
    if (canAssign(schedulerKey, node, NodeType.OFF_SWITCH, reservedContainer)) {
      return assignContainer(clusterResource, node, schedulerKey,
          offSwitchAsk, NodeType.OFF_SWITCH, reservedContainer,
          schedulingMode, currentResourceLimits);
    }

    application.updateAppSkipNodeDiagnostics(
        CSAMContainerLaunchDiagnosticsConstants.SKIP_AM_ALLOCATION_DUE_TO_LOCALITY);
    ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
        activitiesManager, node, application, schedulerKey,
        ActivityDiagnosticConstant.NODE_SKIPPED_BECAUSE_OF_OFF_SWITCH_DELAY,
        ActivityLevel.NODE);
    return ContainerAllocation.APP_SKIPPED;
  }

  private ContainerAllocation assignContainersOnNode(Resource clusterResource,
      FiCaSchedulerNode node, SchedulerRequestKey schedulerKey,
      RMContainer reservedContainer, SchedulingMode schedulingMode,
      ResourceLimits currentResourceLimits) {
    ContainerAllocation allocation;
    NodeType requestLocalityType = null;

    // Data-local
    PendingAsk nodeLocalAsk =
        application.getPendingAsk(schedulerKey, node.getNodeName());
    if (nodeLocalAsk.getCount() > 0) {
      requestLocalityType = NodeType.NODE_LOCAL;
      allocation =
          assignNodeLocalContainers(clusterResource, nodeLocalAsk,
              node, schedulerKey, reservedContainer, schedulingMode,
              currentResourceLimits);
      if (Resources.greaterThan(rc, clusterResource,
          allocation.getResourceToBeAllocated(), Resources.none())) {
        allocation.requestLocalityType = requestLocalityType;
        return allocation;
      }
    }

    // Rack-local
    PendingAsk rackLocalAsk =
        application.getPendingAsk(schedulerKey, node.getRackName());
    if (rackLocalAsk.getCount() > 0) {
      if (!appInfo.canDelayTo(schedulerKey, node.getRackName())) {
        ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
            activitiesManager, node, application, schedulerKey,
            ActivityDiagnosticConstant.NODE_SKIPPED_BECAUSE_OF_RELAX_LOCALITY,
            ActivityLevel.NODE);
        return ContainerAllocation.PRIORITY_SKIPPED;
      }

      requestLocalityType = requestLocalityType == null ?
          NodeType.RACK_LOCAL :
          requestLocalityType;

      allocation =
          assignRackLocalContainers(clusterResource, rackLocalAsk,
              node, schedulerKey, reservedContainer, schedulingMode,
              currentResourceLimits);
      if (Resources.greaterThan(rc, clusterResource,
          allocation.getResourceToBeAllocated(), Resources.none())) {
        allocation.requestLocalityType = requestLocalityType;
        return allocation;
      }
    }

    // Off-switch
    PendingAsk offSwitchAsk =
        application.getPendingAsk(schedulerKey, ResourceRequest.ANY);
    if (offSwitchAsk.getCount() > 0) {
      if (!appInfo.canDelayTo(schedulerKey, ResourceRequest.ANY)) {
        ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
            activitiesManager, node, application, schedulerKey,
            ActivityDiagnosticConstant.NODE_SKIPPED_BECAUSE_OF_RELAX_LOCALITY,
            ActivityLevel.NODE);
        return ContainerAllocation.PRIORITY_SKIPPED;
      }

      requestLocalityType = requestLocalityType == null ?
          NodeType.OFF_SWITCH :
          requestLocalityType;

      allocation =
          assignOffSwitchContainers(clusterResource, offSwitchAsk,
              node, schedulerKey, reservedContainer, schedulingMode,
              currentResourceLimits);

      // When a returned allocation is LOCALITY_SKIPPED, since we're in
      // off-switch request now, we will skip this app w.r.t priorities 
      if (allocation.getAllocationState() == AllocationState.LOCALITY_SKIPPED) {
        allocation = ContainerAllocation.APP_SKIPPED;
      }
      allocation.requestLocalityType = requestLocalityType;

      return allocation;
    }
    ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
        activitiesManager, node, application, schedulerKey,
        ActivityDiagnosticConstant.
            NODE_SKIPPED_BECAUSE_OF_NO_OFF_SWITCH_AND_LOCALITY_VIOLATION,
        ActivityLevel.NODE);
    return ContainerAllocation.PRIORITY_SKIPPED;
  }

  private ContainerAllocation assignContainer(Resource clusterResource,
      FiCaSchedulerNode node, SchedulerRequestKey schedulerKey,
      PendingAsk pendingAsk, NodeType type, RMContainer rmContainer,
      SchedulingMode schedulingMode, ResourceLimits currentResourceLimits) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("assignContainers: node=" + node.getNodeName()
          + " application=" + application.getApplicationId()
          + " priority=" + schedulerKey.getPriority()
          + " pendingAsk=" + pendingAsk + " type=" + type);
    }

    Resource capability = pendingAsk.getPerAllocationResource();
    Resource available = node.getUnallocatedResource();
    Resource totalResource = node.getTotalResource();

    if (!Resources.fitsIn(capability, available)) {
      LOG.warn("Node : " + node.getNodeID()
          + " does not have sufficient resource for ask : " + pendingAsk
          + " node total capability : " + node.getTotalResource());
      // Skip this locality request
      ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
          activitiesManager, node, application, schedulerKey,
          ActivityDiagnosticConstant.
              NODE_TOTAL_RESOURCE_INSUFFICIENT_FOR_REQUEST
              + getResourceDiagnostics(capability, totalResource),
          ActivityLevel.NODE);
      return ContainerAllocation.LOCALITY_SKIPPED;
    }

    boolean shouldAllocOrReserveNewContainer = shouldAllocOrReserveNewContainer(
        schedulerKey, capability);

    // Can we allocate a container on this node?
    long availableContainers =
        rc.computeAvailableContainers(available, capability);
    // available resource for diagnostics collector
    Resource availableForDC = available;

    // How much need to unreserve equals to:
    // max(required - headroom, amountNeedUnreserve)
    Resource resourceNeedToUnReserve =
        Resources.max(rc, clusterResource,
            Resources.subtract(capability, currentResourceLimits.getHeadroom()),
            currentResourceLimits.getAmountNeededUnreserve());

    boolean needToUnreserve =
        rc.isAnyMajorResourceAboveZero(resourceNeedToUnReserve);

    RMContainer unreservedContainer = null;
    boolean reservationsContinueLooking =
        application.getCSLeafQueue().isReservationsContinueLooking();

    // Check if we need to kill some containers to allocate this one
    List<RMContainer> toKillContainers = null;
    if (availableContainers == 0 && currentResourceLimits.isAllowPreemption()) {
      Resource availableAndKillable = Resources.clone(available);
      for (RMContainer killableContainer : node
          .getKillableContainers().values()) {
        if (null == toKillContainers) {
          toKillContainers = new ArrayList<>();
        }
        toKillContainers.add(killableContainer);
        Resources.addTo(availableAndKillable,
                        killableContainer.getAllocatedResource());
        if (Resources.fitsIn(rc, capability, availableAndKillable)) {
          // Stop if we find enough spaces
          availableContainers = 1;
          break;
        }
      }
      availableForDC = availableAndKillable;
    }

    if (availableContainers > 0) {
      // Allocate...
      // We will only do continuous reservation when this is not allocated from
      // reserved container
      if (rmContainer == null && reservationsContinueLooking) {
        // when reservationsContinueLooking is set, we may need to unreserve
        // some containers to meet this queue, its parents', or the users'
        // resource limits.
        if (!shouldAllocOrReserveNewContainer || needToUnreserve) {
          if (!needToUnreserve) {
            // If we shouldn't allocate/reserve new container then we should
            // unreserve one the same size we are asking for since the
            // currentResourceLimits.getAmountNeededUnreserve could be zero. If
            // the limit was hit then use the amount we need to unreserve to be
            // under the limit.
            resourceNeedToUnReserve = capability;
          }
          unreservedContainer = application.findNodeToUnreserve(node,
                  schedulerKey, resourceNeedToUnReserve);
          // When (minimum-unreserved-resource > 0 OR we cannot allocate
          // new/reserved
          // container (That means we *have to* unreserve some resource to
          // continue)). If we failed to unreserve some resource, we can't
          // continue.
          if (null == unreservedContainer) {
            // Skip the locality request
            ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
                activitiesManager, node, application, schedulerKey,
                ActivityDiagnosticConstant.
                    NODE_CAN_NOT_FIND_CONTAINER_TO_BE_UNRESERVED_WHEN_NEEDED,
                ActivityLevel.NODE);
            return ContainerAllocation.LOCALITY_SKIPPED;
          }
        }
      }

      ContainerAllocation result = new ContainerAllocation(unreservedContainer,
          pendingAsk.getPerAllocationResource(), AllocationState.ALLOCATED);
      result.containerNodeType = type;
      result.setToKillContainers(toKillContainers);
      return result;
    } else {
      // if we are allowed to allocate but this node doesn't have space, reserve
      // it or if this was an already a reserved container, reserve it again
      if (shouldAllocOrReserveNewContainer || rmContainer != null) {
        if (reservationsContinueLooking && rmContainer == null) {
          // we could possibly ignoring queue capacity or user limits when
          // reservationsContinueLooking is set. Make sure we didn't need to
          // unreserve one.
          if (needToUnreserve) {
            LOG.debug("we needed to unreserve to be able to allocate");

            // Skip the locality request
            ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
                activitiesManager, node, application, schedulerKey,
                ActivityDiagnosticConstant.NODE_DO_NOT_HAVE_SUFFICIENT_RESOURCE
                    + getResourceDiagnostics(capability, availableForDC),
                ActivityLevel.NODE);
            return ContainerAllocation.LOCALITY_SKIPPED;          
          }
        }

        ActivitiesLogger.APP.recordAppActivityWithoutAllocation(
            activitiesManager, node, application, schedulerKey,
            ActivityDiagnosticConstant.NODE_DO_NOT_HAVE_SUFFICIENT_RESOURCE
                + getResourceDiagnostics(capability, availableForDC),
            rmContainer == null ?
                ActivityState.RESERVED : ActivityState.RE_RESERVED,
            ActivityLevel.NODE);
        ContainerAllocation result = new ContainerAllocation(null,
            pendingAsk.getPerAllocationResource(), AllocationState.RESERVED);
        result.containerNodeType = type;
        result.setToKillContainers(null);
        return result;
      }
      // Skip the locality request
      ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
          activitiesManager, node, application, schedulerKey,
          ActivityDiagnosticConstant.NODE_DO_NOT_HAVE_SUFFICIENT_RESOURCE
              + getResourceDiagnostics(capability, availableForDC),
          ActivityLevel.NODE);
      return ContainerAllocation.LOCALITY_SKIPPED;    
    }
  }

  boolean shouldAllocOrReserveNewContainer(
      SchedulerRequestKey schedulerKey, Resource required) {
    int requiredContainers =
        application.getOutstandingAsksCount(schedulerKey);
    int reservedContainers = application.getNumReservedContainers(schedulerKey);
    int starvation = 0;
    if (reservedContainers > 0) {
      float nodeFactor = Resources.ratio(
          rc, required, application.getCSLeafQueue().getMaximumAllocation());

      // Use percentage of node required to bias against large containers...
      // Protect against corner case where you need the whole node with
      // Math.min(nodeFactor, minimumAllocationFactor)
      starvation =
          (int) ((application.getReReservations(schedulerKey) /
              (float) reservedContainers) * (1.0f - (Math.min(
                  nodeFactor, application.getCSLeafQueue()
                  .getMinimumAllocationFactor()))));

      if (LOG.isDebugEnabled()) {
        LOG.debug("needsContainers:" + " app.#re-reserve="
            + application.getReReservations(schedulerKey) + " reserved="
            + reservedContainers + " nodeFactor=" + nodeFactor
            + " minAllocFactor="
            + application.getCSLeafQueue().getMinimumAllocationFactor()
            + " starvation=" + starvation);
      }
    }
    return (((starvation + requiredContainers) - reservedContainers) > 0);
  }
  
  private Container getContainer(RMContainer rmContainer,
      FiCaSchedulerNode node, Resource capability,
      SchedulerRequestKey schedulerKey) {
    return (rmContainer != null) ? rmContainer.getContainer()
        : createContainer(node, capability, schedulerKey);
  }

  private Container createContainer(FiCaSchedulerNode node, Resource capability,
      SchedulerRequestKey schedulerKey) {
    NodeId nodeId = node.getRMNode().getNodeID();

    // Create the container
    // Now set the containerId to null first, because it is possible the
    // container will be rejected because of concurrent resource allocation.
    // new containerId will be generated and assigned to the container
    // after confirmed.
    return BuilderUtils.newContainer(null, nodeId,
        node.getRMNode().getHttpAddress(), capability,
        schedulerKey.getPriority(), null,
        schedulerKey.getAllocationRequestId());
  }

  private ContainerAllocation handleNewContainerAllocation(
      ContainerAllocation allocationResult, FiCaSchedulerNode node,
      SchedulerRequestKey schedulerKey, Container container) {
    // Inform the application
    RMContainer allocatedContainer = application.allocate(node, schedulerKey,
        container);

    allocationResult.updatedContainer = allocatedContainer;

    // Does the application need this resource?
    if (allocatedContainer == null) {
      // Skip this app if we failed to allocate.
      ContainerAllocation ret =
          new ContainerAllocation(allocationResult.containerToBeUnreserved,
              null, AllocationState.APP_SKIPPED);
      ActivitiesLogger.APP.recordAppActivityWithoutAllocation(activitiesManager,
          node, application, schedulerKey,
          ActivityDiagnosticConstant.APPLICATION_FAIL_TO_ALLOCATE,
          ActivityState.REJECTED, ActivityLevel.APP);
      return ret;
    }
    
    return allocationResult;    
  }

  ContainerAllocation doAllocation(ContainerAllocation allocationResult,
      FiCaSchedulerNode node, SchedulerRequestKey schedulerKey,
      RMContainer reservedContainer) {
    // Create the container if necessary
    Container container =
        getContainer(reservedContainer, node,
            allocationResult.getResourceToBeAllocated(), schedulerKey);

    // something went wrong getting/creating the container
    if (container == null) {
      application
          .updateAppSkipNodeDiagnostics("Scheduling of container failed. ");
      LOG.warn("Couldn't get container for allocation!");
      ActivitiesLogger.APP.recordAppActivityWithoutAllocation(activitiesManager,
          node, application, schedulerKey,
          ActivityDiagnosticConstant.APPLICATION_COULD_NOT_GET_CONTAINER,
          ActivityState.REJECTED, ActivityLevel.APP);
      return ContainerAllocation.APP_SKIPPED;
    }

    if (allocationResult.getAllocationState() == AllocationState.ALLOCATED) {
      // When allocating container
      allocationResult = handleNewContainerAllocation(allocationResult, node,
          schedulerKey, container);
    } else {
      // When reserving container
      RMContainer updatedContainer = reservedContainer;
      if (updatedContainer == null) {
        AppPlacementAllocator<FiCaSchedulerNode> ps =
            application.getAppSchedulingInfo()
                .getAppPlacementAllocator(schedulerKey);
        if (null == ps) {
          LOG.warn("Failed to get " + AppPlacementAllocator.class.getName()
              + " for application=" + application.getApplicationId()
              + " schedulerRequestKey=" + schedulerKey);
          ActivitiesLogger.APP
              .recordAppActivityWithoutAllocation(activitiesManager, node,
                  application, schedulerKey,
                  ActivityDiagnosticConstant.
                      REQUEST_SKIPPED_BECAUSE_NULL_ANY_REQUEST,
                  ActivityState.REJECTED, ActivityLevel.REQUEST);
          return ContainerAllocation.PRIORITY_SKIPPED;
        }
        updatedContainer = new RMContainerImpl(container, schedulerKey,
            application.getApplicationAttemptId(), node.getNodeID(),
            application.getAppSchedulingInfo().getUser(), rmContext,
            ps.getPrimaryRequestedNodePartition());
      }
      allocationResult.updatedContainer = updatedContainer;
    }

    // Only reset opportunities when we FIRST allocate the container. (IAW, When
    // reservedContainer != null, it's not the first time)
    if (reservedContainer == null) {
      // Don't reset scheduling opportunities for off-switch assignments
      // otherwise the app will be delayed for each non-local assignment.
      // This helps apps with many off-cluster requests schedule faster.
      if (allocationResult.containerNodeType != NodeType.OFF_SWITCH) {
        LOG.debug("Resetting scheduling opportunities");

        // Only reset scheduling opportunities for RACK_LOCAL if configured
        // to do so. Not resetting means we will continue to schedule
        // RACK_LOCAL without delay.
        if (allocationResult.containerNodeType == NodeType.NODE_LOCAL
            || application.getCSLeafQueue().getRackLocalityFullReset()) {
          application.resetSchedulingOpportunities(schedulerKey);
        }
      }

      // Non-exclusive scheduling opportunity is different: we need reset
      // it when:
      // - It allocated on the default partition
      //
      // This is to make sure non-labeled resource request will be
      // most likely allocated on non-labeled nodes first.
      if (StringUtils.equals(node.getPartition(),
          RMNodeLabelsManager.NO_LABEL)) {
        application
            .resetMissedNonPartitionedRequestSchedulingOpportunity(schedulerKey);
      }
    }

    return allocationResult;
  }

  private ContainerAllocation allocate(Resource clusterResource,
      CandidateNodeSet<FiCaSchedulerNode> candidates,
      SchedulingMode schedulingMode, ResourceLimits resourceLimits,
      SchedulerRequestKey schedulerKey, RMContainer reservedContainer) {
    // Do checks before determining which node to allocate
    // Directly return if this check fails.
    ContainerAllocation result;
    ContainerAllocation lastReservation = null;

    AppPlacementAllocator<FiCaSchedulerNode> schedulingPS =
        application.getAppSchedulingInfo().getAppPlacementAllocator(
            schedulerKey);

    // This could be null when #pending request decreased by another thread.
    if (schedulingPS == null) {
      ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
          activitiesManager, null, application, schedulerKey,
          ActivityDiagnosticConstant.REQUEST_SKIPPED_BECAUSE_NULL_ANY_REQUEST,
          ActivityLevel.REQUEST);
      return new ContainerAllocation(reservedContainer, null,
          AllocationState.PRIORITY_SKIPPED);
    }

    result = ContainerAllocation.PRIORITY_SKIPPED;

    Iterator<FiCaSchedulerNode> iter = schedulingPS.getPreferredNodeIterator(
        candidates);

    while (iter.hasNext()) {
      FiCaSchedulerNode node = iter.next();

      // Do not schedule if there are any reservations to fulfill on the node
      RMContainer nodeReservedContainer = node.getReservedContainer();
      if (iter.hasNext() &&
          nodeReservedContainer != null &&
          isSkipAllocateOnNodesWithReservedContainer()) {
        LOG.debug("Skipping scheduling on node {} since it has already been"
                + " reserved by {}", node.getNodeID(),
            nodeReservedContainer.getContainerId());
        ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
            activitiesManager, node, application, schedulerKey,
            ActivityDiagnosticConstant.NODE_HAS_BEEN_RESERVED, ActivityLevel.NODE);
        continue;
      }

      if (reservedContainer == null) {
        result = preCheckForNodeCandidateSet(node,
            schedulingMode, resourceLimits, schedulerKey);
        if (null != result) {
          continue;
        }
      } else {
        // pre-check when allocating reserved container
        if (application.getOutstandingAsksCount(schedulerKey) == 0) {
          // Release
          result = new ContainerAllocation(reservedContainer, null,
              AllocationState.QUEUE_SKIPPED);
          continue;
        }
      }

      result = tryAllocateOnNode(clusterResource, node, schedulingMode,
          resourceLimits, schedulerKey, reservedContainer);

      if (AllocationState.ALLOCATED == result.getAllocationState()) {
        result = doAllocation(result, node, schedulerKey, reservedContainer);
        break;
      }

      // In MultiNodePlacement, Try Allocate on other Available nodes
      // from Iterator as well before Reserving. Else there won't be any
      // Allocate of new containers when the first node in the
      // iterator could not fit and returns RESERVED allocation.
      if (AllocationState.RESERVED == result.getAllocationState()) {
        lastReservation = result;
        if (iter.hasNext()) {
          continue;
        } else {
          result = doAllocation(lastReservation, node, schedulerKey,
              reservedContainer);
        }
      }
    }

    return result;
  }

  private boolean isSkipAllocateOnNodesWithReservedContainer() {
    ResourceScheduler scheduler = rmContext.getScheduler();
    boolean skipAllocateOnNodesWithReservedContainer = false;
    if (scheduler instanceof CapacityScheduler) {
      CapacityScheduler cs = (CapacityScheduler) scheduler;
      CapacitySchedulerConfiguration csConf = cs.getConfiguration();
      skipAllocateOnNodesWithReservedContainer =
          csConf.getSkipAllocateOnNodesWithReservedContainer();
    }
    return skipAllocateOnNodesWithReservedContainer;
  }

  @Override
  public CSAssignment assignContainers(Resource clusterResource,
      CandidateNodeSet<FiCaSchedulerNode> candidates,
      SchedulingMode schedulingMode, ResourceLimits resourceLimits,
      RMContainer reservedContainer) {
    FiCaSchedulerNode node = CandidateNodeSetUtils.getSingleNode(candidates);

    if (reservedContainer == null) {
      // Check if application needs more resource, skip if it doesn't need more.
      if (!application.hasPendingResourceRequest(candidates.getPartition(),
          schedulingMode)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skip app_attempt=" + application.getApplicationAttemptId()
              + ", because it doesn't need more resource, schedulingMode="
              + schedulingMode.name() + " node-label=" + candidates
              .getPartition());
        }
        ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
            activitiesManager, node, application, null,
            ActivityDiagnosticConstant.APPLICATION_DO_NOT_NEED_RESOURCE,
            ActivityLevel.APP);
        return CSAssignment.SKIP_ASSIGNMENT;
      }
      
      // Schedule in priority order
      for (SchedulerRequestKey schedulerKey : application.getSchedulerKeys()) {
        ContainerAllocation result = allocate(clusterResource, candidates,
            schedulingMode, resourceLimits, schedulerKey, null);

        AllocationState allocationState = result.getAllocationState();
        if (allocationState == AllocationState.PRIORITY_SKIPPED) {
          continue;
        }
        return getCSAssignmentFromAllocateResult(clusterResource, result,
            null, node);
      }

      // We will reach here if we skipped all priorities of the app, so we will
      // skip the app.
      return CSAssignment.SKIP_ASSIGNMENT;
    } else {
      ContainerAllocation result =
          allocate(clusterResource, candidates, schedulingMode, resourceLimits,
              reservedContainer.getReservedSchedulerKey(), reservedContainer);
      return getCSAssignmentFromAllocateResult(clusterResource, result,
          reservedContainer, node);
    }
  }

  private String getResourceDiagnostics(Resource required, Resource available) {
    if (activitiesManager == null) {
      return ActivitiesManager.EMPTY_DIAGNOSTICS;
    }
    return activitiesManager.getResourceDiagnostics(rc, required, available);
  }
}

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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerReservedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedContainerChangeRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSAMContainerLaunchDiagnosticsConstants;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSAssignment;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityHeadroomProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator.AbstractContainerAllocator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator.ContainerAllocator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerAllocationProposal;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.SchedulerContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.AppPlacementAllocator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSet;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;

/**
 * Represents an application attempt from the viewpoint of the FIFO or Capacity
 * scheduler.
 */
@Private
@Unstable
public class FiCaSchedulerApp extends SchedulerApplicationAttempt {
  private static final Log LOG = LogFactory.getLog(FiCaSchedulerApp.class);

  private final Set<ContainerId> containersToPreempt =
    new HashSet<ContainerId>();

  private CapacityHeadroomProvider headroomProvider;

  private ResourceCalculator rc = new DefaultResourceCalculator();

  private ResourceScheduler scheduler;

  private AbstractContainerAllocator containerAllocator;

  /**
   * to hold the message if its app doesn't not get container from a node
   */
  private String appSkipNodeDiagnostics;

  private Map<ContainerId, SchedContainerChangeRequest> toBeRemovedIncRequests =
      new ConcurrentHashMap<>();

  public FiCaSchedulerApp(ApplicationAttemptId applicationAttemptId,
      String user, Queue queue, AbstractUsersManager abstractUsersManager,
      RMContext rmContext) {
    this(applicationAttemptId, user, queue, abstractUsersManager, rmContext,
        Priority.newInstance(0), false);
  }

  public FiCaSchedulerApp(ApplicationAttemptId applicationAttemptId,
      String user, Queue queue, AbstractUsersManager abstractUsersManager,
      RMContext rmContext, Priority appPriority, boolean isAttemptRecovering) {
    this(applicationAttemptId, user, queue, abstractUsersManager, rmContext,
        appPriority, isAttemptRecovering, null);
  }

  public FiCaSchedulerApp(ApplicationAttemptId applicationAttemptId,
      String user, Queue queue, AbstractUsersManager abstractUsersManager,
      RMContext rmContext, Priority appPriority, boolean isAttemptRecovering,
      ActivitiesManager activitiesManager) {
    super(applicationAttemptId, user, queue, abstractUsersManager, rmContext);

    RMApp rmApp = rmContext.getRMApps().get(getApplicationId());

    Resource amResource;
    String partition;

    if (rmApp == null || rmApp.getAMResourceRequests() == null
        || rmApp.getAMResourceRequests().isEmpty()) {
      // the rmApp may be undefined (the resource manager checks for this too)
      // and unmanaged applications do not provide an amResource request
      // in these cases, provide a default using the scheduler
      amResource = rmContext.getScheduler().getMinimumResourceCapability();
      partition = CommonNodeLabelsManager.NO_LABEL;
    } else {
      amResource = rmApp.getAMResourceRequests().get(0).getCapability();
      partition =
          (rmApp.getAMResourceRequests().get(0)
              .getNodeLabelExpression() == null)
          ? CommonNodeLabelsManager.NO_LABEL
          : rmApp.getAMResourceRequests().get(0).getNodeLabelExpression();
    }

    setAppAMNodePartitionName(partition);
    setAMResource(partition, amResource);
    setPriority(appPriority);
    setAttemptRecovering(isAttemptRecovering);

    scheduler = rmContext.getScheduler();

    if (scheduler.getResourceCalculator() != null) {
      rc = scheduler.getResourceCalculator();
    }

    containerAllocator = new ContainerAllocator(this, rc, rmContext,
        activitiesManager);
  }

  public boolean containerCompleted(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event,
      String partition) {
    try {
      writeLock.lock();
      ContainerId containerId = rmContainer.getContainerId();

      // Remove from the list of containers
      if (null == liveContainers.remove(containerId)) {
        return false;
      }

      // Remove from the list of newly allocated containers if found
      newlyAllocatedContainers.remove(rmContainer);

      // Inform the container
      rmContainer.handle(
          new RMContainerFinishedEvent(containerId, containerStatus, event));

      containersToPreempt.remove(containerId);

      Resource containerResource = rmContainer.getContainer().getResource();
      RMAuditLogger.logSuccess(getUser(), AuditConstants.RELEASE_CONTAINER,
          "SchedulerApp", getApplicationId(), containerId, containerResource);

      // Update usage metrics
      queue.getMetrics().releaseResources(partition,
          getUser(), 1, containerResource);
      attemptResourceUsage.decUsed(partition, containerResource);

      // Clear resource utilization metrics cache.
      lastMemoryAggregateAllocationUpdateTime = -1;

      return true;
    } finally {
      writeLock.unlock();
    }
  }

  public RMContainer allocate(FiCaSchedulerNode node,
      SchedulerRequestKey schedulerKey, Container container) {
    try {
      readLock.lock();

      if (isStopped) {
        return null;
      }

      // Required sanity check - AM can call 'allocate' to update resource
      // request without locking the scheduler, hence we need to check
      if (getOutstandingAsksCount(schedulerKey) <= 0) {
        return null;
      }

      AppPlacementAllocator<FiCaSchedulerNode> ps =
          appSchedulingInfo.getAppPlacementAllocator(schedulerKey);
      if (null == ps) {
        LOG.warn("Failed to get " + AppPlacementAllocator.class.getName()
            + " for application=" + getApplicationId() + " schedulerRequestKey="
            + schedulerKey);
        return null;
      }

      // Create RMContainer
      RMContainer rmContainer = new RMContainerImpl(container, schedulerKey,
          this.getApplicationAttemptId(), node.getNodeID(),
          appSchedulingInfo.getUser(), this.rmContext,
          ps.getPrimaryRequestedNodePartition());
      ((RMContainerImpl) rmContainer).setQueueName(this.getQueueName());

      // FIXME, should set when confirmed
      updateAMContainerDiagnostics(AMState.ASSIGNED, null);

      return rmContainer;
    } finally {
      readLock.unlock();
    }
  }

  private boolean rmContainerInFinalState(RMContainer rmContainer) {
    if (null == rmContainer) {
      return false;
    }

    return rmContainer.completed();
  }

  private boolean anyContainerInFinalState(
      ResourceCommitRequest<FiCaSchedulerApp, FiCaSchedulerNode> request) {
    for (SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode> c : request
        .getContainersToRelease()) {
      if (rmContainerInFinalState(c.getRmContainer())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("To-release container=" + c.getRmContainer()
              + " is in final state");
        }
        return true;
      }
    }

    for (ContainerAllocationProposal<FiCaSchedulerApp, FiCaSchedulerNode> c : request
        .getContainersToAllocate()) {
      for (SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode> r : c
          .getToRelease()) {
        if (rmContainerInFinalState(r.getRmContainer())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("To-release container=" + r.getRmContainer()
                + ", for to a new allocated container, is in final state");
          }
          return true;
        }
      }

      if (null != c.getAllocateFromReservedContainer()) {
        if (rmContainerInFinalState(
            c.getAllocateFromReservedContainer().getRmContainer())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Allocate from reserved container" + c
                .getAllocateFromReservedContainer().getRmContainer()
                + " is in final state");
          }
          return true;
        }
      }
    }

    for (ContainerAllocationProposal<FiCaSchedulerApp, FiCaSchedulerNode> c : request
        .getContainersToReserve()) {
      for (SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode> r : c
          .getToRelease()) {
        if (rmContainerInFinalState(r.getRmContainer())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("To-release container=" + r.getRmContainer()
                + ", for a reserved container, is in final state");
          }
          return true;
        }
      }
    }

    return false;
  }

  private boolean commonCheckContainerAllocation(
      ContainerAllocationProposal<FiCaSchedulerApp, FiCaSchedulerNode> allocation,
      SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode> schedulerContainer) {
    // Make sure node is not reserved by anyone else
    RMContainer reservedContainerOnNode =
        schedulerContainer.getSchedulerNode().getReservedContainer();
    if (reservedContainerOnNode != null) {
      // adding NP check as this proposal could not be allocated from reserved
      // container in async-scheduling mode
      if (allocation.getAllocateFromReservedContainer() == null) {
        return false;
      }
      RMContainer fromReservedContainer =
          allocation.getAllocateFromReservedContainer().getRmContainer();

      if (fromReservedContainer != reservedContainerOnNode) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Try to allocate from a non-existed reserved container");
        }
        return false;
      }
    }

    // Do we have enough space on this node?
    Resource availableResource = Resources.clone(
        schedulerContainer.getSchedulerNode().getUnallocatedResource());

    // If we have any to-release container in non-reserved state, they are
    // from lazy-preemption, add their consumption to available resource
    // of this node
    if (allocation.getToRelease() != null && !allocation.getToRelease()
        .isEmpty()) {
      for (SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode>
          releaseContainer : allocation.getToRelease()) {
        // Only consider non-reserved container (reserved container will
        // not affect available resource of node) on the same node
        if (releaseContainer.getRmContainer().getState()
            != RMContainerState.RESERVED
            && releaseContainer.getSchedulerNode() == schedulerContainer
            .getSchedulerNode()) {
          Resources.addTo(availableResource,
              releaseContainer.getRmContainer().getAllocatedResource());
        }
      }
    }
    if (!Resources.fitsIn(rc, allocation.getAllocatedOrReservedResource(),
        availableResource)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Node doesn't have enough available resource, asked="
            + allocation.getAllocatedOrReservedResource() + " available="
            + availableResource);
      }
      return false;
    }

    return true;
  }

  public boolean accept(Resource cluster,
      ResourceCommitRequest<FiCaSchedulerApp, FiCaSchedulerNode> request,
      boolean checkPending) {
    ContainerRequest containerRequest = null;
    boolean reReservation = false;

    try {
      readLock.lock();

      // First make sure no container in release list in final state
      if (anyContainerInFinalState(request)) {
        return false;
      }

      // TODO, make sure all scheduler nodes are existed
      // TODO, make sure all node labels are not changed

      if (request.anythingAllocatedOrReserved()) {
        /*
         * 1) If this is a newly allocated container, check if the node is reserved
         *    / not-reserved by any other application
         * 2) If this is a newly reserved container, check if the node is reserved or not
         */
        // Assume we have only one container allocated or reserved
        ContainerAllocationProposal<FiCaSchedulerApp, FiCaSchedulerNode>
            allocation = request.getFirstAllocatedOrReservedContainer();
        SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode>
            schedulerContainer = allocation.getAllocatedOrReservedContainer();

        if (schedulerContainer.isAllocated()) {
          // When allocate a new container
          containerRequest =
              schedulerContainer.getRmContainer().getContainerRequest();

          // Check pending resource request
          if (checkPending &&
              !appSchedulingInfo.checkAllocation(
                  allocation.getAllocationLocalityType(),
                  schedulerContainer.getSchedulerNode(),
                  schedulerContainer.getSchedulerRequestKey())) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("No pending resource for: nodeType=" + allocation
                  .getAllocationLocalityType() + ", node=" + schedulerContainer
                  .getSchedulerNode() + ", requestKey=" + schedulerContainer
                  .getSchedulerRequestKey() + ", application="
                  + getApplicationAttemptId());
            }

            return false;
          }

          // Common part of check container allocation regardless if it is a
          // increase container or regular container
          if (!commonCheckContainerAllocation(allocation, schedulerContainer)) {
            return false;
          }
        } else {
          // Container reserved first time will be NEW, after the container
          // accepted & confirmed, it will become RESERVED state
          if (schedulerContainer.getRmContainer().getState()
              == RMContainerState.RESERVED) {
            // Check if node currently reserved by other application, there may
            // be some outdated proposals in async-scheduling environment
            if (schedulerContainer.getRmContainer() != schedulerContainer
                .getSchedulerNode().getReservedContainer()) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Try to re-reserve a container, but node "
                    + schedulerContainer.getSchedulerNode()
                    + " is already reserved by another container="
                    + schedulerContainer.getSchedulerNode()
                    .getReservedContainer());
              }
              return false;
            }
            // Set reReservation == true
            reReservation = true;
          } else {
            // When reserve a resource (state == NEW is for new container,
            // state == RUNNING is for increase container).
            // Just check if the node is not already reserved by someone
            if (schedulerContainer.getSchedulerNode().getReservedContainer()
                != null) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Try to reserve a container, but the node is "
                    + "already reserved by another container="
                    + schedulerContainer.getSchedulerNode()
                    .getReservedContainer().getContainerId());
              }
              return false;
            }
          }
        }
      }
    } finally {
      readLock.unlock();
    }

    // Skip check parent if this is a re-reservation container
    boolean accepted = true;
    if (!reReservation) {
      // Check parent if anything allocated or reserved
      if (request.anythingAllocatedOrReserved()) {
        accepted = getCSLeafQueue().accept(cluster, request);
      }
    }

    // When rejected, recover resource requests for this app
    if (!accepted && containerRequest != null) {
      recoverResourceRequestsForContainer(containerRequest);
    }

    return accepted;
  }

  public void apply(Resource cluster, ResourceCommitRequest<FiCaSchedulerApp,
      FiCaSchedulerNode> request, boolean updatePending) {
    boolean reReservation = false;

    try {
      writeLock.lock();

      // If we allocated something
      if (request.anythingAllocatedOrReserved()) {
        ContainerAllocationProposal<FiCaSchedulerApp, FiCaSchedulerNode>
            allocation = request.getFirstAllocatedOrReservedContainer();
        SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode>
            schedulerContainer = allocation.getAllocatedOrReservedContainer();
        RMContainer rmContainer = schedulerContainer.getRmContainer();

        reReservation =
            (!schedulerContainer.isAllocated()) && (rmContainer.getState()
                == RMContainerState.RESERVED);

        // Generate new containerId if it is not an allocation for increasing
        // Or re-reservation
        if (rmContainer.getContainer().getId() == null) {
          rmContainer.setContainerId(BuilderUtils
              .newContainerId(getApplicationAttemptId(),
                  getNewContainerId()));
        }
        ContainerId containerId = rmContainer.getContainerId();

        if (schedulerContainer.isAllocated()) {
          // This allocation is from a reserved container
          // Unreserve it first
          if (allocation.getAllocateFromReservedContainer() != null) {
            RMContainer reservedContainer =
                allocation.getAllocateFromReservedContainer().getRmContainer();
            // Handling container allocation
            // Did we previously reserve containers at this 'priority'?
            unreserve(schedulerContainer.getSchedulerRequestKey(),
                schedulerContainer.getSchedulerNode(), reservedContainer);
          }

          // Allocate a new container
          addToNewlyAllocatedContainers(
              schedulerContainer.getSchedulerNode(), rmContainer);
          liveContainers.put(containerId, rmContainer);

          // Deduct pending resource requests
          if (updatePending) {
            ContainerRequest containerRequest = appSchedulingInfo.allocate(
                allocation.getAllocationLocalityType(),
                schedulerContainer.getSchedulerNode(),
                schedulerContainer.getSchedulerRequestKey(),
                schedulerContainer.getRmContainer().getContainer());
            ((RMContainerImpl) rmContainer).setContainerRequest(
                containerRequest);

            // If this is from a SchedulingRequest, set allocation tags.
            if (containerRequest.getSchedulingRequest() != null) {
              ((RMContainerImpl) rmContainer).setAllocationTags(
                  containerRequest.getSchedulingRequest().getAllocationTags());
            }
          }

          attemptResourceUsage.incUsed(schedulerContainer.getNodePartition(),
              allocation.getAllocatedOrReservedResource());

          rmContainer.handle(
              new RMContainerEvent(containerId, RMContainerEventType.START));

          // Inform the node
          schedulerContainer.getSchedulerNode().allocateContainer(
              rmContainer);

          // update locality statistics,
          incNumAllocatedContainers(allocation.getAllocationLocalityType(),
              allocation.getRequestLocalityType());

          if (LOG.isDebugEnabled()) {
            LOG.debug("allocate: applicationAttemptId=" + containerId
                .getApplicationAttemptId() + " container=" + containerId
                + " host=" + rmContainer.getAllocatedNode().getHost()
                + " type=" + allocation.getAllocationLocalityType());
          }
          RMAuditLogger.logSuccess(getUser(), AuditConstants.ALLOC_CONTAINER,
              "SchedulerApp", getApplicationId(), containerId,
              allocation.getAllocatedOrReservedResource());
        } else {
          // If the rmContainer's state is already updated to RESERVED, this is
          // a reReservation
          reserve(schedulerContainer.getSchedulerRequestKey(),
              schedulerContainer.getSchedulerNode(),
              schedulerContainer.getRmContainer(),
              schedulerContainer.getRmContainer().getContainer(),
              reReservation);
        }
      }
    } finally {
      writeLock.unlock();
    }

    // Don't bother CS leaf queue if it is a re-reservation
    if (!reReservation) {
      getCSLeafQueue().apply(cluster, request);
    }
  }

  public boolean unreserve(SchedulerRequestKey schedulerKey,
      FiCaSchedulerNode node, RMContainer rmContainer) {
    try {
      writeLock.lock();
      // Done with the reservation?
      if (internalUnreserve(node, schedulerKey)) {
        node.unreserveResource(this);

        // Update reserved metrics
        queue.getMetrics().unreserveResource(node.getPartition(),
            getUser(), rmContainer.getReservedResource());
        queue.decReservedResource(node.getPartition(),
            rmContainer.getReservedResource());
        return true;
      }
      return false;
    } finally {
      writeLock.unlock();
    }
  }

  private boolean internalUnreserve(FiCaSchedulerNode node,
      SchedulerRequestKey schedulerKey) {
    Map<NodeId, RMContainer> reservedContainers =
        this.reservedContainers.get(schedulerKey);

    if (reservedContainers != null) {
      RMContainer reservedContainer =
          reservedContainers.remove(node.getNodeID());

      // unreserve is now triggered in new scenarios (preemption)
      // as a consequence reservedcontainer might be null, adding NP-checks
      if (reservedContainer != null
          && reservedContainer.getContainer() != null
          && reservedContainer.getContainer().getResource() != null) {

        if (reservedContainers.isEmpty()) {
          this.reservedContainers.remove(schedulerKey);
        }
        // Reset the re-reservation count
        resetReReservations(schedulerKey);

        Resource resource = reservedContainer.getReservedResource();
        this.attemptResourceUsage.decReserved(node.getPartition(), resource);

        LOG.info("Application " + getApplicationId() + " unreserved "
            + " on node " + node + ", currently has "
            + reservedContainers.size()
            + " at priority " + schedulerKey.getPriority()
            + "; currentReservation " + this.attemptResourceUsage.getReserved()
            + " on node-label=" + node.getPartition());
        return true;
      }
    }
    return false;
  }

  public Map<String, Resource> getTotalPendingRequestsPerPartition() {
    try {
      readLock.lock();

      Map<String, Resource> ret = new HashMap<>();
      for (SchedulerRequestKey schedulerKey : appSchedulingInfo
          .getSchedulerKeys()) {
        AppPlacementAllocator<FiCaSchedulerNode> ps =
            appSchedulingInfo.getAppPlacementAllocator(schedulerKey);

        String nodePartition = ps.getPrimaryRequestedNodePartition();
        Resource res = ret.get(nodePartition);
        if (null == res) {
          res = Resources.createResource(0);
          ret.put(nodePartition, res);
        }

        PendingAsk ask = ps.getPendingAsk(ResourceRequest.ANY);
        if (ask.getCount() > 0) {
          Resources.addTo(res, Resources
              .multiply(ask.getPerAllocationResource(),
                  ask.getCount()));
        }
      }

      return ret;
    } finally {
      readLock.unlock();
    }

  }

  public void markContainerForPreemption(ContainerId cont) {
    try {
      writeLock.lock();
      // ignore already completed containers
      if (liveContainers.containsKey(cont)) {
        containersToPreempt.add(cont);
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * This method produces an Allocation that includes the current view
   * of the resources that will be allocated to and preempted from this
   * application.
   *
   * @param resourceCalculator resourceCalculator
   * @param clusterResource clusterResource
   * @param minimumAllocation minimumAllocation
   * @return an allocation
   */
  public Allocation getAllocation(ResourceCalculator resourceCalculator,
      Resource clusterResource, Resource minimumAllocation) {
    try {
      writeLock.lock();
      Set<ContainerId> currentContPreemption = Collections.unmodifiableSet(
          new HashSet<ContainerId>(containersToPreempt));
      containersToPreempt.clear();
      Resource tot = Resource.newInstance(0, 0);
      for (ContainerId c : currentContPreemption) {
        Resources.addTo(tot, liveContainers.get(c).getContainer()
            .getResource());
      }
      int numCont = (int) Math.ceil(
          Resources.divide(rc, clusterResource, tot, minimumAllocation));
      ResourceRequest rr = ResourceRequest.newBuilder()
          .priority(Priority.UNDEFINED).resourceName(ResourceRequest.ANY)
          .capability(minimumAllocation).numContainers(numCont).build();
      List<Container> previousAttemptContainers =
          pullPreviousAttemptContainers();
      List<Container> newlyAllocatedContainers = pullNewlyAllocatedContainers();
      List<Container> newlyIncreasedContainers = pullNewlyIncreasedContainers();
      List<Container> newlyDecreasedContainers = pullNewlyDecreasedContainers();
      List<Container> newlyPromotedContainers = pullNewlyPromotedContainers();
      List<Container> newlyDemotedContainers = pullNewlyDemotedContainers();
      List<NMToken> updatedNMTokens = pullUpdatedNMTokens();
      Resource headroom = getHeadroom();
      setApplicationHeadroomForMetrics(headroom);
      return new Allocation(newlyAllocatedContainers, headroom, null,
          currentContPreemption, Collections.singletonList(rr), updatedNMTokens,
          newlyIncreasedContainers, newlyDecreasedContainers,
          newlyPromotedContainers, newlyDemotedContainers,
          previousAttemptContainers);
    } finally {
      writeLock.unlock();
    }
  }

  @VisibleForTesting
  public NodeId getNodeIdToUnreserve(SchedulerRequestKey schedulerKey,
      Resource resourceNeedUnreserve, ResourceCalculator resourceCalculator) {
    // first go around make this algorithm simple and just grab first
    // reservation that has enough resources
    Map<NodeId, RMContainer> reservedContainers = this.reservedContainers.get(
        schedulerKey);

    if ((reservedContainers != null) && (!reservedContainers.isEmpty())) {
      for (Map.Entry<NodeId, RMContainer> entry : reservedContainers
          .entrySet()) {
        NodeId nodeId = entry.getKey();
        RMContainer reservedContainer = entry.getValue();
        Resource reservedResource = reservedContainer.getReservedResource();

        // make sure we unreserve one with at least the same amount of
        // resources, otherwise could affect capacity limits
        if (Resources.fitsIn(resourceCalculator, resourceNeedUnreserve,
            reservedResource)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "unreserving node with reservation size: " + reservedResource
                    + " in order to allocate container with size: "
                    + resourceNeedUnreserve);
          }
          return nodeId;
        }
      }
    }
    return null;
  }

  public void setHeadroomProvider(
    CapacityHeadroomProvider headroomProvider) {
    try {
      writeLock.lock();
      this.headroomProvider = headroomProvider;
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public Resource getHeadroom() {
    try {
      readLock.lock();
      if (headroomProvider != null) {
        return headroomProvider.getHeadroom();
      }
      return super.getHeadroom();
    } finally {
      readLock.unlock();
    }

  }

  @Override
  public void transferStateFromPreviousAttempt(
      SchedulerApplicationAttempt appAttempt) {
    try {
      writeLock.lock();
      super.transferStateFromPreviousAttempt(appAttempt);
      this.headroomProvider = ((FiCaSchedulerApp) appAttempt).headroomProvider;
    } finally {
      writeLock.unlock();
    }
  }

  public void reserve(SchedulerRequestKey schedulerKey, FiCaSchedulerNode node,
      RMContainer rmContainer, Container container, boolean reReservation) {
    // Update reserved metrics if this is the first reservation
    // rmContainer will be moved to reserved in the super.reserve
    if (!reReservation) {
      queue.getMetrics().reserveResource(node.getPartition(),
          getUser(), container.getResource());
    }

    // Inform the application
    rmContainer = super.reserve(node, schedulerKey, rmContainer, container);

    // Update the node
    node.reserveResource(this, schedulerKey, rmContainer);
  }

  @VisibleForTesting
  public RMContainer findNodeToUnreserve(FiCaSchedulerNode node,
      SchedulerRequestKey schedulerKey, Resource minimumUnreservedResource) {
    try {
      readLock.lock();
      // need to unreserve some other container first
      NodeId idToUnreserve = getNodeIdToUnreserve(schedulerKey,
          minimumUnreservedResource, rc);
      if (idToUnreserve == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("checked to see if could unreserve for app but nothing "
              + "reserved that matches for this app");
        }
        return null;
      }
      FiCaSchedulerNode nodeToUnreserve =
          ((CapacityScheduler) scheduler).getNode(idToUnreserve);
      if (nodeToUnreserve == null) {
        LOG.error("node to unreserve doesn't exist, nodeid: " + idToUnreserve);
        return null;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("unreserving for app: " + getApplicationId() + " on nodeId: "
            + idToUnreserve
            + " in order to replace reserved application and place it on node: "
            + node.getNodeID() + " needing: " + minimumUnreservedResource);
      }

      // headroom
      Resources.addTo(getHeadroom(),
          nodeToUnreserve.getReservedContainer().getReservedResource());

      return nodeToUnreserve.getReservedContainer();
    } finally {
      readLock.unlock();
    }
  }

  public LeafQueue getCSLeafQueue() {
    return (LeafQueue)queue;
  }

  public CSAssignment assignContainers(Resource clusterResource,
      CandidateNodeSet<FiCaSchedulerNode> ps,
      ResourceLimits currentResourceLimits, SchedulingMode schedulingMode,
      RMContainer reservedContainer) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("pre-assignContainers for application "
          + getApplicationId());
      showRequests();
    }

    return containerAllocator.assignContainers(clusterResource, ps,
        schedulingMode, currentResourceLimits, reservedContainer);
  }

  public void nodePartitionUpdated(RMContainer rmContainer, String oldPartition,
      String newPartition) {
    Resource containerResource = rmContainer.getAllocatedResource();
    this.attemptResourceUsage.decUsed(oldPartition, containerResource);
    this.attemptResourceUsage.incUsed(newPartition, containerResource);
    getCSLeafQueue().decUsedResource(oldPartition, containerResource, this);
    getCSLeafQueue().incUsedResource(newPartition, containerResource, this);

    // Update new partition name if container is AM and also update AM resource
    if (rmContainer.isAMContainer()) {
      setAppAMNodePartitionName(newPartition);
      this.attemptResourceUsage.decAMUsed(oldPartition, containerResource);
      this.attemptResourceUsage.incAMUsed(newPartition, containerResource);
      getCSLeafQueue().decAMUsedResource(oldPartition, containerResource, this);
      getCSLeafQueue().incAMUsedResource(newPartition, containerResource, this);
    }
  }

  protected void getPendingAppDiagnosticMessage(
      StringBuilder diagnosticMessage) {
    LeafQueue queue = getCSLeafQueue();
    diagnosticMessage.append(" Details : AM Partition = ");
    diagnosticMessage.append(appAMNodePartitionName.isEmpty()
        ? NodeLabel.DEFAULT_NODE_LABEL_PARTITION : appAMNodePartitionName);
    diagnosticMessage.append("; ");
    diagnosticMessage.append("AM Resource Request = ");
    diagnosticMessage.append(getAMResource(appAMNodePartitionName));
    diagnosticMessage.append("; ");
    diagnosticMessage.append("Queue Resource Limit for AM = ");
    diagnosticMessage
        .append(queue.getAMResourceLimitPerPartition(appAMNodePartitionName));
    diagnosticMessage.append("; ");
    diagnosticMessage.append("User AM Resource Limit of the queue = ");
    diagnosticMessage.append(queue.getUserAMResourceLimitPerPartition(
        appAMNodePartitionName, getUser()));
    diagnosticMessage.append("; ");
    diagnosticMessage.append("Queue AM Resource Usage = ");
    diagnosticMessage.append(
        queue.getQueueResourceUsage().getAMUsed(appAMNodePartitionName));
    diagnosticMessage.append("; ");
  }

  protected void getActivedAppDiagnosticMessage(
      StringBuilder diagnosticMessage) {
    LeafQueue queue = getCSLeafQueue();
    QueueCapacities queueCapacities = queue.getQueueCapacities();
    QueueResourceQuotas queueResourceQuotas = queue.getQueueResourceQuotas();
    diagnosticMessage.append(" Details : AM Partition = ");
    diagnosticMessage.append(appAMNodePartitionName.isEmpty()
        ? NodeLabel.DEFAULT_NODE_LABEL_PARTITION : appAMNodePartitionName);
    diagnosticMessage.append(" ; ");
    diagnosticMessage.append("Partition Resource = ");
    diagnosticMessage.append(rmContext.getNodeLabelManager()
        .getResourceByLabel(appAMNodePartitionName, Resources.none()));
    diagnosticMessage.append(" ; ");
    diagnosticMessage.append("Queue's Absolute capacity = ");
    diagnosticMessage.append(
        queueCapacities.getAbsoluteCapacity(appAMNodePartitionName) * 100);
    diagnosticMessage.append(" % ; ");
    diagnosticMessage.append("Queue's Absolute used capacity = ");
    diagnosticMessage.append(
        queueCapacities.getAbsoluteUsedCapacity(appAMNodePartitionName) * 100);
    diagnosticMessage.append(" % ; ");
    diagnosticMessage.append("Queue's Absolute max capacity = ");
    diagnosticMessage.append(
        queueCapacities.getAbsoluteMaximumCapacity(appAMNodePartitionName)
            * 100);
    diagnosticMessage.append(" % ; ");
    diagnosticMessage.append("Queue's capacity (absolute resource) = ");
    diagnosticMessage.append(
        queueResourceQuotas.getEffectiveMinResource(appAMNodePartitionName));
    diagnosticMessage.append(" ; ");
    diagnosticMessage.append("Queue's used capacity (absolute resource) = ");
    diagnosticMessage
        .append(queue.getQueueResourceUsage().getUsed(appAMNodePartitionName));
    diagnosticMessage.append(" ; ");
    diagnosticMessage.append("Queue's max capacity (absolute resource) = ");
    diagnosticMessage.append(
        queueResourceQuotas.getEffectiveMaxResource(appAMNodePartitionName));
    diagnosticMessage.append(" ; ");
  }

  /**
   * Set the message temporarily if the reason is known for why scheduling did
   * not happen for a given node, if not message will be over written
   * @param message Message of app skip diagnostics
   */
  public void updateAppSkipNodeDiagnostics(String message) {
    this.appSkipNodeDiagnostics = message;
  }

  public void updateNodeInfoForAMDiagnostics(FiCaSchedulerNode node) {
    // FIXME, update AM diagnostics when global scheduling is enabled
    if (null == node) {
      return;
    }

    if (isWaitingForAMContainer()) {
      StringBuilder diagnosticMessageBldr = new StringBuilder();
      if (appSkipNodeDiagnostics != null) {
        diagnosticMessageBldr.append(appSkipNodeDiagnostics);
        appSkipNodeDiagnostics = null;
      }
      diagnosticMessageBldr.append(
          CSAMContainerLaunchDiagnosticsConstants.LAST_NODE_PROCESSED_MSG);
      diagnosticMessageBldr.append(node.getNodeID());
      diagnosticMessageBldr.append(" ( Partition : ");
      diagnosticMessageBldr.append(node.getLabels());
      diagnosticMessageBldr.append(", Total resource : ");
      diagnosticMessageBldr.append(node.getTotalResource());
      diagnosticMessageBldr.append(", Available resource : ");
      diagnosticMessageBldr.append(node.getUnallocatedResource());
      diagnosticMessageBldr.append(" ).");
      updateAMContainerDiagnostics(AMState.ACTIVATED, diagnosticMessageBldr.toString());
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public AppPlacementAllocator<FiCaSchedulerNode> getAppPlacementAllocator(
      SchedulerRequestKey schedulerRequestKey) {
    return super.getAppPlacementAllocator(schedulerRequestKey);
  }

  /**
   * Recalculates the per-app, percent of queue metric, specific to the
   * Capacity Scheduler.
   */
  @Override
  public ApplicationResourceUsageReport getResourceUsageReport() {
    try {
      // Use write lock here because
      // SchedulerApplicationAttempt#getResourceUsageReport updated fields
      // TODO: improve this
      writeLock.lock();
      ApplicationResourceUsageReport report = super.getResourceUsageReport();
      Resource cluster = rmContext.getScheduler().getClusterResource();
      Resource totalPartitionRes =
          rmContext.getNodeLabelManager().getResourceByLabel(
              getAppAMNodePartitionName(), cluster);
      ResourceCalculator calc =
          rmContext.getScheduler().getResourceCalculator();
      float queueUsagePerc = 0.0f;
      if (!calc.isInvalidDivisor(totalPartitionRes)) {
        Resource effCap = ((AbstractCSQueue) getQueue())
            .getEffectiveCapacity(getAppAMNodePartitionName());
        if (!effCap.equals(Resources.none())) {
          queueUsagePerc = calc.divide(totalPartitionRes,
              report.getUsedResources(), effCap) * 100;
        }
        report.setQueueUsagePercentage(queueUsagePerc);
      }
      return report;
    } finally {
      writeLock.unlock();
    }
  }

  public ReentrantReadWriteLock.WriteLock getWriteLock() {
    return this.writeLock;
  }

  public void addToBeRemovedIncreaseRequest(
      SchedContainerChangeRequest request) {
    toBeRemovedIncRequests.put(request.getContainerId(), request);
  }

  /*
   * Overriding to appease findbugs
   */
  @Override
  public int hashCode() {
    return super.hashCode();
  }

  /*
   * Overriding to appease findbugs
   */
  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  /**
   * Move reservation from one node to another
   * Comparing to unreserve container on source node and reserve a new
   * container on target node. This method will not create new RMContainer
   * instance. And this operation is atomic.
   *
   * @param reservedContainer to be moved reserved container
   * @param sourceNode source node
   * @param targetNode target node
   *
   * @return succeeded or not
   */
  public boolean moveReservation(RMContainer reservedContainer,
      FiCaSchedulerNode sourceNode, FiCaSchedulerNode targetNode) {
    try {
      writeLock.lock();
      if (!sourceNode.getPartition().equals(targetNode.getPartition())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Failed to move reservation, two nodes are in different partition");
        }
        return false;
      }

      // Update reserved container to node map
      Map<NodeId, RMContainer> map = reservedContainers.get(
          reservedContainer.getReservedSchedulerKey());
      if (null == map) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Cannot find reserved container map.");
        }
        return false;
      }

      // Check if reserved container changed
      if (sourceNode.getReservedContainer() != reservedContainer) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("To-be-moved container already updated.");
        }
        return false;
      }

      // Check if target node is empty, acquires lock of target node to make sure
      // reservation happens transactional
      synchronized (targetNode){
        if (targetNode.getReservedContainer() != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Target node is already occupied before moving");
          }
        }

        try {
          targetNode.reserveResource(this,
              reservedContainer.getReservedSchedulerKey(), reservedContainer);
        } catch (IllegalStateException e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Reserve on target node failed, e=", e);
          }
          return false;
        }

        // Set source node's reserved container to null
        sourceNode.setReservedContainer(null);
        map.remove(sourceNode.getNodeID());

        // Update reserved container
        reservedContainer.handle(
            new RMContainerReservedEvent(reservedContainer.getContainerId(),
                reservedContainer.getReservedResource(), targetNode.getNodeID(),
                reservedContainer.getReservedSchedulerKey()));

        // Add to target node
        map.put(targetNode.getNodeID(), reservedContainer);

        return true;
      }
    } finally {
      writeLock.unlock();
    }
  }
}

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerRequestKey;
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

  public FiCaSchedulerApp(ApplicationAttemptId applicationAttemptId, 
      String user, Queue queue, ActiveUsersManager activeUsersManager,
      RMContext rmContext) {
    this(applicationAttemptId, user, queue, activeUsersManager, rmContext,
        Priority.newInstance(0), false);
  }

  public FiCaSchedulerApp(ApplicationAttemptId applicationAttemptId,
      String user, Queue queue, ActiveUsersManager activeUsersManager,
      RMContext rmContext, Priority appPriority, boolean isAttemptRecovering) {
    this(applicationAttemptId, user, queue, activeUsersManager, rmContext,
        appPriority, isAttemptRecovering, null);
  }

  public FiCaSchedulerApp(ApplicationAttemptId applicationAttemptId,
      String user, Queue queue, ActiveUsersManager activeUsersManager,
      RMContext rmContext, Priority appPriority, boolean isAttemptRecovering,
      ActivitiesManager activitiesManager) {
    super(applicationAttemptId, user, queue, activeUsersManager, rmContext);

    RMApp rmApp = rmContext.getRMApps().get(getApplicationId());

    Resource amResource;
    String partition;

    if (rmApp == null || rmApp.getAMResourceRequest() == null) {
      // the rmApp may be undefined (the resource manager checks for this too)
      // and unmanaged applications do not provide an amResource request
      // in these cases, provide a default using the scheduler
      amResource = rmContext.getScheduler().getMinimumResourceCapability();
      partition = CommonNodeLabelsManager.NO_LABEL;
    } else {
      amResource = rmApp.getAMResourceRequest().getCapability();
      partition =
          (rmApp.getAMResourceRequest().getNodeLabelExpression() == null)
          ? CommonNodeLabelsManager.NO_LABEL
          : rmApp.getAMResourceRequest().getNodeLabelExpression();
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
      queue.getMetrics().releaseResources(getUser(), 1, containerResource);
      attemptResourceUsage.decUsed(partition, containerResource);

      // Clear resource utilization metrics cache.
      lastMemoryAggregateAllocationUpdateTime = -1;

      return true;
    } finally {
      writeLock.unlock();
    }
  }

  public RMContainer allocate(NodeType type, FiCaSchedulerNode node,
      SchedulerRequestKey schedulerKey, ResourceRequest request,
      Container container) {
    try {
      writeLock.lock();

      if (isStopped) {
        return null;
      }

      // Required sanity check - AM can call 'allocate' to update resource
      // request without locking the scheduler, hence we need to check
      if (getTotalRequiredResources(schedulerKey) <= 0) {
        return null;
      }

      // Create RMContainer
      RMContainer rmContainer = new RMContainerImpl(container,
          this.getApplicationAttemptId(), node.getNodeID(),
          appSchedulingInfo.getUser(), this.rmContext,
          request.getNodeLabelExpression());
      ((RMContainerImpl) rmContainer).setQueueName(this.getQueueName());

      updateAMContainerDiagnostics(AMState.ASSIGNED, null);

      // Add it to allContainers list.
      newlyAllocatedContainers.add(rmContainer);

      ContainerId containerId = container.getId();
      liveContainers.put(containerId, rmContainer);

      // Update consumption and track allocations
      List<ResourceRequest> resourceRequestList = appSchedulingInfo.allocate(
          type, node, schedulerKey, request, container);

      attemptResourceUsage.incUsed(node.getPartition(),
          container.getResource());

      // Update resource requests related to "request" and store in RMContainer
      ((RMContainerImpl) rmContainer).setResourceRequests(resourceRequestList);

      // Inform the container
      rmContainer.handle(
          new RMContainerEvent(containerId, RMContainerEventType.START));

      if (LOG.isDebugEnabled()) {
        LOG.debug("allocate: applicationAttemptId=" + containerId
            .getApplicationAttemptId() + " container=" + containerId + " host="
            + container.getNodeId().getHost() + " type=" + type);
      }
      RMAuditLogger.logSuccess(getUser(), AuditConstants.ALLOC_CONTAINER,
          "SchedulerApp", getApplicationId(), containerId,
          container.getResource());

      return rmContainer;
    } finally {
      writeLock.unlock();
    }
  }

  public boolean unreserve(SchedulerRequestKey schedulerKey,
      FiCaSchedulerNode node, RMContainer rmContainer) {
    try {
      writeLock.lock();
      // Cancel increase request (if it has reserved increase request
      rmContainer.cancelIncreaseReservation();

      // Done with the reservation?
      if (internalUnreserve(node, schedulerKey)) {
        node.unreserveResource(this);

        // Update reserved metrics
        queue.getMetrics().unreserveResource(getUser(),
            rmContainer.getReservedResource());
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
   * @param resourceCalculator
   * @param clusterResource
   * @param minimumAllocation
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
      ResourceRequest rr = ResourceRequest.newInstance(Priority.UNDEFINED,
          ResourceRequest.ANY, minimumAllocation, numCont);
      List<Container> newlyAllocatedContainers = pullNewlyAllocatedContainers();
      List<Container> newlyIncreasedContainers = pullNewlyIncreasedContainers();
      List<Container> newlyDecreasedContainers = pullNewlyDecreasedContainers();
      List<NMToken> updatedNMTokens = pullUpdatedNMTokens();
      Resource headroom = getHeadroom();
      setApplicationHeadroomForMetrics(headroom);
      return new Allocation(newlyAllocatedContainers, headroom, null,
          currentContPreemption, Collections.singletonList(rr), updatedNMTokens,
          newlyIncreasedContainers, newlyDecreasedContainers);
    } finally {
      writeLock.unlock();
    }
  }

  @VisibleForTesting
  public NodeId getNodeIdToUnreserve(
      SchedulerRequestKey schedulerKey, Resource resourceNeedUnreserve,
      ResourceCalculator rc, Resource clusterResource) {
    try {
      writeLock.lock();
      // first go around make this algorithm simple and just grab first
      // reservation that has enough resources
      Map<NodeId, RMContainer> reservedContainers = this.reservedContainers.get(
          schedulerKey);

      if ((reservedContainers != null) && (!reservedContainers.isEmpty())) {
        for (Map.Entry<NodeId, RMContainer> entry : reservedContainers
            .entrySet()) {
          NodeId nodeId = entry.getKey();
          RMContainer reservedContainer = entry.getValue();
          if (reservedContainer.hasIncreaseReservation()) {
            // Currently, only regular container allocation supports continuous
            // reservation looking, we don't support canceling increase request
            // reservation when allocating regular container.
            continue;
          }

          Resource reservedResource = reservedContainer.getReservedResource();

          // make sure we unreserve one with at least the same amount of
          // resources, otherwise could affect capacity limits
          if (Resources.fitsIn(rc, clusterResource, resourceNeedUnreserve,
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
    } finally {
      writeLock.unlock();
    }
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
  
  public boolean reserveIncreasedContainer(SchedulerRequestKey schedulerKey,
      FiCaSchedulerNode node,
      RMContainer rmContainer, Resource reservedResource) {
    // Inform the application
    if (super.reserveIncreasedContainer(node, schedulerKey, rmContainer,
        reservedResource)) {

      queue.getMetrics().reserveResource(getUser(), reservedResource);

      // Update the node
      node.reserveResource(this, schedulerKey, rmContainer);

      // Succeeded
      return true;
    }

    return false;
  }

  public void reserve(SchedulerRequestKey schedulerKey,
      FiCaSchedulerNode node, RMContainer rmContainer, Container container) {
    // Update reserved metrics if this is the first reservation
    if (rmContainer == null) {
      queue.getMetrics().reserveResource(
          getUser(), container.getResource());
    }

    // Inform the application
    rmContainer = super.reserve(node, schedulerKey, rmContainer, container);

    // Update the node
    node.reserveResource(this, schedulerKey, rmContainer);
  }

  @VisibleForTesting
  public RMContainer findNodeToUnreserve(Resource clusterResource,
      FiCaSchedulerNode node, SchedulerRequestKey schedulerKey,
      Resource minimumUnreservedResource) {
    // need to unreserve some other container first
    NodeId idToUnreserve =
        getNodeIdToUnreserve(schedulerKey, minimumUnreservedResource,
            rc, clusterResource);
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
      LOG.debug("unreserving for app: " + getApplicationId()
        + " on nodeId: " + idToUnreserve
        + " in order to replace reserved application and place it on node: "
        + node.getNodeID() + " needing: " + minimumUnreservedResource);
    }

    // headroom
    Resources.addTo(getHeadroom(), nodeToUnreserve
        .getReservedContainer().getReservedResource());

    return nodeToUnreserve.getReservedContainer();
  }

  public LeafQueue getCSLeafQueue() {
    return (LeafQueue)queue;
  }

  public CSAssignment assignContainers(Resource clusterResource,
      FiCaSchedulerNode node, ResourceLimits currentResourceLimits,
      SchedulingMode schedulingMode, RMContainer reservedContainer) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("pre-assignContainers for application "
          + getApplicationId());
      showRequests();
    }

    try {
      writeLock.lock();
      return containerAllocator.assignContainers(clusterResource, node,
          schedulingMode, currentResourceLimits, reservedContainer);
    } finally {
      writeLock.unlock();
    }
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
    diagnosticMessage.append(
        queue.getUserAMResourceLimitPerPartition(appAMNodePartitionName));
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
  }

  /**
   * Set the message temporarily if the reason is known for why scheduling did
   * not happen for a given node, if not message will be over written
   * @param message
   */
  public void updateAppSkipNodeDiagnostics(String message) {
    this.appSkipNodeDiagnostics = message;
  }

  public void updateNodeInfoForAMDiagnostics(FiCaSchedulerNode node) {
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
      if (!calc.isInvalidDivisor(totalPartitionRes)) {
        float queueAbsMaxCapPerPartition =
            ((AbstractCSQueue) getQueue()).getQueueCapacities()
                .getAbsoluteCapacity(getAppAMNodePartitionName());
        float queueUsagePerc = calc.divide(totalPartitionRes,
            report.getUsedResources(),
            Resources.multiply(totalPartitionRes, queueAbsMaxCapPerPartition))
            * 100;
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
}

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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Represents an application attempt from the viewpoint of the Fair Scheduler.
 */
@Private
@Unstable
public class FSAppAttempt extends SchedulerApplicationAttempt
    implements Schedulable {

  private static final Log LOG = LogFactory.getLog(FSAppAttempt.class);
  private static final DefaultResourceCalculator RESOURCE_CALCULATOR
      = new DefaultResourceCalculator();

  private final long startTime;
  private final Priority appPriority;
  private Resource demand = Resources.createResource(0);
  private final FairScheduler scheduler;
  private Resource fairShare = Resources.createResource(0, 0);

  // Preemption related variables
  private final Object preemptionVariablesLock = new Object();
  private final Set<RMContainer> containersToBePreempted = new HashSet<>();
  private final Resource resourcesToBePreempted =
      Resources.clone(Resources.none());

  private Resource fairshareStarvation = Resources.none();
  private long lastTimeAtFairShare;
  private long nextStarvationCheck;

  // minShareStarvation attributed to this application by the leaf queue
  private Resource minshareStarvation = Resources.none();

  // Used to record node reservation by an app.
  // Key = RackName, Value = Set of Nodes reserved by app on rack
  private final Map<String, Set<String>> reservations = new HashMap<>();

  private final List<FSSchedulerNode> blacklistNodeIds = new ArrayList<>();
  /**
   * Delay scheduling: We often want to prioritize scheduling of node-local
   * containers over rack-local or off-switch containers. To achieve this
   * we first only allow node-local assignments for a given priority level,
   * then relax the locality threshold once we've had a long enough period
   * without successfully scheduling. We measure both the number of "missed"
   * scheduling opportunities since the last container was scheduled
   * at the current allowed level and the time since the last container
   * was scheduled. Currently we use only the former.
   */
  private final Map<SchedulerRequestKey, NodeType> allowedLocalityLevel =
      new HashMap<>();

  public FSAppAttempt(FairScheduler scheduler,
      ApplicationAttemptId applicationAttemptId, String user, FSLeafQueue queue,
      ActiveUsersManager activeUsersManager, RMContext rmContext) {
    super(applicationAttemptId, user, queue, activeUsersManager, rmContext);

    this.scheduler = scheduler;
    this.startTime = scheduler.getClock().getTime();
    this.lastTimeAtFairShare = this.startTime;
    this.appPriority = Priority.newInstance(1);
  }

  /**
   * Get metrics reference from containing queue.
   */
  public QueueMetrics getMetrics() {
    return queue.getMetrics();
  }

  void containerCompleted(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event) {
    try {
      writeLock.lock();
      Container container = rmContainer.getContainer();
      ContainerId containerId = container.getId();

      // Remove from the list of containers
      if (liveContainers.remove(containerId) == null) {
        LOG.info("Additional complete request on completed container " +
            rmContainer.getContainerId());
        return;
      }

      // Remove from the list of newly allocated containers if found
      newlyAllocatedContainers.remove(rmContainer);

      // Inform the container
      rmContainer.handle(
          new RMContainerFinishedEvent(containerId, containerStatus, event));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Completed container: " + rmContainer.getContainerId()
            + " in state: " + rmContainer.getState() + " event:" + event);
      }

      untrackContainerForPreemption(rmContainer);
      if (containerStatus.getDiagnostics().
          equals(SchedulerUtils.PREEMPTED_CONTAINER)) {
        queue.getMetrics().preemptContainer();
      }

      Resource containerResource = rmContainer.getContainer().getResource();
      RMAuditLogger.logSuccess(getUser(), AuditConstants.RELEASE_CONTAINER,
          "SchedulerApp", getApplicationId(), containerId, containerResource);

      // Update usage metrics
      queue.getMetrics().releaseResources(
          rmContainer.getNodeLabelExpression(),
          getUser(), 1, containerResource);
      this.attemptResourceUsage.decUsed(containerResource);
      getQueue().decUsedResource(containerResource);

      // Clear resource utilization metrics cache.
      lastMemoryAggregateAllocationUpdateTime = -1;
    } finally {
      writeLock.unlock();
    }
  }

  private void unreserveInternal(
      SchedulerRequestKey schedulerKey, FSSchedulerNode node) {
    try {
      writeLock.lock();
      Map<NodeId, RMContainer> reservedContainers = this.reservedContainers.get(
          schedulerKey);
      RMContainer reservedContainer = reservedContainers.remove(
          node.getNodeID());
      if (reservedContainers.isEmpty()) {
        this.reservedContainers.remove(schedulerKey);
      }

      // Reset the re-reservation count
      resetReReservations(schedulerKey);

      Resource resource = reservedContainer.getContainer().getResource();
      this.attemptResourceUsage.decReserved(resource);

      LOG.info(
          "Application " + getApplicationId() + " unreserved " + " on node "
              + node + ", currently has " + reservedContainers.size()
              + " at priority " + schedulerKey.getPriority()
              + "; currentReservation " + this.attemptResourceUsage
              .getReserved());
    } finally {
      writeLock.unlock();
    }
  }

  private void subtractResourcesOnBlacklistedNodes(
      Resource availableResources) {
    if (appSchedulingInfo.getAndResetBlacklistChanged()) {
      blacklistNodeIds.clear();
      blacklistNodeIds.addAll(scheduler.getBlacklistedNodes(this));
    }
    for (FSSchedulerNode node: blacklistNodeIds) {
      Resources.subtractFromNonNegative(availableResources,
          node.getUnallocatedResource());
    }
  }

  /**
   * Headroom depends on resources in the cluster, current usage of the
   * queue, queue's fair-share and queue's max-resources.
   */
  @Override
  public Resource getHeadroom() {
    final FSQueue fsQueue = getQueue();
    SchedulingPolicy policy = fsQueue.getPolicy();

    Resource queueFairShare = fsQueue.getFairShare();
    Resource queueUsage = fsQueue.getResourceUsage();
    Resource clusterResource = this.scheduler.getClusterResource();
    Resource clusterUsage = this.scheduler.getRootQueueMetrics()
        .getAllocatedResources();

    Resource clusterAvailableResources =
        Resources.subtract(clusterResource, clusterUsage);
    subtractResourcesOnBlacklistedNodes(clusterAvailableResources);

    Resource queueMaxAvailableResources =
        Resources.subtract(fsQueue.getMaxShare(), queueUsage);
    Resource maxAvailableResource = Resources.componentwiseMin(
        clusterAvailableResources, queueMaxAvailableResources);

    Resource headroom = policy.getHeadroom(queueFairShare,
        queueUsage, maxAvailableResource);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Headroom calculation for " + this.getName() + ":" +
          "Min(" +
          "(queueFairShare=" + queueFairShare +
          " - queueUsage=" + queueUsage + ")," +
          " maxAvailableResource=" + maxAvailableResource +
          "Headroom=" + headroom);
    }
    return headroom;
  }

  /**
   * Return the level at which we are allowed to schedule containers, given the
   * current size of the cluster and thresholds indicating how many nodes to
   * fail at (as a fraction of cluster size) before relaxing scheduling
   * constraints.
   * @param schedulerKey SchedulerRequestKey
   * @param numNodes Num Nodes
   * @param nodeLocalityThreshold nodeLocalityThreshold
   * @param rackLocalityThreshold rackLocalityThreshold
   * @return NodeType
   */
  NodeType getAllowedLocalityLevel(
      SchedulerRequestKey schedulerKey, int numNodes,
      double nodeLocalityThreshold, double rackLocalityThreshold) {
    // upper limit on threshold
    if (nodeLocalityThreshold > 1.0) {
      nodeLocalityThreshold = 1.0;
    }
    if (rackLocalityThreshold > 1.0) {
      rackLocalityThreshold = 1.0;
    }

    // If delay scheduling is not being used, can schedule anywhere
    if (nodeLocalityThreshold < 0.0 || rackLocalityThreshold < 0.0) {
      return NodeType.OFF_SWITCH;
    }

    try {
      writeLock.lock();

      // Default level is NODE_LOCAL
      if (!allowedLocalityLevel.containsKey(schedulerKey)) {
        allowedLocalityLevel.put(schedulerKey, NodeType.NODE_LOCAL);
        return NodeType.NODE_LOCAL;
      }

      NodeType allowed = allowedLocalityLevel.get(schedulerKey);

      // If level is already most liberal, we're done
      if (allowed.equals(NodeType.OFF_SWITCH)) {
        return NodeType.OFF_SWITCH;
      }

      double threshold = allowed.equals(NodeType.NODE_LOCAL) ?
          nodeLocalityThreshold :
          rackLocalityThreshold;

      // Relax locality constraints once we've surpassed threshold.
      int schedulingOpportunities = getSchedulingOpportunities(schedulerKey);
      double thresholdNum = numNodes * threshold;
      if (schedulingOpportunities > thresholdNum) {
        if (allowed.equals(NodeType.NODE_LOCAL)) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("SchedulingOpportunities: " + schedulingOpportunities
                + ", nodeLocalityThreshold: " + thresholdNum
                + ", change allowedLocality from NODE_LOCAL to RACK_LOCAL"
                + ", priority: " + schedulerKey.getPriority()
                + ", app attempt id: " + this.attemptId);
          }
          allowedLocalityLevel.put(schedulerKey, NodeType.RACK_LOCAL);
          resetSchedulingOpportunities(schedulerKey);
        } else if (allowed.equals(NodeType.RACK_LOCAL)) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("SchedulingOpportunities: " + schedulingOpportunities
                + ", rackLocalityThreshold: " + thresholdNum
                + ", change allowedLocality from RACK_LOCAL to OFF_SWITCH"
                + ", priority: " + schedulerKey.getPriority()
                + ", app attempt id: " + this.attemptId);
          }
          allowedLocalityLevel.put(schedulerKey, NodeType.OFF_SWITCH);
          resetSchedulingOpportunities(schedulerKey);
        }
      }
      return allowedLocalityLevel.get(schedulerKey);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Return the level at which we are allowed to schedule containers.
   * Given the thresholds indicating how much time passed before relaxing
   * scheduling constraints.
   * @param schedulerKey SchedulerRequestKey
   * @param nodeLocalityDelayMs nodeLocalityThreshold
   * @param rackLocalityDelayMs nodeLocalityDelayMs
   * @param currentTimeMs currentTimeMs
   * @return NodeType
   */
  NodeType getAllowedLocalityLevelByTime(
      SchedulerRequestKey schedulerKey, long nodeLocalityDelayMs,
      long rackLocalityDelayMs, long currentTimeMs) {
    // if not being used, can schedule anywhere
    if (nodeLocalityDelayMs < 0 || rackLocalityDelayMs < 0) {
      return NodeType.OFF_SWITCH;
    }

    try {
      writeLock.lock();

      // default level is NODE_LOCAL
      if (!allowedLocalityLevel.containsKey(schedulerKey)) {
        // add the initial time of priority to prevent comparing with FsApp
        // startTime and allowedLocalityLevel degrade
        lastScheduledContainer.put(schedulerKey, currentTimeMs);
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Init the lastScheduledContainer time, priority: " + schedulerKey
                  .getPriority() + ", time: " + currentTimeMs);
        }
        allowedLocalityLevel.put(schedulerKey, NodeType.NODE_LOCAL);
        return NodeType.NODE_LOCAL;
      }

      NodeType allowed = allowedLocalityLevel.get(schedulerKey);

      // if level is already most liberal, we're done
      if (allowed.equals(NodeType.OFF_SWITCH)) {
        return NodeType.OFF_SWITCH;
      }

      // check waiting time
      long waitTime = currentTimeMs;
      if (lastScheduledContainer.containsKey(schedulerKey)) {
        waitTime -= lastScheduledContainer.get(schedulerKey);
      } else{
        waitTime -= getStartTime();
      }

      long thresholdTime = allowed.equals(NodeType.NODE_LOCAL) ?
          nodeLocalityDelayMs :
          rackLocalityDelayMs;

      if (waitTime > thresholdTime) {
        if (allowed.equals(NodeType.NODE_LOCAL)) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Waiting time: " + waitTime
                + " ms, nodeLocalityDelay time: " + nodeLocalityDelayMs + " ms"
                + ", change allowedLocality from NODE_LOCAL to RACK_LOCAL"
                + ", priority: " + schedulerKey.getPriority()
                + ", app attempt id: " + this.attemptId);
          }
          allowedLocalityLevel.put(schedulerKey, NodeType.RACK_LOCAL);
          resetSchedulingOpportunities(schedulerKey, currentTimeMs);
        } else if (allowed.equals(NodeType.RACK_LOCAL)) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Waiting time: " + waitTime
                + " ms, nodeLocalityDelay time: " + nodeLocalityDelayMs + " ms"
                + ", change allowedLocality from RACK_LOCAL to OFF_SWITCH"
                + ", priority: " + schedulerKey.getPriority()
                + ", app attempt id: " + this.attemptId);
          }
          allowedLocalityLevel.put(schedulerKey, NodeType.OFF_SWITCH);
          resetSchedulingOpportunities(schedulerKey, currentTimeMs);
        }
      }
      return allowedLocalityLevel.get(schedulerKey);
    } finally {
      writeLock.unlock();
    }
  }

  public RMContainer allocate(NodeType type, FSSchedulerNode node,
      SchedulerRequestKey schedulerKey, PendingAsk pendingAsk,
      Container reservedContainer) {
    RMContainer rmContainer;
    Container container;

    try {
      writeLock.lock();
      // Update allowed locality level
      NodeType allowed = allowedLocalityLevel.get(schedulerKey);
      if (allowed != null) {
        if (allowed.equals(NodeType.OFF_SWITCH) && (type.equals(
            NodeType.NODE_LOCAL) || type.equals(NodeType.RACK_LOCAL))) {
          this.resetAllowedLocalityLevel(schedulerKey, type);
        } else if (allowed.equals(NodeType.RACK_LOCAL) && type.equals(
            NodeType.NODE_LOCAL)) {
          this.resetAllowedLocalityLevel(schedulerKey, type);
        }
      }

      // Required sanity check - AM can call 'allocate' to update resource
      // request without locking the scheduler, hence we need to check
      if (getOutstandingAsksCount(schedulerKey) <= 0) {
        return null;
      }

      container = reservedContainer;
      if (container == null) {
        container = createContainer(node, pendingAsk.getPerAllocationResource(),
            schedulerKey);
      }

      // Create RMContainer
      rmContainer = new RMContainerImpl(container, schedulerKey,
          getApplicationAttemptId(), node.getNodeID(),
          appSchedulingInfo.getUser(), rmContext);
      ((RMContainerImpl) rmContainer).setQueueName(this.getQueueName());

      // Add it to allContainers list.
      addToNewlyAllocatedContainers(node, rmContainer);
      liveContainers.put(container.getId(), rmContainer);

      // Update consumption and track allocations
      List<ResourceRequest> resourceRequestList = appSchedulingInfo.allocate(
          type, node, schedulerKey, container);
      this.attemptResourceUsage.incUsed(container.getResource());
      getQueue().incUsedResource(container.getResource());

      // Update resource requests related to "request" and store in RMContainer
      ((RMContainerImpl) rmContainer).setResourceRequests(resourceRequestList);

      // Inform the container
      rmContainer.handle(
          new RMContainerEvent(container.getId(), RMContainerEventType.START));

      if (LOG.isDebugEnabled()) {
        LOG.debug("allocate: applicationAttemptId=" + container.getId()
            .getApplicationAttemptId() + " container=" + container.getId()
            + " host=" + container.getNodeId().getHost() + " type=" + type);
      }
      RMAuditLogger.logSuccess(getUser(), AuditConstants.ALLOC_CONTAINER,
          "SchedulerApp", getApplicationId(), container.getId(),
          container.getResource());
    } finally {
      writeLock.unlock();
    }

    return rmContainer;
  }

  /**
   * Should be called when the scheduler assigns a container at a higher
   * degree of locality than the current threshold. Reset the allowed locality
   * level to a higher degree of locality.
   * @param schedulerKey Scheduler Key
   * @param level NodeType
   */
  void resetAllowedLocalityLevel(
      SchedulerRequestKey schedulerKey, NodeType level) {
    NodeType old;
    try {
      writeLock.lock();
      old = allowedLocalityLevel.put(schedulerKey, level);
    } finally {
      writeLock.unlock();
    }

    LOG.info("Raising locality level from " + old + " to " + level + " at "
        + " priority " + schedulerKey.getPriority());
  }

  @Override
  public FSLeafQueue getQueue() {
    return (FSLeafQueue) queue;
  }

  // Preemption related methods

  /**
   * Get overall starvation - fairshare and attributed minshare.
   *
   * @return total starvation attributed to this application
   */
  Resource getStarvation() {
    return Resources.add(fairshareStarvation, minshareStarvation);
  }

  /**
   * Get last computed fairshare starvation.
   *
   * @return last computed fairshare starvation
   */
  Resource getFairshareStarvation() {
    return fairshareStarvation;
  }

  /**
   * Set the minshare attributed to this application. To be called only from
   * {@link FSLeafQueue#updateStarvedApps}.
   *
   * @param starvation minshare starvation attributed to this app
   */
  void setMinshareStarvation(Resource starvation) {
    this.minshareStarvation = starvation;
  }

  /**
   * Reset the minshare starvation attributed to this application. To be
   * called only from {@link FSLeafQueue#updateStarvedApps}
   */
  void resetMinshareStarvation() {
    this.minshareStarvation = Resources.none();
  }

  /**
   * Get last computed minshare starvation.
   *
   * @return last computed minshare starvation
   */
  Resource getMinshareStarvation() {
    return minshareStarvation;
  }

  void trackContainerForPreemption(RMContainer container) {
    synchronized (preemptionVariablesLock) {
      if (containersToBePreempted.add(container)) {
        Resources.addTo(resourcesToBePreempted,
            container.getAllocatedResource());
      }
    }
  }

  private void untrackContainerForPreemption(RMContainer container) {
    synchronized (preemptionVariablesLock) {
      if (containersToBePreempted.remove(container)) {
        Resources.subtractFrom(resourcesToBePreempted,
            container.getAllocatedResource());
      }
    }
  }

  Set<ContainerId> getPreemptionContainerIds() {
    synchronized (preemptionVariablesLock) {
      Set<ContainerId> preemptionContainerIds = new HashSet<>();
      for (RMContainer container : containersToBePreempted) {
        preemptionContainerIds.add(container.getContainerId());
      }
      return preemptionContainerIds;
    }
  }

  boolean canContainerBePreempted(RMContainer container,
                                  Resource alreadyConsideringForPreemption) {
    if (!isPreemptable()) {
      return false;
    }

    // Sanity check that the app owns this container
    if (!getLiveContainersMap().containsKey(container.getContainerId()) &&
        !newlyAllocatedContainers.contains(container)) {
      LOG.error("Looking to preempt container " + container +
          ". Container does not belong to app " + getApplicationId());
      return false;
    }

    synchronized (preemptionVariablesLock) {
      if (containersToBePreempted.contains(container)) {
        // The container is already under consideration for preemption
        return false;
      }
    }

    // Check if the app's allocation will be over its fairshare even
    // after preempting this container
    Resource usageAfterPreemption = getUsageAfterPreemptingContainer(
            container.getAllocatedResource(),
            alreadyConsideringForPreemption);

    return !isUsageBelowShare(usageAfterPreemption, getFairShare());
  }

  private Resource getUsageAfterPreemptingContainer(Resource containerResources,
          Resource alreadyConsideringForPreemption) {
    Resource usageAfterPreemption = Resources.clone(getResourceUsage());

    // Subtract resources of containers already queued for preemption
    synchronized (preemptionVariablesLock) {
      Resources.subtractFrom(usageAfterPreemption, resourcesToBePreempted);
    }

    // Subtract resources of this container and other containers of this app
    // that the FSPreemptionThread is already considering for preemption.
    Resources.subtractFrom(usageAfterPreemption, containerResources);
    Resources.subtractFrom(usageAfterPreemption,
            alreadyConsideringForPreemption);

    return usageAfterPreemption;
  }

  /**
   * Create and return a container object reflecting an allocation for the
   * given application on the given node with the given capability and
   * priority.
   *
   * @param node Node
   * @param capability Capability
   * @param schedulerKey Scheduler Key
   * @return Container
   */
  private Container createContainer(FSSchedulerNode node, Resource capability,
      SchedulerRequestKey schedulerKey) {

    NodeId nodeId = node.getRMNode().getNodeID();
    ContainerId containerId = BuilderUtils.newContainerId(
        getApplicationAttemptId(), getNewContainerId());

    // Create the container
    return BuilderUtils.newContainer(containerId, nodeId,
        node.getRMNode().getHttpAddress(), capability,
        schedulerKey.getPriority(), null,
        schedulerKey.getAllocationRequestId());
  }

  @Override
  public synchronized void recoverContainer(SchedulerNode node,
      RMContainer rmContainer) {
    try {
      writeLock.lock();

      super.recoverContainer(node, rmContainer);

      if (!rmContainer.getState().equals(RMContainerState.COMPLETED)) {
        getQueue().incUsedResource(rmContainer.getContainer().getResource());
      }

      // If not running unmanaged, the first container we recover is always
      // the AM. Set the amResource for this app and update the leaf queue's AM
      // usage
      if (!isAmRunning() && !getUnmanagedAM()) {
        Resource resource = rmContainer.getAllocatedResource();
        setAMResource(resource);
        getQueue().addAMResourceUsage(resource);
        setAmRunning(true);
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Reserve a spot for {@code container} on this {@code node}. If
   * the container is {@code alreadyReserved} on the node, simply
   * update relevant bookeeping. This dispatches ro relevant handlers
   * in {@link FSSchedulerNode}..
   * return whether reservation was possible with the current threshold limits
   */
  private boolean reserve(Resource perAllocationResource, FSSchedulerNode node,
      Container reservedContainer, NodeType type,
      SchedulerRequestKey schedulerKey) {

    RMContainer nodeReservedContainer = node.getReservedContainer();
    boolean reservableForThisApp = nodeReservedContainer == null ||
        nodeReservedContainer.getApplicationAttemptId()
            .equals(getApplicationAttemptId());
    if (reservableForThisApp &&!reservationExceedsThreshold(node, type)) {
      LOG.info("Making reservation: node=" + node.getNodeName() +
              " app_id=" + getApplicationId());
      if (reservedContainer == null) {
        reservedContainer =
            createContainer(node, perAllocationResource,
              schedulerKey);
        getMetrics().reserveResource(node.getPartition(), getUser(),
            reservedContainer.getResource());
        RMContainer rmContainer =
                super.reserve(node, schedulerKey, null, reservedContainer);
        node.reserveResource(this, schedulerKey, rmContainer);
        setReservation(node);
      } else {
        RMContainer rmContainer = node.getReservedContainer();
        super.reserve(node, schedulerKey, rmContainer, reservedContainer);
        node.reserveResource(this, schedulerKey, rmContainer);
        setReservation(node);
      }
      return true;
    }
    return false;
  }

  private boolean reservationExceedsThreshold(FSSchedulerNode node,
                                                 NodeType type) {
    // Only if not node-local
    if (type != NodeType.NODE_LOCAL) {
      int existingReservations = getNumReservations(node.getRackName(),
              type == NodeType.OFF_SWITCH);
      int totalAvailNodes =
              (type == NodeType.OFF_SWITCH) ? scheduler.getNumClusterNodes() :
                      scheduler.getNumNodesInRack(node.getRackName());
      int numAllowedReservations =
              (int)Math.ceil(
                      totalAvailNodes * scheduler.getReservableNodesRatio());
      if (existingReservations >= numAllowedReservations) {
        DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(2);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Reservation Exceeds Allowed number of nodes:" +
                  " app_id=" + getApplicationId() +
                  " existingReservations=" + existingReservations +
                  " totalAvailableNodes=" + totalAvailNodes +
                  " reservableNodesRatio=" + df.format(
                                          scheduler.getReservableNodesRatio()) +
                  " numAllowedReservations=" + numAllowedReservations);
        }
        return true;
      }
    }
    return false;
  }

  /**
   * Remove the reservation on {@code node} at the given SchedulerRequestKey.
   * This dispatches SchedulerNode handlers as well.
   * @param schedulerKey Scheduler Key
   * @param node Node
   */
  public void unreserve(SchedulerRequestKey schedulerKey,
      FSSchedulerNode node) {
    RMContainer rmContainer = node.getReservedContainer();
    unreserveInternal(schedulerKey, node);
    node.unreserveResource(this);
    clearReservation(node);
    getMetrics().unreserveResource(node.getPartition(),
        getUser(), rmContainer.getContainer().getResource());
  }

  private void setReservation(SchedulerNode node) {
    String rackName =
        node.getRackName() == null ? "NULL" : node.getRackName();

    try {
      writeLock.lock();
      Set<String> rackReservations = reservations.get(rackName);
      if (rackReservations == null) {
        rackReservations = new HashSet<>();
        reservations.put(rackName, rackReservations);
      }
      rackReservations.add(node.getNodeName());
    } finally {
      writeLock.unlock();
    }
  }

  private void clearReservation(SchedulerNode node) {
    String rackName =
        node.getRackName() == null ? "NULL" : node.getRackName();

    try {
      writeLock.lock();
      Set<String> rackReservations = reservations.get(rackName);
      if (rackReservations != null) {
        rackReservations.remove(node.getNodeName());
      }
    } finally {
      writeLock.unlock();
    }
  }

  int getNumReservations(String rackName, boolean isAny) {
    int counter = 0;
    if (isAny) {
      for (Set<String> nodes : reservations.values()) {
        if (nodes != null) {
          counter += nodes.size();
        }
      }
    } else {
      Set<String> nodes = reservations.get(
              rackName == null ? "NULL" : rackName);
      if (nodes != null) {
        counter += nodes.size();
      }
    }
    return counter;
  }

  /**
   * Assign a container to this node to facilitate {@code request}. If node does
   * not have enough memory, create a reservation. This is called once we are
   * sure the particular request should be facilitated by this node.
   *
   * @param node
   *     The node to try placing the container on.
   * @param pendingAsk
   *     The {@link PendingAsk} we're trying to satisfy.
   * @param type
   *     The locality of the assignment.
   * @param reserved
   *     Whether there's already a container reserved for this app on the node.
   * @return
   *     If an assignment was made, returns the resources allocated to the
   *     container.  If a reservation was made, returns
   *     FairScheduler.CONTAINER_RESERVED.  If no assignment or reservation was
   *     made, returns an empty resource.
   */
  private Resource assignContainer(
      FSSchedulerNode node, PendingAsk pendingAsk, NodeType type,
      boolean reserved, SchedulerRequestKey schedulerKey) {

    // How much does this request need?
    Resource capability = pendingAsk.getPerAllocationResource();

    // How much does the node have?
    Resource available = node.getUnallocatedResource();

    Container reservedContainer = null;
    if (reserved) {
      reservedContainer = node.getReservedContainer().getContainer();
    }

    // Can we allocate a container on this node?
    if (Resources.fitsIn(capability, available)) {
      // Inform the application of the new container for this request
      RMContainer allocatedContainer =
          allocate(type, node, schedulerKey, pendingAsk,
              reservedContainer);
      if (allocatedContainer == null) {
        // Did the application need this resource?
        if (reserved) {
          unreserve(schedulerKey, node);
        }
        return Resources.none();
      }

      // If we had previously made a reservation, delete it
      if (reserved) {
        unreserve(schedulerKey, node);
      }

      // Inform the node
      node.allocateContainer(allocatedContainer);

      // If not running unmanaged, the first container we allocate is always
      // the AM. Set the amResource for this app and update the leaf queue's AM
      // usage
      if (!isAmRunning() && !getUnmanagedAM()) {
        setAMResource(capability);
        getQueue().addAMResourceUsage(capability);
        setAmRunning(true);
      }

      return capability;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Resource request: " + capability + " exceeds the available"
          + " resources of the node.");
    }

    // The desired container won't fit here, so reserve
    // Reserve only, if app does not wait for preempted resources on the node,
    // otherwise we may end up with duplicate reservations
    if (isReservable(capability) &&
        !node.isPreemptedForApp(this) &&
        reserve(pendingAsk.getPerAllocationResource(), node, reservedContainer,
            type, schedulerKey)) {
      updateAMDiagnosticMsg(capability, " exceeds the available resources of "
          + "the node and the request is reserved)");
      if (LOG.isDebugEnabled()) {
        LOG.debug(getName() + "'s resource request is reserved.");
      }
      return FairScheduler.CONTAINER_RESERVED;
    } else {
      updateAMDiagnosticMsg(capability, " exceeds the available resources of "
          + "the node and the request cannot be reserved)");
      if (LOG.isDebugEnabled()) {
        LOG.debug("Couldn't create reservation for app:  " + getName()
            + ", at priority " +  schedulerKey.getPriority());
      }
      return Resources.none();
    }
  }

  private boolean isReservable(Resource capacity) {
    // Reserve only when the app is starved and the requested container size
    // is larger than the configured threshold
    return isStarved() &&
        scheduler.isAtLeastReservationThreshold(
            getQueue().getPolicy().getResourceCalculator(), capacity);
  }

  /**
   * Whether the AM container for this app is over maxAMShare limit.
   */
  private boolean isOverAMShareLimit() {
    // Check the AM resource usage for the leaf queue
    if (!isAmRunning() && !getUnmanagedAM()) {
      // Return true if we have not ask, or queue is not be able to run app's AM
      PendingAsk ask = appSchedulingInfo.getNextPendingAsk();
      if (ask.getCount() == 0 || !getQueue().canRunAppAM(
          ask.getPerAllocationResource())) {
        return true;
      }
    }
    return false;
  }

  private Resource assignContainer(FSSchedulerNode node, boolean reserved) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Node offered to app: " + getName() + " reserved: " + reserved);
    }

    Collection<SchedulerRequestKey> keysToTry = (reserved) ?
        Collections.singletonList(
            node.getReservedContainer().getReservedSchedulerKey()) :
        getSchedulerKeys();

    // For each priority, see if we can schedule a node local, rack local
    // or off-switch request. Rack of off-switch requests may be delayed
    // (not scheduled) in order to promote better locality.
    try {
      writeLock.lock();

      // TODO (wandga): All logics in this method should be added to
      // SchedulerPlacement#canDelayTo which is independent from scheduler.
      // Scheduler can choose to use various/pluggable delay-scheduling
      // implementation.
      for (SchedulerRequestKey schedulerKey : keysToTry) {
        // Skip it for reserved container, since
        // we already check it in isValidReservation.
        if (!reserved && !hasContainerForNode(schedulerKey, node)) {
          continue;
        }

        addSchedulingOpportunity(schedulerKey);

        PendingAsk rackLocalPendingAsk = getPendingAsk(schedulerKey,
            node.getRackName());
        PendingAsk nodeLocalPendingAsk = getPendingAsk(schedulerKey,
            node.getNodeName());

        if (nodeLocalPendingAsk.getCount() > 0
            && !appSchedulingInfo.canDelayTo(schedulerKey,
            node.getNodeName())) {
          LOG.warn("Relax locality off is not supported on local request: "
              + nodeLocalPendingAsk);
        }

        NodeType allowedLocality;
        if (scheduler.isContinuousSchedulingEnabled()) {
          allowedLocality = getAllowedLocalityLevelByTime(schedulerKey,
              scheduler.getNodeLocalityDelayMs(),
              scheduler.getRackLocalityDelayMs(),
              scheduler.getClock().getTime());
        } else {
          allowedLocality = getAllowedLocalityLevel(schedulerKey,
              scheduler.getNumClusterNodes(),
              scheduler.getNodeLocalityThreshold(),
              scheduler.getRackLocalityThreshold());
        }

        if (rackLocalPendingAsk.getCount() > 0
            && nodeLocalPendingAsk.getCount() > 0) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Assign container on " + node.getNodeName()
                + " node, assignType: NODE_LOCAL" + ", allowedLocality: "
                + allowedLocality + ", priority: " + schedulerKey.getPriority()
                + ", app attempt id: " + this.attemptId);
          }
          return assignContainer(node, nodeLocalPendingAsk, NodeType.NODE_LOCAL,
              reserved, schedulerKey);
        }

        if (!appSchedulingInfo.canDelayTo(schedulerKey, node.getRackName())) {
          continue;
        }

        if (rackLocalPendingAsk.getCount() > 0
            && (allowedLocality.equals(NodeType.RACK_LOCAL) || allowedLocality
            .equals(NodeType.OFF_SWITCH))) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Assign container on " + node.getNodeName()
                + " node, assignType: RACK_LOCAL" + ", allowedLocality: "
                + allowedLocality + ", priority: " + schedulerKey.getPriority()
                + ", app attempt id: " + this.attemptId);
          }
          return assignContainer(node, rackLocalPendingAsk, NodeType.RACK_LOCAL,
              reserved, schedulerKey);
        }

        PendingAsk offswitchAsk = getPendingAsk(schedulerKey,
            ResourceRequest.ANY);
        if (!appSchedulingInfo.canDelayTo(schedulerKey, ResourceRequest.ANY)) {
          continue;
        }

        if (offswitchAsk.getCount() > 0) {
          if (getAppPlacementAllocator(schedulerKey).getUniqueLocationAsks()
              <= 1 || allowedLocality.equals(NodeType.OFF_SWITCH)) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Assign container on " + node.getNodeName()
                  + " node, assignType: OFF_SWITCH" + ", allowedLocality: "
                  + allowedLocality + ", priority: "
                  + schedulerKey.getPriority()
                  + ", app attempt id: " + this.attemptId);
            }
            return assignContainer(node, offswitchAsk, NodeType.OFF_SWITCH,
                reserved, schedulerKey);
          }
        }

        if (LOG.isTraceEnabled()) {
          LOG.trace("Can't assign container on " + node.getNodeName()
              + " node, allowedLocality: " + allowedLocality + ", priority: "
              + schedulerKey.getPriority() + ", app attempt id: "
              + this.attemptId);
        }
      }
    } finally {
      writeLock.unlock();
    }

    return Resources.none();
  }

  /**
   * Whether this app has containers requests that could be satisfied on the
   * given node, if the node had full space.
   */
  private boolean hasContainerForNode(SchedulerRequestKey key,
      FSSchedulerNode node) {
    PendingAsk offswitchAsk = getPendingAsk(key, ResourceRequest.ANY);
    Resource resource = offswitchAsk.getPerAllocationResource();
    boolean hasRequestForOffswitch =
        offswitchAsk.getCount() > 0;
    boolean hasRequestForRack = getOutstandingAsksCount(key,
        node.getRackName()) > 0;
    boolean hasRequestForNode = getOutstandingAsksCount(key,
        node.getNodeName()) > 0;

    boolean ret = true;
    if (!(// There must be outstanding requests at the given priority:
        hasRequestForOffswitch &&
            // If locality relaxation is turned off at *-level, there must be a
            // non-zero request for the node's rack:
            (appSchedulingInfo.canDelayTo(key, ResourceRequest.ANY) ||
                (hasRequestForRack)) &&
            // If locality relaxation is turned off at rack-level,
            // there must be a non-zero request at the node:
            (!hasRequestForRack || appSchedulingInfo.canDelayTo(key,
                node.getRackName()) || (hasRequestForNode)) &&
            // The requested container must be able to fit on the node:
            Resources.lessThanOrEqual(RESOURCE_CALCULATOR, null,
                resource,
                node.getRMNode().getTotalCapability()))) {
      ret = false;
    } else if (!getQueue().fitsInMaxShare(resource)) {
      // The requested container must fit in queue maximum share
      updateAMDiagnosticMsg(resource,
          " exceeds current queue or its parents maximum resource allowed).");

      ret = false;
    }

    return ret;
  }

  private boolean isValidReservation(FSSchedulerNode node) {
    SchedulerRequestKey schedulerKey = node.getReservedContainer().
        getReservedSchedulerKey();
    return hasContainerForNode(schedulerKey, node) &&
        !isOverAMShareLimit();
  }

  /**
   * Called when this application already has an existing reservation on the
   * given node.  Sees whether we can turn the reservation into an allocation.
   * Also checks whether the application needs the reservation anymore, and
   * releases it if not.
   *
   * @param node
   *     Node that the application has an existing reservation on
   * @return whether the reservation on the given node is valid.
   */
  boolean assignReservedContainer(FSSchedulerNode node) {
    RMContainer rmContainer = node.getReservedContainer();
    SchedulerRequestKey reservedSchedulerKey =
        rmContainer.getReservedSchedulerKey();

    if (!isValidReservation(node)) {
      // Don't hold the reservation if app can no longer use it
      LOG.info("Releasing reservation that cannot be satisfied for " +
          "application " + getApplicationAttemptId() + " on node " + node);
      unreserve(reservedSchedulerKey, node);
      return false;
    }

    // Reservation valid; try to fulfill the reservation
    if (LOG.isDebugEnabled()) {
      LOG.debug("Trying to fulfill reservation for application "
          + getApplicationAttemptId() + " on node: " + node);
    }

    // Fail early if the reserved container won't fit.
    // Note that we have an assumption here that
    // there's only one container size per priority.
    if (Resources.fitsIn(node.getReservedContainer().getReservedResource(),
        node.getUnallocatedResource())) {
      assignContainer(node, true);
    }
    return true;
  }

  /**
   * Helper method that computes the extent of fairshare starvation.
   * @return freshly computed fairshare starvation
   */
  Resource fairShareStarvation() {
    long now = scheduler.getClock().getTime();
    Resource threshold = Resources.multiply(
        getFairShare(), getQueue().getFairSharePreemptionThreshold());
    Resource fairDemand = Resources.componentwiseMin(threshold, demand);

    // Check if the queue is starved for fairshare
    boolean starved = isUsageBelowShare(getResourceUsage(), fairDemand);

    if (!starved) {
      lastTimeAtFairShare = now;
    }

    if (!starved ||
        now - lastTimeAtFairShare <
            getQueue().getFairSharePreemptionTimeout()) {
      fairshareStarvation = Resources.none();
    } else {
      // The app has been starved for longer than preemption-timeout.
      fairshareStarvation =
          Resources.subtractFromNonNegative(fairDemand, getResourceUsage());
    }
    return fairshareStarvation;
  }

  /**
   * Helper method that checks if {@code usage} is strictly less than
   * {@code share}.
   */
  private boolean isUsageBelowShare(Resource usage, Resource share) {
    return getQueue().getPolicy().getResourceCalculator().compare(
        scheduler.getClusterResource(), usage, share, true) < 0;
  }

  /**
   * Helper method that captures if this app is identified to be starved.
   * @return true if the app is starved for fairshare, false otherwise
   */
  boolean isStarvedForFairShare() {
    return isUsageBelowShare(getResourceUsage(), getFairShare());
  }

  /**
   * Is application starved for fairshare or minshare.
   */
  boolean isStarved() {
    return isStarvedForFairShare() || !Resources.isNone(minshareStarvation);
  }

  /**
   * Fetch a list of RRs corresponding to the extent the app is starved
   * (fairshare and minshare). This method considers the number of containers
   * in a RR and also only one locality-level (the first encountered
   * resourceName).
   *
   * @return list of {@link ResourceRequest}s corresponding to the amount of
   * starvation.
   */
  List<ResourceRequest> getStarvedResourceRequests() {
    // List of RRs we build in this method to return
    List<ResourceRequest> ret = new ArrayList<>();

    // Track visited RRs to avoid the same RR at multiple locality levels
    VisitedResourceRequestTracker visitedRRs =
        new VisitedResourceRequestTracker(scheduler.getNodeTracker());

    // Start with current starvation and track the pending amount
    Resource pending = getStarvation();
    for (ResourceRequest rr : appSchedulingInfo.getAllResourceRequests()) {
      if (Resources.isNone(pending)) {
        // Found enough RRs to match the starvation
        break;
      }

      // See if we have already seen this RR
      if (!visitedRRs.visit(rr)) {
        continue;
      }

      // A RR can have multiple containers of a capability. We need to
      // compute the number of containers that fit in "pending".
      int numContainersThatFit = (int) Math.floor(
          Resources.ratio(scheduler.getResourceCalculator(),
              pending, rr.getCapability()));
      if (numContainersThatFit == 0) {
        // This RR's capability is too large to fit in pending
        continue;
      }

      // If the RR is only partially being satisfied, include only the
      // partial number of containers.
      if (numContainersThatFit < rr.getNumContainers()) {
        rr = ResourceRequest.newInstance(rr.getPriority(),
            rr.getResourceName(), rr.getCapability(), numContainersThatFit);
      }

      // Add the RR to return list and adjust "pending" accordingly
      ret.add(rr);
      Resources.subtractFromNonNegative(pending,
          Resources.multiply(rr.getCapability(), rr.getNumContainers()));
    }

    return ret;
  }

  /**
   * Notify this app that preemption has been triggered to make room for
   * outstanding demand. The app should not be considered starved until after
   * the specified delay.
   *
   * @param delayBeforeNextStarvationCheck duration to wait
   */
  void preemptionTriggered(long delayBeforeNextStarvationCheck) {
    nextStarvationCheck =
        scheduler.getClock().getTime() + delayBeforeNextStarvationCheck;
  }

  /**
   * Whether this app's starvation should be considered.
   */
  boolean shouldCheckForStarvation() {
    return scheduler.getClock().getTime() >= nextStarvationCheck;
  }

  /* Schedulable methods implementation */

  @Override
  public String getName() {
    return getApplicationId().toString();
  }

  @Override
  public Resource getDemand() {
    return demand;
  }

  /**
   * Get the current app's unsatisfied demand.
   */
  Resource getPendingDemand() {
    return Resources.subtract(demand, getResourceUsage());
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  @Override
  public Resource getMinShare() {
    return Resources.none();
  }

  @Override
  public Resource getMaxShare() {
    return Resources.unbounded();
  }

  @Override
  public Resource getResourceUsage() {
    return getCurrentConsumption();
  }

  @Override
  public float getWeight() {
    float weight = 1.0F;

    if (scheduler.isSizeBasedWeight()) {
      // Set weight based on current memory demand
      weight = (float)(Math.log1p(demand.getMemorySize()) / Math.log(2));
    }

    return weight * appPriority.getPriority();
  }

  @Override
  public Priority getPriority() {
    // Right now per-app priorities are not passed to scheduler,
    // so everyone has the same priority.
    return appPriority;
  }

  @Override
  public Resource getFairShare() {
    return this.fairShare;
  }

  @Override
  public void setFairShare(Resource fairShare) {
    this.fairShare = fairShare;
  }

  @Override
  public void updateDemand() {
    // Demand is current consumption plus outstanding requests
    Resource tmpDemand = Resources.clone(getCurrentConsumption());

    // Add up outstanding resource requests
    for (SchedulerRequestKey k : getSchedulerKeys()) {
      PendingAsk pendingAsk = getPendingAsk(k, ResourceRequest.ANY);
      if (pendingAsk.getCount() > 0) {
        Resources.multiplyAndAddTo(tmpDemand,
            pendingAsk.getPerAllocationResource(),
            pendingAsk.getCount());
      }
    }

    // Update demand
    demand = tmpDemand;
  }

  @Override
  public Resource assignContainer(FSSchedulerNode node) {
    if (isOverAMShareLimit()) {
      PendingAsk amAsk = appSchedulingInfo.getNextPendingAsk();
      updateAMDiagnosticMsg(amAsk.getPerAllocationResource(),
          " exceeds maximum AM resource allowed).");
      if (LOG.isDebugEnabled()) {
        LOG.debug("AM resource request: " + amAsk.getPerAllocationResource()
            + " exceeds maximum AM resource allowed, "
            + getQueue().dumpState());
      }
      return Resources.none();
    }
    return assignContainer(node, false);
  }

  /**
   * Build the diagnostic message and update it.
   *
   * @param resource resource request
   * @param reason the reason why AM doesn't get the resource
   */
  private void updateAMDiagnosticMsg(Resource resource, String reason) {
    if (!isWaitingForAMContainer()) {
      return;
    }

    StringBuilder diagnosticMessageBldr = new StringBuilder();
    diagnosticMessageBldr.append(" (Resource request: ");
    diagnosticMessageBldr.append(resource);
    diagnosticMessageBldr.append(reason);
    updateAMContainerDiagnostics(AMState.INACTIVATED,
        diagnosticMessageBldr.toString());
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

  @Override
  public String toString() {
    return getApplicationAttemptId() + " Alloc: " + getCurrentConsumption();
  }

  @Override
  public boolean isPreemptable() {
    return getQueue().isPreemptable();
  }
}

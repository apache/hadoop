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

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
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
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
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

  private long startTime;
  private Priority appPriority;
  private ResourceWeights resourceWeights;
  private Resource demand = Resources.createResource(0);
  private FairScheduler scheduler;
  private Resource fairShare = Resources.createResource(0, 0);
  private Resource preemptedResources = Resources.createResource(0);
  private RMContainerComparator comparator = new RMContainerComparator();
  private final Map<RMContainer, Long> preemptionMap = new HashMap<RMContainer, Long>();

  // Used to record node reservation by an app.
  // Key = RackName, Value = Set of Nodes reserved by app on rack
  private Map<String, Set<String>> reservations = new HashMap<>();

  private List<FSSchedulerNode> blacklistNodeIds = new ArrayList<>();
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
  private final Map<Priority, NodeType> allowedLocalityLevel =
      new HashMap<Priority, NodeType>();

  public FSAppAttempt(FairScheduler scheduler,
      ApplicationAttemptId applicationAttemptId, String user, FSLeafQueue queue,
      ActiveUsersManager activeUsersManager, RMContext rmContext) {
    super(applicationAttemptId, user, queue, activeUsersManager, rmContext);

    this.scheduler = scheduler;
    this.startTime = scheduler.getClock().getTime();
    this.appPriority = Priority.newInstance(1);
    this.resourceWeights = new ResourceWeights();
  }

  public ResourceWeights getResourceWeights() {
    return resourceWeights;
  }

  /**
   * Get metrics reference from containing queue.
   */
  public QueueMetrics getMetrics() {
    return queue.getMetrics();
  }

  synchronized public void containerCompleted(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event) {
    
    Container container = rmContainer.getContainer();
    ContainerId containerId = container.getId();
    
    // Remove from the list of newly allocated containers if found
    newlyAllocatedContainers.remove(rmContainer);
    
    // Inform the container
    rmContainer.handle(
        new RMContainerFinishedEvent(
            containerId,
            containerStatus,
            event)
    );
    if (LOG.isDebugEnabled()) {
      LOG.debug("Completed container: " + rmContainer.getContainerId() +
              " in state: " + rmContainer.getState() + " event:" + event);
    }

    // Remove from the list of containers
    liveContainers.remove(rmContainer.getContainerId());

    Resource containerResource = rmContainer.getContainer().getResource();
    RMAuditLogger.logSuccess(getUser(), 
        AuditConstants.RELEASE_CONTAINER, "SchedulerApp", 
        getApplicationId(), containerId, containerResource);
    
    // Update usage metrics 
    queue.getMetrics().releaseResources(getUser(), 1, containerResource);
    this.attemptResourceUsage.decUsed(containerResource);

    // remove from preemption map if it is completed
    preemptionMap.remove(rmContainer);

    // Clear resource utilization metrics cache.
    lastMemoryAggregateAllocationUpdateTime = -1;
  }

  private synchronized void unreserveInternal(
      Priority priority, FSSchedulerNode node) {
    Map<NodeId, RMContainer> reservedContainers = 
        this.reservedContainers.get(priority);
    RMContainer reservedContainer = reservedContainers.remove(node.getNodeID());
    if (reservedContainers.isEmpty()) {
      this.reservedContainers.remove(priority);
    }
    
    // Reset the re-reservation count
    resetReReservations(priority);

    Resource resource = reservedContainer.getContainer().getResource();
    this.attemptResourceUsage.decReserved(resource);

    LOG.info("Application " + getApplicationId() + " unreserved " + " on node "
        + node + ", currently has " + reservedContainers.size()
        + " at priority " + priority + "; currentReservation "
        + this.attemptResourceUsage.getReserved());
  }

  private void subtractResourcesOnBlacklistedNodes(
      Resource availableResources) {
    if (appSchedulingInfo.getAndResetBlacklistChanged()) {
      blacklistNodeIds.clear();
      blacklistNodeIds.addAll(scheduler.getBlacklistedNodes(this));
    }
    for (FSSchedulerNode node: blacklistNodeIds) {
      Resources.subtractFrom(availableResources,
          node.getUnallocatedResource());
    }
    if (availableResources.getMemorySize() < 0) {
      availableResources.setMemory(0);
    }
    if (availableResources.getVirtualCores() < 0) {
      availableResources.setVirtualCores(0);
    }
  }

  /**
   * Headroom depends on resources in the cluster, current usage of the
   * queue, queue's fair-share and queue's max-resources.
   */
  @Override
  public Resource getHeadroom() {
    final FSQueue queue = (FSQueue) this.queue;
    SchedulingPolicy policy = queue.getPolicy();

    Resource queueFairShare = queue.getFairShare();
    Resource queueUsage = queue.getResourceUsage();
    Resource clusterResource = this.scheduler.getClusterResource();
    Resource clusterUsage = this.scheduler.getRootQueueMetrics()
        .getAllocatedResources();

    Resource clusterAvailableResources =
        Resources.subtract(clusterResource, clusterUsage);
    subtractResourcesOnBlacklistedNodes(clusterAvailableResources);

    Resource queueMaxAvailableResources =
        Resources.subtract(queue.getMaxShare(), queueUsage);
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

  public synchronized float getLocalityWaitFactor(
      Priority priority, int clusterNodes) {
    // Estimate: Required unique resources (i.e. hosts + racks)
    int requiredResources = 
        Math.max(this.getResourceRequests(priority).size() - 1, 0);
    
    // waitFactor can't be more than '1' 
    // i.e. no point skipping more than clustersize opportunities
    return Math.min(((float)requiredResources / clusterNodes), 1.0f);
  }

  /**
   * Return the level at which we are allowed to schedule containers, given the
   * current size of the cluster and thresholds indicating how many nodes to
   * fail at (as a fraction of cluster size) before relaxing scheduling
   * constraints.
   */
  public synchronized NodeType getAllowedLocalityLevel(Priority priority,
      int numNodes, double nodeLocalityThreshold, double rackLocalityThreshold) {
    // upper limit on threshold
    if (nodeLocalityThreshold > 1.0) { nodeLocalityThreshold = 1.0; }
    if (rackLocalityThreshold > 1.0) { rackLocalityThreshold = 1.0; }

    // If delay scheduling is not being used, can schedule anywhere
    if (nodeLocalityThreshold < 0.0 || rackLocalityThreshold < 0.0) {
      return NodeType.OFF_SWITCH;
    }

    // Default level is NODE_LOCAL
    if (!allowedLocalityLevel.containsKey(priority)) {
      allowedLocalityLevel.put(priority, NodeType.NODE_LOCAL);
      return NodeType.NODE_LOCAL;
    }

    NodeType allowed = allowedLocalityLevel.get(priority);

    // If level is already most liberal, we're done
    if (allowed.equals(NodeType.OFF_SWITCH)) return NodeType.OFF_SWITCH;

    double threshold = allowed.equals(NodeType.NODE_LOCAL) ? nodeLocalityThreshold :
      rackLocalityThreshold;

    // Relax locality constraints once we've surpassed threshold.
    if (getSchedulingOpportunities(priority) > (numNodes * threshold)) {
      if (allowed.equals(NodeType.NODE_LOCAL)) {
        allowedLocalityLevel.put(priority, NodeType.RACK_LOCAL);
        resetSchedulingOpportunities(priority);
      }
      else if (allowed.equals(NodeType.RACK_LOCAL)) {
        allowedLocalityLevel.put(priority, NodeType.OFF_SWITCH);
        resetSchedulingOpportunities(priority);
      }
    }
    return allowedLocalityLevel.get(priority);
  }

  /**
   * Return the level at which we are allowed to schedule containers.
   * Given the thresholds indicating how much time passed before relaxing
   * scheduling constraints.
   */
  public synchronized NodeType getAllowedLocalityLevelByTime(Priority priority,
          long nodeLocalityDelayMs, long rackLocalityDelayMs,
          long currentTimeMs) {

    // if not being used, can schedule anywhere
    if (nodeLocalityDelayMs < 0 || rackLocalityDelayMs < 0) {
      return NodeType.OFF_SWITCH;
    }

    // default level is NODE_LOCAL
    if (!allowedLocalityLevel.containsKey(priority)) {
      // add the initial time of priority to prevent comparing with FsApp
      // startTime and allowedLocalityLevel degrade
      lastScheduledContainer.put(priority, currentTimeMs);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Init the lastScheduledContainer time, priority: " + priority
            + ", time: " + currentTimeMs);
      }
      allowedLocalityLevel.put(priority, NodeType.NODE_LOCAL);
      return NodeType.NODE_LOCAL;
    }

    NodeType allowed = allowedLocalityLevel.get(priority);

    // if level is already most liberal, we're done
    if (allowed.equals(NodeType.OFF_SWITCH)) {
      return NodeType.OFF_SWITCH;
    }

    // check waiting time
    long waitTime = currentTimeMs;
    if (lastScheduledContainer.containsKey(priority)) {
      waitTime -= lastScheduledContainer.get(priority);
    } else {
      waitTime -= getStartTime();
    }

    long thresholdTime = allowed.equals(NodeType.NODE_LOCAL) ?
            nodeLocalityDelayMs : rackLocalityDelayMs;

    if (waitTime > thresholdTime) {
      if (allowed.equals(NodeType.NODE_LOCAL)) {
        allowedLocalityLevel.put(priority, NodeType.RACK_LOCAL);
        resetSchedulingOpportunities(priority, currentTimeMs);
      } else if (allowed.equals(NodeType.RACK_LOCAL)) {
        allowedLocalityLevel.put(priority, NodeType.OFF_SWITCH);
        resetSchedulingOpportunities(priority, currentTimeMs);
      }
    }
    return allowedLocalityLevel.get(priority);
  }

  synchronized public RMContainer allocate(NodeType type, FSSchedulerNode node,
      Priority priority, ResourceRequest request,
      Container reservedContainer) {
    // Update allowed locality level
    NodeType allowed = allowedLocalityLevel.get(priority);
    if (allowed != null) {
      if (allowed.equals(NodeType.OFF_SWITCH) &&
          (type.equals(NodeType.NODE_LOCAL) ||
              type.equals(NodeType.RACK_LOCAL))) {
        this.resetAllowedLocalityLevel(priority, type);
      }
      else if (allowed.equals(NodeType.RACK_LOCAL) &&
          type.equals(NodeType.NODE_LOCAL)) {
        this.resetAllowedLocalityLevel(priority, type);
      }
    }

    // Required sanity check - AM can call 'allocate' to update resource 
    // request without locking the scheduler, hence we need to check
    if (getTotalRequiredResources(priority) <= 0) {
      return null;
    }

    Container container = reservedContainer;
    if (container == null) {
      container =
          createContainer(node, request.getCapability(), request.getPriority());
    }
    
    // Create RMContainer
    RMContainer rmContainer = new RMContainerImpl(container,
        getApplicationAttemptId(), node.getNodeID(),
        appSchedulingInfo.getUser(), rmContext);
    ((RMContainerImpl)rmContainer).setQueueName(this.getQueueName());

    // Add it to allContainers list.
    newlyAllocatedContainers.add(rmContainer);
    liveContainers.put(container.getId(), rmContainer);    

    // Update consumption and track allocations
    List<ResourceRequest> resourceRequestList = appSchedulingInfo.allocate(
        type, node, priority, request, container);
    this.attemptResourceUsage.incUsed(container.getResource());

    // Update resource requests related to "request" and store in RMContainer
    ((RMContainerImpl) rmContainer).setResourceRequests(resourceRequestList);

    // Inform the container
    rmContainer.handle(
        new RMContainerEvent(container.getId(), RMContainerEventType.START));

    if (LOG.isDebugEnabled()) {
      LOG.debug("allocate: applicationAttemptId=" 
          + container.getId().getApplicationAttemptId() 
          + " container=" + container.getId() + " host="
          + container.getNodeId().getHost() + " type=" + type);
    }
    RMAuditLogger.logSuccess(getUser(), 
        AuditConstants.ALLOC_CONTAINER, "SchedulerApp", 
        getApplicationId(), container.getId(), container.getResource());
    
    return rmContainer;
  }

  /**
   * Should be called when the scheduler assigns a container at a higher
   * degree of locality than the current threshold. Reset the allowed locality
   * level to a higher degree of locality.
   */
  public synchronized void resetAllowedLocalityLevel(Priority priority,
      NodeType level) {
    NodeType old = allowedLocalityLevel.get(priority);
    LOG.info("Raising locality level from " + old + " to " + level + " at " +
        " priority " + priority);
    allowedLocalityLevel.put(priority, level);
  }

  // related methods
  public void addPreemption(RMContainer container, long time) {
    assert preemptionMap.get(container) == null;
    preemptionMap.put(container, time);
    Resources.addTo(preemptedResources, container.getAllocatedResource());
  }

  public Long getContainerPreemptionTime(RMContainer container) {
    return preemptionMap.get(container);
  }

  public Set<RMContainer> getPreemptionContainers() {
    return preemptionMap.keySet();
  }
  
  @Override
  public FSLeafQueue getQueue() {
    return (FSLeafQueue)super.getQueue();
  }

  public Resource getPreemptedResources() {
    return preemptedResources;
  }

  public void resetPreemptedResources() {
    preemptedResources = Resources.createResource(0);
    for (RMContainer container : getPreemptionContainers()) {
      Resources.addTo(preemptedResources, container.getAllocatedResource());
    }
  }

  public void clearPreemptedResources() {
    preemptedResources.setMemory(0);
    preemptedResources.setVirtualCores(0);
  }

  /**
   * Create and return a container object reflecting an allocation for the
   * given appliction on the given node with the given capability and
   * priority.
   */
  public Container createContainer(
      FSSchedulerNode node, Resource capability, Priority priority) {

    NodeId nodeId = node.getRMNode().getNodeID();
    ContainerId containerId = BuilderUtils.newContainerId(
        getApplicationAttemptId(), getNewContainerId());

    // Create the container
    Container container =
        BuilderUtils.newContainer(containerId, nodeId, node.getRMNode()
            .getHttpAddress(), capability, priority, null);

    return container;
  }

  /**
   * Reserve a spot for {@code container} on this {@code node}. If
   * the container is {@code alreadyReserved} on the node, simply
   * update relevant bookeeping. This dispatches ro relevant handlers
   * in {@link FSSchedulerNode}..
   * return whether reservation was possible with the current threshold limits
   */
  private boolean reserve(ResourceRequest request, FSSchedulerNode node,
      Container reservedContainer, NodeType type) {

    Priority priority = request.getPriority();
    if (!reservationExceedsThreshold(node, type)) {
      LOG.info("Making reservation: node=" + node.getNodeName() +
              " app_id=" + getApplicationId());
      if (reservedContainer == null) {
        reservedContainer =
            createContainer(node, request.getCapability(),
              request.getPriority());
        getMetrics().reserveResource(getUser(),
            reservedContainer.getResource());
        RMContainer rmContainer =
                super.reserve(node, priority, null, reservedContainer);
        node.reserveResource(this, priority, rmContainer);
        setReservation(node);
      } else {
        RMContainer rmContainer = node.getReservedContainer();
        super.reserve(node, priority, rmContainer, reservedContainer);
        node.reserveResource(this, priority, rmContainer);
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
   * Remove the reservation on {@code node} at the given {@link Priority}.
   * This dispatches SchedulerNode handlers as well.
   */
  public void unreserve(Priority priority, FSSchedulerNode node) {
    RMContainer rmContainer = node.getReservedContainer();
    unreserveInternal(priority, node);
    node.unreserveResource(this);
    clearReservation(node);
    getMetrics().unreserveResource(
        getUser(), rmContainer.getContainer().getResource());
  }

  private synchronized void setReservation(SchedulerNode node) {
    String rackName = node.getRackName() == null ? "NULL" : node.getRackName();
    Set<String> rackReservations = reservations.get(rackName);
    if (rackReservations == null) {
      rackReservations = new HashSet<>();
      reservations.put(rackName, rackReservations);
    }
    rackReservations.add(node.getNodeName());
  }

  private synchronized void clearReservation(SchedulerNode node) {
    String rackName = node.getRackName() == null ? "NULL" : node.getRackName();
    Set<String> rackReservations = reservations.get(rackName);
    if (rackReservations != null) {
      rackReservations.remove(node.getNodeName());
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
   * @param request
   *     The ResourceRequest we're trying to satisfy.
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
      FSSchedulerNode node, ResourceRequest request, NodeType type,
      boolean reserved) {

    // How much does this request need?
    Resource capability = request.getCapability();

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
          allocate(type, node, request.getPriority(), request,
              reservedContainer);
      if (allocatedContainer == null) {
        // Did the application need this resource?
        if (reserved) {
          unreserve(request.getPriority(), node);
        }
        return Resources.none();
      }

      // If we had previously made a reservation, delete it
      if (reserved) {
        unreserve(request.getPriority(), node);
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

    // The desired container won't fit here, so reserve
    if (isReservable(capability) &&
        reserve(request, node, reservedContainer, type)) {
      return FairScheduler.CONTAINER_RESERVED;
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Couldn't creating reservation for " +
            getName() + ",at priority " +  request.getPriority());
      }
      return Resources.none();
    }
  }

  private boolean isReservable(Resource capacity) {
    return scheduler.isAtLeastReservationThreshold(
        getQueue().getPolicy().getResourceCalculator(), capacity);
  }

  private boolean hasNodeOrRackLocalRequests(Priority priority) {
    return getResourceRequests(priority).size() > 1;
  }

  /**
   * Whether the AM container for this app is over maxAMShare limit.
   */
  private boolean isOverAMShareLimit() {
    // Check the AM resource usage for the leaf queue
    if (!isAmRunning() && !getUnmanagedAM()) {
      List<ResourceRequest> ask = appSchedulingInfo.getAllResourceRequests();
      if (ask.isEmpty() || !getQueue().canRunAppAM(
          ask.get(0).getCapability())) {
        return true;
      }
    }
    return false;
  }

  private Resource assignContainer(FSSchedulerNode node, boolean reserved) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Node offered to app: " + getName() + " reserved: " + reserved);
    }

    Collection<Priority> prioritiesToTry = (reserved) ?
        Arrays.asList(node.getReservedContainer().getReservedPriority()) :
        getPriorities();

    // For each priority, see if we can schedule a node local, rack local
    // or off-switch request. Rack of off-switch requests may be delayed
    // (not scheduled) in order to promote better locality.
    synchronized (this) {
      for (Priority priority : prioritiesToTry) {
        // Skip it for reserved container, since
        // we already check it in isValidReservation.
        if (!reserved && !hasContainerForNode(priority, node)) {
          continue;
        }

        addSchedulingOpportunity(priority);

        ResourceRequest rackLocalRequest = getResourceRequest(priority,
            node.getRackName());
        ResourceRequest localRequest = getResourceRequest(priority,
            node.getNodeName());

        if (localRequest != null && !localRequest.getRelaxLocality()) {
          LOG.warn("Relax locality off is not supported on local request: "
              + localRequest);
        }

        NodeType allowedLocality;
        if (scheduler.isContinuousSchedulingEnabled()) {
          allowedLocality = getAllowedLocalityLevelByTime(priority,
              scheduler.getNodeLocalityDelayMs(),
              scheduler.getRackLocalityDelayMs(),
              scheduler.getClock().getTime());
        } else {
          allowedLocality = getAllowedLocalityLevel(priority,
              scheduler.getNumClusterNodes(),
              scheduler.getNodeLocalityThreshold(),
              scheduler.getRackLocalityThreshold());
        }

        if (rackLocalRequest != null && rackLocalRequest.getNumContainers() != 0
            && localRequest != null && localRequest.getNumContainers() != 0) {
          return assignContainer(node, localRequest,
              NodeType.NODE_LOCAL, reserved);
        }

        if (rackLocalRequest != null && !rackLocalRequest.getRelaxLocality()) {
          continue;
        }

        if (rackLocalRequest != null && rackLocalRequest.getNumContainers() != 0
            && (allowedLocality.equals(NodeType.RACK_LOCAL) ||
            allowedLocality.equals(NodeType.OFF_SWITCH))) {
          return assignContainer(node, rackLocalRequest,
              NodeType.RACK_LOCAL, reserved);
        }

        ResourceRequest offSwitchRequest =
            getResourceRequest(priority, ResourceRequest.ANY);
        if (offSwitchRequest != null && !offSwitchRequest.getRelaxLocality()) {
          continue;
        }

        if (offSwitchRequest != null &&
            offSwitchRequest.getNumContainers() != 0) {
          if (!hasNodeOrRackLocalRequests(priority) ||
              allowedLocality.equals(NodeType.OFF_SWITCH)) {
            return assignContainer(
                node, offSwitchRequest, NodeType.OFF_SWITCH, reserved);
          }
        }
      }
    }
    return Resources.none();
  }

  /**
   * Whether this app has containers requests that could be satisfied on the
   * given node, if the node had full space.
   */
  private boolean hasContainerForNode(Priority prio, FSSchedulerNode node) {
    ResourceRequest anyRequest = getResourceRequest(prio, ResourceRequest.ANY);
    ResourceRequest rackRequest = getResourceRequest(prio, node.getRackName());
    ResourceRequest nodeRequest = getResourceRequest(prio, node.getNodeName());

    return
        // There must be outstanding requests at the given priority:
        anyRequest != null && anyRequest.getNumContainers() > 0 &&
            // If locality relaxation is turned off at *-level, there must be a
            // non-zero request for the node's rack:
            (anyRequest.getRelaxLocality() ||
                (rackRequest != null && rackRequest.getNumContainers() > 0)) &&
            // If locality relaxation is turned off at rack-level, there must be a
            // non-zero request at the node:
            (rackRequest == null || rackRequest.getRelaxLocality() ||
                (nodeRequest != null && nodeRequest.getNumContainers() > 0)) &&
            // The requested container must be able to fit on the node:
            Resources.lessThanOrEqual(RESOURCE_CALCULATOR, null,
                anyRequest.getCapability(),
                node.getRMNode().getTotalCapability()) &&
            // The requested container must fit in queue maximum share:
            getQueue().fitsInMaxShare(anyRequest.getCapability());
  }

  private boolean isValidReservation(FSSchedulerNode node) {
    Priority reservedPriority = node.getReservedContainer().
        getReservedPriority();
    return hasContainerForNode(reservedPriority, node) &&
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
  public boolean assignReservedContainer(FSSchedulerNode node) {
    RMContainer rmContainer = node.getReservedContainer();
    Priority reservedPriority = rmContainer.getReservedPriority();

    if (!isValidReservation(node)) {
      // Don't hold the reservation if app can no longer use it
      LOG.info("Releasing reservation that cannot be satisfied for " +
          "application " + getApplicationAttemptId() + " on node " + node);
      unreserve(reservedPriority, node);
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

  static class RMContainerComparator implements Comparator<RMContainer>,
      Serializable {
    @Override
    public int compare(RMContainer c1, RMContainer c2) {
      int ret = c1.getContainer().getPriority().compareTo(
          c2.getContainer().getPriority());
      if (ret == 0) {
        return c2.getContainerId().compareTo(c1.getContainerId());
      }
      return ret;
    }
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
    // Here the getPreemptedResources() always return zero, except in
    // a preemption round
    // In the common case where preempted resource is zero, return the
    // current consumption Resource object directly without calling
    // Resources.subtract which creates a new Resource object for each call.
    return getPreemptedResources().equals(Resources.none()) ?
        getCurrentConsumption() :
        Resources.subtract(getCurrentConsumption(), getPreemptedResources());
  }

  @Override
  public ResourceWeights getWeights() {
    return scheduler.getAppWeight(this);
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
    demand = Resources.createResource(0);
    // Demand is current consumption plus outstanding requests
    Resources.addTo(demand, getCurrentConsumption());

    // Add up outstanding resource requests
    synchronized (this) {
      for (Priority p : getPriorities()) {
        ResourceRequest r = getResourceRequest(p, ResourceRequest.ANY);
        if (r != null) {
          Resources.multiplyAndAddTo(demand,
              r.getCapability(), r.getNumContainers());
        }
      }
    }
  }

  @Override
  public Resource assignContainer(FSSchedulerNode node) {
    if (isOverAMShareLimit()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping allocation because maxAMShare limit would " +
            "be exceeded");
      }
      return Resources.none();
    }
    return assignContainer(node, false);
  }

  /**
   * Preempt a running container according to the priority
   */
  @Override
  public RMContainer preemptContainer() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("App " + getName() + " is going to preempt a running " +
          "container");
    }

    RMContainer toBePreempted = null;
    for (RMContainer container : getLiveContainers()) {
      if (!getPreemptionContainers().contains(container) &&
          (toBePreempted == null ||
              comparator.compare(toBePreempted, container) > 0)) {
        toBePreempted = container;
      }
    }
    return toBePreempted;
  }
}

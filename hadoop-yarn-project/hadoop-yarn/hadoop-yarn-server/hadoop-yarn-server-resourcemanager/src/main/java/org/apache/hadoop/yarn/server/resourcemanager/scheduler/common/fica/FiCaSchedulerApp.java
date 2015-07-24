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

import org.apache.commons.lang.mutable.MutableObject;
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
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSAssignment;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityHeadroomProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
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

  static final CSAssignment NULL_ASSIGNMENT =
      new CSAssignment(Resources.createResource(0, 0), NodeType.NODE_LOCAL);

  static final CSAssignment SKIP_ASSIGNMENT = new CSAssignment(true);

  private final Set<ContainerId> containersToPreempt =
    new HashSet<ContainerId>();
    
  private CapacityHeadroomProvider headroomProvider;

  private ResourceCalculator rc = new DefaultResourceCalculator();

  private ResourceScheduler scheduler;

  public FiCaSchedulerApp(ApplicationAttemptId applicationAttemptId, 
      String user, Queue queue, ActiveUsersManager activeUsersManager,
      RMContext rmContext) {
    this(applicationAttemptId, user, queue, activeUsersManager, rmContext,
        Priority.newInstance(0));
  }

  public FiCaSchedulerApp(ApplicationAttemptId applicationAttemptId,
      String user, Queue queue, ActiveUsersManager activeUsersManager,
      RMContext rmContext, Priority appPriority) {
    super(applicationAttemptId, user, queue, activeUsersManager, rmContext);
    
    RMApp rmApp = rmContext.getRMApps().get(getApplicationId());
    
    Resource amResource;
    if (rmApp == null || rmApp.getAMResourceRequest() == null) {
      //the rmApp may be undefined (the resource manager checks for this too)
      //and unmanaged applications do not provide an amResource request
      //in these cases, provide a default using the scheduler
      amResource = rmContext.getScheduler().getMinimumResourceCapability();
    } else {
      amResource = rmApp.getAMResourceRequest().getCapability();
    }
    
    setAMResource(amResource);
    setPriority(appPriority);

    scheduler = rmContext.getScheduler();

    if (scheduler.getResourceCalculator() != null) {
      rc = scheduler.getResourceCalculator();
    }
  }

  synchronized public boolean containerCompleted(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event,
      String partition) {

    // Remove from the list of containers
    if (null == liveContainers.remove(rmContainer.getContainerId())) {
      return false;
    }
    
    // Remove from the list of newly allocated containers if found
    newlyAllocatedContainers.remove(rmContainer);

    Container container = rmContainer.getContainer();
    ContainerId containerId = container.getId();

    // Inform the container
    rmContainer.handle(
        new RMContainerFinishedEvent(
            containerId,
            containerStatus, 
            event)
        );
    LOG.info("Completed container: " + rmContainer.getContainerId() + 
        " in state: " + rmContainer.getState() + " event:" + event);

    containersToPreempt.remove(rmContainer.getContainerId());

    RMAuditLogger.logSuccess(getUser(), 
        AuditConstants.RELEASE_CONTAINER, "SchedulerApp", 
        getApplicationId(), containerId);
    
    // Update usage metrics 
    Resource containerResource = rmContainer.getContainer().getResource();
    queue.getMetrics().releaseResources(getUser(), 1, containerResource);
    attemptResourceUsage.decUsed(partition, containerResource);

    // Clear resource utilization metrics cache.
    lastMemoryAggregateAllocationUpdateTime = -1;

    return true;
  }

  synchronized public RMContainer allocate(NodeType type, FiCaSchedulerNode node,
      Priority priority, ResourceRequest request, 
      Container container) {

    if (isStopped) {
      return null;
    }
    
    // Required sanity check - AM can call 'allocate' to update resource 
    // request without locking the scheduler, hence we need to check
    if (getTotalRequiredResources(priority) <= 0) {
      return null;
    }
    
    // Create RMContainer
    RMContainer rmContainer =
        new RMContainerImpl(container, this.getApplicationAttemptId(),
            node.getNodeID(), appSchedulingInfo.getUser(), this.rmContext,
            request.getNodeLabelExpression());

    // Add it to allContainers list.
    newlyAllocatedContainers.add(rmContainer);
    liveContainers.put(container.getId(), rmContainer);    

    // Update consumption and track allocations
    List<ResourceRequest> resourceRequestList = appSchedulingInfo.allocate(
        type, node, priority, request, container);
    attemptResourceUsage.incUsed(node.getPartition(),
        container.getResource());
    
    // Update resource requests related to "request" and store in RMContainer 
    ((RMContainerImpl)rmContainer).setResourceRequests(resourceRequestList);

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
        getApplicationId(), container.getId());
    
    return rmContainer;
  }

  public boolean unreserve(Priority priority,
      FiCaSchedulerNode node, RMContainer rmContainer) {
    // Done with the reservation?
    if (unreserve(node, priority)) {
      node.unreserveResource(this);

      // Update reserved metrics
      queue.getMetrics().unreserveResource(getUser(),
          rmContainer.getContainer().getResource());
      return true;
    }
    return false;
  }

  @VisibleForTesting
  public synchronized boolean unreserve(FiCaSchedulerNode node, Priority priority) {
    Map<NodeId, RMContainer> reservedContainers =
      this.reservedContainers.get(priority);

    if (reservedContainers != null) {
      RMContainer reservedContainer = reservedContainers.remove(node.getNodeID());

      // unreserve is now triggered in new scenarios (preemption)
      // as a consequence reservedcontainer might be null, adding NP-checks
      if (reservedContainer != null
          && reservedContainer.getContainer() != null
          && reservedContainer.getContainer().getResource() != null) {

        if (reservedContainers.isEmpty()) {
          this.reservedContainers.remove(priority);
        }
        // Reset the re-reservation count
        resetReReservations(priority);

        Resource resource = reservedContainer.getContainer().getResource();
        this.attemptResourceUsage.decReserved(node.getPartition(), resource);

        LOG.info("Application " + getApplicationId() + " unreserved "
            + " on node " + node + ", currently has "
            + reservedContainers.size() + " at priority " + priority
            + "; currentReservation " + this.attemptResourceUsage.getReserved()
            + " on node-label=" + node.getPartition());
        return true;
      }
    }
    return false;
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

  public synchronized Resource getTotalPendingRequests() {
    Resource ret = Resource.newInstance(0, 0);
    for (ResourceRequest rr : appSchedulingInfo.getAllResourceRequests()) {
      // to avoid double counting we count only "ANY" resource requests
      if (ResourceRequest.isAnyLocation(rr.getResourceName())){
        Resources.addTo(ret,
            Resources.multiply(rr.getCapability(), rr.getNumContainers()));
      }
    }
    return ret;
  }

  public synchronized void addPreemptContainer(ContainerId cont){
    // ignore already completed containers
    if (liveContainers.containsKey(cont)) {
      containersToPreempt.add(cont);
    }
  }

  /**
   * This method produces an Allocation that includes the current view
   * of the resources that will be allocated to and preempted from this
   * application.
   *
   * @param rc
   * @param clusterResource
   * @param minimumAllocation
   * @return an allocation
   */
  public synchronized Allocation getAllocation(ResourceCalculator rc,
      Resource clusterResource, Resource minimumAllocation) {

    Set<ContainerId> currentContPreemption = Collections.unmodifiableSet(
        new HashSet<ContainerId>(containersToPreempt));
    containersToPreempt.clear();
    Resource tot = Resource.newInstance(0, 0);
    for(ContainerId c : currentContPreemption){
      Resources.addTo(tot,
          liveContainers.get(c).getContainer().getResource());
    }
    int numCont = (int) Math.ceil(
        Resources.divide(rc, clusterResource, tot, minimumAllocation));
    ResourceRequest rr = ResourceRequest.newInstance(
        Priority.UNDEFINED, ResourceRequest.ANY,
        minimumAllocation, numCont);
    ContainersAndNMTokensAllocation allocation =
        pullNewlyAllocatedContainersAndNMTokens();
    Resource headroom = getHeadroom();
    setApplicationHeadroomForMetrics(headroom);
    return new Allocation(allocation.getContainerList(), headroom, null,
      currentContPreemption, Collections.singletonList(rr),
      allocation.getNMTokenList());
  }
  
  synchronized public NodeId getNodeIdToUnreserve(Priority priority,
      Resource resourceNeedUnreserve, ResourceCalculator rc,
      Resource clusterResource) {

    // first go around make this algorithm simple and just grab first
    // reservation that has enough resources
    Map<NodeId, RMContainer> reservedContainers = this.reservedContainers
        .get(priority);

    if ((reservedContainers != null) && (!reservedContainers.isEmpty())) {
      for (Map.Entry<NodeId, RMContainer> entry : reservedContainers.entrySet()) {
        NodeId nodeId = entry.getKey();
        Resource containerResource = entry.getValue().getContainer().getResource();
        
        // make sure we unreserve one with at least the same amount of
        // resources, otherwise could affect capacity limits
        if (Resources.lessThanOrEqual(rc, clusterResource,
            resourceNeedUnreserve, containerResource)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("unreserving node with reservation size: "
                + containerResource
                + " in order to allocate container with size: " + resourceNeedUnreserve);
          }
          return nodeId;
        }
      }
    }
    return null;
  }
  
  public synchronized void setHeadroomProvider(
    CapacityHeadroomProvider headroomProvider) {
    this.headroomProvider = headroomProvider;
  }

  public synchronized CapacityHeadroomProvider getHeadroomProvider() {
    return headroomProvider;
  }
  
  @Override
  public synchronized Resource getHeadroom() {
    if (headroomProvider != null) {
      return headroomProvider.getHeadroom();
    }
    return super.getHeadroom();
  }
  
  @Override
  public synchronized void transferStateFromPreviousAttempt(
      SchedulerApplicationAttempt appAttempt) {
    super.transferStateFromPreviousAttempt(appAttempt);
    this.headroomProvider = 
      ((FiCaSchedulerApp) appAttempt).getHeadroomProvider();
  }

  private int getActualNodeLocalityDelay() {
    return Math.min(scheduler.getNumClusterNodes(), getCSLeafQueue()
        .getNodeLocalityDelay());
  }

  private boolean canAssign(Priority priority, FiCaSchedulerNode node,
      NodeType type, RMContainer reservedContainer) {

    // Clearly we need containers for this application...
    if (type == NodeType.OFF_SWITCH) {
      if (reservedContainer != null) {
        return true;
      }

      // 'Delay' off-switch
      ResourceRequest offSwitchRequest =
          getResourceRequest(priority, ResourceRequest.ANY);
      long missedOpportunities = getSchedulingOpportunities(priority);
      long requiredContainers = offSwitchRequest.getNumContainers();

      float localityWaitFactor =
          getLocalityWaitFactor(priority, scheduler.getNumClusterNodes());

      return ((requiredContainers * localityWaitFactor) < missedOpportunities);
    }

    // Check if we need containers on this rack
    ResourceRequest rackLocalRequest =
        getResourceRequest(priority, node.getRackName());
    if (rackLocalRequest == null || rackLocalRequest.getNumContainers() <= 0) {
      return false;
    }

    // If we are here, we do need containers on this rack for RACK_LOCAL req
    if (type == NodeType.RACK_LOCAL) {
      // 'Delay' rack-local just a little bit...
      long missedOpportunities = getSchedulingOpportunities(priority);
      return getActualNodeLocalityDelay() < missedOpportunities;
    }

    // Check if we need containers on this host
    if (type == NodeType.NODE_LOCAL) {
      // Now check if we need containers on this host...
      ResourceRequest nodeLocalRequest =
          getResourceRequest(priority, node.getNodeName());
      if (nodeLocalRequest != null) {
        return nodeLocalRequest.getNumContainers() > 0;
      }
    }

    return false;
  }

  boolean
      shouldAllocOrReserveNewContainer(Priority priority, Resource required) {
    int requiredContainers = getTotalRequiredResources(priority);
    int reservedContainers = getNumReservedContainers(priority);
    int starvation = 0;
    if (reservedContainers > 0) {
      float nodeFactor =
          Resources.ratio(
              rc, required, getCSLeafQueue().getMaximumAllocation()
              );

      // Use percentage of node required to bias against large containers...
      // Protect against corner case where you need the whole node with
      // Math.min(nodeFactor, minimumAllocationFactor)
      starvation =
          (int)((getReReservations(priority) / (float)reservedContainers) *
                (1.0f - (Math.min(nodeFactor, getCSLeafQueue().getMinimumAllocationFactor())))
               );

      if (LOG.isDebugEnabled()) {
        LOG.debug("needsContainers:" +
            " app.#re-reserve=" + getReReservations(priority) +
            " reserved=" + reservedContainers +
            " nodeFactor=" + nodeFactor +
            " minAllocFactor=" + getCSLeafQueue().getMinimumAllocationFactor() +
            " starvation=" + starvation);
      }
    }
    return (((starvation + requiredContainers) - reservedContainers) > 0);
  }

  private CSAssignment assignNodeLocalContainers(Resource clusterResource,
      ResourceRequest nodeLocalResourceRequest, FiCaSchedulerNode node,
      Priority priority,
      RMContainer reservedContainer, MutableObject allocatedContainer,
      SchedulingMode schedulingMode, ResourceLimits currentResoureLimits) {
    if (canAssign(priority, node, NodeType.NODE_LOCAL,
        reservedContainer)) {
      return assignContainer(clusterResource, node, priority,
          nodeLocalResourceRequest, NodeType.NODE_LOCAL, reservedContainer,
          allocatedContainer, schedulingMode, currentResoureLimits);
    }

    return new CSAssignment(Resources.none(), NodeType.NODE_LOCAL);
  }

  private CSAssignment assignRackLocalContainers(Resource clusterResource,
      ResourceRequest rackLocalResourceRequest, FiCaSchedulerNode node,
      Priority priority,
      RMContainer reservedContainer, MutableObject allocatedContainer,
      SchedulingMode schedulingMode, ResourceLimits currentResoureLimits) {
    if (canAssign(priority, node, NodeType.RACK_LOCAL,
        reservedContainer)) {
      return assignContainer(clusterResource, node, priority,
          rackLocalResourceRequest, NodeType.RACK_LOCAL, reservedContainer,
          allocatedContainer, schedulingMode, currentResoureLimits);
    }

    return new CSAssignment(Resources.none(), NodeType.RACK_LOCAL);
  }

  private CSAssignment assignOffSwitchContainers(Resource clusterResource,
      ResourceRequest offSwitchResourceRequest, FiCaSchedulerNode node,
      Priority priority,
      RMContainer reservedContainer, MutableObject allocatedContainer,
      SchedulingMode schedulingMode, ResourceLimits currentResoureLimits) {
    if (canAssign(priority, node, NodeType.OFF_SWITCH,
        reservedContainer)) {
      return assignContainer(clusterResource, node, priority,
          offSwitchResourceRequest, NodeType.OFF_SWITCH, reservedContainer,
          allocatedContainer, schedulingMode, currentResoureLimits);
    }

    return new CSAssignment(Resources.none(), NodeType.OFF_SWITCH);
  }

  private CSAssignment assignContainersOnNode(Resource clusterResource,
      FiCaSchedulerNode node, Priority priority,
      RMContainer reservedContainer, SchedulingMode schedulingMode,
      ResourceLimits currentResoureLimits) {

    CSAssignment assigned;

    NodeType requestType = null;
    MutableObject allocatedContainer = new MutableObject();
    // Data-local
    ResourceRequest nodeLocalResourceRequest =
        getResourceRequest(priority, node.getNodeName());
    if (nodeLocalResourceRequest != null) {
      requestType = NodeType.NODE_LOCAL;
      assigned =
          assignNodeLocalContainers(clusterResource, nodeLocalResourceRequest,
            node, priority, reservedContainer,
            allocatedContainer, schedulingMode, currentResoureLimits);
      if (Resources.greaterThan(rc, clusterResource,
        assigned.getResource(), Resources.none())) {

        //update locality statistics
        if (allocatedContainer.getValue() != null) {
          incNumAllocatedContainers(NodeType.NODE_LOCAL,
            requestType);
        }
        assigned.setType(NodeType.NODE_LOCAL);
        return assigned;
      }
    }

    // Rack-local
    ResourceRequest rackLocalResourceRequest =
        getResourceRequest(priority, node.getRackName());
    if (rackLocalResourceRequest != null) {
      if (!rackLocalResourceRequest.getRelaxLocality()) {
        return SKIP_ASSIGNMENT;
      }

      if (requestType != NodeType.NODE_LOCAL) {
        requestType = NodeType.RACK_LOCAL;
      }

      assigned =
          assignRackLocalContainers(clusterResource, rackLocalResourceRequest,
            node, priority, reservedContainer,
            allocatedContainer, schedulingMode, currentResoureLimits);
      if (Resources.greaterThan(rc, clusterResource,
        assigned.getResource(), Resources.none())) {

        //update locality statistics
        if (allocatedContainer.getValue() != null) {
          incNumAllocatedContainers(NodeType.RACK_LOCAL,
            requestType);
        }
        assigned.setType(NodeType.RACK_LOCAL);
        return assigned;
      }
    }

    // Off-switch
    ResourceRequest offSwitchResourceRequest =
        getResourceRequest(priority, ResourceRequest.ANY);
    if (offSwitchResourceRequest != null) {
      if (!offSwitchResourceRequest.getRelaxLocality()) {
        return SKIP_ASSIGNMENT;
      }
      if (requestType != NodeType.NODE_LOCAL
          && requestType != NodeType.RACK_LOCAL) {
        requestType = NodeType.OFF_SWITCH;
      }

      assigned =
          assignOffSwitchContainers(clusterResource, offSwitchResourceRequest,
            node, priority, reservedContainer,
            allocatedContainer, schedulingMode, currentResoureLimits);

      // update locality statistics
      if (allocatedContainer.getValue() != null) {
        incNumAllocatedContainers(NodeType.OFF_SWITCH, requestType);
      }
      assigned.setType(NodeType.OFF_SWITCH);
      return assigned;
    }

    return SKIP_ASSIGNMENT;
  }

  public void reserve(Priority priority,
      FiCaSchedulerNode node, RMContainer rmContainer, Container container) {
    // Update reserved metrics if this is the first reservation
    if (rmContainer == null) {
      queue.getMetrics().reserveResource(
          getUser(), container.getResource());
    }

    // Inform the application
    rmContainer = super.reserve(node, priority, rmContainer, container);

    // Update the node
    node.reserveResource(this, priority, rmContainer);
  }

  private Container getContainer(RMContainer rmContainer,
      FiCaSchedulerNode node, Resource capability, Priority priority) {
    return (rmContainer != null) ? rmContainer.getContainer()
        : createContainer(node, capability, priority);
  }

  Container createContainer(FiCaSchedulerNode node, Resource capability,
      Priority priority) {

    NodeId nodeId = node.getRMNode().getNodeID();
    ContainerId containerId =
        BuilderUtils.newContainerId(getApplicationAttemptId(),
            getNewContainerId());

    // Create the container
    return BuilderUtils.newContainer(containerId, nodeId, node.getRMNode()
        .getHttpAddress(), capability, priority, null);
  }

  @VisibleForTesting
  public RMContainer findNodeToUnreserve(Resource clusterResource,
      FiCaSchedulerNode node, Priority priority,
      Resource minimumUnreservedResource) {
    // need to unreserve some other container first
    NodeId idToUnreserve =
        getNodeIdToUnreserve(priority, minimumUnreservedResource,
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

  private LeafQueue getCSLeafQueue() {
    return (LeafQueue)queue;
  }

  private CSAssignment assignContainer(Resource clusterResource, FiCaSchedulerNode node,
      Priority priority,
      ResourceRequest request, NodeType type, RMContainer rmContainer,
      MutableObject createdContainer, SchedulingMode schedulingMode,
      ResourceLimits currentResoureLimits) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("assignContainers: node=" + node.getNodeName()
        + " application=" + getApplicationId()
        + " priority=" + priority.getPriority()
        + " request=" + request + " type=" + type);
    }

    // check if the resource request can access the label
    if (!SchedulerUtils.checkResourceRequestMatchingNodePartition(request,
        node.getPartition(), schedulingMode)) {
      // this is a reserved container, but we cannot allocate it now according
      // to label not match. This can be caused by node label changed
      // We should un-reserve this container.
      if (rmContainer != null) {
        unreserve(priority, node, rmContainer);
      }
      return new CSAssignment(Resources.none(), type);
    }

    Resource capability = request.getCapability();
    Resource available = node.getAvailableResource();
    Resource totalResource = node.getTotalResource();

    if (!Resources.lessThanOrEqual(rc, clusterResource,
        capability, totalResource)) {
      LOG.warn("Node : " + node.getNodeID()
          + " does not have sufficient resource for request : " + request
          + " node total capability : " + node.getTotalResource());
      return new CSAssignment(Resources.none(), type);
    }

    assert Resources.greaterThan(
        rc, clusterResource, available, Resources.none());

    // Create the container if necessary
    Container container =
        getContainer(rmContainer, node, capability, priority);

    // something went wrong getting/creating the container
    if (container == null) {
      LOG.warn("Couldn't get container for allocation!");
      return new CSAssignment(Resources.none(), type);
    }

    boolean shouldAllocOrReserveNewContainer = shouldAllocOrReserveNewContainer(
        priority, capability);

    // Can we allocate a container on this node?
    int availableContainers =
        rc.computeAvailableContainers(available, capability);

    // How much need to unreserve equals to:
    // max(required - headroom, amountNeedUnreserve)
    Resource resourceNeedToUnReserve =
        Resources.max(rc, clusterResource,
            Resources.subtract(capability, currentResoureLimits.getHeadroom()),
            currentResoureLimits.getAmountNeededUnreserve());

    boolean needToUnreserve =
        Resources.greaterThan(rc, clusterResource,
            resourceNeedToUnReserve, Resources.none());

    RMContainer unreservedContainer = null;
    boolean reservationsContinueLooking =
        getCSLeafQueue().getReservationContinueLooking();

    if (availableContainers > 0) {
      // Allocate...

      // Did we previously reserve containers at this 'priority'?
      if (rmContainer != null) {
        unreserve(priority, node, rmContainer);
      } else if (reservationsContinueLooking && node.getLabels().isEmpty()) {
        // when reservationsContinueLooking is set, we may need to unreserve
        // some containers to meet this queue, its parents', or the users' resource limits.
        // TODO, need change here when we want to support continuous reservation
        // looking for labeled partitions.
        if (!shouldAllocOrReserveNewContainer || needToUnreserve) {
          if (!needToUnreserve) {
            // If we shouldn't allocate/reserve new container then we should
            // unreserve one the same size we are asking for since the
            // currentResoureLimits.getAmountNeededUnreserve could be zero. If
            // the limit was hit then use the amount we need to unreserve to be
            // under the limit.
            resourceNeedToUnReserve = capability;
          }
          unreservedContainer =
              findNodeToUnreserve(clusterResource, node, priority,
                  resourceNeedToUnReserve);
          // When (minimum-unreserved-resource > 0 OR we cannot allocate new/reserved
          // container (That means we *have to* unreserve some resource to
          // continue)). If we failed to unreserve some resource, we can't continue.
          if (null == unreservedContainer) {
            return new CSAssignment(Resources.none(), type);
          }
        }
      }

      // Inform the application
      RMContainer allocatedContainer =
          allocate(type, node, priority, request, container);

      // Does the application need this resource?
      if (allocatedContainer == null) {
        CSAssignment csAssignment =  new CSAssignment(Resources.none(), type);
        csAssignment.setApplication(this);
        csAssignment.setExcessReservation(unreservedContainer);
        return csAssignment;
      }

      // Inform the node
      node.allocateContainer(allocatedContainer);

      // Inform the ordering policy
      getCSLeafQueue().getOrderingPolicy().containerAllocated(this,
          allocatedContainer);

      LOG.info("assignedContainer" +
          " application attempt=" + getApplicationAttemptId() +
          " container=" + container +
          " queue=" + this +
          " clusterResource=" + clusterResource);
      createdContainer.setValue(allocatedContainer);
      CSAssignment assignment = new CSAssignment(container.getResource(), type);
      assignment.getAssignmentInformation().addAllocationDetails(
        container.getId(), getCSLeafQueue().getQueuePath());
      assignment.getAssignmentInformation().incrAllocations();
      assignment.setApplication(this);
      Resources.addTo(assignment.getAssignmentInformation().getAllocated(),
        container.getResource());

      assignment.setExcessReservation(unreservedContainer);
      return assignment;
    } else {
      // if we are allowed to allocate but this node doesn't have space, reserve it or
      // if this was an already a reserved container, reserve it again
      if (shouldAllocOrReserveNewContainer || rmContainer != null) {

        if (reservationsContinueLooking && rmContainer == null) {
          // we could possibly ignoring queue capacity or user limits when
          // reservationsContinueLooking is set. Make sure we didn't need to unreserve
          // one.
          if (needToUnreserve) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("we needed to unreserve to be able to allocate");
            }
            return new CSAssignment(Resources.none(), type);
          }
        }

        // Reserve by 'charging' in advance...
        reserve(priority, node, rmContainer, container);

        LOG.info("Reserved container " +
            " application=" + getApplicationId() +
            " resource=" + request.getCapability() +
            " queue=" + this.toString() +
            " cluster=" + clusterResource);
        CSAssignment assignment =
            new CSAssignment(request.getCapability(), type);
        assignment.getAssignmentInformation().addReservationDetails(
          container.getId(), getCSLeafQueue().getQueuePath());
        assignment.getAssignmentInformation().incrReservations();
        Resources.addTo(assignment.getAssignmentInformation().getReserved(),
          request.getCapability());
        return assignment;
      }
      return new CSAssignment(Resources.none(), type);
    }
  }

  private boolean checkHeadroom(Resource clusterResource,
      ResourceLimits currentResourceLimits, Resource required, FiCaSchedulerNode node) {
    // If headroom + currentReservation < required, we cannot allocate this
    // require
    Resource resourceCouldBeUnReserved = getCurrentReservation();
    if (!getCSLeafQueue().getReservationContinueLooking() || !node.getPartition().equals(RMNodeLabelsManager.NO_LABEL)) {
      // If we don't allow reservation continuous looking, OR we're looking at
      // non-default node partition, we won't allow to unreserve before
      // allocation.
      resourceCouldBeUnReserved = Resources.none();
    }
    return Resources
        .greaterThanOrEqual(rc, clusterResource, Resources.add(
            currentResourceLimits.getHeadroom(), resourceCouldBeUnReserved),
            required);
  }

  public CSAssignment assignContainers(Resource clusterResource,
      FiCaSchedulerNode node, ResourceLimits currentResourceLimits,
      SchedulingMode schedulingMode) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("pre-assignContainers for application "
          + getApplicationId());
      showRequests();
    }

    // Check if application needs more resource, skip if it doesn't need more.
    if (!hasPendingResourceRequest(rc,
        node.getPartition(), clusterResource, schedulingMode)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skip app_attempt=" + getApplicationAttemptId()
            + ", because it doesn't need more resource, schedulingMode="
            + schedulingMode.name() + " node-label=" + node.getPartition());
      }
      return SKIP_ASSIGNMENT;
    }

    synchronized (this) {
      // Check if this resource is on the blacklist
      if (SchedulerAppUtils.isBlacklisted(this, node, LOG)) {
        return SKIP_ASSIGNMENT;
      }

      // Schedule in priority order
      for (Priority priority : getPriorities()) {
        ResourceRequest anyRequest =
            getResourceRequest(priority, ResourceRequest.ANY);
        if (null == anyRequest) {
          continue;
        }

        // Required resource
        Resource required = anyRequest.getCapability();

        // Do we need containers at this 'priority'?
        if (getTotalRequiredResources(priority) <= 0) {
          continue;
        }

        // AM container allocation doesn't support non-exclusive allocation to
        // avoid painful of preempt an AM container
        if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {

          RMAppAttempt rmAppAttempt =
              rmContext.getRMApps()
                  .get(getApplicationId()).getCurrentAppAttempt();
          if (rmAppAttempt.getSubmissionContext().getUnmanagedAM() == false
              && null == rmAppAttempt.getMasterContainer()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Skip allocating AM container to app_attempt="
                  + getApplicationAttemptId()
                  + ", don't allow to allocate AM container in non-exclusive mode");
            }
            break;
          }
        }

        // Is the node-label-expression of this offswitch resource request
        // matches the node's label?
        // If not match, jump to next priority.
        if (!SchedulerUtils.checkResourceRequestMatchingNodePartition(
            anyRequest, node.getPartition(), schedulingMode)) {
          continue;
        }

        if (!getCSLeafQueue().getReservationContinueLooking()) {
          if (!shouldAllocOrReserveNewContainer(priority, required)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("doesn't need containers based on reservation algo!");
            }
            continue;
          }
        }

        if (!checkHeadroom(clusterResource, currentResourceLimits, required,
            node)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("cannot allocate required resource=" + required
                + " because of headroom");
          }
          return NULL_ASSIGNMENT;
        }

        // Inform the application it is about to get a scheduling opportunity
        addSchedulingOpportunity(priority);

        // Increase missed-non-partitioned-resource-request-opportunity.
        // This is to make sure non-partitioned-resource-request will prefer
        // to be allocated to non-partitioned nodes
        int missedNonPartitionedRequestSchedulingOpportunity = 0;
        if (anyRequest.getNodeLabelExpression().equals(
            RMNodeLabelsManager.NO_LABEL)) {
          missedNonPartitionedRequestSchedulingOpportunity =
              addMissedNonPartitionedRequestSchedulingOpportunity(priority);
        }

        if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
          // Before doing allocation, we need to check scheduling opportunity to
          // make sure : non-partitioned resource request should be scheduled to
          // non-partitioned partition first.
          if (missedNonPartitionedRequestSchedulingOpportunity < rmContext
              .getScheduler().getNumClusterNodes()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Skip app_attempt="
                  + getApplicationAttemptId() + " priority="
                  + priority
                  + " because missed-non-partitioned-resource-request"
                  + " opportunity under requred:" + " Now="
                  + missedNonPartitionedRequestSchedulingOpportunity
                  + " required="
                  + rmContext.getScheduler().getNumClusterNodes());
            }

            return SKIP_ASSIGNMENT;
          }
        }

        // Try to schedule
        CSAssignment assignment =
            assignContainersOnNode(clusterResource, node,
                priority, null, schedulingMode, currentResourceLimits);

        // Did the application skip this node?
        if (assignment.getSkipped()) {
          // Don't count 'skipped nodes' as a scheduling opportunity!
          subtractSchedulingOpportunity(priority);
          continue;
        }

        // Did we schedule or reserve a container?
        Resource assigned = assignment.getResource();
        if (Resources.greaterThan(rc, clusterResource,
            assigned, Resources.none())) {
          // Don't reset scheduling opportunities for offswitch assignments
          // otherwise the app will be delayed for each non-local assignment.
          // This helps apps with many off-cluster requests schedule faster.
          if (assignment.getType() != NodeType.OFF_SWITCH) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Resetting scheduling opportunities");
            }
            resetSchedulingOpportunities(priority);
          }
          // Non-exclusive scheduling opportunity is different: we need reset
          // it every time to make sure non-labeled resource request will be
          // most likely allocated on non-labeled nodes first.
          resetMissedNonPartitionedRequestSchedulingOpportunity(priority);

          // Done
          return assignment;
        } else {
          // Do not assign out of order w.r.t priorities
          return SKIP_ASSIGNMENT;
        }
      }
    }

    return SKIP_ASSIGNMENT;
  }


  public synchronized CSAssignment assignReservedContainer(
      FiCaSchedulerNode node, RMContainer rmContainer,
      Resource clusterResource, SchedulingMode schedulingMode) {
    // Do we still need this reservation?
    Priority priority = rmContainer.getReservedPriority();
    if (getTotalRequiredResources(priority) == 0) {
      // Release
      return new CSAssignment(this, rmContainer);
    }

    // Try to assign if we have sufficient resources
    CSAssignment tmp =
        assignContainersOnNode(clusterResource, node, priority,
          rmContainer, schedulingMode, new ResourceLimits(Resources.none()));

    // Doesn't matter... since it's already charged for at time of reservation
    // "re-reservation" is *free*
    CSAssignment ret = new CSAssignment(Resources.none(), NodeType.NODE_LOCAL);
    if (tmp.getAssignmentInformation().getNumAllocations() > 0) {
      ret.setFulfilledReservation(true);
    }
    return ret;
  }

}

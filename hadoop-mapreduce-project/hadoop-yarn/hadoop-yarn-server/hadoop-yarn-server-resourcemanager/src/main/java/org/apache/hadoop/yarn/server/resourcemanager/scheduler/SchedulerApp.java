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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore.ApplicationStore;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerReservedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

public class SchedulerApp {

  private static final Log LOG = LogFactory.getLog(SchedulerApp.class);

  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private final AppSchedulingInfo appSchedulingInfo;
  private final Queue queue;

  private final Resource currentConsumption = recordFactory
      .newRecordInstance(Resource.class);
  private Resource resourceLimit = recordFactory
      .newRecordInstance(Resource.class);

  private Map<ContainerId, RMContainer> liveContainers
  = new HashMap<ContainerId, RMContainer>();
  private List<RMContainer> newlyAllocatedContainers = 
      new ArrayList<RMContainer>();

  final Map<Priority, Map<NodeId, RMContainer>> reservedContainers = 
      new HashMap<Priority, Map<NodeId, RMContainer>>();
  
  Map<Priority, Integer> schedulingOpportunities = 
      new HashMap<Priority, Integer>();
  
  Map<Priority, Integer> reReservations =
      new HashMap<Priority, Integer>();

  Resource currentReservation = recordFactory
      .newRecordInstance(Resource.class);

  private final RMContext rmContext;
  public SchedulerApp(ApplicationAttemptId applicationAttemptId, 
      String user, Queue queue, 
      RMContext rmContext, ApplicationStore store) {
    this.rmContext = rmContext;
    this.appSchedulingInfo = 
        new AppSchedulingInfo(applicationAttemptId, user, queue, store);
    this.queue = queue;
  }

  public ApplicationId getApplicationId() {
    return this.appSchedulingInfo.getApplicationId();
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    return this.appSchedulingInfo.getApplicationAttemptId();
  }

  public String getUser() {
    return this.appSchedulingInfo.getUser();
  }

  public synchronized void updateResourceRequests(
      List<ResourceRequest> requests) {
    this.appSchedulingInfo.updateResourceRequests(requests);
  }

  public Map<String, ResourceRequest> getResourceRequests(Priority priority) {
    return this.appSchedulingInfo.getResourceRequests(priority);
  }

  public int getNewContainerId() {
    return this.appSchedulingInfo.getNewContainerId();
  }
  
  public Collection<Priority> getPriorities() {
    return this.appSchedulingInfo.getPriorities();
  }

  public ResourceRequest getResourceRequest(Priority priority, String nodeAddress) {
    return this.appSchedulingInfo.getResourceRequest(priority, nodeAddress);
  }

  public synchronized int getTotalRequiredResources(Priority priority) {
    return getResourceRequest(priority, RMNode.ANY).getNumContainers();
  }
  
  public Resource getResource(Priority priority) {
    return this.appSchedulingInfo.getResource(priority);
  }

  public boolean isPending() {
    return this.appSchedulingInfo.isPending();
  }

  public String getQueueName() {
    return this.appSchedulingInfo.getQueueName();
  }

  public synchronized Collection<RMContainer> getLiveContainers() {
    return new ArrayList<RMContainer>(liveContainers.values());
  }

  public synchronized void stop(RMAppAttemptState rmAppAttemptFinalState) {
    // Cleanup all scheduling information
    this.appSchedulingInfo.stop(rmAppAttemptFinalState);
  }

  synchronized public void containerLaunchedOnNode(ContainerId containerId) {
    // Inform the container
    RMContainer rmContainer = 
        getRMContainer(containerId);
    rmContainer.handle(
        new RMContainerEvent(containerId, 
            RMContainerEventType.LAUNCHED));
  }

  synchronized public void containerCompleted(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event) {
    
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
    
    // Remove from the list of containers
    liveContainers.remove(rmContainer.getContainerId());

    RMAuditLogger.logSuccess(getUser(), 
        AuditConstants.RELEASE_CONTAINER, "SchedulerApp", 
        getApplicationId(), containerId);
    
    // Update usage metrics 
    Resource containerResource = rmContainer.getContainer().getResource();
    queue.getMetrics().releaseResources(getUser(), 1, containerResource);
    Resources.subtractFrom(currentConsumption, containerResource);
  }

  synchronized public RMContainer allocate(NodeType type, SchedulerNode node,
      Priority priority, ResourceRequest request, 
      Container container) {
    
    // Required sanity check - AM can call 'allocate' to update resource 
    // request without locking the scheduler, hence we need to check
    if (getTotalRequiredResources(priority) <= 0) {
      return null;
    }
    
    // Create RMContainer
    RMContainer rmContainer = new RMContainerImpl(container, this
        .getApplicationAttemptId(), node.getNodeID(), this.rmContext
        .getDispatcher().getEventHandler(), this.rmContext
        .getContainerAllocationExpirer());

    // Update consumption and track allocations
    
    // Inform the container
    rmContainer.handle(
        new RMContainerEvent(container.getId(), RMContainerEventType.START));

    Resources.addTo(currentConsumption, container.getResource());
    if (LOG.isDebugEnabled()) {
      LOG.debug("allocate: applicationId=" + container.getId().getAppId()
          + " container=" + container.getId() + " host="
          + container.getNodeId().getHost() + " type=" + type);
    }
    RMAuditLogger.logSuccess(getUser(), 
        AuditConstants.ALLOC_CONTAINER, "SchedulerApp", 
        getApplicationId(), container.getId());

    // Add it to allContainers list.
    newlyAllocatedContainers.add(rmContainer);
    liveContainers.put(container.getId(), rmContainer);
    
    appSchedulingInfo.allocate(type, node, priority, request, container);
    
    return rmContainer;
  }
  
  synchronized public List<Container> pullNewlyAllocatedContainers() {
    List<Container> returnContainerList = new ArrayList<Container>(
        newlyAllocatedContainers.size());
    for (RMContainer rmContainer : newlyAllocatedContainers) {
      rmContainer.handle(new RMContainerEvent(rmContainer.getContainerId(),
          RMContainerEventType.ACQUIRED));
      returnContainerList.add(rmContainer.getContainer());
    }
    newlyAllocatedContainers.clear();
    return returnContainerList;
  }

  public Resource getCurrentConsumption() {
    return this.currentConsumption;
  }

  synchronized public void showRequests() {
    if (LOG.isDebugEnabled()) {
      for (Priority priority : getPriorities()) {
        Map<String, ResourceRequest> requests = getResourceRequests(priority);
        if (requests != null) {
          LOG.debug("showRequests:" + " application=" + getApplicationId() + 
              " headRoom=" + getHeadroom() + 
              " currentConsumption=" + currentConsumption.getMemory());
          for (ResourceRequest request : requests.values()) {
            LOG.debug("showRequests:" + " application=" + getApplicationId()
                + " request=" + request);
          }
        }
      }
    }
  }

  public synchronized void setAvailableResourceLimit(Resource globalLimit) {
    this.resourceLimit = globalLimit; 
  }

  public synchronized RMContainer getRMContainer(ContainerId id) {
    return liveContainers.get(id);
  }

  synchronized public void resetSchedulingOpportunities(Priority priority) {
    this.schedulingOpportunities.put(priority, Integer.valueOf(0));
  }

  synchronized public void addSchedulingOpportunity(Priority priority) {
    Integer schedulingOpportunities = 
        this.schedulingOpportunities.get(priority);
    if (schedulingOpportunities == null) {
      schedulingOpportunities = 0;
    }
    ++schedulingOpportunities;
    this.schedulingOpportunities.put(priority, schedulingOpportunities);
  }

  synchronized public int getSchedulingOpportunities(Priority priority) {
    Integer schedulingOpportunities = 
        this.schedulingOpportunities.get(priority);
    if (schedulingOpportunities == null) {
      schedulingOpportunities = 0;
      this.schedulingOpportunities.put(priority, schedulingOpportunities);
    }
    return schedulingOpportunities;
  }

  synchronized void resetReReservations(Priority priority) {
    this.reReservations.put(priority, Integer.valueOf(0));
  }

  synchronized void addReReservation(Priority priority) {
    Integer reReservations = this.reReservations.get(priority);
    if (reReservations == null) {
      reReservations = 0;
    }
    ++reReservations;
    this.reReservations.put(priority, reReservations);
  }

  synchronized public int getReReservations(Priority priority) {
    Integer reReservations = this.reReservations.get(priority);
    if (reReservations == null) {
      reReservations = 0;
      this.reReservations.put(priority, reReservations);
    }
    return reReservations;
  }

  public synchronized int getNumReservedContainers(Priority priority) {
    Map<NodeId, RMContainer> reservedContainers = 
        this.reservedContainers.get(priority);
    return (reservedContainers == null) ? 0 : reservedContainers.size();
  }
  
  /**
   * Get total current reservations.
   * Used only by unit tests
   * @return total current reservations
   */
  @Stable
  @Private
  public synchronized Resource getCurrentReservation() {
    return currentReservation;
  }

  public synchronized RMContainer reserve(SchedulerNode node, Priority priority,
      RMContainer rmContainer, Container container) {
    // Create RMContainer if necessary
    if (rmContainer == null) {
      rmContainer = 
          new RMContainerImpl(container, getApplicationAttemptId(), 
              node.getNodeID(), rmContext.getDispatcher().getEventHandler(), 
              rmContext.getContainerAllocationExpirer());
        
      Resources.addTo(currentReservation, container.getResource());
      
      // Reset the re-reservation count
      resetReReservations(priority);
    } else {
      // Note down the re-reservation
      addReReservation(priority);
    }
    rmContainer.handle(new RMContainerReservedEvent(container.getId(), 
        container.getResource(), node.getNodeID(), priority));
    
    Map<NodeId, RMContainer> reservedContainers = 
        this.reservedContainers.get(priority);
    if (reservedContainers == null) {
      reservedContainers = new HashMap<NodeId, RMContainer>();
      this.reservedContainers.put(priority, reservedContainers);
    }
    reservedContainers.put(node.getNodeID(), rmContainer);
    
    LOG.info("Application " + getApplicationId() 
        + " reserved container " + rmContainer
        + " on node " + node + ", currently has " + reservedContainers.size()
        + " at priority " + priority 
        + "; currentReservation " + currentReservation.getMemory());
    
    return rmContainer;
  }

  public synchronized void unreserve(SchedulerNode node, Priority priority) {
    Map<NodeId, RMContainer> reservedContainers = 
        this.reservedContainers.get(priority);
    RMContainer reservedContainer = reservedContainers.remove(node.getNodeID());
    if (reservedContainers.isEmpty()) {
      this.reservedContainers.remove(priority);
    }
    
    // Reset the re-reservation count
    resetReReservations(priority);

    Resource resource = reservedContainer.getContainer().getResource();
    Resources.subtractFrom(currentReservation, resource);

    LOG.info("Application " + getApplicationId() + " unreserved " + " on node "
        + node + ", currently has " + reservedContainers.size() + " at priority "
        + priority + "; currentReservation " + currentReservation);
  }

  /**
   * Has the application reserved the given <code>node</code> at the
   * given <code>priority</code>?
   * @param node node to be checked
   * @param priority priority of reserved container
   * @return true is reserved, false if not
   */
  public synchronized boolean isReserved(SchedulerNode node, Priority priority) {
    Map<NodeId, RMContainer> reservedContainers = 
        this.reservedContainers.get(priority);
    if (reservedContainers != null) {
      return reservedContainers.containsKey(node.getNodeID());
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

  public synchronized List<RMContainer> getAllReservedContainers() {
    List<RMContainer> reservedContainers = new ArrayList<RMContainer>();
    for (Map.Entry<Priority, Map<NodeId, RMContainer>> e : 
      this.reservedContainers.entrySet()) {
      reservedContainers.addAll(e.getValue().values());
    }
    return reservedContainers;
  }
  
  /**
   * Get available headroom in terms of resources for the application's user.
   * @return available resource headroom
   */
  public synchronized Resource getHeadroom() {
    Resource limit = Resources.subtract(resourceLimit, currentConsumption);
    Resources.subtractFrom(limit, currentReservation);

    // Corner case to deal with applications being slightly over-limit
    if (limit.getMemory() < 0) {
      limit.setMemory(0);
    }
    
    return limit;
  }

  public Queue getQueue() {
    return queue;
  }

}

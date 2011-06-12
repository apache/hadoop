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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore.ApplicationStore;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;

/**
 * This class keeps track of all the consumption of an application. This also
 * keeps track of current running/completed containers for the application.
 */
@Private
@Unstable
public class Application {
  
  private static final Log LOG = LogFactory.getLog(Application.class);
  final ApplicationId applicationId;
  final Queue queue;
  final String user;
  
  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  final Set<Priority> priorities = new TreeSet<Priority>(
      new org.apache.hadoop.yarn.server.resourcemanager.resource.Priority.Comparator());
  final Map<Priority, Map<String, ResourceRequest>> requests = 
    new HashMap<Priority, Map<String, ResourceRequest>>();
  final Resource currentConsumption = recordFactory
      .newRecordInstance(Resource.class);
  final Resource currentReservation = recordFactory
  .newRecordInstance(Resource.class);
  final Resource overallConsumption = recordFactory
      .newRecordInstance(Resource.class);
  Resource resourceLimit = recordFactory.newRecordInstance(Resource.class);
  
  Map<Priority, Integer> schedulingOpportunities = new HashMap<Priority, Integer>();

  private final ApplicationStore store;

  /* Current consumption */
  List<Container> acquired = new ArrayList<Container>();
  List<Container> completedContainers = new ArrayList<Container>();
  /* Allocated by scheduler */
  List<Container> allocated = new ArrayList<Container>();
  Set<NodeInfo> applicationOnNodes = new HashSet<NodeInfo>();
  ApplicationMaster master;
  boolean pending = true; // for app metrics

  /* Reserved containers */
  private final Comparator<NodeInfo> nodeComparator = new Comparator<NodeInfo>() {
    @Override
    public int compare(NodeInfo o1, NodeInfo o2) {
      return o1.getNodeID().getId() - o2.getNodeID().getId();
    }
  };
  final Map<Priority, Set<NodeInfo>> reservedContainers = new HashMap<Priority, Set<NodeInfo>>();

  public Application(ApplicationId applicationId, ApplicationMaster master,
      Queue queue, String user, ApplicationStore store) {
    this.applicationId = applicationId;
    this.queue = queue;
    this.user = user;
    this.master = master;
    this.store = store;
  }

  public ApplicationId getApplicationId() {
    return applicationId;
  }

  public Queue getQueue() {
    return queue;
  }

  public String getUser() {
    return user;
  }

  public synchronized ApplicationState getState() {
    return master.getState();
  }

  public synchronized boolean isPending() {
    return pending;
  }

  public synchronized boolean isSchedulable() {
    ApplicationState state = getState();
    return 
      (state == ApplicationState.ALLOCATED || state == ApplicationState.ALLOCATING ||
       state == ApplicationState.LAUNCHED || state == ApplicationState.LAUNCHING || 
       state == ApplicationState.PENDING || state == ApplicationState.RUNNING);
  }
  
  public synchronized Map<Priority, Map<String, ResourceRequest>> getRequests() {
    return requests;
  }

  public int getNewContainerId() {
    int i = master.getContainerCount();
    master.setContainerCount(++i);
    return master.getContainerCount();
  }
  
  /**
   * Clear any pending requests from this application.
   */
  public synchronized void clearRequests() {
    requests.clear();
    LOG.info("Application " + applicationId + " requests cleared");
  }

  /**
   * the currently acquired/allocated containers by the application masters.
   * 
   * @return the current containers being used by the application masters.
   */
  public synchronized List<Container> getCurrentContainers() {
    List<Container> currentContainers = new ArrayList<Container>(acquired);
    currentContainers.addAll(allocated);
    return currentContainers;
  }

  /**
   * The ApplicationMaster is acquiring the allocated/completed resources.
   * 
   * @return allocated resources
   */
  synchronized public List<Container> acquire() {
    // Return allocated containers
    acquired.addAll(allocated);
    List<Container> heartbeatContainers = allocated;
    allocated = new ArrayList<Container>();

    for (Container container : heartbeatContainers) {
      Resources.addTo(overallConsumption, container.getResource());
    }

    LOG.info("acquire:" + " application=" + applicationId + " #acquired="
        + heartbeatContainers.size());
    heartbeatContainers = (heartbeatContainers == null) ? new ArrayList<Container>()
        : heartbeatContainers;

    heartbeatContainers.addAll(completedContainers);
    completedContainers.clear();
    return heartbeatContainers;
  }

  /**
   * The ApplicationMaster is updating resource requirements for the
   * application, by asking for more resources and releasing resources acquired
   * by the application.
   * 
   * @param requests
   *          resources to be acquired
   */
  synchronized public void updateResourceRequests(List<ResourceRequest> requests) {
    QueueMetrics metrics = queue.getMetrics();
    // Update resource requests
    for (ResourceRequest request : requests) {
      Priority priority = request.getPriority();
      String hostName = request.getHostName();
      boolean updatePendingResources = false;
      ResourceRequest lastRequest = null;

      if (hostName.equals(NodeManager.ANY)) {
        LOG.debug("update:" + " application=" + applicationId + " request="
            + request);
        updatePendingResources = true;
      }

      Map<String, ResourceRequest> asks = this.requests.get(priority);

      if (asks == null) {
        asks = new HashMap<String, ResourceRequest>();
        this.requests.put(priority, asks);
        this.priorities.add(priority);
      } else if (updatePendingResources) {
        lastRequest = asks.get(hostName);
      }

      asks.put(hostName, request);

      if (updatePendingResources) {
        int lastRequestContainers = lastRequest != null ? lastRequest
            .getNumContainers() : 0;
        Resource lastRequestCapability = lastRequest != null ? lastRequest
            .getCapability() : Resources.none();
        metrics.incrPendingResources(user, request.getNumContainers()
            - lastRequestContainers, Resources.subtractFrom( // save a clone
            Resources.multiply(request.getCapability(), request
                .getNumContainers()), Resources.multiply(lastRequestCapability,
                lastRequestContainers)));
      }
    }
  }

  public synchronized void releaseContainers(List<Container> release) {
    // Release containers and update consumption
    for (Container container : release) {
      LOG.debug("update: " + "application=" + applicationId + " released="
          + container);
      Resources.subtractFrom(currentConsumption, container.getResource());
      for (Iterator<Container> i = acquired.iterator(); i.hasNext();) {
        Container c = i.next();
        if (c.getId().equals(container.getId())) {
          i.remove();
          LOG.info("Removed acquired container: " + container.getId());
        }
      }
    }
  }

  synchronized public Collection<Priority> getPriorities() {
    return priorities;
  }

  synchronized public Map<String, ResourceRequest> getResourceRequests(
      Priority priority) {
    return requests.get(priority);
  }

  synchronized public ResourceRequest getResourceRequest(Priority priority,
      String nodeAddress) {
    Map<String, ResourceRequest> nodeRequests = requests.get(priority);
    return (nodeRequests == null) ? null : nodeRequests.get(nodeAddress);
  }

  public synchronized Resource getResource(Priority priority) {
    ResourceRequest request = getResourceRequest(priority, NodeManager.ANY);
    return request.getCapability();
  }

  synchronized public void completedContainer(Container container, 
      Resource containerResource) {
    if (container != null) {
      LOG.info("Completed container: " + container);
      completedContainers.add(container);
    }
    queue.getMetrics().releaseResources(user, 1, containerResource);
    Resources.subtractFrom(currentConsumption, containerResource);
  }

  synchronized public void completedContainers(List<Container> containers) {
    completedContainers.addAll(containers);
  }

  /**
   * Resources have been allocated to this application by the resource
   * scheduler. Track them.
   * 
   * @param type
   *          the type of the node
   * @param node
   *          the nodeinfo of the node
   * @param priority
   *          the priority of the request.
   * @param request
   *          the request
   * @param containers
   *          the containers allocated.
   */
  synchronized public void allocate(NodeType type, NodeInfo node,
      Priority priority, ResourceRequest request, List<Container> containers) {
    applicationOnNodes.add(node);
    if (type == NodeType.DATA_LOCAL) {
      allocateNodeLocal(node, priority, request, containers);
    } else if (type == NodeType.RACK_LOCAL) {
      allocateRackLocal(node, priority, request, containers);
    } else {
      allocateOffSwitch(node, priority, request, containers);
    }
    QueueMetrics metrics = queue.getMetrics();
    if (pending) {
      // once an allocation is done we assume the application is
      // running from scheduler's POV.
      pending = false;
      metrics.incrAppsRunning(user);
    }
    LOG.debug("allocate: user: " + user + ", memory: "
        + request.getCapability());
    metrics.allocateResources(user, containers.size(), request.getCapability());
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   * 
   * @param allocatedContainers
   *          resources allocated to the application
   */
  synchronized private void allocateNodeLocal(NodeInfo node, Priority priority,
      ResourceRequest nodeLocalRequest, List<Container> containers) {
    // Update consumption and track allocations
    allocate(containers);

    // Update future requirements
    nodeLocalRequest.setNumContainers(nodeLocalRequest.getNumContainers()
        - containers.size());
    if (nodeLocalRequest.getNumContainers() == 0) {
      this.requests.get(priority).remove(node.getNodeAddress());
    }

    ResourceRequest rackLocalRequest = requests.get(priority).get(
        node.getRackName());
    rackLocalRequest.setNumContainers(rackLocalRequest.getNumContainers()
        - containers.size());
    if (rackLocalRequest.getNumContainers() == 0) {
      this.requests.get(priority).remove(node.getRackName());
    }

    // Do not remove ANY
    ResourceRequest offSwitchRequest = requests.get(priority).get(
        NodeManager.ANY);
    offSwitchRequest.setNumContainers(offSwitchRequest.getNumContainers()
        - containers.size());
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   * 
   * @param allocatedContainers
   *          resources allocated to the application
   */
  synchronized private void allocateRackLocal(NodeInfo node, Priority priority,
      ResourceRequest rackLocalRequest, List<Container> containers) {

    // Update consumption and track allocations
    allocate(containers);

    // Update future requirements
    rackLocalRequest.setNumContainers(rackLocalRequest.getNumContainers()
        - containers.size());
    if (rackLocalRequest.getNumContainers() == 0) {
      this.requests.get(priority).remove(node.getRackName());
    }

    // Do not remove ANY
    ResourceRequest offSwitchRequest = requests.get(priority).get(
        NodeManager.ANY);
    offSwitchRequest.setNumContainers(offSwitchRequest.getNumContainers()
        - containers.size());
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   * 
   * @param allocatedContainers
   *          resources allocated to the application
   */
  synchronized private void allocateOffSwitch(NodeInfo node, Priority priority,
      ResourceRequest offSwitchRequest, List<Container> containers) {

    // Update consumption and track allocations
    allocate(containers);

    // Update future requirements

    // Do not remove ANY
    offSwitchRequest.setNumContainers(offSwitchRequest.getNumContainers()
        - containers.size());
  }

  synchronized public void allocate(List<Container> containers) {
    // Update consumption and track allocations
    for (Container container : containers) {
      Resources.addTo(currentConsumption, container.getResource());

      allocated.add(container);
      try {
        store.storeContainer(container);
      } catch (IOException ie) {
        // TODO fix this. we shouldnt ignore
      }
      LOG.debug("allocate: applicationId=" + applicationId + " container="
          + container.getId() + " host="
          + container.getContainerManagerAddress());
    }
  }

  synchronized public void resetSchedulingOpportunities(Priority priority) {
    Integer schedulingOpportunities = this.schedulingOpportunities
        .get(priority);
    schedulingOpportunities = 0;
    this.schedulingOpportunities.put(priority, schedulingOpportunities);
  }

  synchronized public void addSchedulingOpportunity(Priority priority) {
    Integer schedulingOpportunities = this.schedulingOpportunities
        .get(priority);
    if (schedulingOpportunities == null) {
      schedulingOpportunities = 0;
    }
    ++schedulingOpportunities;
    this.schedulingOpportunities.put(priority, schedulingOpportunities);
  }

  synchronized public int getSchedulingOpportunities(Priority priority) {
    Integer schedulingOpportunities = this.schedulingOpportunities
        .get(priority);
    if (schedulingOpportunities == null) {
      schedulingOpportunities = 0;
      this.schedulingOpportunities.put(priority, schedulingOpportunities);
    }
    return schedulingOpportunities;
  }

  synchronized public void showRequests() {
    if (LOG.isDebugEnabled()) {
      for (Priority priority : getPriorities()) {
        Map<String, ResourceRequest> requests = getResourceRequests(priority);
        if (requests != null) {
          LOG.debug("showRequests:" + " application=" + applicationId + 
              " available=" + getHeadroom() + 
              " current=" + currentConsumption + " state=" + getState());
          for (ResourceRequest request : requests.values()) {
            LOG.debug("showRequests:" + " application=" + applicationId
                + " request=" + request);
          }
        }
      }
    }
  }

  synchronized public List<NodeInfo> getAllNodesForApplication() {
    return new ArrayList<NodeInfo>(applicationOnNodes);
  }

  synchronized public org.apache.hadoop.yarn.api.records.Application getApplicationInfo() {
    org.apache.hadoop.yarn.api.records.Application application = recordFactory
        .newRecordInstance(org.apache.hadoop.yarn.api.records.Application.class);
    application.setApplicationId(applicationId);
    application.setName("");
    application.setQueue(queue.getQueueName());
    application.setState(org.apache.hadoop.yarn.api.records.ApplicationState.RUNNING);
    application.setUser(user);

    ApplicationStatus status = recordFactory
        .newRecordInstance(ApplicationStatus.class);
    status.setApplicationId(applicationId);
    application.setStatus(status);

    return application;
  }

  public synchronized int getReservedContainers(Priority priority) {
    Set<NodeInfo> reservedNodes = this.reservedContainers.get(priority);
    return (reservedNodes == null) ? 0 : reservedNodes.size();
  }

  public synchronized void reserveResource(NodeInfo node, Priority priority,
      Resource resource) {
    Set<NodeInfo> reservedNodes = this.reservedContainers.get(priority);
    if (reservedNodes == null) {
      reservedNodes = new TreeSet<NodeInfo>(nodeComparator);
      reservedContainers.put(priority, reservedNodes);
    }
    reservedNodes.add(node);
    Resources.add(currentReservation, resource);
    LOG.info("Application " + applicationId + " reserved " + resource
        + " on node " + node + ", currently has " + reservedNodes.size()
        + " at priority " + priority 
        + "; currentReservation " + currentReservation);
    queue.getMetrics().reserveResource(user, resource);
  }

  public synchronized void unreserveResource(NodeInfo node, Priority priority) {
    Set<NodeInfo> reservedNodes = reservedContainers.get(priority);
    reservedNodes.remove(node);
    if (reservedNodes.isEmpty()) {
      this.reservedContainers.remove(priority);
    }
    
    Resource resource = getResource(priority);
    Resources.subtract(currentReservation, resource);

    LOG.info("Application " + applicationId + " unreserved " + " on node "
        + node + ", currently has " + reservedNodes.size() + " at priority "
        + priority + "; currentReservation " + currentReservation);
    queue.getMetrics().unreserveResource(user, node.getReservedResource());
  }

  public synchronized boolean isReserved(NodeInfo node, Priority priority) {
    Set<NodeInfo> reservedNodes = reservedContainers.get(priority);
    if (reservedNodes != null) {
      return reservedNodes.contains(node);
    }
    return false;
  }

  public float getLocalityWaitFactor(Priority priority, int clusterNodes) {
    // Estimate: Required unique resources (i.e. hosts + racks)
    int requiredResources = Math.max(this.requests.get(priority).size() - 1, 1);
    return ((float) requiredResources / clusterNodes);
  }

  synchronized public void stop() {
    // clear pending resources metrics for the application
    QueueMetrics metrics = queue.getMetrics();
    for (Map<String, ResourceRequest> asks : requests.values()) {
      ResourceRequest request = asks.get(NodeManager.ANY);
      if (request != null) {
        metrics.decrPendingResources(user, request.getNumContainers(),
            Resources.multiply(request.getCapability(), request
                .getNumContainers()));
      }
    }
    metrics.finishApp(this);
    
    // Clear requests themselves
    clearRequests();
  }

  public Map<Priority, Set<NodeInfo>> getAllReservations() {
    return new HashMap<Priority, Set<NodeInfo>>(reservedContainers);
  }

  public synchronized void setAvailableResourceLimit(Resource globalLimit) {
    this.resourceLimit = globalLimit; 
  }

  /**
   * Get available headroom in terms of resources for the application's user.
   * @return available resource headroom
   */
  public synchronized Resource getHeadroom() {
    Resource limit = 
      Resources.subtract(Resources.subtract(resourceLimit, currentConsumption), 
          currentReservation);

    // Corner case to deal with applications being slightly over-limit
    if (limit.getMemory() < 0) {
      limit.setMemory(0);
    }
    
    return limit;
  }
}

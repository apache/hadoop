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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore.ApplicationStore;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

/**
 * This class keeps track of all the consumption of an application. This also
 * keeps track of current running/completed containers for the application.
 */
@Private
@Unstable
public class AppSchedulingInfo {
  
  private static final Log LOG = LogFactory.getLog(AppSchedulingInfo.class);
  private final ApplicationAttemptId applicationAttemptId;
  final ApplicationId applicationId;
  private final String queueName;
  Queue queue;
  final String user;
  private final AtomicInteger containerIdCounter = new AtomicInteger(0);

  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  final Set<Priority> priorities = new TreeSet<Priority>(
      new org.apache.hadoop.yarn.server.resourcemanager.resource.Priority.Comparator());
  final Map<Priority, Map<String, ResourceRequest>> requests = 
    new HashMap<Priority, Map<String, ResourceRequest>>();

  private final ApplicationStore store;

  /* Current consumption */
  List<Container> acquired = new ArrayList<Container>();
  List<Container> completedContainers = new ArrayList<Container>();
  /* Allocated by scheduler */
  List<Container> allocated = new ArrayList<Container>();
  ApplicationMaster master;
  boolean pending = true; // for app metrics

  public AppSchedulingInfo(ApplicationAttemptId appAttemptId,
      ApplicationMaster master, String queueName, String user,
      ApplicationStore store) {
    this.applicationAttemptId = appAttemptId;
    this.applicationId = appAttemptId.getApplicationId();
    this.queueName = queueName;
    this.user = user;
    this.master = master;
    this.store = store;
  }

  public ApplicationId getApplicationId() {
    return applicationId;
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    return applicationAttemptId;
  }

  public String getQueueName() {
    return queueName;
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
  
  /**
   * Clear any pending requests from this application.
   */
  private synchronized void clearRequests() {
    priorities.clear();
    requests.clear();
    LOG.info("Application " + applicationId + " requests cleared");
  }

  public int getNewContainerId() {
    return this.containerIdCounter.incrementAndGet();
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
  synchronized private List<Container> acquire() {
    // Return allocated containers
    acquired.addAll(allocated);
    List<Container> heartbeatContainers = allocated;
    allocated = new ArrayList<Container>();

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

      if (hostName.equals(RMNode.ANY)) {
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

  private synchronized void releaseContainers(List<Container> release) {
    // Release containers and update consumption
    for (Container container : release) {
      LOG.debug("update: " + "application=" + applicationId + " released="
          + container);
      // TOday in all code paths, this is taken by completedContainer called by
      // the caller. So commenting this.
      // Resources.subtractFrom(currentConsumption, container.getResource());
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
    ResourceRequest request = getResourceRequest(priority, RMNode.ANY);
    return request.getCapability();
  }

  synchronized private void completedContainer(Container container, 
      Resource containerResource) {
    if (container != null) {
      LOG.info("Completed container: " + container);
      completedContainers.add(container);
    }
    queue.getMetrics().releaseResources(user, 1, containerResource);
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
  synchronized public void allocate(NodeType type, SchedulerNode node,
      Priority priority, ResourceRequest request, List<Container> containers) {
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
  synchronized private void allocateNodeLocal(SchedulerNode node, Priority priority,
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
        RMNode.ANY);
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
  synchronized private void allocateRackLocal(SchedulerNode node, Priority priority,
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
        RMNode.ANY);
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
  synchronized private void allocateOffSwitch(SchedulerNode node, Priority priority,
      ResourceRequest offSwitchRequest, List<Container> containers) {

    // Update consumption and track allocations
    allocate(containers);

    // Update future requirements

    // Do not remove ANY
    offSwitchRequest.setNumContainers(offSwitchRequest.getNumContainers()
        - containers.size());
  }

  synchronized private void allocate(List<Container> containers) {
    // Update consumption and track allocations
    for (Container container : containers) {

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

  synchronized public void stop() {
    // clear pending resources metrics for the application
    QueueMetrics metrics = queue.getMetrics();
    for (Map<String, ResourceRequest> asks : requests.values()) {
      ResourceRequest request = asks.get(RMNode.ANY);
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

  public void setQueue(Queue queue) {
    this.queue = queue;
  }
}

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

import java.util.Collection;
import java.util.HashMap;
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
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;

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

  final Set<Priority> priorities = new TreeSet<Priority>(
      new org.apache.hadoop.yarn.server.resourcemanager.resource.Priority.Comparator());
  final Map<Priority, Map<String, ResourceRequest>> requests = 
    new HashMap<Priority, Map<String, ResourceRequest>>();

  //private final ApplicationStore store;
  private final ActiveUsersManager activeUsersManager;
  
  /* Allocated by scheduler */
  boolean pending = true; // for app metrics

  public AppSchedulingInfo(ApplicationAttemptId appAttemptId,
      String user, Queue queue, ActiveUsersManager activeUsersManager) {
    this.applicationAttemptId = appAttemptId;
    this.applicationId = appAttemptId.getApplicationId();
    this.queue = queue;
    this.queueName = queue.getQueueName();
    this.user = user;
    this.activeUsersManager = activeUsersManager;
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
   * The ApplicationMaster is updating resource requirements for the
   * application, by asking for more resources and releasing resources acquired
   * by the application.
   * 
   * @param requests
   *          resources to be acquired
   */
  synchronized public void updateResourceRequests(
      List<ResourceRequest> requests) {
    QueueMetrics metrics = queue.getMetrics();
    // Update resource requests
    for (ResourceRequest request : requests) {
      Priority priority = request.getPriority();
      String hostName = request.getHostName();
      boolean updatePendingResources = false;
      ResourceRequest lastRequest = null;

      if (hostName.equals(RMNode.ANY)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("update:" + " application=" + applicationId + " request="
              + request);
        }
        updatePendingResources = true;
        
        // Premature optimization?
        // Assumes that we won't see more than one priority request updated
        // in one call, reasonable assumption... however, it's totally safe
        // to activate same application more than once.
        // Thus we don't need another loop ala the one in decrementOutstanding()  
        // which is needed during deactivate.
        if (request.getNumContainers() > 0) {
          activeUsersManager.activateApplication(user, applicationId);
        }
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
        
        // Similarly, deactivate application?
        if (request.getNumContainers() <= 0) {
          LOG.info("checking for deactivate... ");
          checkForDeactivation();
        }
        
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
   * @param container
   *          the containers allocated.
   */
  synchronized public void allocate(NodeType type, SchedulerNode node,
      Priority priority, ResourceRequest request, Container container) {
    if (type == NodeType.NODE_LOCAL) {
      allocateNodeLocal(node, priority, request, container);
    } else if (type == NodeType.RACK_LOCAL) {
      allocateRackLocal(node, priority, request, container);
    } else {
      allocateOffSwitch(node, priority, request, container);
    }
    QueueMetrics metrics = queue.getMetrics();
    if (pending) {
      // once an allocation is done we assume the application is
      // running from scheduler's POV.
      pending = false;
      metrics.incrAppsRunning(this, user);
    }
    LOG.debug("allocate: user: " + user + ", memory: "
        + request.getCapability());
    metrics.allocateResources(user, 1, request.getCapability());
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   * 
   * @param allocatedContainers
   *          resources allocated to the application
   */
  synchronized private void allocateNodeLocal( 
      SchedulerNode node, Priority priority, 
      ResourceRequest nodeLocalRequest, Container container) {
    // Update consumption and track allocations
    allocate(container);

    // Update future requirements
    nodeLocalRequest.setNumContainers(nodeLocalRequest.getNumContainers() - 1);
    if (nodeLocalRequest.getNumContainers() == 0) {
      this.requests.get(priority).remove(node.getHostName());
    }

    ResourceRequest rackLocalRequest = requests.get(priority).get(
        node.getRackName());
    rackLocalRequest.setNumContainers(rackLocalRequest.getNumContainers() - 1);
    if (rackLocalRequest.getNumContainers() == 0) {
      this.requests.get(priority).remove(node.getRackName());
    }

    decrementOutstanding(requests.get(priority).get(RMNode.ANY));
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   * 
   * @param allocatedContainers
   *          resources allocated to the application
   */
  synchronized private void allocateRackLocal(
      SchedulerNode node, Priority priority,
      ResourceRequest rackLocalRequest, Container container) {

    // Update consumption and track allocations
    allocate(container);

    // Update future requirements
    rackLocalRequest.setNumContainers(rackLocalRequest.getNumContainers() - 1);
    if (rackLocalRequest.getNumContainers() == 0) {
      this.requests.get(priority).remove(node.getRackName());
    }

    decrementOutstanding(requests.get(priority).get(RMNode.ANY));
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   * 
   * @param allocatedContainers
   *          resources allocated to the application
   */
  synchronized private void allocateOffSwitch(
      SchedulerNode node, Priority priority,
      ResourceRequest offSwitchRequest, Container container) {

    // Update consumption and track allocations
    allocate(container);

    // Update future requirements
    decrementOutstanding(offSwitchRequest);
  }

  synchronized private void decrementOutstanding(
      ResourceRequest offSwitchRequest) {
    int numOffSwitchContainers = offSwitchRequest.getNumContainers() - 1;

    // Do not remove ANY
    offSwitchRequest.setNumContainers(numOffSwitchContainers);
    
    // Do we have any outstanding requests?
    // If there is nothing, we need to deactivate this application
    if (numOffSwitchContainers == 0) {
      checkForDeactivation();
    }
  }
  
  synchronized private void checkForDeactivation() {
    boolean deactivate = true;
    for (Priority priority : getPriorities()) {
      ResourceRequest request = getResourceRequest(priority, RMNodeImpl.ANY);
      if (request.getNumContainers() > 0) {
        deactivate = false;
        break;
      }
    }
    if (deactivate) {
      activeUsersManager.deactivateApplication(user, applicationId);
    }
  }
  
  synchronized private void allocate(Container container) {
    // Update consumption and track allocations
    //TODO: fixme sharad
    /* try {
        store.storeContainer(container);
      } catch (IOException ie) {
        // TODO fix this. we shouldnt ignore
      }*/
    
    LOG.debug("allocate: applicationId=" + applicationId + " container="
        + container.getId() + " host="
        + container.getNodeId().toString());
  }

  synchronized public void stop(RMAppAttemptState rmAppAttemptFinalState) {
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
    metrics.finishApp(this, rmAppAttemptFinalState);
    
    // Clear requests themselves
    clearRequests();
  }

  public synchronized void setQueue(Queue queue) {
    this.queue = queue;
  }
}

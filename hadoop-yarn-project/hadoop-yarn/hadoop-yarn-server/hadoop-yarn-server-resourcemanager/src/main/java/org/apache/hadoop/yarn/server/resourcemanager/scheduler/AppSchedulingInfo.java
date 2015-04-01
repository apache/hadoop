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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

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
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.util.resource.Resources;

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
  private String queueName;
  Queue queue;
  final String user;
  // TODO making containerIdCounter long
  private final AtomicLong containerIdCounter;
  private final int EPOCH_BIT_SHIFT = 40;

  final Set<Priority> priorities = new TreeSet<Priority>(
      new org.apache.hadoop.yarn.server.resourcemanager.resource.Priority.Comparator());
  final Map<Priority, Map<String, ResourceRequest>> requests =
    new ConcurrentHashMap<Priority, Map<String, ResourceRequest>>();
  private Set<String> blacklist = new HashSet<String>();

  //private final ApplicationStore store;
  private ActiveUsersManager activeUsersManager;
  
  /* Allocated by scheduler */
  boolean pending = true; // for app metrics
  
 
  public AppSchedulingInfo(ApplicationAttemptId appAttemptId,
      String user, Queue queue, ActiveUsersManager activeUsersManager,
      long epoch) {
    this.applicationAttemptId = appAttemptId;
    this.applicationId = appAttemptId.getApplicationId();
    this.queue = queue;
    this.queueName = queue.getQueueName();
    this.user = user;
    this.activeUsersManager = activeUsersManager;
    this.containerIdCounter = new AtomicLong(epoch << EPOCH_BIT_SHIFT);
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

  public long getNewContainerId() {
    return this.containerIdCounter.incrementAndGet();
  }

  /**
   * The ApplicationMaster is updating resource requirements for the
   * application, by asking for more resources and releasing resources acquired
   * by the application.
   *
   * @param requests resources to be acquired
   * @param recoverPreemptedRequest recover Resource Request on preemption
   */
  synchronized public void updateResourceRequests(
      List<ResourceRequest> requests, boolean recoverPreemptedRequest) {
    QueueMetrics metrics = queue.getMetrics();
    
    // Update resource requests
    for (ResourceRequest request : requests) {
      Priority priority = request.getPriority();
      String resourceName = request.getResourceName();
      boolean updatePendingResources = false;
      ResourceRequest lastRequest = null;

      if (resourceName.equals(ResourceRequest.ANY)) {
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
        asks = new ConcurrentHashMap<String, ResourceRequest>();
        this.requests.put(priority, asks);
        this.priorities.add(priority);
      }
      lastRequest = asks.get(resourceName);

      if (recoverPreemptedRequest && lastRequest != null) {
        // Increment the number of containers to 1, as it is recovering a
        // single container.
        request.setNumContainers(lastRequest.getNumContainers() + 1);
      }

      asks.put(resourceName, request);
      if (updatePendingResources) {
        
        // Similarly, deactivate application?
        if (request.getNumContainers() <= 0) {
          LOG.info("checking for deactivate of application :"
              + this.applicationId);
          checkForDeactivation();
        }
        
        int lastRequestContainers = lastRequest != null ? lastRequest
            .getNumContainers() : 0;
        Resource lastRequestCapability = lastRequest != null ? lastRequest
            .getCapability() : Resources.none();
        metrics.incrPendingResources(user, request.getNumContainers(),
            request.getCapability());
        metrics.decrPendingResources(user, lastRequestContainers,
            lastRequestCapability);
      }
    }
  }

  /**
   * The ApplicationMaster is updating the blacklist
   *
   * @param blacklistAdditions resources to be added to the blacklist
   * @param blacklistRemovals resources to be removed from the blacklist
   */
  synchronized public void updateBlacklist(
      List<String> blacklistAdditions, List<String> blacklistRemovals) {
    // Add to blacklist
    if (blacklistAdditions != null) {
      blacklist.addAll(blacklistAdditions);
    }

    // Remove from blacklist
    if (blacklistRemovals != null) {
      blacklist.removeAll(blacklistRemovals);
    }
  }

  synchronized public Collection<Priority> getPriorities() {
    return priorities;
  }

  synchronized public Map<String, ResourceRequest> getResourceRequests(
      Priority priority) {
    return requests.get(priority);
  }

  public List<ResourceRequest> getAllResourceRequests() {
    List<ResourceRequest> ret = new ArrayList<ResourceRequest>();
    for (Map<String, ResourceRequest> r : requests.values()) {
      ret.addAll(r.values());
    }
    return ret;
  }

  synchronized public ResourceRequest getResourceRequest(Priority priority,
      String resourceName) {
    Map<String, ResourceRequest> nodeRequests = requests.get(priority);
    return (nodeRequests == null) ? null : nodeRequests.get(resourceName);
  }

  public synchronized Resource getResource(Priority priority) {
    ResourceRequest request = getResourceRequest(priority, ResourceRequest.ANY);
    return (request == null) ? null : request.getCapability();
  }

  public synchronized boolean isBlacklisted(String resourceName) {
    return blacklist.contains(resourceName);
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
  synchronized public List<ResourceRequest> allocate(NodeType type,
      SchedulerNode node, Priority priority, ResourceRequest request,
      Container container) {
    List<ResourceRequest> resourceRequests = new ArrayList<ResourceRequest>();
    if (type == NodeType.NODE_LOCAL) {
      allocateNodeLocal(node, priority, request, container, resourceRequests);
    } else if (type == NodeType.RACK_LOCAL) {
      allocateRackLocal(node, priority, request, container, resourceRequests);
    } else {
      allocateOffSwitch(node, priority, request, container, resourceRequests);
    }
    QueueMetrics metrics = queue.getMetrics();
    if (pending) {
      // once an allocation is done we assume the application is
      // running from scheduler's POV.
      pending = false;
      metrics.runAppAttempt(applicationId, user);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("allocate: applicationId=" + applicationId
          + " container=" + container.getId()
          + " host=" + container.getNodeId().toString()
          + " user=" + user
          + " resource=" + request.getCapability());
    }
    metrics.allocateResources(user, 1, request.getCapability(), true);
    return resourceRequests;
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   * 
   * @param allocatedContainers
   *          resources allocated to the application
   */
  synchronized private void allocateNodeLocal(SchedulerNode node,
      Priority priority, ResourceRequest nodeLocalRequest, Container container,
      List<ResourceRequest> resourceRequests) {
    // Update future requirements
    decResourceRequest(node.getNodeName(), priority, nodeLocalRequest);

    ResourceRequest rackLocalRequest = requests.get(priority).get(
        node.getRackName());
    decResourceRequest(node.getRackName(), priority, rackLocalRequest);

    ResourceRequest offRackRequest = requests.get(priority).get(
        ResourceRequest.ANY);
    decrementOutstanding(offRackRequest);

    // Update cloned NodeLocal, RackLocal and OffRack requests for recovery
    resourceRequests.add(cloneResourceRequest(nodeLocalRequest));
    resourceRequests.add(cloneResourceRequest(rackLocalRequest));
    resourceRequests.add(cloneResourceRequest(offRackRequest));
  }

  private void decResourceRequest(String resourceName, Priority priority,
      ResourceRequest request) {
    request.setNumContainers(request.getNumContainers() - 1);
    if (request.getNumContainers() == 0) {
      requests.get(priority).remove(resourceName);
    }
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   * 
   * @param allocatedContainers
   *          resources allocated to the application
   */
  synchronized private void allocateRackLocal(SchedulerNode node,
      Priority priority, ResourceRequest rackLocalRequest, Container container,
      List<ResourceRequest> resourceRequests) {
    // Update future requirements
    decResourceRequest(node.getRackName(), priority, rackLocalRequest);
    
    ResourceRequest offRackRequest = requests.get(priority).get(
        ResourceRequest.ANY);
    decrementOutstanding(offRackRequest);

    // Update cloned RackLocal and OffRack requests for recovery
    resourceRequests.add(cloneResourceRequest(rackLocalRequest));
    resourceRequests.add(cloneResourceRequest(offRackRequest));
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   * 
   * @param allocatedContainers
   *          resources allocated to the application
   */
  synchronized private void allocateOffSwitch(SchedulerNode node,
      Priority priority, ResourceRequest offSwitchRequest, Container container,
      List<ResourceRequest> resourceRequests) {
    // Update future requirements
    decrementOutstanding(offSwitchRequest);
    // Update cloned OffRack requests for recovery
    resourceRequests.add(cloneResourceRequest(offSwitchRequest));
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
      ResourceRequest request = getResourceRequest(priority, ResourceRequest.ANY);
      if (request != null) {
        if (request.getNumContainers() > 0) {
          deactivate = false;
          break;
        }
      }
    }
    if (deactivate) {
      activeUsersManager.deactivateApplication(user, applicationId);
    }
  }
  
  synchronized public void move(Queue newQueue) {
    QueueMetrics oldMetrics = queue.getMetrics();
    QueueMetrics newMetrics = newQueue.getMetrics();
    for (Map<String, ResourceRequest> asks : requests.values()) {
      ResourceRequest request = asks.get(ResourceRequest.ANY);
      if (request != null) {
        oldMetrics.decrPendingResources(user, request.getNumContainers(),
            request.getCapability());
        newMetrics.incrPendingResources(user, request.getNumContainers(),
            request.getCapability());
      }
    }
    oldMetrics.moveAppFrom(this);
    newMetrics.moveAppTo(this);
    activeUsersManager.deactivateApplication(user, applicationId);
    activeUsersManager = newQueue.getActiveUsersManager();
    activeUsersManager.activateApplication(user, applicationId);
    this.queue = newQueue;
    this.queueName = newQueue.getQueueName();
  }

  synchronized public void stop(RMAppAttemptState rmAppAttemptFinalState) {
    // clear pending resources metrics for the application
    QueueMetrics metrics = queue.getMetrics();
    for (Map<String, ResourceRequest> asks : requests.values()) {
      ResourceRequest request = asks.get(ResourceRequest.ANY);
      if (request != null) {
        metrics.decrPendingResources(user, request.getNumContainers(),
            request.getCapability());
      }
    }
    metrics.finishAppAttempt(applicationId, pending, user);
    
    // Clear requests themselves
    clearRequests();
  }

  public synchronized void setQueue(Queue queue) {
    this.queue = queue;
  }

  public synchronized Set<String> getBlackList() {
    return this.blacklist;
  }

  public synchronized Set<String> getBlackListCopy() {
    return new HashSet<>(this.blacklist);
  }

  public synchronized void transferStateFromPreviousAppSchedulingInfo(
      AppSchedulingInfo appInfo) {
    //    this.priorities = appInfo.getPriorities();
    //    this.requests = appInfo.getRequests();
    this.blacklist = appInfo.getBlackList();
  }

  public synchronized void recoverContainer(RMContainer rmContainer) {
    QueueMetrics metrics = queue.getMetrics();
    if (pending) {
      // If there was any container to recover, the application was
      // running from scheduler's POV.
      pending = false;
      metrics.runAppAttempt(applicationId, user);
    }

    // Container is completed. Skip recovering resources.
    if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
      return;
    }

    metrics.allocateResources(user, 1, rmContainer.getAllocatedResource(),
      false);
  }
  
  public ResourceRequest cloneResourceRequest(ResourceRequest request) {
    ResourceRequest newRequest = ResourceRequest.newInstance(
        request.getPriority(), request.getResourceName(),
        request.getCapability(), 1, request.getRelaxLocality());
    return newRequest;
  }
}

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
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
  final Map<Priority, Map<String, ResourceRequest>> resourceRequestMap =
      new ConcurrentHashMap<Priority, Map<String, ResourceRequest>>();
  final Map<NodeId, Map<Priority, Map<ContainerId, 
      SchedContainerChangeRequest>>> increaseRequestMap =
      new ConcurrentHashMap<>();
  private Set<String> userBlacklist = new HashSet<>();
  private Set<String> amBlacklist = new HashSet<>();

  //private final ApplicationStore store;
  private ActiveUsersManager activeUsersManager;
  
  /* Allocated by scheduler */
  boolean pending = true; // for app metrics
  
  private ResourceUsage appResourceUsage;
 
  public AppSchedulingInfo(ApplicationAttemptId appAttemptId,
      String user, Queue queue, ActiveUsersManager activeUsersManager,
      long epoch, ResourceUsage appResourceUsage) {
    this.applicationAttemptId = appAttemptId;
    this.applicationId = appAttemptId.getApplicationId();
    this.queue = queue;
    this.queueName = queue.getQueueName();
    this.user = user;
    this.activeUsersManager = activeUsersManager;
    this.containerIdCounter = new AtomicLong(epoch << EPOCH_BIT_SHIFT);
    this.appResourceUsage = appResourceUsage;
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
    resourceRequestMap.clear();
    LOG.info("Application " + applicationId + " requests cleared");
  }

  public long getNewContainerId() {
    return this.containerIdCounter.incrementAndGet();
  }
  
  public boolean hasIncreaseRequest(NodeId nodeId) {
    Map<Priority, Map<ContainerId, SchedContainerChangeRequest>> requestsOnNode =
        increaseRequestMap.get(nodeId);
    if (null == requestsOnNode) {
      return false;
    }
    return requestsOnNode.size() > 0;
  }
  
  public Map<ContainerId, SchedContainerChangeRequest>
      getIncreaseRequests(NodeId nodeId, Priority priority) {
    Map<Priority, Map<ContainerId, SchedContainerChangeRequest>> requestsOnNode =
        increaseRequestMap.get(nodeId);
    if (null == requestsOnNode) {
      return null;
    }

    return requestsOnNode.get(priority);
  }

  public synchronized boolean updateIncreaseRequests(
      List<SchedContainerChangeRequest> increaseRequests) {
    boolean resourceUpdated = false;

    for (SchedContainerChangeRequest r : increaseRequests) {
      NodeId nodeId = r.getRMContainer().getAllocatedNode();

      Map<Priority, Map<ContainerId, SchedContainerChangeRequest>> requestsOnNode =
          increaseRequestMap.get(nodeId);
      if (null == requestsOnNode) {
        requestsOnNode = new TreeMap<>();
        increaseRequestMap.put(nodeId, requestsOnNode);
      }

      SchedContainerChangeRequest prevChangeRequest =
          getIncreaseRequest(nodeId, r.getPriority(), r.getContainerId());
      if (null != prevChangeRequest) {
        if (Resources.equals(prevChangeRequest.getTargetCapacity(),
            r.getTargetCapacity())) {
          // New target capacity is as same as what we have, just ignore the new
          // one
          continue;
        }

        // remove the old one
        removeIncreaseRequest(nodeId, prevChangeRequest.getPriority(),
            prevChangeRequest.getContainerId());
      }

      if (Resources.equals(r.getTargetCapacity(), r.getRMContainer().getAllocatedResource())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Trying to increase/decrease container, "
              + "target capacity = previous capacity = " + prevChangeRequest
              + " for container=" + r.getContainerId()
              + ". Will ignore this increase request");
        }
        continue;
      }

      // add the new one
      resourceUpdated = true;
      insertIncreaseRequest(r);
    }
    return resourceUpdated;
  }

  // insert increase request and add missing hierarchy if missing
  private void insertIncreaseRequest(SchedContainerChangeRequest request) {
    NodeId nodeId = request.getNodeId();
    Priority priority = request.getPriority();
    ContainerId containerId = request.getContainerId();
    
    Map<Priority, Map<ContainerId, SchedContainerChangeRequest>> requestsOnNode =
        increaseRequestMap.get(nodeId);
    if (null == requestsOnNode) {
      requestsOnNode =
          new HashMap<Priority, Map<ContainerId, SchedContainerChangeRequest>>();
      increaseRequestMap.put(nodeId, requestsOnNode);
    }

    Map<ContainerId, SchedContainerChangeRequest> requestsOnNodeWithPriority =
        requestsOnNode.get(priority);
    if (null == requestsOnNodeWithPriority) {
      requestsOnNodeWithPriority =
          new TreeMap<ContainerId, SchedContainerChangeRequest>();
      requestsOnNode.put(priority, requestsOnNodeWithPriority);
    }

    requestsOnNodeWithPriority.put(containerId, request);

    // update resources
    String partition = request.getRMContainer().getNodeLabelExpression();
    Resource delta = request.getDeltaCapacity();
    appResourceUsage.incPending(partition, delta);
    queue.incPendingResource(partition, delta);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Added increase request:" + request.getContainerId()
          + " delta=" + request.getDeltaCapacity());
    }
    
    // update priorities
    priorities.add(priority);
  }
  
  public synchronized boolean removeIncreaseRequest(NodeId nodeId, Priority priority,
      ContainerId containerId) {
    Map<Priority, Map<ContainerId, SchedContainerChangeRequest>> requestsOnNode =
        increaseRequestMap.get(nodeId);
    if (null == requestsOnNode) {
      return false;
    }

    Map<ContainerId, SchedContainerChangeRequest> requestsOnNodeWithPriority =
        requestsOnNode.get(priority);
    if (null == requestsOnNodeWithPriority) {
      return false;
    }

    SchedContainerChangeRequest request =
        requestsOnNodeWithPriority.remove(containerId);
    
    // remove hierarchies if it becomes empty
    if (requestsOnNodeWithPriority.isEmpty()) {
      requestsOnNode.remove(priority);
    }
    if (requestsOnNode.isEmpty()) {
      increaseRequestMap.remove(nodeId);
    }
    
    if (request == null) {
      return false;
    }

    // update queue's pending resource if request exists
    String partition = request.getRMContainer().getNodeLabelExpression();
    Resource delta = request.getDeltaCapacity();
    appResourceUsage.decPending(partition, delta);
    queue.decPendingResource(partition, delta);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("remove increase request:" + request);
    }
    
    return true;
  }
  
  public SchedContainerChangeRequest getIncreaseRequest(NodeId nodeId,
      Priority priority, ContainerId containerId) {
    Map<Priority, Map<ContainerId, SchedContainerChangeRequest>> requestsOnNode =
        increaseRequestMap.get(nodeId);
    if (null == requestsOnNode) {
      return null;
    }

    Map<ContainerId, SchedContainerChangeRequest> requestsOnNodeWithPriority =
        requestsOnNode.get(priority);
    if (null == requestsOnNodeWithPriority) {
      return null;
    }

    return requestsOnNodeWithPriority.get(containerId);
  }

  /**
   * The ApplicationMaster is updating resource requirements for the
   * application, by asking for more resources and releasing resources acquired
   * by the application.
   *
   * @param requests resources to be acquired
   * @param recoverPreemptedRequest recover Resource Request on preemption
   * @return true if any resource was updated, false else
   */
  synchronized public boolean updateResourceRequests(
      List<ResourceRequest> requests, boolean recoverPreemptedRequest) {
    QueueMetrics metrics = queue.getMetrics();
    
    boolean anyResourcesUpdated = false;

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
        anyResourcesUpdated = true;
        
        // Premature optimization?
        // Assumes that we won't see more than one priority request updated
        // in one call, reasonable assumption... however, it's totally safe
        // to activate same application more than once.
        // Thus we don't need another loop ala the one in decrementOutstanding()  
        // which is needed during deactivate.
        if (request.getNumContainers() > 0) {
          activeUsersManager.activateApplication(user, applicationId);
        }
        ResourceRequest previousAnyRequest =
            getResourceRequest(priority, resourceName);

        // When there is change in ANY request label expression, we should
        // update label for all resource requests already added of same
        // priority as ANY resource request.
        if ((null == previousAnyRequest)
            || isRequestLabelChanged(previousAnyRequest, request)) {
          Map<String, ResourceRequest> resourceRequest =
              getResourceRequests(priority);
          if (resourceRequest != null) {
            for (ResourceRequest r : resourceRequest.values()) {
              if (!r.getResourceName().equals(ResourceRequest.ANY)) {
                r.setNodeLabelExpression(request.getNodeLabelExpression());
              }
            }
          }
        }
      } else {
        ResourceRequest anyRequest =
            getResourceRequest(priority, ResourceRequest.ANY);
        if (anyRequest != null) {
          request.setNodeLabelExpression(anyRequest.getNodeLabelExpression());
        }
      }

      Map<String, ResourceRequest> asks = this.resourceRequestMap.get(priority);

      if (asks == null) {
        asks = new ConcurrentHashMap<String, ResourceRequest>();
        this.resourceRequestMap.put(priority, asks);
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
        
        // update queue:
        Resource increasedResource =
            Resources.multiply(request.getCapability(),
                request.getNumContainers());
        queue.incPendingResource(request.getNodeLabelExpression(),
            increasedResource);
        appResourceUsage.incPending(request.getNodeLabelExpression(),
            increasedResource);
        if (lastRequest != null) {
          Resource decreasedResource =
              Resources.multiply(lastRequestCapability, lastRequestContainers);
          queue.decPendingResource(lastRequest.getNodeLabelExpression(),
              decreasedResource);
          appResourceUsage.decPending(lastRequest.getNodeLabelExpression(),
              decreasedResource);
        }
      }
    }
    return anyResourcesUpdated;
  }

  private boolean isRequestLabelChanged(ResourceRequest requestOne,
      ResourceRequest requestTwo) {
    String requestOneLabelExp = requestOne.getNodeLabelExpression();
    String requestTwoLabelExp = requestTwo.getNodeLabelExpression();
    // First request label expression can be null and second request
    // is not null then we have to consider it as changed.
    if ((null == requestOneLabelExp) && (null != requestTwoLabelExp)) {
      return true;
    }
    // If the label is not matching between both request when
    // requestOneLabelExp is not null.
    return ((null != requestOneLabelExp) && !(requestOneLabelExp
        .equals(requestTwoLabelExp)));
  }

  /**
   * The ApplicationMaster is updating the userBlacklist used for containers
   * other than AMs.
   *
   * @param blacklistAdditions resources to be added to the userBlacklist
   * @param blacklistRemovals resources to be removed from the userBlacklist
   */
   public void updateBlacklist(
      List<String> blacklistAdditions, List<String> blacklistRemovals) {
     updateUserOrAMBlacklist(userBlacklist, blacklistAdditions,
         blacklistRemovals);
  }

  /**
   * RM is updating blacklist for AM containers.
   * @param blacklistAdditions resources to be added to the amBlacklist
   * @param blacklistRemovals resources to be added to the amBlacklist
   */
  public void updateAMBlacklist(
      List<String> blacklistAdditions, List<String> blacklistRemovals) {
    updateUserOrAMBlacklist(amBlacklist, blacklistAdditions,
        blacklistRemovals);
  }

  void updateUserOrAMBlacklist(Set<String> blacklist,
      List<String> blacklistAdditions, List<String> blacklistRemovals) {
    synchronized (blacklist) {
      if (blacklistAdditions != null) {
        blacklist.addAll(blacklistAdditions);
      }

      if (blacklistRemovals != null) {
        blacklist.removeAll(blacklistRemovals);
      }
    }
  }

  synchronized public Collection<Priority> getPriorities() {
    return priorities;
  }

  synchronized public Map<String, ResourceRequest> getResourceRequests(
      Priority priority) {
    return resourceRequestMap.get(priority);
  }

  public List<ResourceRequest> getAllResourceRequests() {
    List<ResourceRequest> ret = new ArrayList<ResourceRequest>();
    for (Map<String, ResourceRequest> r : resourceRequestMap.values()) {
      ret.addAll(r.values());
    }
    return ret;
  }

  synchronized public ResourceRequest getResourceRequest(Priority priority,
      String resourceName) {
    Map<String, ResourceRequest> nodeRequests = resourceRequestMap.get(priority);
    return (nodeRequests == null) ? null : nodeRequests.get(resourceName);
  }

  public synchronized Resource getResource(Priority priority) {
    ResourceRequest request = getResourceRequest(priority, ResourceRequest.ANY);
    return (request == null) ? null : request.getCapability();
  }

  /**
   * Returns if the node is either blacklisted by the user or the system
   * @param resourceName the resourcename
   * @param useAMBlacklist true if it should check amBlacklist
   * @return true if its blacklisted
   */
  public boolean isBlacklisted(String resourceName,
      boolean useAMBlacklist) {
    if (useAMBlacklist){
      synchronized (amBlacklist) {
        return amBlacklist.contains(resourceName);
      }
    } else {
      synchronized (userBlacklist) {
        return userBlacklist.contains(resourceName);
      }
    }
  }
  
  public synchronized void increaseContainer(
      SchedContainerChangeRequest increaseRequest) {
    NodeId nodeId = increaseRequest.getNodeId();
    Priority priority = increaseRequest.getPriority();
    ContainerId containerId = increaseRequest.getContainerId();
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("allocated increase request : applicationId=" + applicationId
          + " container=" + containerId + " host="
          + increaseRequest.getNodeId() + " user=" + user + " resource="
          + increaseRequest.getDeltaCapacity());
    }
    
    // Set queue metrics
    queue.getMetrics().allocateResources(user, 0,
        increaseRequest.getDeltaCapacity(), true);
    
    // remove the increase request from pending increase request map
    removeIncreaseRequest(nodeId, priority, containerId);
    
    // update usage
    appResourceUsage.incUsed(increaseRequest.getNodePartition(),
        increaseRequest.getDeltaCapacity());
  }
  
  public synchronized void decreaseContainer(
      SchedContainerChangeRequest decreaseRequest) {
    // Delta is negative when it's a decrease request
    Resource absDelta = Resources.negate(decreaseRequest.getDeltaCapacity());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Decrease container : applicationId=" + applicationId
          + " container=" + decreaseRequest.getContainerId() + " host="
          + decreaseRequest.getNodeId() + " user=" + user + " resource="
          + absDelta);
    }
    
    // Set queue metrics
    queue.getMetrics().releaseResources(user, 0, absDelta);

    // update usage
    appResourceUsage.decUsed(decreaseRequest.getNodePartition(), absDelta);
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
          + " resource=" + request.getCapability()
          + " type=" + type);
    }
    metrics.allocateResources(user, 1, request.getCapability(), true);
    metrics.incrNodeTypeAggregations(user, type);
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

    ResourceRequest rackLocalRequest = resourceRequestMap.get(priority).get(
        node.getRackName());
    decResourceRequest(node.getRackName(), priority, rackLocalRequest);

    ResourceRequest offRackRequest = resourceRequestMap.get(priority).get(
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
      resourceRequestMap.get(priority).remove(resourceName);
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
    
    ResourceRequest offRackRequest = resourceRequestMap.get(priority).get(
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
    
    appResourceUsage.decPending(offSwitchRequest.getNodeLabelExpression(),
        offSwitchRequest.getCapability());
    queue.decPendingResource(offSwitchRequest.getNodeLabelExpression(),
        offSwitchRequest.getCapability());
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
    
    // also we need to check increase request
    if (!deactivate) {
      deactivate = increaseRequestMap.isEmpty();
    }

    if (deactivate) {
      activeUsersManager.deactivateApplication(user, applicationId);
    }
  }
  
  synchronized public void move(Queue newQueue) {
    QueueMetrics oldMetrics = queue.getMetrics();
    QueueMetrics newMetrics = newQueue.getMetrics();
    for (Map<String, ResourceRequest> asks : resourceRequestMap.values()) {
      ResourceRequest request = asks.get(ResourceRequest.ANY);
      if (request != null) {
        oldMetrics.decrPendingResources(user, request.getNumContainers(),
            request.getCapability());
        newMetrics.incrPendingResources(user, request.getNumContainers(),
            request.getCapability());
        
        Resource delta = Resources.multiply(request.getCapability(),
            request.getNumContainers()); 
        // Update Queue
        queue.decPendingResource(request.getNodeLabelExpression(), delta);
        newQueue.incPendingResource(request.getNodeLabelExpression(), delta);
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
    for (Map<String, ResourceRequest> asks : resourceRequestMap.values()) {
      ResourceRequest request = asks.get(ResourceRequest.ANY);
      if (request != null) {
        metrics.decrPendingResources(user, request.getNumContainers(),
            request.getCapability());
        
        // Update Queue
        queue.decPendingResource(
            request.getNodeLabelExpression(),
            Resources.multiply(request.getCapability(),
                request.getNumContainers()));
      }
    }
    metrics.finishAppAttempt(applicationId, pending, user);
    
    // Clear requests themselves
    clearRequests();
  }

  public synchronized void setQueue(Queue queue) {
    this.queue = queue;
  }

  public Set<String> getBlackList() {
    return this.userBlacklist;
  }

  public Set<String> getBlackListCopy() {
    synchronized (userBlacklist) {
      return new HashSet<>(this.userBlacklist);
    }
  }

  public synchronized void transferStateFromPreviousAppSchedulingInfo(
      AppSchedulingInfo appInfo) {
    //    this.priorities = appInfo.getPriorities();
    //    this.requests = appInfo.getRequests();
    // This should not require locking the userBlacklist since it will not be
    // used by this instance until after setCurrentAppAttempt.
    // Should cleanup this to avoid sharing between instances and can
    // then remove getBlacklist as well.
    this.userBlacklist = appInfo.getBlackList();
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
    ResourceRequest newRequest =
        ResourceRequest.newInstance(request.getPriority(),
            request.getResourceName(), request.getCapability(), 1,
            request.getRelaxLocality(), request.getNodeLabelExpression());
    return newRequest;
  }
}

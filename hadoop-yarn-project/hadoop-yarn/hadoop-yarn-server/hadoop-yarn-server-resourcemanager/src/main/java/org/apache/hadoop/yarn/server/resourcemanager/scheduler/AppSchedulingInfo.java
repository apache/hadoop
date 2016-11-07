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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.PlacementSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.ResourceRequestUpdateResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.SchedulingPlacementSet;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * This class keeps track of all the consumption of an application. This also
 * keeps track of current running/completed containers for the application.
 */
@Private
@Unstable
public class AppSchedulingInfo {
  
  private static final Log LOG = LogFactory.getLog(AppSchedulingInfo.class);

  private final ApplicationId applicationId;
  private final ApplicationAttemptId applicationAttemptId;
  private final AtomicLong containerIdCounter;
  private final String user;

  private Queue queue;
  private ActiveUsersManager activeUsersManager;
  // whether accepted/allocated by scheduler
  private volatile boolean pending = true;
  private ResourceUsage appResourceUsage;

  private AtomicBoolean userBlacklistChanged = new AtomicBoolean(false);
  // Set of places (nodes / racks) blacklisted by the system. Today, this only
  // has places blacklisted for AM containers.
  private final Set<String> placesBlacklistedBySystem = new HashSet<>();
  private Set<String> placesBlacklistedByApp = new HashSet<>();

  private Set<String> requestedPartitions = new HashSet<>();

  private final ConcurrentSkipListMap<SchedulerRequestKey, Integer>
      schedulerKeys = new ConcurrentSkipListMap<>();
  final Map<SchedulerRequestKey, Map<String, ResourceRequest>>
      resourceRequestMap = new ConcurrentHashMap<>();
  final Map<NodeId, Map<SchedulerRequestKey, Map<ContainerId,
      SchedContainerChangeRequest>>> containerIncreaseRequestMap =
      new ConcurrentHashMap<>();

  private final ReentrantReadWriteLock.ReadLock readLock;
  private final ReentrantReadWriteLock.WriteLock writeLock;

  public AppSchedulingInfo(ApplicationAttemptId appAttemptId,
      String user, Queue queue, ActiveUsersManager activeUsersManager,
      long epoch, ResourceUsage appResourceUsage) {
    this.applicationAttemptId = appAttemptId;
    this.applicationId = appAttemptId.getApplicationId();
    this.queue = queue;
    this.user = user;
    this.activeUsersManager = activeUsersManager;
    this.containerIdCounter = new AtomicLong(
        epoch << ResourceManager.EPOCH_BIT_SHIFT);
    this.appResourceUsage = appResourceUsage;

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  public ApplicationId getApplicationId() {
    return applicationId;
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    return applicationAttemptId;
  }

  public String getUser() {
    return user;
  }

  public long getNewContainerId() {
    return this.containerIdCounter.incrementAndGet();
  }

  public String getQueueName() {
    try {
      this.readLock.lock();
      return queue.getQueueName();
    } finally {
      this.readLock.unlock();
    }
  }

  public boolean isPending() {
    return pending;
  }

  public Set<String> getRequestedPartitions() {
    return requestedPartitions;
  }

  /**
   * Clear any pending requests from this application.
   */
  private void clearRequests() {
    schedulerKeys.clear();
    resourceRequestMap.clear();
    LOG.info("Application " + applicationId + " requests cleared");
  }

  public boolean hasIncreaseRequest(NodeId nodeId) {
    try {
      this.readLock.lock();
      Map<SchedulerRequestKey, Map<ContainerId, SchedContainerChangeRequest>>
          requestsOnNode = containerIncreaseRequestMap.get(nodeId);
      return requestsOnNode == null ? false : requestsOnNode.size() > 0;
    } finally {
      this.readLock.unlock();
    }
  }

  public Map<ContainerId, SchedContainerChangeRequest>
      getIncreaseRequests(NodeId nodeId, SchedulerRequestKey schedulerKey) {
    try {
      this.readLock.lock();
      Map<SchedulerRequestKey, Map<ContainerId, SchedContainerChangeRequest>>
          requestsOnNode = containerIncreaseRequestMap.get(nodeId);
      return requestsOnNode == null ? null : requestsOnNode.get(
          schedulerKey);
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * return true if any of the existing increase requests are updated,
   *        false if none of them are updated
   */
  public boolean updateIncreaseRequests(
      List<SchedContainerChangeRequest> increaseRequests) {
    boolean resourceUpdated = false;

    try {
      this.writeLock.lock();
      for (SchedContainerChangeRequest r : increaseRequests) {
        if (r.getRMContainer().getState() != RMContainerState.RUNNING) {
          LOG.warn("rmContainer's state is not RUNNING, for increase request"
              + " with container-id=" + r.getContainerId());
          continue;
        }
        try {
          RMServerUtils.checkSchedContainerChangeRequest(r, true);
        } catch (YarnException e) {
          LOG.warn("Error happens when checking increase request, Ignoring.."
              + " exception=", e);
          continue;
        }
        NodeId nodeId = r.getRMContainer().getAllocatedNode();

        Map<SchedulerRequestKey, Map<ContainerId, SchedContainerChangeRequest>>
            requestsOnNode = containerIncreaseRequestMap.get(nodeId);
        if (null == requestsOnNode) {
          requestsOnNode = new TreeMap<>();
          containerIncreaseRequestMap.put(nodeId, requestsOnNode);
        }

        SchedContainerChangeRequest prevChangeRequest =
            getIncreaseRequest(nodeId,
                r.getRMContainer().getAllocatedSchedulerKey(),
                r.getContainerId());
        if (null != prevChangeRequest) {
          if (Resources.equals(prevChangeRequest.getTargetCapacity(),
              r.getTargetCapacity())) {
            // increase request hasn't changed
            continue;
          }

          // remove the old one, as we will use the new one going forward
          removeIncreaseRequest(nodeId,
              prevChangeRequest.getRMContainer().getAllocatedSchedulerKey(),
              prevChangeRequest.getContainerId());
        }

        if (Resources.equals(r.getTargetCapacity(),
            r.getRMContainer().getAllocatedResource())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Trying to increase container " + r.getContainerId()
                + ", target capacity = previous capacity = " + prevChangeRequest
                + ". Will ignore this increase request.");
          }
          continue;
        }

        // add the new one
        resourceUpdated = true;
        insertIncreaseRequest(r);
      }
      return resourceUpdated;
    } finally {
      this.writeLock.unlock();
    }
  }

  /**
   * Insert increase request, adding any missing items in the data-structure
   * hierarchy.
   */
  private void insertIncreaseRequest(SchedContainerChangeRequest request) {
    NodeId nodeId = request.getNodeId();
    SchedulerRequestKey schedulerKey =
        request.getRMContainer().getAllocatedSchedulerKey();
    ContainerId containerId = request.getContainerId();
    
    Map<SchedulerRequestKey, Map<ContainerId, SchedContainerChangeRequest>>
        requestsOnNode = containerIncreaseRequestMap.get(nodeId);
    if (null == requestsOnNode) {
      requestsOnNode = new HashMap<>();
      containerIncreaseRequestMap.put(nodeId, requestsOnNode);
    }

    Map<ContainerId, SchedContainerChangeRequest> requestsOnNodeWithPriority =
        requestsOnNode.get(schedulerKey);
    if (null == requestsOnNodeWithPriority) {
      requestsOnNodeWithPriority = new TreeMap<>();
      requestsOnNode.put(schedulerKey, requestsOnNodeWithPriority);
      incrementSchedulerKeyReference(schedulerKey);
    }

    requestsOnNodeWithPriority.put(containerId, request);

    // update resources
    String partition = request.getRMContainer().getNodeLabelExpression();
    Resource delta = request.getDeltaCapacity();
    appResourceUsage.incPending(partition, delta);
    queue.incPendingResource(partition, delta);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Added increase request:" + request.getContainerId()
          + " delta=" + delta);
    }
  }

  private void incrementSchedulerKeyReference(
      SchedulerRequestKey schedulerKey) {
    Integer schedulerKeyCount = schedulerKeys.get(schedulerKey);
    if (schedulerKeyCount == null) {
      schedulerKeys.put(schedulerKey, 1);
    } else {
      schedulerKeys.put(schedulerKey, schedulerKeyCount + 1);
    }
  }

  private void decrementSchedulerKeyReference(
      SchedulerRequestKey schedulerKey) {
    Integer schedulerKeyCount = schedulerKeys.get(schedulerKey);
    if (schedulerKeyCount != null) {
      if (schedulerKeyCount > 1) {
        schedulerKeys.put(schedulerKey, schedulerKeyCount - 1);
      } else {
        schedulerKeys.remove(schedulerKey);
      }
    }
  }

  public boolean removeIncreaseRequest(NodeId nodeId,
      SchedulerRequestKey schedulerKey, ContainerId containerId) {
    try {
      this.writeLock.lock();
      Map<SchedulerRequestKey, Map<ContainerId, SchedContainerChangeRequest>>
          requestsOnNode = containerIncreaseRequestMap.get(nodeId);
      if (null == requestsOnNode) {
        return false;
      }

      Map<ContainerId, SchedContainerChangeRequest> requestsOnNodeWithPriority =
          requestsOnNode.get(schedulerKey);
      if (null == requestsOnNodeWithPriority) {
        return false;
      }

      SchedContainerChangeRequest request =
          requestsOnNodeWithPriority.remove(containerId);
    
      // remove hierarchies if it becomes empty
      if (requestsOnNodeWithPriority.isEmpty()) {
        requestsOnNode.remove(schedulerKey);
        decrementSchedulerKeyReference(schedulerKey);
      }
      if (requestsOnNode.isEmpty()) {
        containerIncreaseRequestMap.remove(nodeId);
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
    } finally {
      this.writeLock.unlock();
    }
  }
  
  public SchedContainerChangeRequest getIncreaseRequest(NodeId nodeId,
      SchedulerRequestKey schedulerKey, ContainerId containerId) {
    try {
      this.readLock.lock();
      Map<SchedulerRequestKey, Map<ContainerId, SchedContainerChangeRequest>>
          requestsOnNode = containerIncreaseRequestMap.get(nodeId);
      if (null == requestsOnNode) {
        return null;
      }

      Map<ContainerId, SchedContainerChangeRequest> requestsOnNodeWithPriority =
          requestsOnNode.get(schedulerKey);
      return requestsOnNodeWithPriority == null ? null
          : requestsOnNodeWithPriority.get(containerId);
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * The ApplicationMaster is updating resource requirements for the
   * application, by asking for more resources and releasing resources acquired
   * by the application.
   *
   * @param requests
   *          resources to be acquired
   * @param recoverPreemptedRequestForAContainer
   *          recover ResourceRequest on preemption
   * @return true if any resource was updated, false otherwise
   */
  public boolean updateResourceRequests(List<ResourceRequest> requests,
      boolean recoverPreemptedRequestForAContainer) {
    // Flag to track if any incoming requests update "ANY" requests
    boolean anyResourcesUpdated = false;

    try {
      this.writeLock.lock();
      // Update resource requests
      for (ResourceRequest request : requests) {
        SchedulerRequestKey schedulerKey = SchedulerRequestKey.create(request);
        String resourceName = request.getResourceName();

        // Update node labels if required
        updateNodeLabels(request);

        Map<String, ResourceRequest> asks =
            this.resourceRequestMap.get(schedulerKey);
        if (asks == null) {
          asks = new ConcurrentHashMap<>();
          this.resourceRequestMap.put(schedulerKey, asks);
        }

        // Increment number of containers if recovering preempted resources
        ResourceRequest lastRequest = asks.get(resourceName);
        if (recoverPreemptedRequestForAContainer && lastRequest != null) {
          request.setNumContainers(lastRequest.getNumContainers() + 1);
        }

        // Update asks
        asks.put(resourceName, request);

        if (resourceName.equals(ResourceRequest.ANY)) {
          //update the applications requested labels set
          requestedPartitions.add(request.getNodeLabelExpression() == null
              ? RMNodeLabelsManager.NO_LABEL :
                  request.getNodeLabelExpression());

          anyResourcesUpdated = true;

          // Update pendingResources
          updatePendingResources(lastRequest, request, schedulerKey,
              queue.getMetrics());
        }
      }
      return anyResourcesUpdated;
    } finally {
      this.writeLock.unlock();
    }
  }

  private void updatePendingResources(ResourceRequest lastRequest,
      ResourceRequest request, SchedulerRequestKey schedulerKey,
      QueueMetrics metrics) {
    int lastRequestContainers =
        (lastRequest != null) ? lastRequest.getNumContainers() : 0;
    if (request.getNumContainers() <= 0) {
      if (lastRequestContainers >= 0) {
        decrementSchedulerKeyReference(schedulerKey);
      }
      LOG.info("checking for deactivate of application :"
          + this.applicationId);
      checkForDeactivation();
    } else {
      // Activate application. Metrics activation is done here.
      if (lastRequestContainers <= 0) {
        incrementSchedulerKeyReference(schedulerKey);
        activeUsersManager.activateApplication(user, applicationId);
      }
    }

    Resource lastRequestCapability =
        lastRequest != null ? lastRequest.getCapability() : Resources.none();
    metrics.incrPendingResources(user,
        request.getNumContainers(), request.getCapability());
    metrics.decrPendingResources(user,
        lastRequestContainers, lastRequestCapability);

    // update queue:
    Resource increasedResource =
        Resources.multiply(request.getCapability(), request.getNumContainers());
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

  private void updateNodeLabels(ResourceRequest request) {
    SchedulerRequestKey schedulerKey = SchedulerRequestKey.create(request);
    String resourceName = request.getResourceName();
    if (resourceName.equals(ResourceRequest.ANY)) {
      ResourceRequest previousAnyRequest =
          getResourceRequest(schedulerKey, resourceName);

      // When there is change in ANY request label expression, we should
      // update label for all resource requests already added of same
      // priority as ANY resource request.
      if ((null == previousAnyRequest)
          || hasRequestLabelChanged(previousAnyRequest, request)) {
        Map<String, ResourceRequest> resourceRequest =
            getResourceRequests(schedulerKey);
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
          getResourceRequest(schedulerKey, ResourceRequest.ANY);
      if (anyRequest != null) {
        request.setNodeLabelExpression(anyRequest.getNodeLabelExpression());
      }
    }
  }

  private boolean hasRequestLabelChanged(ResourceRequest requestOne,
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
   * The ApplicationMaster is updating the placesBlacklistedByApp used for
   * containers other than AMs.
   *
   * @param blacklistAdditions
   *          resources to be added to the userBlacklist
   * @param blacklistRemovals
   *          resources to be removed from the userBlacklist
   */
  public void updatePlacesBlacklistedByApp(
      List<String> blacklistAdditions, List<String> blacklistRemovals) {
    if (updateBlacklistedPlaces(placesBlacklistedByApp, blacklistAdditions,
        blacklistRemovals)) {
      userBlacklistChanged.set(true);
    }
  }

  /**
   * Update the list of places that are blacklisted by the system. Today the
   * system only blacklists places when it sees that AMs failed there
   *
   * @param blacklistAdditions
   *          resources to be added to placesBlacklistedBySystem
   * @param blacklistRemovals
   *          resources to be removed from placesBlacklistedBySystem
   */
  public void updatePlacesBlacklistedBySystem(
      List<String> blacklistAdditions, List<String> blacklistRemovals) {
    updateBlacklistedPlaces(placesBlacklistedBySystem, blacklistAdditions,
        blacklistRemovals);
  }

  private static boolean updateBlacklistedPlaces(Set<String> blacklist,
      List<String> blacklistAdditions, List<String> blacklistRemovals) {
    boolean changed = false;
    synchronized (blacklist) {
      if (blacklistAdditions != null) {
        changed = blacklist.addAll(blacklistAdditions);
      }

      if (blacklistRemovals != null) {
        changed = blacklist.removeAll(blacklistRemovals) || changed;
      }
    }
    return changed;
  }

  public boolean getAndResetBlacklistChanged() {
    return userBlacklistChanged.getAndSet(false);
  }

  public Collection<SchedulerRequestKey> getSchedulerKeys() {
    return schedulerKeys.keySet();
  }

  public Map<String, ResourceRequest> getResourceRequests(
      SchedulerRequestKey schedulerKey) {
    return resourceRequestMap.get(schedulerKey);
  }

  public List<ResourceRequest> getAllResourceRequests() {
    List<ResourceRequest> ret = new ArrayList<>();
    try {
      this.readLock.lock();
      for (Map<String, ResourceRequest> r : resourceRequestMap.values()) {
        ret.addAll(r.values());
      }
    } finally {
      this.readLock.unlock();
    }
    return ret;
  }

  public ResourceRequest getResourceRequest(SchedulerRequestKey schedulerKey,
      String resourceName) {
    try {
      this.readLock.lock();
      Map<String, ResourceRequest> nodeRequests =
          resourceRequestMap.get(schedulerKey);
      return (nodeRequests == null) ? null : nodeRequests.get(resourceName);
    } finally {
      this.readLock.unlock();
    }
  }

  public Resource getResource(SchedulerRequestKey schedulerKey) {
    try {
      this.readLock.lock();
      ResourceRequest request =
          getResourceRequest(schedulerKey, ResourceRequest.ANY);
      return (request == null) ? null : request.getCapability();
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * Returns if the place (node/rack today) is either blacklisted by the
   * application (user) or the system
   *
   * @param resourceName
   *          the resourcename
   * @param blacklistedBySystem
   *          true if it should check amBlacklist
   * @return true if its blacklisted
   */
  public boolean isPlaceBlacklisted(String resourceName,
      boolean blacklistedBySystem) {
    if (blacklistedBySystem){
      synchronized (placesBlacklistedBySystem) {
        return placesBlacklistedBySystem.contains(resourceName);
      }
    } else {
      synchronized (placesBlacklistedByApp) {
        return placesBlacklistedByApp.contains(resourceName);
      }
    }
  }

  public void increaseContainer(SchedContainerChangeRequest increaseRequest) {
    NodeId nodeId = increaseRequest.getNodeId();
    SchedulerRequestKey schedulerKey =
        increaseRequest.getRMContainer().getAllocatedSchedulerKey();
    ContainerId containerId = increaseRequest.getContainerId();
    Resource deltaCapacity = increaseRequest.getDeltaCapacity();

    if (LOG.isDebugEnabled()) {
      LOG.debug("allocated increase request : applicationId=" + applicationId
          + " container=" + containerId + " host="
          + increaseRequest.getNodeId() + " user=" + user + " resource="
          + deltaCapacity);
    }
    try {
      this.writeLock.lock();
      // Set queue metrics
      queue.getMetrics().allocateResources(user, deltaCapacity);
      // remove the increase request from pending increase request map
      removeIncreaseRequest(nodeId, schedulerKey, containerId);
      // update usage
      appResourceUsage.incUsed(increaseRequest.getNodePartition(),
          deltaCapacity);
    } finally {
      this.writeLock.unlock();
    }
  }
  
  public void decreaseContainer(SchedContainerChangeRequest decreaseRequest) {
    // Delta is negative when it's a decrease request
    Resource absDelta = Resources.negate(decreaseRequest.getDeltaCapacity());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Decrease container : applicationId=" + applicationId
          + " container=" + decreaseRequest.getContainerId() + " host="
          + decreaseRequest.getNodeId() + " user=" + user + " resource="
          + absDelta);
    }

    try {
      this.writeLock.lock();
      // Set queue metrics
      queue.getMetrics().releaseResources(user, absDelta);

      // update usage
      appResourceUsage.decUsed(decreaseRequest.getNodePartition(), absDelta);
    } finally {
      this.writeLock.unlock();
    }
  }

  public List<ResourceRequest> allocate(NodeType type,
      SchedulerNode node, SchedulerRequestKey schedulerKey,
      Container containerAllocated) {
    try {
      writeLock.lock();
      ResourceRequest request;
      if (type == NodeType.NODE_LOCAL) {
        request = resourceRequestMap.get(schedulerKey).get(node.getNodeName());
      } else if (type == NodeType.RACK_LOCAL) {
        request = resourceRequestMap.get(schedulerKey).get(node.getRackName());
      } else{
        request = resourceRequestMap.get(schedulerKey).get(ResourceRequest.ANY);
      }
      return allocate(type, node, schedulerKey, request, containerAllocated);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Resources have been allocated to this application by the resource
   * scheduler. Track them.
   * @param type Node Type
   * @param node SchedulerNode
   * @param schedulerKey SchedulerRequestKey
   * @param request ResourceRequest
   * @param containerAllocated Container Allocated
   * @return List of ResourceRequests
   */
  public List<ResourceRequest> allocate(NodeType type,
      SchedulerNode node, SchedulerRequestKey schedulerKey,
      ResourceRequest request, Container containerAllocated) {
    try {
      writeLock.lock();
      List<ResourceRequest> resourceRequests = new ArrayList<>();
      if (type == NodeType.NODE_LOCAL) {
        allocateNodeLocal(node, schedulerKey, request, resourceRequests);
      } else if (type == NodeType.RACK_LOCAL) {
        allocateRackLocal(node, schedulerKey, request, resourceRequests);
      } else{
        allocateOffSwitch(request, resourceRequests, schedulerKey);
      }

      if (null != containerAllocated) {
        updateMetricsForAllocatedContainer(request, type, containerAllocated);
      }
      return resourceRequests;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   */
  private void allocateNodeLocal(SchedulerNode node,
      SchedulerRequestKey schedulerKey, ResourceRequest nodeLocalRequest,
      List<ResourceRequest> resourceRequests) {
    // Update future requirements
    decResourceRequest(node.getNodeName(), schedulerKey, nodeLocalRequest);

    ResourceRequest rackLocalRequest = resourceRequestMap.get(schedulerKey).get(
        node.getRackName());
    decResourceRequest(node.getRackName(), schedulerKey, rackLocalRequest);

    ResourceRequest offRackRequest = resourceRequestMap.get(schedulerKey).get(
        ResourceRequest.ANY);
    decrementOutstanding(offRackRequest, schedulerKey);

    // Update cloned NodeLocal, RackLocal and OffRack requests for recovery
    resourceRequests.add(cloneResourceRequest(nodeLocalRequest));
    resourceRequests.add(cloneResourceRequest(rackLocalRequest));
    resourceRequests.add(cloneResourceRequest(offRackRequest));
  }

  private void decResourceRequest(String resourceName,
      SchedulerRequestKey schedulerKey, ResourceRequest request) {
    request.setNumContainers(request.getNumContainers() - 1);
    if (request.getNumContainers() == 0) {
      resourceRequestMap.get(schedulerKey).remove(resourceName);
    }
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   */
  private void allocateRackLocal(SchedulerNode node,
      SchedulerRequestKey schedulerKey, ResourceRequest rackLocalRequest,
      List<ResourceRequest> resourceRequests) {
    // Update future requirements
    decResourceRequest(node.getRackName(), schedulerKey, rackLocalRequest);
    
    ResourceRequest offRackRequest = resourceRequestMap.get(schedulerKey).get(
        ResourceRequest.ANY);
    decrementOutstanding(offRackRequest, schedulerKey);

    // Update cloned RackLocal and OffRack requests for recovery
    resourceRequests.add(cloneResourceRequest(rackLocalRequest));
    resourceRequests.add(cloneResourceRequest(offRackRequest));
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   */
  private void allocateOffSwitch(ResourceRequest offSwitchRequest,
      List<ResourceRequest> resourceRequests,
      SchedulerRequestKey schedulerKey) {
    // Update future requirements
    decrementOutstanding(offSwitchRequest, schedulerKey);
    // Update cloned OffRack requests for recovery
    resourceRequests.add(cloneResourceRequest(offSwitchRequest));
  }

  private void decrementOutstanding(ResourceRequest offSwitchRequest,
      SchedulerRequestKey schedulerKey) {
    int numOffSwitchContainers = offSwitchRequest.getNumContainers() - 1;

    // Do not remove ANY
    offSwitchRequest.setNumContainers(numOffSwitchContainers);
    
    // Do we have any outstanding requests?
    // If there is nothing, we need to deactivate this application
    if (numOffSwitchContainers == 0) {
      decrementSchedulerKeyReference(schedulerKey);
      checkForDeactivation();
    }
    
    appResourceUsage.decPending(offSwitchRequest.getNodeLabelExpression(),
        offSwitchRequest.getCapability());
    queue.decPendingResource(offSwitchRequest.getNodeLabelExpression(),
        offSwitchRequest.getCapability());
  }

  private void checkForDeactivation() {
    if (schedulerKeys.isEmpty()) {
      activeUsersManager.deactivateApplication(user, applicationId);
    }
  }
  
  public void move(Queue newQueue) {
    try {
      this.writeLock.lock();
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
    } finally {
      this.writeLock.unlock();
    }
  }

  public void stop() {
    // clear pending resources metrics for the application
    try {
      this.writeLock.lock();
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
    } finally {
      this.writeLock.unlock();
    }
  }

  public void setQueue(Queue queue) {
    try {
      this.writeLock.lock();
      this.queue = queue;
    } finally {
      this.writeLock.unlock();
    }
  }

  private Set<String> getBlackList() {
    return this.placesBlacklistedByApp;
  }

  public Set<String> getBlackListCopy() {
    synchronized (placesBlacklistedByApp) {
      return new HashSet<>(this.placesBlacklistedByApp);
    }
  }

  public void transferStateFromPreviousAppSchedulingInfo(
      AppSchedulingInfo appInfo) {
    // This should not require locking the placesBlacklistedByApp since it will
    // not be used by this instance until after setCurrentAppAttempt.
    this.placesBlacklistedByApp = appInfo.getBlackList();
  }

  public void recoverContainer(RMContainer rmContainer) {
    try {
      this.writeLock.lock();
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
    } finally {
      this.writeLock.unlock();
    }
  }

  public ResourceRequest cloneResourceRequest(ResourceRequest request) {
    ResourceRequest newRequest =
        ResourceRequest.newInstance(request.getPriority(),
            request.getResourceName(), request.getCapability(), 1,
            request.getRelaxLocality(), request.getNodeLabelExpression());
    return newRequest;
  }

  /*
   * In async environment, pending resource request could be updated during
   * scheduling, this method checks pending request before allocating
   */
  public boolean checkAllocation(NodeType type, SchedulerNode node,
      SchedulerRequestKey schedulerKey) {
    try {
      readLock.lock();
      ResourceRequest r = resourceRequestMap.get(schedulerKey).get(
          ResourceRequest.ANY);
      if (r == null || r.getNumContainers() <= 0) {
        return false;
      }
      if (type == NodeType.RACK_LOCAL || type == NodeType.NODE_LOCAL) {
        r = resourceRequestMap.get(schedulerKey).get(node.getRackName());
        if (r == null || r.getNumContainers() <= 0) {
          return false;
        }
        if (type == NodeType.NODE_LOCAL) {
          r = resourceRequestMap.get(schedulerKey).get(node.getNodeName());
          if (r == null || r.getNumContainers() <= 0) {
            return false;
          }
        }
      }

      return true;
    } finally {
      readLock.unlock();
    }
  }

  public void updateMetricsForAllocatedContainer(
      ResourceRequest request, NodeType type, Container containerAllocated) {
    try {
      writeLock.lock();
      QueueMetrics metrics = queue.getMetrics();
      if (pending) {
        // once an allocation is done we assume the application is
        // running from scheduler's POV.
        pending = false;
        metrics.runAppAttempt(applicationId, user);
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("allocate: applicationId=" + applicationId + " container="
            + containerAllocated.getId() + " host=" + containerAllocated
            .getNodeId().toString() + " user=" + user + " resource=" + request
            .getCapability() + " type=" + type);
      }
      metrics.allocateResources(user, 1, request.getCapability(), true);
      metrics.incrNodeTypeAggregations(user, type);
    } finally {
      writeLock.unlock();
    }
  }

  // Get placement-set by specified schedulerKey
  // Now simply return all node of the input clusterPlacementSet
  // TODO, need update this when we support global scheduling
  public <N extends SchedulerNode> SchedulingPlacementSet<N> getSchedulingPlacementSet(
      SchedulerRequestKey schedulerkey) {
    return new SchedulingPlacementSet<N>() {
      @Override
      @SuppressWarnings("unchecked")
      public Iterator<N> getPreferredNodeIterator(
          PlacementSet<N> clusterPlacementSet) {
        return IteratorUtils.singletonIterator(
            clusterPlacementSet.getAllNodes().values().iterator().next());
      }

      @Override
      public ResourceRequestUpdateResult updateResourceRequests(
          List<ResourceRequest> requests,
          boolean recoverPreemptedRequestForAContainer) {
        return null;
      }

      @Override
      public Map<String, ResourceRequest> getResourceRequests() {
        return null;
      }

      @Override
      public ResourceRequest getResourceRequest(String resourceName,
          SchedulerRequestKey requestKey) {
        return null;
      }

      @Override
      public List<ResourceRequest> allocate(NodeType type, SchedulerNode node,
          ResourceRequest request) {
        return null;
      }

      @Override
      public Map<NodeId, N> getAllNodes() {
        return null;
      }

      @Override
      public long getVersion() {
        return 0;
      }

      @Override
      public String getPartition() {
        return null;
      }
    };
  }
}

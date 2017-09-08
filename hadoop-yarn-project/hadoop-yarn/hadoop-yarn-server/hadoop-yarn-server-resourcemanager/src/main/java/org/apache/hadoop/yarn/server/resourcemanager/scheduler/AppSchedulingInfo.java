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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.LocalitySchedulingPlacementSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.ResourceRequestUpdateResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.SchedulingPlacementSet;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
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
  private AbstractUsersManager abstractUsersManager;
  // whether accepted/allocated by scheduler
  private volatile boolean pending = true;
  private ResourceUsage appResourceUsage;

  private AtomicBoolean userBlacklistChanged = new AtomicBoolean(false);
  // Set of places (nodes / racks) blacklisted by the system. Today, this only
  // has places blacklisted for AM containers.
  private final Set<String> placesBlacklistedBySystem = new HashSet<>();
  private Set<String> placesBlacklistedByApp = new HashSet<>();

  private Set<String> requestedPartitions = new HashSet<>();

  private final ConcurrentSkipListSet<SchedulerRequestKey>
      schedulerKeys = new ConcurrentSkipListSet<>();
  final Map<SchedulerRequestKey, SchedulingPlacementSet<SchedulerNode>>
      schedulerKeyToPlacementSets = new ConcurrentHashMap<>();

  private final ReentrantReadWriteLock.ReadLock readLock;
  private final ReentrantReadWriteLock.WriteLock writeLock;

  public final ContainerUpdateContext updateContext;

  public AppSchedulingInfo(ApplicationAttemptId appAttemptId,
      String user, Queue queue, AbstractUsersManager abstractUsersManager,
      long epoch, ResourceUsage appResourceUsage) {
    this.applicationAttemptId = appAttemptId;
    this.applicationId = appAttemptId.getApplicationId();
    this.queue = queue;
    this.user = user;
    this.abstractUsersManager = abstractUsersManager;
    this.containerIdCounter = new AtomicLong(
        epoch << ResourceManager.EPOCH_BIT_SHIFT);
    this.appResourceUsage = appResourceUsage;

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    updateContext = new ContainerUpdateContext(this);
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
    schedulerKeyToPlacementSets.clear();
    LOG.info("Application " + applicationId + " requests cleared");
  }

  public ContainerUpdateContext getUpdateContext() {
    return updateContext;
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
    if (null == requests || requests.isEmpty()) {
      return false;
    }

    // Flag to track if any incoming requests update "ANY" requests
    boolean offswitchResourcesUpdated = false;

    try {
      this.writeLock.lock();

      // A map to group resource requests and dedup
      Map<SchedulerRequestKey, Map<String, ResourceRequest>> dedupRequests =
          new HashMap<>();

      // Group resource request by schedulerRequestKey and resourceName
      for (ResourceRequest request : requests) {
        SchedulerRequestKey schedulerKey = SchedulerRequestKey.create(request);
        if (!dedupRequests.containsKey(schedulerKey)) {
          dedupRequests.put(schedulerKey, new HashMap<>());
        }
        dedupRequests.get(schedulerKey).put(request.getResourceName(), request);
      }

      // Update scheduling placement set
      offswitchResourcesUpdated =
          addToPlacementSets(
              recoverPreemptedRequestForAContainer, dedupRequests);

      return offswitchResourcesUpdated;
    } finally {
      this.writeLock.unlock();
    }
  }

  public void removePlacementSets(SchedulerRequestKey schedulerRequestKey) {
    schedulerKeyToPlacementSets.remove(schedulerRequestKey);
  }

  boolean addToPlacementSets(
      boolean recoverPreemptedRequestForAContainer,
      Map<SchedulerRequestKey, Map<String, ResourceRequest>> dedupRequests) {
    boolean offswitchResourcesUpdated = false;
    for (Map.Entry<SchedulerRequestKey, Map<String, ResourceRequest>> entry :
        dedupRequests.entrySet()) {
      SchedulerRequestKey schedulerRequestKey = entry.getKey();

      if (!schedulerKeyToPlacementSets.containsKey(schedulerRequestKey)) {
        schedulerKeyToPlacementSets.put(schedulerRequestKey,
            new LocalitySchedulingPlacementSet<>(this));
      }

      // Update placement set
      ResourceRequestUpdateResult pendingAmountChanges =
          schedulerKeyToPlacementSets.get(schedulerRequestKey)
              .updateResourceRequests(
                  entry.getValue().values(),
                  recoverPreemptedRequestForAContainer);

      if (null != pendingAmountChanges) {
        updatePendingResources(
            pendingAmountChanges.getLastAnyResourceRequest(),
            pendingAmountChanges.getNewResourceRequest(), schedulerRequestKey,
            queue.getMetrics());
        offswitchResourcesUpdated = true;
      }
    }
    return offswitchResourcesUpdated;
  }

  private void updatePendingResources(ResourceRequest lastRequest,
      ResourceRequest request, SchedulerRequestKey schedulerKey,
      QueueMetrics metrics) {
    int lastRequestContainers =
        (lastRequest != null) ? lastRequest.getNumContainers() : 0;
    if (request.getNumContainers() <= 0) {
      if (lastRequestContainers >= 0) {
        schedulerKeys.remove(schedulerKey);
        schedulerKeyToPlacementSets.remove(schedulerKey);
      }
      LOG.info("checking for deactivate of application :"
          + this.applicationId);
      checkForDeactivation();
    } else {
      // Activate application. Metrics activation is done here.
      if (lastRequestContainers <= 0) {
        schedulerKeys.add(schedulerKey);
        abstractUsersManager.activateApplication(user, applicationId);
      }
    }

    Resource lastRequestCapability =
        lastRequest != null ? lastRequest.getCapability() : Resources.none();
    metrics.incrPendingResources(request.getNodeLabelExpression(), user,
        request.getNumContainers(), request.getCapability());

    if(lastRequest != null) {
      metrics.decrPendingResources(lastRequest.getNodeLabelExpression(), user,
          lastRequestContainers, lastRequestCapability);
    }

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

  public void addRequestedPartition(String partition) {
    requestedPartitions.add(partition);
  }

  public void decPendingResource(String partition, Resource toDecrease) {
    queue.decPendingResource(partition, toDecrease);
    appResourceUsage.decPending(partition, toDecrease);
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
    return schedulerKeys;
  }

  /**
   * Used by REST API to fetch ResourceRequest
   * @return All pending ResourceRequests.
   */
  public List<ResourceRequest> getAllResourceRequests() {
    List<ResourceRequest> ret = new ArrayList<>();
    try {
      this.readLock.lock();
      for (SchedulingPlacementSet ps : schedulerKeyToPlacementSets.values()) {
        ret.addAll(ps.getResourceRequests().values());
      }
    } finally {
      this.readLock.unlock();
    }
    return ret;
  }

  public PendingAsk getNextPendingAsk() {
    try {
      readLock.lock();
      SchedulerRequestKey firstRequestKey = schedulerKeys.first();
      return getPendingAsk(firstRequestKey, ResourceRequest.ANY);
    } finally {
      readLock.unlock();
    }

  }

  public PendingAsk getPendingAsk(SchedulerRequestKey schedulerKey) {
    return getPendingAsk(schedulerKey, ResourceRequest.ANY);
  }

  public PendingAsk getPendingAsk(SchedulerRequestKey schedulerKey,
      String resourceName) {
    try {
      this.readLock.lock();
      SchedulingPlacementSet ps = schedulerKeyToPlacementSets.get(schedulerKey);
      return (ps == null) ? PendingAsk.ZERO : ps.getPendingAsk(resourceName);
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * Returns if the place (node/rack today) is either blacklisted by the
   * application (user) or the system.
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

  public List<ResourceRequest> allocate(NodeType type,
      SchedulerNode node, SchedulerRequestKey schedulerKey,
      Container containerAllocated) {
    try {
      writeLock.lock();

      if (null != containerAllocated) {
        updateMetricsForAllocatedContainer(type, node, containerAllocated);
      }

      return schedulerKeyToPlacementSets.get(schedulerKey).allocate(
          schedulerKey, type, node);
    } finally {
      writeLock.unlock();
    }
  }

  public void checkForDeactivation() {
    if (schedulerKeys.isEmpty()) {
      abstractUsersManager.deactivateApplication(user, applicationId);
    }
  }
  
  public void move(Queue newQueue) {
    try {
      this.writeLock.lock();
      QueueMetrics oldMetrics = queue.getMetrics();
      QueueMetrics newMetrics = newQueue.getMetrics();
      for (SchedulingPlacementSet ps : schedulerKeyToPlacementSets.values()) {
        PendingAsk ask = ps.getPendingAsk(ResourceRequest.ANY);
        if (ask.getCount() > 0) {
          oldMetrics.decrPendingResources(
              ps.getPrimaryRequestedNodePartition(),
              user, ask.getCount(), ask.getPerAllocationResource());
          newMetrics.incrPendingResources(
              ps.getPrimaryRequestedNodePartition(),
              user, ask.getCount(), ask.getPerAllocationResource());

          Resource delta = Resources.multiply(ask.getPerAllocationResource(),
              ask.getCount());
          // Update Queue
          queue.decPendingResource(
              ps.getPrimaryRequestedNodePartition(), delta);
          newQueue.incPendingResource(
              ps.getPrimaryRequestedNodePartition(), delta);
        }
      }
      oldMetrics.moveAppFrom(this);
      newMetrics.moveAppTo(this);
      abstractUsersManager.deactivateApplication(user, applicationId);
      abstractUsersManager = newQueue.getAbstractUsersManager();
      abstractUsersManager.activateApplication(user, applicationId);
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
      for (SchedulingPlacementSet ps : schedulerKeyToPlacementSets.values()) {
        PendingAsk ask = ps.getPendingAsk(ResourceRequest.ANY);
        if (ask.getCount() > 0) {
          metrics.decrPendingResources(ps.getPrimaryRequestedNodePartition(),
              user, ask.getCount(), ask.getPerAllocationResource());

          // Update Queue
          queue.decPendingResource(
              ps.getPrimaryRequestedNodePartition(),
              Resources.multiply(ask.getPerAllocationResource(),
                  ask.getCount()));
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

  public void recoverContainer(RMContainer rmContainer, String partition) {
    if (rmContainer.getExecutionType() != ExecutionType.GUARANTEED) {
      return;
    }
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

      metrics.allocateResources(partition, user, 1,
          rmContainer.getAllocatedResource(), false);
    } finally {
      this.writeLock.unlock();
    }
  }

  /*
   * In async environment, pending resource request could be updated during
   * scheduling, this method checks pending request before allocating
   */
  public boolean checkAllocation(NodeType type, SchedulerNode node,
      SchedulerRequestKey schedulerKey) {
    try {
      readLock.lock();
      SchedulingPlacementSet ps = schedulerKeyToPlacementSets.get(schedulerKey);
      if (null == ps) {
        return false;
      }
      return ps.canAllocate(type, node);
    } finally {
      readLock.unlock();
    }
  }

  private void updateMetricsForAllocatedContainer(NodeType type,
      SchedulerNode node, Container containerAllocated) {
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
          .getNodeId().toString() + " user=" + user + " resource="
          + containerAllocated.getResource() + " type="
          + type);
    }
    if(node != null) {
      metrics.allocateResources(node.getPartition(), user, 1,
          containerAllocated.getResource(), true);
    }
    metrics.incrNodeTypeAggregations(user, type);
  }

  // Get placement-set by specified schedulerKey
  // Now simply return all node of the input clusterPlacementSet
  public <N extends SchedulerNode> SchedulingPlacementSet<N> getSchedulingPlacementSet(
      SchedulerRequestKey schedulerkey) {
    return (SchedulingPlacementSet<N>) schedulerKeyToPlacementSets.get(
        schedulerkey);
  }

  /**
   * Can delay to next?.
   *
   * @param schedulerKey schedulerKey
   * @param resourceName resourceName
   *
   * @return If request exists, return {relaxLocality}
   *         Otherwise, return true.
   */
  public boolean canDelayTo(
      SchedulerRequestKey schedulerKey, String resourceName) {
    try {
      this.readLock.lock();
      SchedulingPlacementSet ps =
          schedulerKeyToPlacementSets.get(schedulerKey);
      return (ps == null) || ps.canDelayTo(resourceName);
    } finally {
      this.readLock.unlock();
    }
  }

  public boolean acceptNodePartition(SchedulerRequestKey schedulerKey,
      String nodePartition, SchedulingMode schedulingMode) {
    try {
      this.readLock.lock();
      SchedulingPlacementSet ps =
          schedulerKeyToPlacementSets.get(schedulerKey);
      return (ps != null) && ps.acceptNodePartition(nodePartition,
          schedulingMode);
    } finally {
      this.readLock.unlock();
    }
  }
}

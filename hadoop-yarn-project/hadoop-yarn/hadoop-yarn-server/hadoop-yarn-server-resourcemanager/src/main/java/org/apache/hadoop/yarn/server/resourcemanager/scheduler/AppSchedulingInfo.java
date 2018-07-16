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
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ApplicationSchedulingConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.AppPlacementAllocator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.PendingAskUpdateResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.SingleConstraintAppPlacementAllocator;
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
  private final Map<SchedulerRequestKey, AppPlacementAllocator<SchedulerNode>>
      schedulerKeyToAppPlacementAllocator = new ConcurrentHashMap<>();

  private final ReentrantReadWriteLock.ReadLock readLock;
  private final ReentrantReadWriteLock.WriteLock writeLock;

  public final ContainerUpdateContext updateContext;
  public final Map<String, String> applicationSchedulingEnvs = new HashMap<>();
  private final RMContext rmContext;

  public AppSchedulingInfo(ApplicationAttemptId appAttemptId, String user,
      Queue queue, AbstractUsersManager abstractUsersManager, long epoch,
      ResourceUsage appResourceUsage,
      Map<String, String> applicationSchedulingEnvs, RMContext rmContext) {
    this.applicationAttemptId = appAttemptId;
    this.applicationId = appAttemptId.getApplicationId();
    this.queue = queue;
    this.user = user;
    this.abstractUsersManager = abstractUsersManager;
    this.containerIdCounter = new AtomicLong(
        epoch << ResourceManager.EPOCH_BIT_SHIFT);
    this.appResourceUsage = appResourceUsage;
    this.applicationSchedulingEnvs.putAll(applicationSchedulingEnvs);
    this.rmContext = rmContext;

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
    schedulerKeyToAppPlacementAllocator.clear();
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
   * @param resourceRequests resource requests to be allocated
   * @param recoverPreemptedRequestForAContainer
   *          recover ResourceRequest/SchedulingRequest on preemption
   * @return true if any resource was updated, false otherwise
   */
  public boolean updateResourceRequests(List<ResourceRequest> resourceRequests,
      boolean recoverPreemptedRequestForAContainer) {
    // Flag to track if any incoming requests update "ANY" requests
    boolean offswitchResourcesUpdated;

    writeLock.lock();
    try {
      // Update AppPlacementAllocator by requests
      offswitchResourcesUpdated = internalAddResourceRequests(
          recoverPreemptedRequestForAContainer, resourceRequests);
    } finally {
      writeLock.unlock();
    }

    return offswitchResourcesUpdated;
  }

  /**
   * The ApplicationMaster is updating resource requirements for the
   * application, by asking for more resources and releasing resources acquired
   * by the application.
   *
   * @param dedupRequests (dedup) resource requests to be allocated
   * @param recoverPreemptedRequestForAContainer
   *          recover ResourceRequest/SchedulingRequest on preemption
   * @return true if any resource was updated, false otherwise
   */
  public boolean updateResourceRequests(
      Map<SchedulerRequestKey, Map<String, ResourceRequest>> dedupRequests,
      boolean recoverPreemptedRequestForAContainer) {
    // Flag to track if any incoming requests update "ANY" requests
    boolean offswitchResourcesUpdated;

    writeLock.lock();
    try {
      // Update AppPlacementAllocator by requests
      offswitchResourcesUpdated = internalAddResourceRequests(
          recoverPreemptedRequestForAContainer, dedupRequests);
    } finally {
      writeLock.unlock();
    }

    return offswitchResourcesUpdated;
  }

  /**
   * The ApplicationMaster is updating resource requirements for the
   * application, by asking for more resources and releasing resources acquired
   * by the application.
   *
   * @param schedulingRequests resource requests to be allocated
   * @param recoverPreemptedRequestForAContainer
   *          recover ResourceRequest/SchedulingRequest on preemption
   * @return true if any resource was updated, false otherwise
   */
  public boolean updateSchedulingRequests(
      List<SchedulingRequest> schedulingRequests,
      boolean recoverPreemptedRequestForAContainer) {
    // Flag to track if any incoming requests update "ANY" requests
    boolean offswitchResourcesUpdated;

    writeLock.lock();
    try {
      // Update AppPlacementAllocator by requests
      offswitchResourcesUpdated = addSchedulingRequests(
          recoverPreemptedRequestForAContainer, schedulingRequests);
    } finally {
      writeLock.unlock();
    }

    return offswitchResourcesUpdated;
  }

  public void removeAppPlacement(SchedulerRequestKey schedulerRequestKey) {
    schedulerKeyToAppPlacementAllocator.remove(schedulerRequestKey);
  }

  private boolean addSchedulingRequests(
      boolean recoverPreemptedRequestForAContainer,
      List<SchedulingRequest> schedulingRequests) {
    // Do we need to update pending resource for app/queue, etc.?
    boolean requireUpdatePendingResource = false;

    for (SchedulingRequest request : schedulingRequests) {
      SchedulerRequestKey schedulerRequestKey = SchedulerRequestKey.create(
          request);

      AppPlacementAllocator appPlacementAllocator =
          getAndAddAppPlacementAllocatorIfNotExist(schedulerRequestKey,
              SingleConstraintAppPlacementAllocator.class.getCanonicalName());

      // Update AppPlacementAllocator
      PendingAskUpdateResult pendingAmountChanges =
          appPlacementAllocator.updatePendingAsk(schedulerRequestKey,
              request, recoverPreemptedRequestForAContainer);

      if (null != pendingAmountChanges) {
        updatePendingResources(pendingAmountChanges, schedulerRequestKey,
            queue.getMetrics());
        requireUpdatePendingResource = true;
      }
    }

    return requireUpdatePendingResource;
  }

  /**
   * Get and insert AppPlacementAllocator if it doesn't exist, this should be
   * protected by write lock.
   * @param schedulerRequestKey schedulerRequestKey
   * @param placementTypeClass placementTypeClass
   * @return AppPlacementAllocator
   */
  private AppPlacementAllocator<SchedulerNode> getAndAddAppPlacementAllocatorIfNotExist(
      SchedulerRequestKey schedulerRequestKey, String placementTypeClass) {
    AppPlacementAllocator<SchedulerNode> appPlacementAllocator;
    if ((appPlacementAllocator = schedulerKeyToAppPlacementAllocator.get(
        schedulerRequestKey)) == null) {
      appPlacementAllocator =
          ApplicationPlacementAllocatorFactory.getAppPlacementAllocator(
              placementTypeClass, this, schedulerRequestKey, rmContext);
      schedulerKeyToAppPlacementAllocator.put(schedulerRequestKey,
          appPlacementAllocator);
    }
    return appPlacementAllocator;
  }

  private boolean internalAddResourceRequests(
      boolean recoverPreemptedRequestForAContainer,
      Map<SchedulerRequestKey, Map<String, ResourceRequest>> dedupRequests) {
    boolean offswitchResourcesUpdated = false;
    for (Map.Entry<SchedulerRequestKey, Map<String, ResourceRequest>> entry :
    dedupRequests.entrySet()) {
      SchedulerRequestKey schedulerRequestKey = entry.getKey();
      AppPlacementAllocator<SchedulerNode> appPlacementAllocator =
          getAndAddAppPlacementAllocatorIfNotExist(schedulerRequestKey,
              applicationSchedulingEnvs.get(
                  ApplicationSchedulingConfig.ENV_APPLICATION_PLACEMENT_TYPE_CLASS));

      // Update AppPlacementAllocator
      PendingAskUpdateResult pendingAmountChanges =
          appPlacementAllocator.updatePendingAsk(entry.getValue().values(),
              recoverPreemptedRequestForAContainer);

      if (null != pendingAmountChanges) {
        updatePendingResources(pendingAmountChanges, schedulerRequestKey,
            queue.getMetrics());
        offswitchResourcesUpdated = true;
      }
    }
    return offswitchResourcesUpdated;
  }

  private boolean internalAddResourceRequests(boolean recoverPreemptedRequestForAContainer,
      List<ResourceRequest> resourceRequests) {
    if (null == resourceRequests || resourceRequests.isEmpty()) {
      return false;
    }

    // A map to group resource requests and dedup
    Map<SchedulerRequestKey, Map<String, ResourceRequest>> dedupRequests =
        new HashMap<>();

    // Group resource request by schedulerRequestKey and resourceName
    for (ResourceRequest request : resourceRequests) {
      SchedulerRequestKey schedulerKey = SchedulerRequestKey.create(request);
      if (!dedupRequests.containsKey(schedulerKey)) {
        dedupRequests.put(schedulerKey, new HashMap<>());
      }
      dedupRequests.get(schedulerKey).put(request.getResourceName(), request);
    }

    return internalAddResourceRequests(recoverPreemptedRequestForAContainer,
        dedupRequests);
  }

  private void updatePendingResources(PendingAskUpdateResult updateResult,
      SchedulerRequestKey schedulerKey, QueueMetrics metrics) {

    PendingAsk lastPendingAsk = updateResult.getLastPendingAsk();
    PendingAsk newPendingAsk = updateResult.getNewPendingAsk();
    String lastNodePartition = updateResult.getLastNodePartition();
    String newNodePartition = updateResult.getNewNodePartition();

    int lastRequestContainers =
        (lastPendingAsk != null) ? lastPendingAsk.getCount() : 0;
    if (newPendingAsk.getCount() <= 0) {
      if (lastRequestContainers >= 0) {
        schedulerKeys.remove(schedulerKey);
        schedulerKeyToAppPlacementAllocator.remove(schedulerKey);
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

    if (lastPendingAsk != null) {
      // Deduct resources from metrics / pending resources of queue/app.
      metrics.decrPendingResources(lastNodePartition, user,
          lastPendingAsk.getCount(), lastPendingAsk.getPerAllocationResource());
      Resource decreasedResource = Resources.multiply(
          lastPendingAsk.getPerAllocationResource(), lastRequestContainers);
      queue.decPendingResource(lastNodePartition, decreasedResource);
      appResourceUsage.decPending(lastNodePartition, decreasedResource);
    }

    // Increase resources to metrics / pending resources of queue/app.
    metrics.incrPendingResources(newNodePartition, user,
        newPendingAsk.getCount(), newPendingAsk.getPerAllocationResource());
    Resource increasedResource = Resources.multiply(
        newPendingAsk.getPerAllocationResource(), newPendingAsk.getCount());
    queue.incPendingResource(newNodePartition, increasedResource);
    appResourceUsage.incPending(newNodePartition, increasedResource);
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
      for (AppPlacementAllocator ap : schedulerKeyToAppPlacementAllocator
          .values()) {
        ret.addAll(ap.getResourceRequests().values());
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
      AppPlacementAllocator ap = schedulerKeyToAppPlacementAllocator.get(
          schedulerKey);
      return (ap == null) ? PendingAsk.ZERO : ap.getPendingAsk(resourceName);
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

  public ContainerRequest allocate(NodeType type,
      SchedulerNode node, SchedulerRequestKey schedulerKey,
      Container containerAllocated) {
    try {
      writeLock.lock();

      if (null != containerAllocated) {
        updateMetricsForAllocatedContainer(type, node, containerAllocated);
      }

      return schedulerKeyToAppPlacementAllocator.get(schedulerKey).allocate(
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
      for (AppPlacementAllocator ap : schedulerKeyToAppPlacementAllocator
          .values()) {
        PendingAsk ask = ap.getPendingAsk(ResourceRequest.ANY);
        if (ask.getCount() > 0) {
          oldMetrics.decrPendingResources(
              ap.getPrimaryRequestedNodePartition(),
              user, ask.getCount(), ask.getPerAllocationResource());
          newMetrics.incrPendingResources(
              ap.getPrimaryRequestedNodePartition(),
              user, ask.getCount(), ask.getPerAllocationResource());

          Resource delta = Resources.multiply(ask.getPerAllocationResource(),
              ask.getCount());
          // Update Queue
          queue.decPendingResource(
              ap.getPrimaryRequestedNodePartition(), delta);
          newQueue.incPendingResource(
              ap.getPrimaryRequestedNodePartition(), delta);
        }
      }
      oldMetrics.moveAppFrom(this);
      newMetrics.moveAppTo(this);
      abstractUsersManager.deactivateApplication(user, applicationId);
      abstractUsersManager = newQueue.getAbstractUsersManager();
      if (!schedulerKeys.isEmpty()) {
        abstractUsersManager.activateApplication(user, applicationId);
      }
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
      for (AppPlacementAllocator ap : schedulerKeyToAppPlacementAllocator
          .values()) {
        PendingAsk ask = ap.getPendingAsk(ResourceRequest.ANY);
        if (ask.getCount() > 0) {
          metrics.decrPendingResources(ap.getPrimaryRequestedNodePartition(),
              user, ask.getCount(), ask.getPerAllocationResource());

          // Update Queue
          queue.decPendingResource(
              ap.getPrimaryRequestedNodePartition(),
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
      AppPlacementAllocator ap = schedulerKeyToAppPlacementAllocator.get(
          schedulerKey);
      if (null == ap) {
        return false;
      }
      return ap.canAllocate(type, node);
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

    updateMetrics(applicationId, type, node, containerAllocated, user, queue);
  }

  public static void updateMetrics(ApplicationId applicationId, NodeType type,
      SchedulerNode node, Container containerAllocated, String user,
      Queue queue) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("allocate: applicationId=" + applicationId + " container="
          + containerAllocated.getId() + " host=" + containerAllocated
          .getNodeId().toString() + " user=" + user + " resource="
          + containerAllocated.getResource() + " type="
          + type);
    }
    if(node != null) {
      queue.getMetrics().allocateResources(node.getPartition(), user, 1,
          containerAllocated.getResource(), true);
    }
    queue.getMetrics().incrNodeTypeAggregations(user, type);
  }

  // Get AppPlacementAllocator by specified schedulerKey
  public <N extends SchedulerNode> AppPlacementAllocator<N> getAppPlacementAllocator(
      SchedulerRequestKey schedulerkey) {
    return (AppPlacementAllocator<N>) schedulerKeyToAppPlacementAllocator.get(
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
      AppPlacementAllocator ap =
          schedulerKeyToAppPlacementAllocator.get(schedulerKey);
      return (ap == null) || ap.canDelayTo(resourceName);
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * Pre-check node to see if it satisfy the given schedulerKey and
   * scheduler mode.
   *
   * @param schedulerKey schedulerKey
   * @param schedulerNode schedulerNode
   * @param schedulingMode schedulingMode
   * @return can use the node or not.
   */
  public boolean precheckNode(SchedulerRequestKey schedulerKey,
      SchedulerNode schedulerNode, SchedulingMode schedulingMode) {
    try {
      this.readLock.lock();
      AppPlacementAllocator ap =
          schedulerKeyToAppPlacementAllocator.get(schedulerKey);
      return (ap != null) && ap.precheckNode(schedulerNode,
          schedulingMode);
    } finally {
      this.readLock.unlock();
    }
  }
}

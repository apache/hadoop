/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .queuemanagement;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractParentQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueueUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractAutoCreatedLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedLeafQueueConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueManagementPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ManagedParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueManagementChange;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .capacity.CSQueueUtils.EPSILON;

/**
 * Capacity Management policy for auto created leaf queues
 * <p>
 * Assigns capacity if available to leaf queues based on application
 * submission order i.e leaf queues are assigned capacity in FCFS order based
 * on application submission time.  Updates leaf queue capacities to 0 when
 * there are no pending or running apps under that queue.
 */
public class GuaranteedOrZeroCapacityOverTimePolicy
    implements AutoCreatedQueueManagementPolicy {

  private static final int DEFAULT_QUEUE_PRINT_SIZE_LIMIT = 25;
  private ManagedParentQueue managedParentQueue;

  private static final Logger LOG =
      LoggerFactory.getLogger(GuaranteedOrZeroCapacityOverTimePolicy.class);

  private ReentrantReadWriteLock.WriteLock writeLock;

  private ReentrantReadWriteLock.ReadLock readLock;

  private ParentQueueState parentQueueState = new ParentQueueState();

  private AutoCreatedLeafQueueConfig leafQueueTemplate;

  private QueueCapacities leafQueueTemplateCapacities;

  private Set<String> leafQueueTemplateNodeLabels;

  private LeafQueueState leafQueueState = new LeafQueueState();

  private Clock clock = new MonotonicClock();

  private class LeafQueueState {

    //map of partition-> queueName->{leaf queue's state}
    private Map<String, Map<String, LeafQueueStatePerPartition>>
        leafQueueStateMap = new HashMap<>();

    public boolean containsLeafQueue(String leafQueueName, String partition) {
      if (leafQueueStateMap.containsKey(partition)) {
        return leafQueueStateMap.get(partition).containsKey(leafQueueName);
      }
      return false;
    }

    private boolean containsPartition(String partition) {
      if (leafQueueStateMap.containsKey(partition)) {
        return true;
      }
      return false;
    }

    private boolean addLeafQueueStateIfNotExists(String leafQueuePath,
        String partition, LeafQueueStatePerPartition leafQueueState) {
      if (!containsPartition(partition)) {
        leafQueueStateMap.put(partition, new HashMap<>());
      }
      if (!containsLeafQueue(leafQueuePath, partition)) {
        leafQueueStateMap.get(partition).put(leafQueuePath, leafQueueState);
        return true;
      }
      return false;
    }

    public boolean createLeafQueueStateIfNotExists(AbstractLeafQueue leafQueue,
        String partition) {
      return addLeafQueueStateIfNotExists(leafQueue.getQueuePath(), partition,
          new LeafQueueStatePerPartition());
    }

    public LeafQueueStatePerPartition getLeafQueueStatePerPartition(
        String leafQueuePath, String partition) {
      if (leafQueueStateMap.get(partition) != null) {
        return leafQueueStateMap.get(partition).get(leafQueuePath);
      }
      return null;
    }

    public Map<String, Map<String, LeafQueueStatePerPartition>>
    getLeafQueueStateMap() {
      return leafQueueStateMap;
    }

    private void clear() {
      leafQueueStateMap.clear();
    }
  }

  private class LeafQueueStatePerPartition {

    private AtomicBoolean isActive = new AtomicBoolean(false);

    private long mostRecentActivationTime;

    private long mostRecentDeactivationTime;

    public long getMostRecentActivationTime() {
      return mostRecentActivationTime;
    }

    public long getMostRecentDeactivationTime() {
      return mostRecentDeactivationTime;
    }

    /**
     * Is the queue currently active or deactivated?
     *
     * @return true if Active else false
     */
    public boolean isActive() {
      return isActive.get();
    }

    private boolean activate() {
      boolean ret = isActive.compareAndSet(false, true);
      mostRecentActivationTime = clock.getTime();
      return ret;
    }

    private boolean deactivate() {
      boolean ret = isActive.compareAndSet(true, false);
      mostRecentDeactivationTime = clock.getTime();
      return ret;
    }
  }

  private class ParentQueueState {

    private Map<String, Float> totalAbsoluteActivatedChildQueueCapacityByLabel =
        new HashMap<String, Float>();

    private float getAbsoluteActivatedChildQueueCapacity(String nodeLabel) {
      readLock.lock();
      try {
        Float totalActivatedCapacity = getAbsActivatedChildQueueCapacityByLabel(
            nodeLabel);
        if (totalActivatedCapacity != null) {
          return totalActivatedCapacity;
        } else{
          return 0;
        }
      } finally {
        readLock.unlock();
      }
    }

    private void incAbsoluteActivatedChildCapacity(String nodeLabel,
        float childQueueCapacity) {
      writeLock.lock();
      try {
        Float activatedChildCapacity = getAbsActivatedChildQueueCapacityByLabel(
            nodeLabel);
        if (activatedChildCapacity != null) {
          setAbsActivatedChildQueueCapacityByLabel(nodeLabel,
              activatedChildCapacity + childQueueCapacity);
        } else{
          setAbsActivatedChildQueueCapacityByLabel(nodeLabel,
              childQueueCapacity);
        }
      } finally {
        writeLock.unlock();
      }
    }

    private void decAbsoluteActivatedChildCapacity(String nodeLabel,
        float childQueueCapacity) {
      writeLock.lock();
      try {
        Float activatedChildCapacity = getAbsActivatedChildQueueCapacityByLabel(
            nodeLabel);
        if (activatedChildCapacity != null) {
          setAbsActivatedChildQueueCapacityByLabel(nodeLabel,
              activatedChildCapacity - childQueueCapacity);
        } else{
          setAbsActivatedChildQueueCapacityByLabel(nodeLabel,
              childQueueCapacity);
        }
      } finally {
        writeLock.unlock();
      }
    }

    Float getAbsActivatedChildQueueCapacityByLabel(String label) {
      return totalAbsoluteActivatedChildQueueCapacityByLabel.get(label);
    }

    Float setAbsActivatedChildQueueCapacityByLabel(String label, float val) {
      return totalAbsoluteActivatedChildQueueCapacityByLabel.put(label, val);
    }

    void clear() {
      totalAbsoluteActivatedChildQueueCapacityByLabel.clear();
    }
  }

  @Override
  public void init(final AbstractParentQueue parentQueue) throws IOException {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
    if (!(parentQueue instanceof ManagedParentQueue)) {
      throw new IllegalArgumentException(
          "Expected instance of type " + ManagedParentQueue.class);
    }

    this.managedParentQueue = (ManagedParentQueue) parentQueue;

    initializeLeafQueueTemplate(this.managedParentQueue);

    LOG.info(
        "Initialized queue management policy for parent queue " + parentQueue
            .getQueuePath() + " with leaf queue template capacities : ["
            + leafQueueTemplate.getQueueCapacities() + "]");
  }

  private void initializeLeafQueueTemplate(ManagedParentQueue parentQueue)
      throws IOException {
    leafQueueTemplate = parentQueue.getLeafQueueTemplate();

    leafQueueTemplateCapacities = leafQueueTemplate.getQueueCapacities();

    Set<String> parentQueueLabels = parentQueue.getNodeLabelsForQueue();
    for (String nodeLabel : leafQueueTemplateCapacities
        .getExistingNodeLabels()) {

      if (!parentQueueLabels.contains(nodeLabel)) {
        LOG.error("Invalid node label " + nodeLabel
            + " on configured leaf template on parent" + " queue " + parentQueue
            .getQueuePath());
        throw new IOException("Invalid node label " + nodeLabel
            + " on configured leaf template on parent" + " queue " + parentQueue
            .getQueuePath());
      }
    }

    leafQueueTemplateNodeLabels =
        leafQueueTemplateCapacities.getExistingNodeLabels();

  }

  /**
   * Computes / adjusts child queue capacities for auto created leaf queues.
   * This method computes queue entitlements but does not update LeafQueueState or
   * queue capacities.
   * Scheduler calls commitQueueManagementChanges after validation after applying queue changes
   * and commits to LeafQueueState are done in commitQueueManagementChanges.
   *
   * @return List of Queue Management change suggestions which could potentially
   * be committed/rejected by the scheduler due to validation failures
   * @throws SchedulerDynamicEditException when compute queueManagement changes fails.
   */
  @Override
  public List<QueueManagementChange> computeQueueManagementChanges()
      throws SchedulerDynamicEditException {

    // Update template absolute capacities as the capacities could have changed
    // in weight mode
    updateTemplateAbsoluteCapacities(managedParentQueue.getQueueCapacities(),
        (GuaranteedOrZeroCapacityOverTimePolicy)
            managedParentQueue.getAutoCreatedQueueManagementPolicy());

    //TODO : Add support for node labels on leaf queue template configurations
    //sync / add missing leaf queue(s) if any TO state
    updateLeafQueueState();

    readLock.lock();
    try {
      LeafQueueEntitlements leafQueueEntitlements = new LeafQueueEntitlements();
      for (String nodeLabel : leafQueueTemplateNodeLabels) {
        DeactivatedLeafQueuesByLabel deactivatedLeafQueues =
            deactivateLeafQueues(nodeLabel, leafQueueEntitlements);
        deactivatedLeafQueues.printToDebug(LOG);

        //Check if we need to activate anything at all?
        if (deactivatedLeafQueues.canActivateLeafQueues()) {
          activateLeafQueues(leafQueueEntitlements, nodeLabel, deactivatedLeafQueues);
        }
      }

      //Populate new entitlements
      return leafQueueEntitlements.mapToQueueManagementChanges((leafQueueName, capacities) -> {
        AutoCreatedLeafQueue leafQueue =
            (AutoCreatedLeafQueue) managedParentQueue.getQueueContext().getQueueManager()
                .getQueue(leafQueueName);
        AutoCreatedLeafQueueConfig newTemplate = buildTemplate(capacities);
        return new QueueManagementChange.UpdateQueue(leafQueue, newTemplate);
      });
    } finally {
      readLock.unlock();
    }
  }

  private void activateLeafQueues(LeafQueueEntitlements leafQueueEntitlements, String nodeLabel,
      DeactivatedLeafQueuesByLabel deactivatedLeafQueues) throws SchedulerDynamicEditException {
    //sort applications across leaf queues by submit time
    List<FiCaSchedulerApp> pendingApps = getSortedPendingApplications();
    if (pendingApps.size() > 0) {
      int maxLeafQueuesTobeActivated = deactivatedLeafQueues.
          getMaxLeavesToBeActivated(pendingApps.size());

      if (LOG.isDebugEnabled()) {
        LOG.debug("Parent queue = {}, Found {} leaf queues to be activated with {} aps",
            managedParentQueue.getQueuePath(), maxLeafQueuesTobeActivated, pendingApps.size());
      }

      Set<String> leafQueuesToBeActivated = getSortedLeafQueues(
          nodeLabel, pendingApps, maxLeafQueuesTobeActivated,
          deactivatedLeafQueues.getQueues());

      // Compute entitlement changes for the identified leaf queues
      // which is appended to the List of computedEntitlements
      updateLeafQueueCapacitiesByLabel(nodeLabel, leafQueuesToBeActivated, leafQueueEntitlements);

      if (LOG.isDebugEnabled() && leafQueuesToBeActivated.size() > 0) {
        LOG.debug("Activated leaf queues : [{}]",
            getListContentsUpToLimit(leafQueuesToBeActivated));
      }
    }
  }

  private Object getListContentsUpToLimit(Set<String> leafQueuesToBeActivated) {
    return leafQueuesToBeActivated.size() < DEFAULT_QUEUE_PRINT_SIZE_LIMIT ?
        leafQueuesToBeActivated : leafQueuesToBeActivated.size();
  }

  private Object getMapUpToLimit(Map<String, QueueCapacities> deactivatedLeafQueues) {
    return deactivatedLeafQueues.size() > DEFAULT_QUEUE_PRINT_SIZE_LIMIT ?
        deactivatedLeafQueues.size() : deactivatedLeafQueues;
  }

  private DeactivatedLeafQueuesByLabel deactivateLeafQueues(String nodeLabel,
      LeafQueueEntitlements leafQueueEntitlements) throws SchedulerDynamicEditException {
    // check if any leaf queues need to be deactivated based on pending applications
    float parentAbsoluteCapacity =
        managedParentQueue.getQueueCapacities().getAbsoluteCapacity(nodeLabel);
    float leafQueueTemplateAbsoluteCapacity =
        leafQueueTemplateCapacities.getAbsoluteCapacity(nodeLabel);
    Map<String, QueueCapacities> deactivatedLeafQueues =
        deactivateLeafQueuesIfInActive(managedParentQueue, nodeLabel, leafQueueEntitlements);

    if (LOG.isDebugEnabled() && deactivatedLeafQueues.size() > 0) {
      LOG.debug("Parent queue = {}, nodeLabel = {}, deactivated leaf queues = [{}] ",
          managedParentQueue.getQueuePath(), nodeLabel,
          getMapUpToLimit(deactivatedLeafQueues));
    }

    return new DeactivatedLeafQueuesByLabel(deactivatedLeafQueues,
        managedParentQueue.getQueuePath(),
        nodeLabel,
        parentQueueState.getAbsoluteActivatedChildQueueCapacity(nodeLabel),
        parentAbsoluteCapacity,
        leafQueueTemplateAbsoluteCapacity);
  }

  private void updateTemplateAbsoluteCapacities(QueueCapacities parentQueueCapacities,
                                                GuaranteedOrZeroCapacityOverTimePolicy policy) {
    writeLock.lock();
    try {
      CSQueueUtils.updateAbsoluteCapacitiesByNodeLabels(
          policy.leafQueueTemplate.getQueueCapacities(),
          parentQueueCapacities, policy.leafQueueTemplateNodeLabels,
          managedParentQueue.getQueueContext().getConfiguration().isLegacyQueueMode());
      policy.leafQueueTemplateCapacities =
          policy.leafQueueTemplate.getQueueCapacities();
    } finally {
      writeLock.unlock();
    }
  }

  public void updateTemplateAbsoluteCapacities(QueueCapacities queueCapacities) {
    updateTemplateAbsoluteCapacities(queueCapacities, this);
  }

  @VisibleForTesting
  void updateLeafQueueState() {
    writeLock.lock();
    try {
      Set<String> newPartitions = new HashSet<>();
      Set<String> newQueues = new HashSet<>();

      for (CSQueue newQueue : managedParentQueue.getChildQueues()) {
        if (newQueue instanceof AbstractLeafQueue) {
          for (String nodeLabel : leafQueueTemplateNodeLabels) {
            leafQueueState.createLeafQueueStateIfNotExists((AbstractLeafQueue) newQueue,
                nodeLabel);
            newPartitions.add(nodeLabel);
          }
          newQueues.add(newQueue.getQueuePath());
        }
      }

      for (Iterator<Map.Entry<String, Map<String, LeafQueueStatePerPartition>>>
           itr = leafQueueState.getLeafQueueStateMap().entrySet().iterator();
           itr.hasNext(); ) {
        Map.Entry<String, Map<String, LeafQueueStatePerPartition>> e =
            itr.next();
        String partition = e.getKey();
        if (!newPartitions.contains(partition)) {
          itr.remove();
          LOG.info(managedParentQueue.getQueuePath()  +
              " : Removed partition " + partition + " from leaf queue " +
              "state");
        } else{
          Map<String, LeafQueueStatePerPartition> queues = e.getValue();
          for (
              Iterator<Map.Entry<String, LeafQueueStatePerPartition>> queueItr =
              queues.entrySet().iterator(); queueItr.hasNext(); ) {
            String queue = queueItr.next().getKey();
            if (!newQueues.contains(queue)) {
              queueItr.remove();
              LOG.info(managedParentQueue.getQueuePath() + " : Removed queue"
                  + queue + " from "
                  + "leaf queue "
                  + "state from partition " + partition);
            }
          }
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  private LinkedHashSet<String> getSortedLeafQueues(String nodeLabel,
      final List<FiCaSchedulerApp> pendingApps, int leafQueuesNeeded,
      Set<String> deactivatedQueues) throws SchedulerDynamicEditException {

    LinkedHashSet<String> leafQueues = new LinkedHashSet<>(leafQueuesNeeded);
    int ctr = 0;
    for (FiCaSchedulerApp app : pendingApps) {
      AutoCreatedLeafQueue leafQueue =
          (AutoCreatedLeafQueue) app.getCSLeafQueue();
      String leafQueueName = leafQueue.getQueuePath();

      //Check if leafQueue is not active already and has any pending apps
      if (ctr < leafQueuesNeeded) {
        if (!isActive(leafQueue, nodeLabel)) {
          if (!deactivatedQueues.contains(leafQueueName)) {
            if (addLeafQueueIfNotExists(leafQueues, leafQueueName)) {
              ctr++;
            }
          }
        }
      } else{
        break;
      }
    }
    return leafQueues;
  }

  private boolean addLeafQueueIfNotExists(Set<String> leafQueues,
      String leafQueueName) {
    boolean ret = false;
    if (!leafQueues.contains(leafQueueName)) {
      ret = leafQueues.add(leafQueueName);
    }
    return ret;
  }

  @VisibleForTesting
  public boolean isActive(final AutoCreatedLeafQueue leafQueue,
      String nodeLabel) throws SchedulerDynamicEditException {
    readLock.lock();
    try {
      LeafQueueStatePerPartition leafQueueStatus = getLeafQueueState(leafQueue,
          nodeLabel);
      return leafQueueStatus.isActive();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Map of LeafQueue -> QueueCapacities - keep adding the computed
   * entitlements to this map and finally
   * build the leaf queue configuration Template for all identified leaf
   * queues
   */
  private Map<String, QueueCapacities> deactivateLeafQueuesIfInActive(
      AbstractParentQueue parentQueue, String nodeLabel,
      LeafQueueEntitlements leafQueueEntitlements)
      throws SchedulerDynamicEditException {
    Map<String, QueueCapacities> deactivatedQueues = new HashMap<>();

    for (CSQueue childQueue : parentQueue.getChildQueues()) {
      AutoCreatedLeafQueue leafQueue = (AutoCreatedLeafQueue) childQueue;
      if (leafQueue != null) {
        if (isActive(leafQueue, nodeLabel) && !hasPendingApps(leafQueue)) {
          QueueCapacities capacities = leafQueueEntitlements.getCapacityOfQueue(leafQueue);
          updateToZeroCapacity(capacities, nodeLabel, (AbstractLeafQueue) childQueue);
          deactivatedQueues.put(leafQueue.getQueuePath(), leafQueueTemplateCapacities);
        }
      } else {
        LOG.warn("Could not find queue in scheduler while trying" + " to "
            + "deactivate for " + parentQueue);
      }
    }

    return deactivatedQueues;
  }

  private void updateLeafQueueCapacitiesByLabel(String nodeLabel,
      Set<String> leafQueuesToBeActivated,
      LeafQueueEntitlements leafQueueEntitlements) {
    for (String leafQueue : leafQueuesToBeActivated) {
      QueueCapacities capacities = leafQueueEntitlements.getCapacityOfQueueByPath(leafQueue);
      updateCapacityFromTemplate(capacities, nodeLabel);
    }
  }

  /**
   * Commit queue management changes - which involves updating required state
   * on parent/underlying leaf queues.
   *
   * @param queueManagementChanges Queue Management changes to commit
   * @throws SchedulerDynamicEditException when validation fails
   */
  @Override
  public void commitQueueManagementChanges(
      List<QueueManagementChange> queueManagementChanges)
      throws SchedulerDynamicEditException {
    writeLock.lock();
    try {
      for (QueueManagementChange queueManagementChange :
          queueManagementChanges) {
        AutoCreatedLeafQueueConfig updatedQueueTemplate =
            queueManagementChange.getUpdatedQueueTemplate();
        CSQueue queue = queueManagementChange.getQueue();
        if (!(queue instanceof AutoCreatedLeafQueue)) {
          throw new SchedulerDynamicEditException(
              "Expected queue management change for AutoCreatedLeafQueue. "
                  + "Found " + queue.getClass().getName());
        }

        AutoCreatedLeafQueue leafQueue = (AutoCreatedLeafQueue) queue;

        for (String nodeLabel : updatedQueueTemplate.getQueueCapacities()
            .getExistingNodeLabels()) {
          if (updatedQueueTemplate.getQueueCapacities().getCapacity(nodeLabel) > 0) {
            if (isActive(leafQueue, nodeLabel)) {
              LOG.debug("Queue is already active. Skipping activation : {}",
                  leafQueue.getQueuePath());
            } else{
              activate(leafQueue, nodeLabel);
            }
          } else {
            if (!isActive(leafQueue, nodeLabel)) {
              LOG.debug("Queue is already de-activated. Skipping "
                  + "de-activation : {}", leafQueue.getQueuePath());
            } else {
              /**
               * While deactivating queues of type ABSOLUTE_RESOURCE, configured
               * min resource has to be set based on updated capacity (which is
               * again based on updated queue entitlements). Otherwise,
               * ParentQueue#calculateEffectiveResourcesAndCapacity calculations
               * leads to incorrect results.
               */
              leafQueue
                  .mergeCapacities(updatedQueueTemplate.getQueueCapacities(), leafQueueTemplate.getResourceQuotas());
              leafQueue.getQueueResourceQuotas()
                  .setConfiguredMinResource(Resources.multiply(
                      managedParentQueue.getQueueContext().getClusterResource(),
                      updatedQueueTemplate
                          .getQueueCapacities().getCapacity(nodeLabel)));
              deactivate(leafQueue, nodeLabel);
            }
          }
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void activate(final AbstractAutoCreatedLeafQueue leafQueue,
      String nodeLabel) throws SchedulerDynamicEditException {
    writeLock.lock();
    try {
      getLeafQueueState(leafQueue, nodeLabel).activate();
      parentQueueState.incAbsoluteActivatedChildCapacity(nodeLabel,
          leafQueueTemplateCapacities.getAbsoluteCapacity(nodeLabel));
    } finally {
      writeLock.unlock();
    }
  }

  private void deactivate(final AbstractAutoCreatedLeafQueue leafQueue,
      String nodeLabel) throws SchedulerDynamicEditException {
    writeLock.lock();
    try {
      getLeafQueueState(leafQueue, nodeLabel).deactivate();

      parentQueueState.decAbsoluteActivatedChildCapacity(nodeLabel,
          leafQueueTemplateCapacities.getAbsoluteCapacity(nodeLabel));
    } finally {
      writeLock.unlock();
    }
  }

  public boolean hasPendingApps(final AutoCreatedLeafQueue leafQueue) {
    return leafQueue.getNumApplications() > 0;
  }

  @Override
  public void reinitialize(final AbstractParentQueue parentQueue) throws IOException {
    if (!(parentQueue instanceof ManagedParentQueue)) {
      throw new IllegalStateException(
          "Expected instance of type " + ManagedParentQueue.class + " found  "
              + " : " + parentQueue.getClass());
    }

    if (this.managedParentQueue != null && !parentQueue.getQueuePath().equals(
        this.managedParentQueue.getQueuePath())) {
      throw new IllegalStateException(
          "Expected parent queue path to match " + this.managedParentQueue
              .getQueuePath() + " found : " + parentQueue.getQueuePath());
    }

    this.managedParentQueue = (ManagedParentQueue) parentQueue;

    initializeLeafQueueTemplate(this.managedParentQueue);

    //clear state
    parentQueueState.clear();
    leafQueueState.clear();

    LOG.info(
        "Reinitialized queue management policy for parent queue " + parentQueue
            .getQueuePath() + " with leaf queue template " + "capacities : ["
            + leafQueueTemplate.getQueueCapacities() + "]");
  }

  @Override
  public AutoCreatedLeafQueueConfig getInitialLeafQueueConfiguration(
      AbstractAutoCreatedLeafQueue leafQueue)
      throws SchedulerDynamicEditException {

    AutoCreatedLeafQueueConfig template;

    if (!(leafQueue instanceof AutoCreatedLeafQueue)) {
      throw new SchedulerDynamicEditException(
          "Not an instance of " + "AutoCreatedLeafQueue : " + leafQueue
              .getClass());
    }

    writeLock.lock();
    try {
      QueueCapacities capacities = new QueueCapacities(false);
      for (String nodeLabel : leafQueueTemplateNodeLabels) {
        if (!leafQueueState.createLeafQueueStateIfNotExists(leafQueue,
            nodeLabel)) {
          String message =
              "Leaf queue already exists in state : " + getLeafQueueState(
                  leafQueue, nodeLabel);
          LOG.error(message);
        }

        float availableCapacity = managedParentQueue.getQueueCapacities().
            getAbsoluteCapacity(nodeLabel) - parentQueueState.
            getAbsoluteActivatedChildQueueCapacity(nodeLabel) + EPSILON;

        if (availableCapacity >= leafQueueTemplateCapacities
            .getAbsoluteCapacity(nodeLabel)) {
          updateCapacityFromTemplate(capacities, nodeLabel);
          activate(leafQueue, nodeLabel);
        } else{
          updateToZeroCapacity(capacities, nodeLabel, leafQueue);
        }
      }

      template = buildTemplate(capacities);
    } finally {
      writeLock.unlock();
    }
    return template;
  }

  private void updateToZeroCapacity(QueueCapacities capacities,
      String nodeLabel, AbstractLeafQueue leafQueue) {
    capacities.setCapacity(nodeLabel, 0.0f);
    capacities.setMaximumCapacity(nodeLabel,
        leafQueueTemplateCapacities.getMaximumCapacity(nodeLabel));
    leafQueue.getQueueResourceQuotas().
        setConfiguredMinResource(nodeLabel, Resource.newInstance(0, 0));
  }

  private void updateCapacityFromTemplate(QueueCapacities capacities,
      String nodeLabel) {
    capacities.setCapacity(nodeLabel,
        leafQueueTemplateCapacities.getCapacity(nodeLabel));
    capacities.setMaximumCapacity(nodeLabel,
        leafQueueTemplateCapacities.getMaximumCapacity(nodeLabel));
    capacities.setAbsoluteCapacity(nodeLabel,
        leafQueueTemplateCapacities.getAbsoluteCapacity(nodeLabel));
    capacities.setAbsoluteMaximumCapacity(nodeLabel,
        leafQueueTemplateCapacities.getAbsoluteMaximumCapacity(nodeLabel));
  }

  @VisibleForTesting
  LeafQueueStatePerPartition getLeafQueueState(AbstractLeafQueue queue,
      String partition) throws SchedulerDynamicEditException {
    readLock.lock();
    try {
      String queuePath = queue.getQueuePath();
      if (!leafQueueState.containsLeafQueue(queuePath, partition)) {
        throw new SchedulerDynamicEditException(
            "Could not find leaf queue in " + "state " + queuePath);
      } else{
        return leafQueueState.
            getLeafQueueStatePerPartition(queuePath, partition);
      }
    } finally {
      readLock.unlock();
    }
  }

  @VisibleForTesting
  public float getAbsoluteActivatedChildQueueCapacity(String nodeLabel) {
    return parentQueueState.getAbsoluteActivatedChildQueueCapacity(nodeLabel);
  }

  private List<FiCaSchedulerApp> getSortedPendingApplications() {
    List<FiCaSchedulerApp> apps = new ArrayList<>(
        managedParentQueue.getAllApplications());
    apps.sort(managedParentQueue.getQueueContext().getApplicationComparator());
    return apps;
  }

  private AutoCreatedLeafQueueConfig buildTemplate(QueueCapacities capacities) {
    AutoCreatedLeafQueueConfig.Builder templateBuilder =
        new AutoCreatedLeafQueueConfig.Builder();
    templateBuilder.capacities(capacities);
    templateBuilder.resourceQuotas(managedParentQueue.getLeafQueueTemplate().getResourceQuotas());
    return new AutoCreatedLeafQueueConfig(templateBuilder);
  }
}

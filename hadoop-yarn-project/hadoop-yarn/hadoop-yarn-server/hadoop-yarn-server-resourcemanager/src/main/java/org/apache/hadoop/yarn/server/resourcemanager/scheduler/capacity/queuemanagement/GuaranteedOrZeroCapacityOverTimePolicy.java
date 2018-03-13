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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .AbstractAutoCreatedLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .AutoCreatedLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .AutoCreatedLeafQueueConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .AutoCreatedQueueManagementPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .CapacitySchedulerContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .ManagedParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .QueueManagementChange;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica
    .FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager
    .NO_LABEL;
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

  private CapacitySchedulerContext scheduler;
  private ManagedParentQueue managedParentQueue;

  private static final Log LOG = LogFactory.getLog(
      GuaranteedOrZeroCapacityOverTimePolicy.class);

  private AutoCreatedLeafQueueConfig ZERO_CAPACITY_ENTITLEMENT;

  private ReentrantReadWriteLock.WriteLock writeLock;

  private ReentrantReadWriteLock.ReadLock readLock;

  private ParentQueueState parentQueueState = new ParentQueueState();

  private AutoCreatedLeafQueueConfig leafQueueTemplate;

  private QueueCapacities leafQueueTemplateCapacities;

  private Map<String, LeafQueueState> leafQueueStateMap = new HashMap<>();

  private Clock clock = new MonotonicClock();

  private class LeafQueueState {

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

  private boolean containsLeafQueue(String leafQueueName) {
    return leafQueueStateMap.containsKey(leafQueueName);
  }

  private boolean addLeafQueueStateIfNotExists(String leafQueueName,
      LeafQueueState leafQueueState) {
    if (!containsLeafQueue(leafQueueName)) {
      leafQueueStateMap.put(leafQueueName, leafQueueState);
      return true;
    }
    return false;
  }

  private boolean addLeafQueueStateIfNotExists(LeafQueue leafQueue) {
    return addLeafQueueStateIfNotExists(leafQueue.getQueueName(),
        new LeafQueueState());
  }

  private void clearLeafQueueState() {
    leafQueueStateMap.clear();
  }

  private class ParentQueueState {

    private Map<String, Float> totalAbsoluteActivatedChildQueueCapacityByLabel =
        new HashMap<String, Float>();

    private float getAbsoluteActivatedChildQueueCapacity() {
      return getAbsoluteActivatedChildQueueCapacity(NO_LABEL);
    }

    private float getAbsoluteActivatedChildQueueCapacity(String nodeLabel) {
      try {
        readLock.lock();
        Float totalActivatedCapacity = getByLabel(nodeLabel);
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
      try {
        writeLock.lock();
        Float activatedChildCapacity = getByLabel(nodeLabel);
        if (activatedChildCapacity != null) {
          setByLabel(nodeLabel, activatedChildCapacity + childQueueCapacity);
        } else{
          setByLabel(nodeLabel, childQueueCapacity);
        }
      } finally {
        writeLock.unlock();
      }
    }

    private void decAbsoluteActivatedChildCapacity(String nodeLabel,
        float childQueueCapacity) {
      try {
        writeLock.lock();
        Float activatedChildCapacity = getByLabel(nodeLabel);
        if (activatedChildCapacity != null) {
          setByLabel(nodeLabel, activatedChildCapacity - childQueueCapacity);
        } else{
          setByLabel(nodeLabel, childQueueCapacity);
        }
      } finally {
        writeLock.unlock();
      }
    }

    Float getByLabel(String label) {
      return totalAbsoluteActivatedChildQueueCapacityByLabel.get(label);
    }

    Float setByLabel(String label, float val) {
      return totalAbsoluteActivatedChildQueueCapacityByLabel.put(label, val);
    }

    void clear() {
      totalAbsoluteActivatedChildQueueCapacityByLabel.clear();
    }
  }

  /**
   * Comparator that orders applications by their submit time
   */
  private class PendingApplicationComparator
      implements Comparator<FiCaSchedulerApp> {

    @Override
    public int compare(FiCaSchedulerApp app1, FiCaSchedulerApp app2) {
      RMApp rmApp1 = scheduler.getRMContext().getRMApps().get(
          app1.getApplicationId());
      RMApp rmApp2 = scheduler.getRMContext().getRMApps().get(
          app2.getApplicationId());
      if (rmApp1 != null && rmApp2 != null) {
        return Long.compare(rmApp1.getSubmitTime(), rmApp2.getSubmitTime());
      } else if (rmApp1 != null) {
        return -1;
      } else if (rmApp2 != null) {
        return 1;
      } else{
        return 0;
      }
    }
  }

  private PendingApplicationComparator applicationComparator =
      new PendingApplicationComparator();

  @Override
  public void init(final CapacitySchedulerContext schedulerContext,
      final ParentQueue parentQueue) {
    this.scheduler = schedulerContext;

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
            .getQueueName() + " with leaf queue template capacities : ["
            + leafQueueTemplate.getQueueCapacities() + "]");
  }

  private void initializeLeafQueueTemplate(ManagedParentQueue parentQueue) {
    leafQueueTemplate = parentQueue.getLeafQueueTemplate();

    leafQueueTemplateCapacities = leafQueueTemplate.getQueueCapacities();

    ZERO_CAPACITY_ENTITLEMENT = buildTemplate(0.0f,
        leafQueueTemplateCapacities.getMaximumCapacity());
  }

  @Override
  public List<QueueManagementChange> computeQueueManagementChanges()
      throws SchedulerDynamicEditException {

    //TODO : Add support for node labels on leaf queue template configurations
    //synch/add missing leaf queue(s) if any to state
    updateLeafQueueState();

    try {
      readLock.lock();
      List<QueueManagementChange> queueManagementChanges = new ArrayList<>();

      // check if any leaf queues need to be deactivated based on pending
      // applications and
      float parentAbsoluteCapacity =
          managedParentQueue.getQueueCapacities().getAbsoluteCapacity();

      float leafQueueTemplateAbsoluteCapacity =
          leafQueueTemplateCapacities.getAbsoluteCapacity();
      Map<String, QueueCapacities> deactivatedLeafQueues =
          deactivateLeafQueuesIfInActive(managedParentQueue, queueManagementChanges);

      float deactivatedCapacity = getTotalDeactivatedCapacity(
          deactivatedLeafQueues);

      float sumOfChildQueueActivatedCapacity = parentQueueState.
          getAbsoluteActivatedChildQueueCapacity();

      //Check if we need to activate anything at all?
      float availableCapacity = getAvailableCapacity(parentAbsoluteCapacity,
          deactivatedCapacity, sumOfChildQueueActivatedCapacity);

      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Parent queue : " + managedParentQueue.getQueueName() + " absCapacity = "
                + parentAbsoluteCapacity + ", leafQueueAbsoluteCapacity = "
                + leafQueueTemplateAbsoluteCapacity + ", deactivatedCapacity = "
                + deactivatedCapacity + " , absChildActivatedCapacity = "
                + sumOfChildQueueActivatedCapacity + ", availableCapacity = "
                + availableCapacity);
      }

      if (availableCapacity >= leafQueueTemplateAbsoluteCapacity) {
        //sort applications across leaf queues by submit time
        List<FiCaSchedulerApp> pendingApps = getSortedPendingApplications();

        if (pendingApps.size() > 0) {
          int maxLeafQueuesTobeActivated = getMaxLeavesToBeActivated(
              availableCapacity, leafQueueTemplateAbsoluteCapacity,
              pendingApps.size());

          if (LOG.isDebugEnabled()) {
            LOG.debug("Found " + maxLeafQueuesTobeActivated
                + " leaf queues to be activated with " + pendingApps.size()
                + " apps ");
          }

          LinkedHashSet<String> leafQueuesToBeActivated = getSortedLeafQueues(
              pendingApps, maxLeafQueuesTobeActivated,
              deactivatedLeafQueues.keySet());

          //Compute entitlement changes for the identified leaf queues
          // which is appended to the List of queueManagementChanges
          computeQueueManagementChanges(leafQueuesToBeActivated,
              queueManagementChanges, availableCapacity,
              leafQueueTemplateAbsoluteCapacity);

          if (LOG.isDebugEnabled()) {
            if (leafQueuesToBeActivated.size() > 0) {
              LOG.debug(
                  "Activated leaf queues : [" + leafQueuesToBeActivated + "]");
            }
          }
        }
      }
      return queueManagementChanges;
    } finally {
      readLock.unlock();
    }
  }

  private float getTotalDeactivatedCapacity(
      Map<String, QueueCapacities> deactivatedLeafQueues) {
    float deactivatedCapacity = 0;
    for (Iterator<Map.Entry<String, QueueCapacities>> iterator =
         deactivatedLeafQueues.entrySet().iterator(); iterator.hasNext(); ) {
      Map.Entry<String, QueueCapacities> deactivatedQueueCapacity =
          iterator.next();
      deactivatedCapacity +=
          deactivatedQueueCapacity.getValue().getAbsoluteCapacity();
    }
    return deactivatedCapacity;
  }

  @VisibleForTesting
  void updateLeafQueueState() {
    try {
      writeLock.lock();
      Set<String> newQueues = new HashSet<>();
      for (CSQueue newQueue : managedParentQueue.getChildQueues()) {
        if (newQueue instanceof LeafQueue) {
          addLeafQueueStateIfNotExists((LeafQueue) newQueue);
          newQueues.add(newQueue.getQueueName());
        }
      }

      for (Iterator<Map.Entry<String, LeafQueueState>> itr =
           leafQueueStateMap.entrySet().iterator(); itr.hasNext(); ) {
        Map.Entry<String, LeafQueueState> e = itr.next();
        String queueName = e.getKey();
        if (!newQueues.contains(queueName)) {
          itr.remove();
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  private LinkedHashSet<String> getSortedLeafQueues(
      final List<FiCaSchedulerApp> pendingApps, int leafQueuesNeeded,
      Set<String> deactivatedQueues) throws SchedulerDynamicEditException {

    LinkedHashSet<String> leafQueues = new LinkedHashSet<>(leafQueuesNeeded);
    int ctr = 0;
    for (FiCaSchedulerApp app : pendingApps) {

      AutoCreatedLeafQueue leafQueue =
          (AutoCreatedLeafQueue) app.getCSLeafQueue();
      String leafQueueName = leafQueue.getQueueName();

      //Check if leafQueue is not active already and has any pending apps
      if (ctr < leafQueuesNeeded) {

        if (!isActive(leafQueue)) {
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
  public boolean isActive(final AutoCreatedLeafQueue leafQueue)
      throws SchedulerDynamicEditException {
    try {
      readLock.lock();
      LeafQueueState leafQueueStatus = getLeafQueueState(leafQueue);
      return leafQueueStatus.isActive();
    } finally {
      readLock.unlock();
    }
  }

  private Map<String, QueueCapacities> deactivateLeafQueuesIfInActive(
      ParentQueue parentQueue,
      List<QueueManagementChange> queueManagementChanges)
      throws SchedulerDynamicEditException {
    Map<String, QueueCapacities> deactivatedQueues = new HashMap<>();

    for (CSQueue childQueue : parentQueue.getChildQueues()) {
      AutoCreatedLeafQueue leafQueue = (AutoCreatedLeafQueue) childQueue;

      if (isActive(leafQueue) && !hasPendingApps(leafQueue)) {
        queueManagementChanges.add(
            new QueueManagementChange.UpdateQueue(leafQueue,
                ZERO_CAPACITY_ENTITLEMENT));
        deactivatedQueues.put(leafQueue.getQueueName(),
            leafQueueTemplateCapacities);
      } else{
        if (LOG.isDebugEnabled()) {
          LOG.debug(" Leaf queue has pending applications :  " + leafQueue
              .getNumApplications() + ".Skipping deactivation for "
              + leafQueue);
        }
      }
    }

    if (LOG.isDebugEnabled()) {
      if (deactivatedQueues.size() > 0) {
        LOG.debug("Deactivated leaf queues : " + deactivatedQueues);
      }
    }
    return deactivatedQueues;
  }

  private void computeQueueManagementChanges(
      Set<String> leafQueuesToBeActivated,
      List<QueueManagementChange> queueManagementChanges,
      final float availableCapacity,
      final float leafQueueTemplateAbsoluteCapacity) {

    float curAvailableCapacity = availableCapacity;

    for (String curLeafQueue : leafQueuesToBeActivated) {
      // Activate queues if capacity is available
      if (curAvailableCapacity >= leafQueueTemplateAbsoluteCapacity) {
        AutoCreatedLeafQueue leafQueue =
            (AutoCreatedLeafQueue) scheduler.getCapacitySchedulerQueueManager()
                .getQueue(curLeafQueue);
        if (leafQueue != null) {
          AutoCreatedLeafQueueConfig newTemplate = buildTemplate(
              leafQueueTemplateCapacities.getCapacity(),
              leafQueueTemplateCapacities.getMaximumCapacity());
          queueManagementChanges.add(
              new QueueManagementChange.UpdateQueue(leafQueue, newTemplate));
          curAvailableCapacity -= leafQueueTemplateAbsoluteCapacity;
        } else{
          LOG.warn(
              "Could not find queue in scheduler while trying to deactivate "
                  + curLeafQueue);
        }
      }
    }
  }

  @VisibleForTesting
  public int getMaxLeavesToBeActivated(float availableCapacity,
      float childQueueAbsoluteCapacity, int numPendingApps)
      throws SchedulerDynamicEditException {

    if (childQueueAbsoluteCapacity > 0) {
      int numLeafQueuesNeeded = (int) Math.floor(
          availableCapacity / childQueueAbsoluteCapacity);

      return Math.min(numLeafQueuesNeeded, numPendingApps);
    } else{
      throw new SchedulerDynamicEditException("Child queue absolute capacity "
          + "is initialized to 0. Check parent queue's  " + managedParentQueue
          .getQueueName() + " leaf queue template configuration");
    }
  }

  private float getAvailableCapacity(float parentAbsCapacity,
      float deactivatedAbsCapacity, float totalChildQueueActivatedCapacity) {
    return parentAbsCapacity - totalChildQueueActivatedCapacity
        + deactivatedAbsCapacity + EPSILON;
  }

  /**
   * Commit queue management changes - which involves updating required state
   * on parent/underlying leaf queues
   *
   * @param queueManagementChanges Queue Management changes to commit
   * @throws SchedulerDynamicEditException when validation fails
   */
  @Override
  public void commitQueueManagementChanges(
      List<QueueManagementChange> queueManagementChanges)
      throws SchedulerDynamicEditException {
    try {
      writeLock.lock();
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

        if (updatedQueueTemplate.getQueueCapacities().getCapacity() > 0) {
          if (isActive(leafQueue)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                  "Queue is already active. Skipping activation : " + queue
                      .getQueuePath());
            }
          } else{
            activate(leafQueue);
          }
        } else{
          if (!isActive(leafQueue)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                  "Queue is already de-activated. " + "Skipping de-activation "
                      + ": " + leafQueue.getQueuePath());
            }
          } else{
            deactivate(leafQueue);
          }
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void activate(final AutoCreatedLeafQueue leafQueue)
      throws SchedulerDynamicEditException {
    try {
      writeLock.lock();
      getLeafQueueState(leafQueue).activate();

      parentQueueState.incAbsoluteActivatedChildCapacity(NO_LABEL,
          leafQueueTemplateCapacities.getAbsoluteCapacity());
    } finally {
      writeLock.unlock();
    }
  }

  private void deactivate(final AutoCreatedLeafQueue leafQueue)
      throws SchedulerDynamicEditException {
    try {
      writeLock.lock();
      getLeafQueueState(leafQueue).deactivate();

      for (String nodeLabel : managedParentQueue.getQueueCapacities()
          .getExistingNodeLabels()) {
        parentQueueState.decAbsoluteActivatedChildCapacity(nodeLabel,
            leafQueueTemplateCapacities.getAbsoluteCapacity());
      }
    } finally {
      writeLock.unlock();
    }
  }

  public boolean hasPendingApps(final AutoCreatedLeafQueue leafQueue) {
    return leafQueue.getNumApplications() > 0;
  }

  @Override
  public void reinitialize(CapacitySchedulerContext schedulerContext,
      final ParentQueue parentQueue) {
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
    clearLeafQueueState();

    LOG.info(
        "Reinitialized queue management policy for parent queue "
            + parentQueue.getQueueName() +" with leaf queue template "
            + "capacities : ["
            + leafQueueTemplate.getQueueCapacities() + "]");
  }

  @Override
  public AutoCreatedLeafQueueConfig getInitialLeafQueueConfiguration(
      AbstractAutoCreatedLeafQueue leafQueue)
      throws SchedulerDynamicEditException {

    if ( !(leafQueue instanceof  AutoCreatedLeafQueue)) {
      throw new SchedulerDynamicEditException("Not an instance of "
          + "AutoCreatedLeafQueue : " + leafQueue.getClass());
    }

    AutoCreatedLeafQueue autoCreatedLeafQueue =
        (AutoCreatedLeafQueue) leafQueue;
    AutoCreatedLeafQueueConfig template = ZERO_CAPACITY_ENTITLEMENT;
    try {
      writeLock.lock();
      if (!addLeafQueueStateIfNotExists(leafQueue)) {
        LOG.error("Leaf queue already exists in state : " + getLeafQueueState(
            leafQueue));
        throw new SchedulerDynamicEditException(
            "Leaf queue already exists in state : " + getLeafQueueState(
                leafQueue));
      }

      float availableCapacity = getAvailableCapacity(
          managedParentQueue.getQueueCapacities().getAbsoluteCapacity(), 0,
          parentQueueState.getAbsoluteActivatedChildQueueCapacity());

      if (availableCapacity >= leafQueueTemplateCapacities
          .getAbsoluteCapacity()) {
        activate(autoCreatedLeafQueue);
        template = buildTemplate(leafQueueTemplateCapacities.getCapacity(),
            leafQueueTemplateCapacities.getMaximumCapacity());
      }
    } finally {
      writeLock.unlock();
    }
    return template;
  }

  @VisibleForTesting
  LeafQueueState getLeafQueueState(LeafQueue queue)
      throws SchedulerDynamicEditException {
    try {
      readLock.lock();
      String queueName = queue.getQueueName();
      if (!containsLeafQueue(queueName)) {
        throw new SchedulerDynamicEditException(
            "Could not find leaf queue in " + "state " + queueName);
      } else{
        return leafQueueStateMap.get(queueName);
      }
    } finally {
      readLock.unlock();
    }
  }

  @VisibleForTesting
  public float getAbsoluteActivatedChildQueueCapacity() {
    return parentQueueState.getAbsoluteActivatedChildQueueCapacity();
  }

  private List<FiCaSchedulerApp> getSortedPendingApplications() {
    List<FiCaSchedulerApp> apps = new ArrayList<>(
        managedParentQueue.getAllApplications());
    Collections.sort(apps, applicationComparator);
    return apps;
  }

  private AutoCreatedLeafQueueConfig buildTemplate(float capacity,
      float maxCapacity) {
    AutoCreatedLeafQueueConfig.Builder templateBuilder =
        new AutoCreatedLeafQueueConfig.Builder();

    QueueCapacities capacities = new QueueCapacities(false);
    templateBuilder.capacities(capacities);

    for (String nodeLabel : managedParentQueue.getQueueCapacities()
        .getExistingNodeLabels()) {
      capacities.setCapacity(nodeLabel, capacity);
      capacities.setMaximumCapacity(nodeLabel, maxCapacity);
    }

    return new AutoCreatedLeafQueueConfig(templateBuilder);
  }
}

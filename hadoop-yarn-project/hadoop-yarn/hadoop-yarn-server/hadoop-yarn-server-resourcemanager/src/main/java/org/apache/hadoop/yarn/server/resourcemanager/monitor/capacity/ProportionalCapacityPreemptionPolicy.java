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
package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingEditPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PreemptableResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptableQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class implement a {@link SchedulingEditPolicy} that is designed to be
 * paired with the {@code CapacityScheduler}. At every invocation of {@code
 * editSchedule()} it computes the ideal amount of resources assigned to each
 * queue (for each queue in the hierarchy), and determines whether preemption
 * is needed. Overcapacity is distributed among queues in a weighted fair manner,
 * where the weight is the amount of guaranteed capacity for the queue.
 * Based on this ideal assignment it determines whether preemption is required
 * and select a set of containers from each application that would be killed if
 * the corresponding amount of resources is not freed up by the application.
 *
 * If not in {@code observeOnly} mode, it triggers preemption requests via a
 * {@link ContainerPreemptEvent} that the {@code ResourceManager} will ensure
 * to deliver to the application (or to execute).
 *
 * If the deficit of resources is persistent over a long enough period of time
 * this policy will trigger forced termination of containers (again by generating
 * {@link ContainerPreemptEvent}).
 */
public class ProportionalCapacityPreemptionPolicy
    implements SchedulingEditPolicy, CapacitySchedulerPreemptionContext {
  private static final Log LOG =
    LogFactory.getLog(ProportionalCapacityPreemptionPolicy.class);

  private final Clock clock;

  // Configurable fields
  private double maxIgnoredOverCapacity;
  private long maxWaitTime;
  private long monitoringInterval;
  private float percentageClusterPreemptionAllowed;
  private double naturalTerminationFactor;
  private boolean observeOnly;
  private boolean lazyPreempionEnabled;

  // Pointer to other RM components
  private RMContext rmContext;
  private ResourceCalculator rc;
  private CapacityScheduler scheduler;
  private RMNodeLabelsManager nlm;

  // Internal properties to make decisions of what to preempt
  private final Map<RMContainer,Long> preemptionCandidates =
    new HashMap<>();
  private Map<String, Map<String, TempQueuePerPartition>> queueToPartitions =
      new HashMap<>();
  private List<PreemptionCandidatesSelector>
      candidatesSelectionPolicies = new ArrayList<>();
  private Set<String> allPartitions;
  private Set<String> leafQueueNames;

  // Preemptable Entities, synced from scheduler at every run
  private Map<String, PreemptableQueue> preemptableQueues;
  private Set<ContainerId> killableContainers;

  @SuppressWarnings("unchecked")
  public ProportionalCapacityPreemptionPolicy() {
    clock = SystemClock.getInstance();
    allPartitions = Collections.EMPTY_SET;
    leafQueueNames = Collections.EMPTY_SET;
    preemptableQueues = Collections.EMPTY_MAP;
  }

  @SuppressWarnings("unchecked")
  @VisibleForTesting
  public ProportionalCapacityPreemptionPolicy(RMContext context,
      CapacityScheduler scheduler, Clock clock) {
    init(context.getYarnConfiguration(), context, scheduler);
    this.clock = clock;
    allPartitions = Collections.EMPTY_SET;
    leafQueueNames = Collections.EMPTY_SET;
    preemptableQueues = Collections.EMPTY_MAP;
  }

  public void init(Configuration config, RMContext context,
      PreemptableResourceScheduler sched) {
    LOG.info("Preemption monitor:" + this.getClass().getCanonicalName());
    assert null == scheduler : "Unexpected duplicate call to init";
    if (!(sched instanceof CapacityScheduler)) {
      throw new YarnRuntimeException("Class " +
          sched.getClass().getCanonicalName() + " not instance of " +
          CapacityScheduler.class.getCanonicalName());
    }
    rmContext = context;
    scheduler = (CapacityScheduler) sched;
    CapacitySchedulerConfiguration csConfig = scheduler.getConfiguration();

    maxIgnoredOverCapacity = csConfig.getDouble(
        CapacitySchedulerConfiguration.PREEMPTION_MAX_IGNORED_OVER_CAPACITY,
        CapacitySchedulerConfiguration.DEFAULT_PREEMPTION_MAX_IGNORED_OVER_CAPACITY);

    naturalTerminationFactor = csConfig.getDouble(
        CapacitySchedulerConfiguration.PREEMPTION_NATURAL_TERMINATION_FACTOR,
        CapacitySchedulerConfiguration.DEFAULT_PREEMPTION_NATURAL_TERMINATION_FACTOR);

    maxWaitTime = csConfig.getLong(
        CapacitySchedulerConfiguration.PREEMPTION_WAIT_TIME_BEFORE_KILL,
        CapacitySchedulerConfiguration.DEFAULT_PREEMPTION_WAIT_TIME_BEFORE_KILL);

    monitoringInterval = csConfig.getLong(
        CapacitySchedulerConfiguration.PREEMPTION_MONITORING_INTERVAL,
        CapacitySchedulerConfiguration.DEFAULT_PREEMPTION_MONITORING_INTERVAL);

    percentageClusterPreemptionAllowed = csConfig.getFloat(
        CapacitySchedulerConfiguration.TOTAL_PREEMPTION_PER_ROUND,
        CapacitySchedulerConfiguration.DEFAULT_TOTAL_PREEMPTION_PER_ROUND);

    observeOnly = csConfig.getBoolean(
        CapacitySchedulerConfiguration.PREEMPTION_OBSERVE_ONLY,
        CapacitySchedulerConfiguration.DEFAULT_PREEMPTION_OBSERVE_ONLY);

    lazyPreempionEnabled = csConfig.getBoolean(
        CapacitySchedulerConfiguration.LAZY_PREEMPTION_ENALBED,
        CapacitySchedulerConfiguration.DEFAULT_LAZY_PREEMPTION_ENABLED);

    rc = scheduler.getResourceCalculator();
    nlm = scheduler.getRMContext().getNodeLabelManager();

    // Do we need to specially consider reserved containers?
    boolean selectCandidatesForResevedContainers = csConfig.getBoolean(
        CapacitySchedulerConfiguration.PREEMPTION_SELECT_CANDIDATES_FOR_RESERVED_CONTAINERS,
        CapacitySchedulerConfiguration.DEFAULT_PREEMPTION_SELECT_CANDIDATES_FOR_RESERVED_CONTAINERS);
    if (selectCandidatesForResevedContainers) {
      candidatesSelectionPolicies.add(
          new ReservedContainerCandidatesSelector(this));
    }

    // initialize candidates preemption selection policies
    candidatesSelectionPolicies.add(
        new FifoCandidatesSelector(this));
  }
  
  @Override
  public ResourceCalculator getResourceCalculator() {
    return rc;
  }

  @Override
  public synchronized void editSchedule() {
    long startTs = clock.getTime();

    CSQueue root = scheduler.getRootQueue();
    Resource clusterResources = Resources.clone(scheduler.getClusterResource());
    containerBasedPreemptOrKill(root, clusterResources);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Total time used=" + (clock.getTime() - startTs) + " ms.");
    }
  }

  @SuppressWarnings("unchecked")
  private void preemptOrkillSelectedContainerAfterWait(
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      long currentTime) {
    // preempt (or kill) the selected containers
    for (Map.Entry<ApplicationAttemptId, Set<RMContainer>> e : selectedCandidates
        .entrySet()) {
      ApplicationAttemptId appAttemptId = e.getKey();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Send to scheduler: in app=" + appAttemptId
            + " #containers-to-be-preemptionCandidates=" + e.getValue().size());
      }
      for (RMContainer container : e.getValue()) {
        // if we tried to preempt this for more than maxWaitTime
        if (preemptionCandidates.get(container) != null
            && preemptionCandidates.get(container)
                + maxWaitTime <= currentTime) {
          // kill it
          rmContext.getDispatcher().getEventHandler().handle(
              new ContainerPreemptEvent(appAttemptId, container,
                  SchedulerEventType.MARK_CONTAINER_FOR_KILLABLE));
          preemptionCandidates.remove(container);
        } else {
          if (preemptionCandidates.get(container) != null) {
            // We already updated the information to scheduler earlier, we need
            // not have to raise another event.
            continue;
          }
          //otherwise just send preemption events
          rmContext.getDispatcher().getEventHandler().handle(
              new ContainerPreemptEvent(appAttemptId, container,
                  SchedulerEventType.MARK_CONTAINER_FOR_PREEMPTION));
          preemptionCandidates.put(container, currentTime);
        }
      }
    }
  }

  private void syncKillableContainersFromScheduler() {
    // sync preemptable entities from scheduler
    preemptableQueues =
        scheduler.getPreemptionManager().getShallowCopyOfPreemptableQueues();

    killableContainers = new HashSet<>();
    for (Map.Entry<String, PreemptableQueue> entry : preemptableQueues
        .entrySet()) {
      PreemptableQueue entity = entry.getValue();
      for (Map<ContainerId, RMContainer> map : entity.getKillableContainers()
          .values()) {
        killableContainers.addAll(map.keySet());
      }
    }
  }

  private void cleanupStaledPreemptionCandidates(long currentTime) {
    // Keep the preemptionCandidates list clean
    for (Iterator<RMContainer> i = preemptionCandidates.keySet().iterator();
         i.hasNext(); ) {
      RMContainer id = i.next();
      // garbage collect containers that are irrelevant for preemption
      // And avoid preempt selected containers for *this execution*
      // or within 1 ms
      if (preemptionCandidates.get(id) + 2 * maxWaitTime < currentTime) {
        i.remove();
      }
    }
  }

  private Set<String> getLeafQueueNames(TempQueuePerPartition q) {
    if (q.children == null || q.children.isEmpty()) {
      return ImmutableSet.of(q.queueName);
    }

    Set<String> leafQueueNames = new HashSet<>();
    for (TempQueuePerPartition child : q.children) {
      leafQueueNames.addAll(getLeafQueueNames(child));
    }

    return leafQueueNames;
  }
  
  /**
   * This method selects and tracks containers to be preemptionCandidates. If a container
   * is in the target list for more than maxWaitTime it is killed.
   *
   * @param root the root of the CapacityScheduler queue hierarchy
   * @param clusterResources the total amount of resources in the cluster
   */
  @SuppressWarnings("unchecked")
  private void containerBasedPreemptOrKill(CSQueue root,
      Resource clusterResources) {
    // Sync killable containers from scheduler when lazy preemption enabled
    if (lazyPreempionEnabled) {
      syncKillableContainersFromScheduler();
    }

    // All partitions to look at
    Set<String> partitions = new HashSet<>();
    partitions.addAll(scheduler.getRMContext()
        .getNodeLabelManager().getClusterNodeLabelNames());
    partitions.add(RMNodeLabelsManager.NO_LABEL);
    this.allPartitions = ImmutableSet.copyOf(partitions);

    // extract a summary of the queues from scheduler
    synchronized (scheduler) {
      queueToPartitions.clear();

      for (String partitionToLookAt : allPartitions) {
        cloneQueues(root, Resources
                .clone(nlm.getResourceByLabel(partitionToLookAt, clusterResources)),
            partitionToLookAt);
      }
    }

    this.leafQueueNames = ImmutableSet.copyOf(getLeafQueueNames(
        getQueueByPartition(CapacitySchedulerConfiguration.ROOT,
            RMNodeLabelsManager.NO_LABEL)));

    // compute total preemption allowed
    Resource totalPreemptionAllowed = Resources.multiply(clusterResources,
        percentageClusterPreemptionAllowed);

    // based on ideal allocation select containers to be preemptionCandidates from each
    // queue and each application
    Map<ApplicationAttemptId, Set<RMContainer>> toPreempt =
        new HashMap<>();
    for (PreemptionCandidatesSelector selector :
        candidatesSelectionPolicies) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(MessageFormat
            .format("Trying to use {0} to select preemption candidates",
                selector.getClass().getName()));
      }
      toPreempt = selector.selectCandidates(toPreempt,
          clusterResources, totalPreemptionAllowed);
    }

    if (LOG.isDebugEnabled()) {
      logToCSV(new ArrayList<>(leafQueueNames));
    }

    // if we are in observeOnly mode return before any action is taken
    if (observeOnly) {
      return;
    }

    // TODO: need consider revert killable containers when no more demandings.
    // Since we could have several selectors to make decisions concurrently.
    // So computed ideal-allocation varies between different selectors.
    //
    // We may need to "score" killable containers and revert the most preferred
    // containers. The bottom line is, we shouldn't preempt a queue which is already
    // below its guaranteed resource.

    long currentTime = clock.getTime();

    // preempt (or kill) the selected containers
    preemptOrkillSelectedContainerAfterWait(toPreempt, currentTime);

    // cleanup staled preemption candidates
    cleanupStaledPreemptionCandidates(currentTime);
  }

  @Override
  public long getMonitoringInterval() {
    return monitoringInterval;
  }

  @Override
  public String getPolicyName() {
    return "ProportionalCapacityPreemptionPolicy";
  }

  @VisibleForTesting
  public Map<RMContainer, Long> getToPreemptContainers() {
    return preemptionCandidates;
  }

  /**
   * This method walks a tree of CSQueue and clones the portion of the state
   * relevant for preemption in TempQueue(s). It also maintains a pointer to
   * the leaves. Finally it aggregates pending resources in each queue and rolls
   * it up to higher levels.
   *
   * @param curQueue current queue which I'm looking at now
   * @param partitionResource the total amount of resources in the cluster
   * @return the root of the cloned queue hierarchy
   */
  private TempQueuePerPartition cloneQueues(CSQueue curQueue,
      Resource partitionResource, String partitionToLookAt) {
    TempQueuePerPartition ret;
    synchronized (curQueue) {
      String queueName = curQueue.getQueueName();
      QueueCapacities qc = curQueue.getQueueCapacities();
      float absCap = qc.getAbsoluteCapacity(partitionToLookAt);
      float absMaxCap = qc.getAbsoluteMaximumCapacity(partitionToLookAt);
      boolean preemptionDisabled = curQueue.getPreemptionDisabled();

      Resource current = Resources.clone(
          curQueue.getQueueResourceUsage().getUsed(partitionToLookAt));
      Resource killable = Resources.none();

      Resource reserved = Resources.clone(
          curQueue.getQueueResourceUsage().getReserved(partitionToLookAt));
      if (null != preemptableQueues.get(queueName)) {
        killable = Resources.clone(preemptableQueues.get(queueName)
            .getKillableResource(partitionToLookAt));
      }

      // when partition is a non-exclusive partition, the actual maxCapacity
      // could more than specified maxCapacity
      try {
        if (!scheduler.getRMContext().getNodeLabelManager()
            .isExclusiveNodeLabel(partitionToLookAt)) {
          absMaxCap = 1.0f;
        }
      } catch (IOException e) {
        // This may cause by partition removed when running capacity monitor,
        // just ignore the error, this will be corrected when doing next check.
      }

      ret = new TempQueuePerPartition(queueName, current, preemptionDisabled,
          partitionToLookAt, killable, absCap, absMaxCap, partitionResource,
          reserved, curQueue);

      if (curQueue instanceof ParentQueue) {
        // Recursively add children
        for (CSQueue c : curQueue.getChildQueues()) {
          TempQueuePerPartition subq = cloneQueues(c, partitionResource,
              partitionToLookAt);
          ret.addChild(subq);
        }
      }
    }
    addTempQueuePartition(ret);
    return ret;
  }

  // simple printout function that reports internal queue state (useful for
  // plotting)
  private void logToCSV(List<String> leafQueueNames){
    Collections.sort(leafQueueNames);
    String queueState = " QUEUESTATE: " + clock.getTime();
    StringBuilder sb = new StringBuilder();
    sb.append(queueState);

    for (String queueName : leafQueueNames) {
      TempQueuePerPartition tq =
          getQueueByPartition(queueName, RMNodeLabelsManager.NO_LABEL);
      sb.append(", ");
      tq.appendLogString(sb);
    }
    LOG.debug(sb.toString());
  }

  private void addTempQueuePartition(TempQueuePerPartition queuePartition) {
    String queueName = queuePartition.queueName;

    Map<String, TempQueuePerPartition> queuePartitions;
    if (null == (queuePartitions = queueToPartitions.get(queueName))) {
      queuePartitions = new HashMap<>();
      queueToPartitions.put(queueName, queuePartitions);
    }
    queuePartitions.put(queuePartition.partition, queuePartition);
  }

  /**
   * Get queue partition by given queueName and partitionName
   */
  @Override
  public TempQueuePerPartition getQueueByPartition(String queueName,
      String partition) {
    Map<String, TempQueuePerPartition> partitionToQueues;
    if (null == (partitionToQueues = queueToPartitions.get(queueName))) {
      throw new YarnRuntimeException("This shouldn't happen, cannot find "
          + "TempQueuePerPartition for queueName=" + queueName);
    }
    return partitionToQueues.get(partition);
  }

  /**
   * Get all queue partitions by given queueName
   */
  @Override
  public Collection<TempQueuePerPartition> getQueuePartitions(String queueName) {
    if (!queueToPartitions.containsKey(queueName)) {
      throw new YarnRuntimeException("This shouldn't happen, cannot find "
          + "TempQueuePerPartition collection for queueName=" + queueName);
    }
    return queueToPartitions.get(queueName).values();
  }

  @Override
  public CapacityScheduler getScheduler() {
    return scheduler;
  }

  @Override
  public RMContext getRMContext() {
    return rmContext;
  }

  @Override
  public boolean isObserveOnly() {
    return observeOnly;
  }

  @Override
  public Set<ContainerId> getKillableContainers() {
    return killableContainers;
  }

  @Override
  public double getMaxIgnoreOverCapacity() {
    return maxIgnoredOverCapacity;
  }

  @Override
  public double getNaturalTerminationFactor() {
    return naturalTerminationFactor;
  }

  @Override
  public Set<String> getLeafQueueNames() {
    return leafQueueNames;
  }

  @Override
  public Set<String> getAllPartitions() {
    return allPartitions;
  }

  @VisibleForTesting
  Map<String, Map<String, TempQueuePerPartition>> getQueuePartitions() {
    return queueToPartitions;
  }
}

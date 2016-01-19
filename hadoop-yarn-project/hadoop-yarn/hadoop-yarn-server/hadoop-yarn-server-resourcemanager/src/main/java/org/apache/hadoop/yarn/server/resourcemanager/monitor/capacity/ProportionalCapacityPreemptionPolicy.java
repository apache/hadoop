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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingEditPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PreemptableResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

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
public class ProportionalCapacityPreemptionPolicy implements SchedulingEditPolicy {

  private static final Log LOG =
    LogFactory.getLog(ProportionalCapacityPreemptionPolicy.class);

  /** If true, run the policy but do not affect the cluster with preemption and
   * kill events. */
  public static final String OBSERVE_ONLY =
      "yarn.resourcemanager.monitor.capacity.preemption.observe_only";
  /** Time in milliseconds between invocations of this policy */
  public static final String MONITORING_INTERVAL =
      "yarn.resourcemanager.monitor.capacity.preemption.monitoring_interval";
  /** Time in milliseconds between requesting a preemption from an application
   * and killing the container. */
  public static final String WAIT_TIME_BEFORE_KILL =
      "yarn.resourcemanager.monitor.capacity.preemption.max_wait_before_kill";
  /** Maximum percentage of resources preempted in a single round. By
   * controlling this value one can throttle the pace at which containers are
   * reclaimed from the cluster. After computing the total desired preemption,
   * the policy scales it back within this limit. */
  public static final String TOTAL_PREEMPTION_PER_ROUND =
      "yarn.resourcemanager.monitor.capacity.preemption.total_preemption_per_round";
  /** Maximum amount of resources above the target capacity ignored for
   * preemption. This defines a deadzone around the target capacity that helps
   * prevent thrashing and oscillations around the computed target balance.
   * High values would slow the time to capacity and (absent natural
   * completions) it might prevent convergence to guaranteed capacity. */
  public static final String MAX_IGNORED_OVER_CAPACITY =
    "yarn.resourcemanager.monitor.capacity.preemption.max_ignored_over_capacity";
  /**
   * Given a computed preemption target, account for containers naturally
   * expiring and preempt only this percentage of the delta. This determines
   * the rate of geometric convergence into the deadzone ({@link
   * #MAX_IGNORED_OVER_CAPACITY}). For example, a termination factor of 0.5
   * will reclaim almost 95% of resources within 5 * {@link
   * #WAIT_TIME_BEFORE_KILL}, even absent natural termination. */
  public static final String NATURAL_TERMINATION_FACTOR =
      "yarn.resourcemanager.monitor.capacity.preemption.natural_termination_factor";

  private RMContext rmContext;

  private final Clock clock;
  private double maxIgnoredOverCapacity;
  private long maxWaitTime;
  private CapacityScheduler scheduler;
  private long monitoringInterval;
  private final Map<RMContainer,Long> preempted =
    new HashMap<RMContainer,Long>();
  private ResourceCalculator rc;
  private float percentageClusterPreemptionAllowed;
  private double naturalTerminationFactor;
  private boolean observeOnly;
  private Map<String, Map<String, TempQueuePerPartition>> queueToPartitions =
      new HashMap<>();
  private RMNodeLabelsManager nlm;

  public ProportionalCapacityPreemptionPolicy() {
    clock = new SystemClock();
  }

  public ProportionalCapacityPreemptionPolicy(Configuration config,
      RMContext context, CapacityScheduler scheduler) {
    this(config, context, scheduler, new SystemClock());
  }

  public ProportionalCapacityPreemptionPolicy(Configuration config,
      RMContext context, CapacityScheduler scheduler, Clock clock) {
    init(config, context, scheduler);
    this.clock = clock;
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
    maxIgnoredOverCapacity = config.getDouble(MAX_IGNORED_OVER_CAPACITY, 0.1);
    naturalTerminationFactor =
      config.getDouble(NATURAL_TERMINATION_FACTOR, 0.2);
    maxWaitTime = config.getLong(WAIT_TIME_BEFORE_KILL, 15000);
    monitoringInterval = config.getLong(MONITORING_INTERVAL, 3000);
    percentageClusterPreemptionAllowed =
      config.getFloat(TOTAL_PREEMPTION_PER_ROUND, (float) 0.1);
    observeOnly = config.getBoolean(OBSERVE_ONLY, false);
    rc = scheduler.getResourceCalculator();
    nlm = scheduler.getRMContext().getNodeLabelManager();
  }
  
  @VisibleForTesting
  public ResourceCalculator getResourceCalculator() {
    return rc;
  }

  @Override
  public void editSchedule() {
    CSQueue root = scheduler.getRootQueue();
    Resource clusterResources = Resources.clone(scheduler.getClusterResource());
    containerBasedPreemptOrKill(root, clusterResources);
  }
  
  /**
   * This method selects and tracks containers to be preempted. If a container
   * is in the target list for more than maxWaitTime it is killed.
   *
   * @param root the root of the CapacityScheduler queue hierarchy
   * @param clusterResources the total amount of resources in the cluster
   */
  @SuppressWarnings("unchecked")
  private void containerBasedPreemptOrKill(CSQueue root,
      Resource clusterResources) {
    // All partitions to look at
    Set<String> allPartitions = new HashSet<>();
    allPartitions.addAll(scheduler.getRMContext()
        .getNodeLabelManager().getClusterNodeLabelNames());
    allPartitions.add(RMNodeLabelsManager.NO_LABEL);

    // extract a summary of the queues from scheduler
    synchronized (scheduler) {
      queueToPartitions.clear();

      for (String partitionToLookAt : allPartitions) {
        cloneQueues(root,
            nlm.getResourceByLabel(partitionToLookAt, clusterResources),
            partitionToLookAt);
      }
    }

    // compute total preemption allowed
    Resource totalPreemptionAllowed = Resources.multiply(clusterResources,
        percentageClusterPreemptionAllowed);

    Set<String> leafQueueNames = null;
    for (String partition : allPartitions) {
      TempQueuePerPartition tRoot =
          getQueueByPartition(CapacitySchedulerConfiguration.ROOT, partition);
      // compute the ideal distribution of resources among queues
      // updates cloned queues state accordingly
      tRoot.idealAssigned = tRoot.guaranteed;

      leafQueueNames =
          recursivelyComputeIdealAssignment(tRoot, totalPreemptionAllowed);
    }

    // based on ideal allocation select containers to be preempted from each
    // queue and each application
    Map<ApplicationAttemptId,Set<RMContainer>> toPreempt =
        getContainersToPreempt(leafQueueNames, clusterResources);

    if (LOG.isDebugEnabled()) {
      logToCSV(new ArrayList<String>(leafQueueNames));
    }

    // if we are in observeOnly mode return before any action is taken
    if (observeOnly) {
      return;
    }

    // preempt (or kill) the selected containers
    for (Map.Entry<ApplicationAttemptId,Set<RMContainer>> e
         : toPreempt.entrySet()) {
      ApplicationAttemptId appAttemptId = e.getKey();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Send to scheduler: in app=" + appAttemptId
            + " #containers-to-be-preempted=" + e.getValue().size());
      }
      for (RMContainer container : e.getValue()) {
        // if we tried to preempt this for more than maxWaitTime
        if (preempted.get(container) != null &&
            preempted.get(container) + maxWaitTime < clock.getTime()) {
          // kill it
          rmContext.getDispatcher().getEventHandler().handle(
              new ContainerPreemptEvent(appAttemptId, container,
                  SchedulerEventType.KILL_CONTAINER));
          preempted.remove(container);
        } else {
          if (preempted.get(container) != null) {
            // We already updated the information to scheduler earlier, we need
            // not have to raise another event.
            continue;
          }
          //otherwise just send preemption events
          rmContext.getDispatcher().getEventHandler().handle(
              new ContainerPreemptEvent(appAttemptId, container,
                  SchedulerEventType.PREEMPT_CONTAINER));
          preempted.put(container, clock.getTime());
        }
      }
    }

    // Keep the preempted list clean
    for (Iterator<RMContainer> i = preempted.keySet().iterator(); i.hasNext();){
      RMContainer id = i.next();
      // garbage collect containers that are irrelevant for preemption
      if (preempted.get(id) + 2 * maxWaitTime < clock.getTime()) {
        i.remove();
      }
    }
  }

  /**
   * This method recursively computes the ideal assignment of resources to each
   * level of the hierarchy. This ensures that leafs that are over-capacity but
   * with parents within capacity will not be preempted. Preemptions are allowed
   * within each subtree according to local over/under capacity.
   *
   * @param root the root of the cloned queue hierachy
   * @param totalPreemptionAllowed maximum amount of preemption allowed
   * @return a list of leaf queues updated with preemption targets
   */
  private Set<String> recursivelyComputeIdealAssignment(
      TempQueuePerPartition root, Resource totalPreemptionAllowed) {
    Set<String> leafQueueNames = new HashSet<>();
    if (root.getChildren() != null &&
        root.getChildren().size() > 0) {
      // compute ideal distribution at this level
      computeIdealResourceDistribution(rc, root.getChildren(),
          totalPreemptionAllowed, root.idealAssigned);
      // compute recursively for lower levels and build list of leafs
      for(TempQueuePerPartition t : root.getChildren()) {
        leafQueueNames.addAll(recursivelyComputeIdealAssignment(t,
            totalPreemptionAllowed));
      }
    } else {
      // we are in a leaf nothing to do, just return yourself
      return ImmutableSet.of(root.queueName);
    }
    return leafQueueNames;
  }

  /**
   * This method computes (for a single level in the tree, passed as a {@code
   * List<TempQueue>}) the ideal assignment of resources. This is done
   * recursively to allocate capacity fairly across all queues with pending
   * demands. It terminates when no resources are left to assign, or when all
   * demand is satisfied.
   *
   * @param rc resource calculator
   * @param queues a list of cloned queues to be assigned capacity to (this is
   * an out param)
   * @param totalPreemptionAllowed total amount of preemption we allow
   * @param tot_guarant the amount of capacity assigned to this pool of queues
   */
  private void computeIdealResourceDistribution(ResourceCalculator rc,
      List<TempQueuePerPartition> queues, Resource totalPreemptionAllowed,
      Resource tot_guarant) {

    // qAlloc tracks currently active queues (will decrease progressively as
    // demand is met)
    List<TempQueuePerPartition> qAlloc = new ArrayList<TempQueuePerPartition>(queues);
    // unassigned tracks how much resources are still to assign, initialized
    // with the total capacity for this set of queues
    Resource unassigned = Resources.clone(tot_guarant);

    // group queues based on whether they have non-zero guaranteed capacity
    Set<TempQueuePerPartition> nonZeroGuarQueues = new HashSet<TempQueuePerPartition>();
    Set<TempQueuePerPartition> zeroGuarQueues = new HashSet<TempQueuePerPartition>();

    for (TempQueuePerPartition q : qAlloc) {
      if (Resources
          .greaterThan(rc, tot_guarant, q.guaranteed, Resources.none())) {
        nonZeroGuarQueues.add(q);
      } else {
        zeroGuarQueues.add(q);
      }
    }

    // first compute the allocation as a fixpoint based on guaranteed capacity
    computeFixpointAllocation(rc, tot_guarant, nonZeroGuarQueues, unassigned,
        false);

    // if any capacity is left unassigned, distributed among zero-guarantee 
    // queues uniformly (i.e., not based on guaranteed capacity, as this is zero)
    if (!zeroGuarQueues.isEmpty()
        && Resources.greaterThan(rc, tot_guarant, unassigned, Resources.none())) {
      computeFixpointAllocation(rc, tot_guarant, zeroGuarQueues, unassigned,
          true);
    }
    
    // based on ideal assignment computed above and current assignment we derive
    // how much preemption is required overall
    Resource totPreemptionNeeded = Resource.newInstance(0, 0);
    for (TempQueuePerPartition t:queues) {
      if (Resources.greaterThan(rc, tot_guarant, t.current, t.idealAssigned)) {
        Resources.addTo(totPreemptionNeeded,
            Resources.subtract(t.current, t.idealAssigned));
      }
    }

    // if we need to preempt more than is allowed, compute a factor (0<f<1)
    // that is used to scale down how much we ask back from each queue
    float scalingFactor = 1.0F;
    if (Resources.greaterThan(rc, tot_guarant,
          totPreemptionNeeded, totalPreemptionAllowed)) {
       scalingFactor = Resources.divide(rc, tot_guarant,
           totalPreemptionAllowed, totPreemptionNeeded);
    }

    // assign to each queue the amount of actual preemption based on local
    // information of ideal preemption and scaling factor
    for (TempQueuePerPartition t : queues) {
      t.assignPreemption(scalingFactor, rc, tot_guarant);
    }
    if (LOG.isDebugEnabled()) {
      long time = clock.getTime();
      for (TempQueuePerPartition t : queues) {
        LOG.debug(time + ": " + t);
      }
    }

  }
  
  /**
   * Given a set of queues compute the fix-point distribution of unassigned
   * resources among them. As pending request of a queue are exhausted, the
   * queue is removed from the set and remaining capacity redistributed among
   * remaining queues. The distribution is weighted based on guaranteed
   * capacity, unless asked to ignoreGuarantee, in which case resources are
   * distributed uniformly.
   */
  private void computeFixpointAllocation(ResourceCalculator rc,
      Resource tot_guarant, Collection<TempQueuePerPartition> qAlloc,
      Resource unassigned, boolean ignoreGuarantee) {
    // Prior to assigning the unused resources, process each queue as follows:
    // If current > guaranteed, idealAssigned = guaranteed + untouchable extra
    // Else idealAssigned = current;
    // Subtract idealAssigned resources from unassigned.
    // If the queue has all of its needs met (that is, if 
    // idealAssigned >= current + pending), remove it from consideration.
    // Sort queues from most under-guaranteed to most over-guaranteed.
    TQComparator tqComparator = new TQComparator(rc, tot_guarant);
    PriorityQueue<TempQueuePerPartition> orderedByNeed =
        new PriorityQueue<TempQueuePerPartition>(10, tqComparator);
    for (Iterator<TempQueuePerPartition> i = qAlloc.iterator(); i.hasNext();) {
      TempQueuePerPartition q = i.next();
      if (Resources.greaterThan(rc, tot_guarant, q.current, q.guaranteed)) {
        q.idealAssigned = Resources.add(q.guaranteed, q.untouchableExtra);
      } else {
        q.idealAssigned = Resources.clone(q.current);
      }
      Resources.subtractFrom(unassigned, q.idealAssigned);
      // If idealAssigned < (current + pending), q needs more resources, so
      // add it to the list of underserved queues, ordered by need.
      Resource curPlusPend = Resources.add(q.current, q.pending);
      if (Resources.lessThan(rc, tot_guarant, q.idealAssigned, curPlusPend)) {
        orderedByNeed.add(q);
      }
    }

    //assign all cluster resources until no more demand, or no resources are left
    while (!orderedByNeed.isEmpty()
       && Resources.greaterThan(rc,tot_guarant, unassigned,Resources.none())) {
      Resource wQassigned = Resource.newInstance(0, 0);
      // we compute normalizedGuarantees capacity based on currently active
      // queues
      resetCapacity(rc, unassigned, orderedByNeed, ignoreGuarantee);

      // For each underserved queue (or set of queues if multiple are equally
      // underserved), offer its share of the unassigned resources based on its
      // normalized guarantee. After the offer, if the queue is not satisfied,
      // place it back in the ordered list of queues, recalculating its place
      // in the order of most under-guaranteed to most over-guaranteed. In this
      // way, the most underserved queue(s) are always given resources first.
      Collection<TempQueuePerPartition> underserved =
          getMostUnderservedQueues(orderedByNeed, tqComparator);
      for (Iterator<TempQueuePerPartition> i = underserved.iterator(); i
          .hasNext();) {
        TempQueuePerPartition sub = i.next();
        Resource wQavail = Resources.multiplyAndNormalizeUp(rc,
            unassigned, sub.normalizedGuarantee, Resource.newInstance(1, 1));
        Resource wQidle = sub.offer(wQavail, rc, tot_guarant);
        Resource wQdone = Resources.subtract(wQavail, wQidle);

        if (Resources.greaterThan(rc, tot_guarant,
              wQdone, Resources.none())) {
          // The queue is still asking for more. Put it back in the priority
          // queue, recalculating its order based on need.
          orderedByNeed.add(sub);
        }
        Resources.addTo(wQassigned, wQdone);
      }
      Resources.subtractFrom(unassigned, wQassigned);
    }
  }

  // Take the most underserved TempQueue (the one on the head). Collect and
  // return the list of all queues that have the same idealAssigned
  // percentage of guaranteed.
  protected Collection<TempQueuePerPartition> getMostUnderservedQueues(
      PriorityQueue<TempQueuePerPartition> orderedByNeed, TQComparator tqComparator) {
    ArrayList<TempQueuePerPartition> underserved = new ArrayList<TempQueuePerPartition>();
    while (!orderedByNeed.isEmpty()) {
      TempQueuePerPartition q1 = orderedByNeed.remove();
      underserved.add(q1);
      TempQueuePerPartition q2 = orderedByNeed.peek();
      // q1's pct of guaranteed won't be larger than q2's. If it's less, then
      // return what has already been collected. Otherwise, q1's pct of
      // guaranteed == that of q2, so add q2 to underserved list during the
      // next pass.
      if (q2 == null || tqComparator.compare(q1,q2) < 0) {
        return underserved;
      }
    }
    return underserved;
  }

  /**
   * Computes a normalizedGuaranteed capacity based on active queues
   * @param rc resource calculator
   * @param clusterResource the total amount of resources in the cluster
   * @param queues the list of queues to consider
   */
  private void resetCapacity(ResourceCalculator rc, Resource clusterResource,
      Collection<TempQueuePerPartition> queues, boolean ignoreGuar) {
    Resource activeCap = Resource.newInstance(0, 0);
    
    if (ignoreGuar) {
      for (TempQueuePerPartition q : queues) {
        q.normalizedGuarantee = (float)  1.0f / ((float) queues.size());
      }
    } else {
      for (TempQueuePerPartition q : queues) {
        Resources.addTo(activeCap, q.guaranteed);
      }
      for (TempQueuePerPartition q : queues) {
        q.normalizedGuarantee = Resources.divide(rc, clusterResource,
            q.guaranteed, activeCap);
      }
    }
  }

  private String getPartitionByNodeId(NodeId nodeId) {
    return scheduler.getSchedulerNode(nodeId).getPartition();
  }

  /**
   * Return should we preempt rmContainer. If we should, deduct from
   * <code>resourceToObtainByPartition</code>
   */
  private boolean tryPreemptContainerAndDeductResToObtain(
      Map<String, Resource> resourceToObtainByPartitions,
      RMContainer rmContainer, Resource clusterResource,
      Map<ApplicationAttemptId, Set<RMContainer>> preemptMap) {
    ApplicationAttemptId attemptId = rmContainer.getApplicationAttemptId();

    // We will not account resource of a container twice or more
    if (preemptMapContains(preemptMap, attemptId, rmContainer)) {
      return false;
    }

    String nodePartition = getPartitionByNodeId(rmContainer.getAllocatedNode());
    Resource toObtainByPartition =
        resourceToObtainByPartitions.get(nodePartition);

    if (null != toObtainByPartition
        && Resources.greaterThan(rc, clusterResource, toObtainByPartition,
            Resources.none())) {
      Resources.subtractFrom(toObtainByPartition,
          rmContainer.getAllocatedResource());
      // When we have no more resource need to obtain, remove from map.
      if (Resources.lessThanOrEqual(rc, clusterResource, toObtainByPartition,
          Resources.none())) {
        resourceToObtainByPartitions.remove(nodePartition);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Marked container=" + rmContainer.getContainerId()
            + " in partition=" + nodePartition + " will be preempted");
      }
      // Add to preemptMap
      addToPreemptMap(preemptMap, attemptId, rmContainer);
      return true;
    }

    return false;
  }

  private boolean preemptMapContains(
      Map<ApplicationAttemptId, Set<RMContainer>> preemptMap,
      ApplicationAttemptId attemptId, RMContainer rmContainer) {
    Set<RMContainer> rmContainers;
    if (null == (rmContainers = preemptMap.get(attemptId))) {
      return false;
    }
    return rmContainers.contains(rmContainer);
  }

  private void addToPreemptMap(
      Map<ApplicationAttemptId, Set<RMContainer>> preemptMap,
      ApplicationAttemptId appAttemptId, RMContainer containerToPreempt) {
    Set<RMContainer> set;
    if (null == (set = preemptMap.get(appAttemptId))) {
      set = new HashSet<RMContainer>();
      preemptMap.put(appAttemptId, set);
    }
    set.add(containerToPreempt);
  }

  /**
   * Based a resource preemption target drop reservations of containers and
   * if necessary select containers for preemption from applications in each
   * over-capacity queue. It uses {@link #NATURAL_TERMINATION_FACTOR} to
   * account for containers that will naturally complete.
   *
   * @param queues set of leaf queues to preempt from
   * @param clusterResource total amount of cluster resources
   * @return a map of applciationID to set of containers to preempt
   */
  private Map<ApplicationAttemptId,Set<RMContainer>> getContainersToPreempt(
      Set<String> leafQueueNames, Resource clusterResource) {

    Map<ApplicationAttemptId, Set<RMContainer>> preemptMap =
        new HashMap<ApplicationAttemptId, Set<RMContainer>>();
    List<RMContainer> skippedAMContainerlist = new ArrayList<RMContainer>();

    // Loop all leaf queues
    for (String queueName : leafQueueNames) {
      // check if preemption disabled for the queue
      if (getQueueByPartition(queueName,
          RMNodeLabelsManager.NO_LABEL).preemptionDisabled) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("skipping from queue=" + queueName
              + " because it's a non-preemptable queue");
        }
        continue;
      }

      // compute resToObtainByPartition considered inter-queue preemption
      LeafQueue leafQueue = null;

      Map<String, Resource> resToObtainByPartition =
          new HashMap<String, Resource>();
      for (TempQueuePerPartition qT : getQueuePartitions(queueName)) {
        leafQueue = qT.leafQueue;
        // we act only if we are violating balance by more than
        // maxIgnoredOverCapacity
        if (Resources.greaterThan(rc, clusterResource, qT.current,
            Resources.multiply(qT.guaranteed, 1.0 + maxIgnoredOverCapacity))) {
          // we introduce a dampening factor naturalTerminationFactor that
          // accounts for natural termination of containers
          Resource resToObtain =
              Resources.multiply(qT.toBePreempted, naturalTerminationFactor);
          // Only add resToObtain when it >= 0
          if (Resources.greaterThan(rc, clusterResource, resToObtain,
              Resources.none())) {
            resToObtainByPartition.put(qT.partition, resToObtain);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Queue=" + queueName + " partition=" + qT.partition
                  + " resource-to-obtain=" + resToObtain);
            }
          }
          qT.actuallyPreempted = Resources.clone(resToObtain);
        } else {
          qT.actuallyPreempted = Resources.none();
        }
      }

      synchronized (leafQueue) {
        // go through all ignore-partition-exclusivity containers first to make
        // sure such containers will be preempted first
        Map<String, TreeSet<RMContainer>> ignorePartitionExclusivityContainers =
            leafQueue.getIgnoreExclusivityRMContainers();
        for (String partition : resToObtainByPartition.keySet()) {
          if (ignorePartitionExclusivityContainers.containsKey(partition)) {
            TreeSet<RMContainer> rmContainers =
                ignorePartitionExclusivityContainers.get(partition);
            // We will check container from reverse order, so latter submitted
            // application's containers will be preempted first.
            for (RMContainer c : rmContainers.descendingSet()) {
              boolean preempted =
                  tryPreemptContainerAndDeductResToObtain(
                      resToObtainByPartition, c, clusterResource, preemptMap);
              if (!preempted) {
                break;
              }
            }
          }
        }

        // preempt other containers
        Resource skippedAMSize = Resource.newInstance(0, 0);
        Iterator<FiCaSchedulerApp> desc =
            leafQueue.getOrderingPolicy().getPreemptionIterator();
        while (desc.hasNext()) {
          FiCaSchedulerApp fc = desc.next();
          // When we complete preempt from one partition, we will remove from
          // resToObtainByPartition, so when it becomes empty, we can get no
          // more preemption is needed
          if (resToObtainByPartition.isEmpty()) {
            break;
          }

          preemptFrom(fc, clusterResource, resToObtainByPartition,
              skippedAMContainerlist, skippedAMSize, preemptMap);
        }

        // Can try preempting AMContainers (still saving atmost
        // maxAMCapacityForThisQueue AMResource's) if more resources are
        // required to be preempted from this Queue.
        Resource maxAMCapacityForThisQueue = Resources.multiply(
            Resources.multiply(clusterResource,
                leafQueue.getAbsoluteCapacity()),
            leafQueue.getMaxAMResourcePerQueuePercent());

        preemptAMContainers(clusterResource, preemptMap, skippedAMContainerlist,
            resToObtainByPartition, skippedAMSize, maxAMCapacityForThisQueue);
      }
    }

    return preemptMap;
  }

  /**
   * As more resources are needed for preemption, saved AMContainers has to be
   * rescanned. Such AMContainers can be preempted based on resToObtain, but 
   * maxAMCapacityForThisQueue resources will be still retained.
   *  
   * @param clusterResource
   * @param preemptMap
   * @param skippedAMContainerlist
   * @param resToObtain
   * @param skippedAMSize
   * @param maxAMCapacityForThisQueue
   */
  private void preemptAMContainers(Resource clusterResource,
      Map<ApplicationAttemptId, Set<RMContainer>> preemptMap,
      List<RMContainer> skippedAMContainerlist,
      Map<String, Resource> resToObtainByPartition, Resource skippedAMSize,
      Resource maxAMCapacityForThisQueue) {
    for (RMContainer c : skippedAMContainerlist) {
      // Got required amount of resources for preemption, can stop now
      if (resToObtainByPartition.isEmpty()) {
        break;
      }
      // Once skippedAMSize reaches down to maxAMCapacityForThisQueue,
      // container selection iteration for preemption will be stopped.
      if (Resources.lessThanOrEqual(rc, clusterResource, skippedAMSize,
          maxAMCapacityForThisQueue)) {
        break;
      }

      boolean preempted =
          tryPreemptContainerAndDeductResToObtain(resToObtainByPartition, c,
              clusterResource, preemptMap);
      if (preempted) {
        Resources.subtractFrom(skippedAMSize, c.getAllocatedResource());
      }
    }
    skippedAMContainerlist.clear();
  }

  /**
   * Given a target preemption for a specific application, select containers
   * to preempt (after unreserving all reservation for that app).
   */
  @SuppressWarnings("unchecked")
  private void preemptFrom(FiCaSchedulerApp app,
      Resource clusterResource, Map<String, Resource> resToObtainByPartition,
      List<RMContainer> skippedAMContainerlist, Resource skippedAMSize,
      Map<ApplicationAttemptId, Set<RMContainer>> preemptMap) {
    ApplicationAttemptId appId = app.getApplicationAttemptId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Looking at application=" + app.getApplicationAttemptId()
          + " resourceToObtain=" + resToObtainByPartition);
    }

    // first drop reserved containers towards rsrcPreempt
    List<RMContainer> reservedContainers =
        new ArrayList<RMContainer>(app.getReservedContainers());
    for (RMContainer c : reservedContainers) {
      if (resToObtainByPartition.isEmpty()) {
        return;
      }

      // Try to preempt this container
      tryPreemptContainerAndDeductResToObtain(resToObtainByPartition, c,
          clusterResource, preemptMap);

      if (!observeOnly) {
        rmContext.getDispatcher().getEventHandler().handle(
            new ContainerPreemptEvent(
                appId, c, SchedulerEventType.DROP_RESERVATION));
      }
    }

    // if more resources are to be freed go through all live containers in
    // reverse priority and reverse allocation order and mark them for
    // preemption
    List<RMContainer> liveContainers =
      new ArrayList<RMContainer>(app.getLiveContainers());

    sortContainers(liveContainers);

    for (RMContainer c : liveContainers) {
      if (resToObtainByPartition.isEmpty()) {
        return;
      }

      // Skip AM Container from preemption for now.
      if (c.isAMContainer()) {
        skippedAMContainerlist.add(c);
        Resources.addTo(skippedAMSize, c.getAllocatedResource());
        continue;
      }

      // Try to preempt this container
      tryPreemptContainerAndDeductResToObtain(resToObtainByPartition, c,
          clusterResource, preemptMap);
    }
  }

  /**
   * Compare by reversed priority order first, and then reversed containerId
   * order
   * @param containers
   */
  @VisibleForTesting
  static void sortContainers(List<RMContainer> containers){
    Collections.sort(containers, new Comparator<RMContainer>() {
      @Override
      public int compare(RMContainer a, RMContainer b) {
        Comparator<Priority> c = new org.apache.hadoop.yarn.server
            .resourcemanager.resource.Priority.Comparator();
        int priorityComp = c.compare(b.getContainer().getPriority(),
                                     a.getContainer().getPriority());
        if (priorityComp != 0) {
          return priorityComp;
        }
        return b.getContainerId().compareTo(a.getContainerId());
      }
    });
  }

  @Override
  public long getMonitoringInterval() {
    return monitoringInterval;
  }

  @Override
  public String getPolicyName() {
    return "ProportionalCapacityPreemptionPolicy";
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

      Resource current = curQueue.getQueueResourceUsage().getUsed(
          partitionToLookAt);
      Resource guaranteed = Resources.multiply(partitionResource, absCap);
      Resource maxCapacity = Resources.multiply(partitionResource, absMaxCap);

      // when partition is a non-exclusive partition, the actual maxCapacity
      // could more than specified maxCapacity
      try {
        if (!scheduler.getRMContext().getNodeLabelManager()
            .isExclusiveNodeLabel(partitionToLookAt)) {
          maxCapacity =
              Resources.max(rc, partitionResource, maxCapacity, current);
        }
      } catch (IOException e) {
        // This may cause by partition removed when running capacity monitor,
        // just ignore the error, this will be corrected when doing next check.
      }

      Resource extra = Resource.newInstance(0, 0);
      if (Resources.greaterThan(rc, partitionResource, current, guaranteed)) {
        extra = Resources.subtract(current, guaranteed);
      }
      if (curQueue instanceof LeafQueue) {
        LeafQueue l = (LeafQueue) curQueue;
        Resource pending =
              l.getTotalPendingResourcesConsideringUserLimit(
                  partitionResource, partitionToLookAt);
        ret = new TempQueuePerPartition(queueName, current, pending, guaranteed,
            maxCapacity, preemptionDisabled, partitionToLookAt);
        if (preemptionDisabled) {
          ret.untouchableExtra = extra;
        } else {
          ret.preemptableExtra = extra;
        }
        ret.setLeafQueue(l);
      } else {
        Resource pending = Resource.newInstance(0, 0);
        ret =
            new TempQueuePerPartition(curQueue.getQueueName(), current, pending,
                guaranteed, maxCapacity, false, partitionToLookAt);
        Resource childrensPreemptable = Resource.newInstance(0, 0);
        for (CSQueue c : curQueue.getChildQueues()) {
          TempQueuePerPartition subq =
              cloneQueues(c, partitionResource, partitionToLookAt);
          Resources.addTo(childrensPreemptable, subq.preemptableExtra);
          ret.addChild(subq);
        }
        // untouchableExtra = max(extra - childrenPreemptable, 0)
        if (Resources.greaterThanOrEqual(
              rc, partitionResource, childrensPreemptable, extra)) {
          ret.untouchableExtra = Resource.newInstance(0, 0);
        } else {
          ret.untouchableExtra =
                Resources.subtract(extra, childrensPreemptable);
        }
        ret.preemptableExtra = Resources.min(
            rc, partitionResource, childrensPreemptable, extra);
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
      queuePartitions = new HashMap<String, TempQueuePerPartition>();
      queueToPartitions.put(queueName, queuePartitions);
    }
    queuePartitions.put(queuePartition.partition, queuePartition);
  }

  /**
   * Get queue partition by given queueName and partitionName
   */
  private TempQueuePerPartition getQueueByPartition(String queueName,
      String partition) {
    Map<String, TempQueuePerPartition> partitionToQueues = null;
    if (null == (partitionToQueues = queueToPartitions.get(queueName))) {
      return null;
    }
    return partitionToQueues.get(partition);
  }

  /**
   * Get all queue partitions by given queueName
   */
  private Collection<TempQueuePerPartition> getQueuePartitions(String queueName) {
    if (!queueToPartitions.containsKey(queueName)) {
      return null;
    }
    return queueToPartitions.get(queueName).values();
  }

  /**
   * Temporary data-structure tracking resource availability, pending resource
   * need, current utilization. This is per-queue-per-partition data structure
   */
  static class TempQueuePerPartition {
    final String queueName;
    final Resource current;
    final Resource pending;
    final Resource guaranteed;
    final Resource maxCapacity;
    final String partition;
    Resource idealAssigned;
    Resource toBePreempted;
    // For logging purpose
    Resource actuallyPreempted;
    Resource untouchableExtra;
    Resource preemptableExtra;

    double normalizedGuarantee;

    final ArrayList<TempQueuePerPartition> children;
    LeafQueue leafQueue;
    boolean preemptionDisabled;

    TempQueuePerPartition(String queueName, Resource current, Resource pending,
        Resource guaranteed, Resource maxCapacity, boolean preemptionDisabled,
        String partition) {
      this.queueName = queueName;
      this.current = current;
      this.pending = pending;
      this.guaranteed = guaranteed;
      this.maxCapacity = maxCapacity;
      this.idealAssigned = Resource.newInstance(0, 0);
      this.actuallyPreempted = Resource.newInstance(0, 0);
      this.toBePreempted = Resource.newInstance(0, 0);
      this.normalizedGuarantee = Float.NaN;
      this.children = new ArrayList<TempQueuePerPartition>();
      this.untouchableExtra = Resource.newInstance(0, 0);
      this.preemptableExtra = Resource.newInstance(0, 0);
      this.preemptionDisabled = preemptionDisabled;
      this.partition = partition;
    }

    public void setLeafQueue(LeafQueue l){
      assert children.size() == 0;
      this.leafQueue = l;
    }

    /**
     * When adding a child we also aggregate its pending resource needs.
     * @param q the child queue to add to this queue
     */
    public void addChild(TempQueuePerPartition q) {
      assert leafQueue == null;
      children.add(q);
      Resources.addTo(pending, q.pending);
    }

    public void addChildren(ArrayList<TempQueuePerPartition> queues) {
      assert leafQueue == null;
      children.addAll(queues);
    }


    public ArrayList<TempQueuePerPartition> getChildren(){
      return children;
    }

    // This function "accepts" all the resources it can (pending) and return
    // the unused ones
    Resource offer(Resource avail, ResourceCalculator rc,
        Resource clusterResource) {
      Resource absMaxCapIdealAssignedDelta = Resources.componentwiseMax(
                      Resources.subtract(maxCapacity, idealAssigned),
                      Resource.newInstance(0, 0));
      // remain = avail - min(avail, (max - assigned), (current + pending - assigned))
      Resource accepted = 
          Resources.min(rc, clusterResource, 
              absMaxCapIdealAssignedDelta,
          Resources.min(rc, clusterResource, avail, Resources.subtract(
              Resources.add(current, pending), idealAssigned)));
      Resource remain = Resources.subtract(avail, accepted);
      Resources.addTo(idealAssigned, accepted);
      return remain;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(" NAME: " + queueName)
        .append(" CUR: ").append(current)
        .append(" PEN: ").append(pending)
        .append(" GAR: ").append(guaranteed)
        .append(" NORM: ").append(normalizedGuarantee)
        .append(" IDEAL_ASSIGNED: ").append(idealAssigned)
        .append(" IDEAL_PREEMPT: ").append(toBePreempted)
        .append(" ACTUAL_PREEMPT: ").append(actuallyPreempted)
        .append(" UNTOUCHABLE: ").append(untouchableExtra)
        .append(" PREEMPTABLE: ").append(preemptableExtra)
        .append("\n");

      return sb.toString();
    }

    public void printAll() {
      LOG.info(this.toString());
      for (TempQueuePerPartition sub : this.getChildren()) {
        sub.printAll();
      }
    }

    public void assignPreemption(float scalingFactor,
        ResourceCalculator rc, Resource clusterResource) {
      if (Resources.greaterThan(rc, clusterResource, current, idealAssigned)) {
          toBePreempted = Resources.multiply(
              Resources.subtract(current, idealAssigned), scalingFactor);
      } else {
        toBePreempted = Resource.newInstance(0, 0);
      }
    }

    void appendLogString(StringBuilder sb) {
      sb.append(queueName).append(", ")
        .append(current.getMemory()).append(", ")
        .append(current.getVirtualCores()).append(", ")
        .append(pending.getMemory()).append(", ")
        .append(pending.getVirtualCores()).append(", ")
        .append(guaranteed.getMemory()).append(", ")
        .append(guaranteed.getVirtualCores()).append(", ")
        .append(idealAssigned.getMemory()).append(", ")
        .append(idealAssigned.getVirtualCores()).append(", ")
        .append(toBePreempted.getMemory()).append(", ")
        .append(toBePreempted.getVirtualCores() ).append(", ")
        .append(actuallyPreempted.getMemory()).append(", ")
        .append(actuallyPreempted.getVirtualCores());
    }

  }

  static class TQComparator implements Comparator<TempQueuePerPartition> {
    private ResourceCalculator rc;
    private Resource clusterRes;

    TQComparator(ResourceCalculator rc, Resource clusterRes) {
      this.rc = rc;
      this.clusterRes = clusterRes;
    }

    @Override
    public int compare(TempQueuePerPartition tq1, TempQueuePerPartition tq2) {
      if (getIdealPctOfGuaranteed(tq1) < getIdealPctOfGuaranteed(tq2)) {
        return -1;
      }
      if (getIdealPctOfGuaranteed(tq1) > getIdealPctOfGuaranteed(tq2)) {
        return 1;
      }
      return 0;
    }

    // Calculates idealAssigned / guaranteed
    // TempQueues with 0 guarantees are always considered the most over
    // capacity and therefore considered last for resources.
    private double getIdealPctOfGuaranteed(TempQueuePerPartition q) {
      double pctOver = Integer.MAX_VALUE;
      if (q != null && Resources.greaterThan(
          rc, clusterRes, q.guaranteed, Resources.none())) {
        pctOver =
            Resources.divide(rc, clusterRes, q.idealAssigned, q.guaranteed);
      }
      return (pctOver);
    }
  }

  @VisibleForTesting
  public Map<String, Map<String, TempQueuePerPartition>> getQueuePartitions() {
    return queueToPartitions;
  }
}

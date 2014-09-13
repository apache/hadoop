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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingEditPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PreemptableResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;

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

  // the dispatcher to send preempt and kill events
  public EventHandler<ContainerPreemptEvent> dispatcher;

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

  public ProportionalCapacityPreemptionPolicy() {
    clock = new SystemClock();
  }

  public ProportionalCapacityPreemptionPolicy(Configuration config,
      EventHandler<ContainerPreemptEvent> dispatcher,
      CapacityScheduler scheduler) {
    this(config, dispatcher, scheduler, new SystemClock());
  }

  public ProportionalCapacityPreemptionPolicy(Configuration config,
      EventHandler<ContainerPreemptEvent> dispatcher,
      CapacityScheduler scheduler, Clock clock) {
    init(config, dispatcher, scheduler);
    this.clock = clock;
  }

  public void init(Configuration config,
      EventHandler<ContainerPreemptEvent> disp,
      PreemptableResourceScheduler sched) {
    LOG.info("Preemption monitor:" + this.getClass().getCanonicalName());
    assert null == scheduler : "Unexpected duplicate call to init";
    if (!(sched instanceof CapacityScheduler)) {
      throw new YarnRuntimeException("Class " +
          sched.getClass().getCanonicalName() + " not instance of " +
          CapacityScheduler.class.getCanonicalName());
    }
    dispatcher = disp;
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
  }
  
  @VisibleForTesting
  public ResourceCalculator getResourceCalculator() {
    return rc;
  }

  @Override
  public void editSchedule(){
    CSQueue root = scheduler.getRootQueue();
    Resource clusterResources =
      Resources.clone(scheduler.getClusterResource());
    containerBasedPreemptOrKill(root, clusterResources);
  }

  /**
   * This method selects and tracks containers to be preempted. If a container
   * is in the target list for more than maxWaitTime it is killed.
   *
   * @param root the root of the CapacityScheduler queue hierarchy
   * @param clusterResources the total amount of resources in the cluster
   */
  private void containerBasedPreemptOrKill(CSQueue root,
      Resource clusterResources) {

    // extract a summary of the queues from scheduler
    TempQueue tRoot;
    synchronized (scheduler) {
      tRoot = cloneQueues(root, clusterResources);
    }

    // compute the ideal distribution of resources among queues
    // updates cloned queues state accordingly
    tRoot.idealAssigned = tRoot.guaranteed;
    Resource totalPreemptionAllowed = Resources.multiply(clusterResources,
        percentageClusterPreemptionAllowed);
    List<TempQueue> queues =
      recursivelyComputeIdealAssignment(tRoot, totalPreemptionAllowed);

    // based on ideal allocation select containers to be preempted from each
    // queue and each application
    Map<ApplicationAttemptId,Set<RMContainer>> toPreempt =
        getContainersToPreempt(queues, clusterResources);

    if (LOG.isDebugEnabled()) {
      logToCSV(queues);
    }

    // if we are in observeOnly mode return before any action is taken
    if (observeOnly) {
      return;
    }

    // preempt (or kill) the selected containers
    for (Map.Entry<ApplicationAttemptId,Set<RMContainer>> e
         : toPreempt.entrySet()) {
      for (RMContainer container : e.getValue()) {
        // if we tried to preempt this for more than maxWaitTime
        if (preempted.get(container) != null &&
            preempted.get(container) + maxWaitTime < clock.getTime()) {
          // kill it
          dispatcher.handle(new ContainerPreemptEvent(e.getKey(), container,
                ContainerPreemptEventType.KILL_CONTAINER));
          preempted.remove(container);
        } else {
          //otherwise just send preemption events
          dispatcher.handle(new ContainerPreemptEvent(e.getKey(), container,
                ContainerPreemptEventType.PREEMPT_CONTAINER));
          if (preempted.get(container) == null) {
            preempted.put(container, clock.getTime());
          }
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
  private List<TempQueue> recursivelyComputeIdealAssignment(
      TempQueue root, Resource totalPreemptionAllowed) {
    List<TempQueue> leafs = new ArrayList<TempQueue>();
    if (root.getChildren() != null &&
        root.getChildren().size() > 0) {
      // compute ideal distribution at this level
      computeIdealResourceDistribution(rc, root.getChildren(),
          totalPreemptionAllowed, root.idealAssigned);
      // compute recursively for lower levels and build list of leafs
      for(TempQueue t : root.getChildren()) {
        leafs.addAll(recursivelyComputeIdealAssignment(t, totalPreemptionAllowed));
      }
    } else {
      // we are in a leaf nothing to do, just return yourself
      return Collections.singletonList(root);
    }
    return leafs;
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
      List<TempQueue> queues, Resource totalPreemptionAllowed, Resource tot_guarant) {

    // qAlloc tracks currently active queues (will decrease progressively as
    // demand is met)
    List<TempQueue> qAlloc = new ArrayList<TempQueue>(queues);
    // unassigned tracks how much resources are still to assign, initialized
    // with the total capacity for this set of queues
    Resource unassigned = Resources.clone(tot_guarant);

    // group queues based on whether they have non-zero guaranteed capacity
    Set<TempQueue> nonZeroGuarQueues = new HashSet<TempQueue>();
    Set<TempQueue> zeroGuarQueues = new HashSet<TempQueue>();

    for (TempQueue q : qAlloc) {
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
    for (TempQueue t:queues) {
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
    for (TempQueue t : queues) {
      t.assignPreemption(scalingFactor, rc, tot_guarant);
    }
    if (LOG.isDebugEnabled()) {
      long time = clock.getTime();
      for (TempQueue t : queues) {
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
      Resource tot_guarant, Collection<TempQueue> qAlloc, Resource unassigned, 
      boolean ignoreGuarantee) {
    //assign all cluster resources until no more demand, or no resources are left
    while (!qAlloc.isEmpty() && Resources.greaterThan(rc, tot_guarant,
          unassigned, Resources.none())) {
      Resource wQassigned = Resource.newInstance(0, 0);

      // we compute normalizedGuarantees capacity based on currently active
      // queues
      resetCapacity(rc, unassigned, qAlloc, ignoreGuarantee);
      
      // offer for each queue their capacity first and in following invocations
      // their share of over-capacity
      for (Iterator<TempQueue> i = qAlloc.iterator(); i.hasNext();) {
        TempQueue sub = i.next();
        Resource wQavail =
          Resources.multiply(unassigned, sub.normalizedGuarantee);
        Resource wQidle = sub.offer(wQavail, rc, tot_guarant);
        Resource wQdone = Resources.subtract(wQavail, wQidle);
        // if the queue returned a value > 0 it means it is fully satisfied
        // and it is removed from the list of active queues qAlloc
        if (!Resources.greaterThan(rc, tot_guarant,
              wQdone, Resources.none())) {
          i.remove();
        }
        Resources.addTo(wQassigned, wQdone);
      }
      Resources.subtractFrom(unassigned, wQassigned);
    }
  }

  /**
   * Computes a normalizedGuaranteed capacity based on active queues
   * @param rc resource calculator
   * @param clusterResource the total amount of resources in the cluster
   * @param queues the list of queues to consider
   */
  private void resetCapacity(ResourceCalculator rc, Resource clusterResource,
      Collection<TempQueue> queues, boolean ignoreGuar) {
    Resource activeCap = Resource.newInstance(0, 0);
    
    if (ignoreGuar) {
      for (TempQueue q : queues) {
        q.normalizedGuarantee = (float)  1.0f / ((float) queues.size());
      }
    } else {
      for (TempQueue q : queues) {
        Resources.addTo(activeCap, q.guaranteed);
      }
      for (TempQueue q : queues) {
        q.normalizedGuarantee = Resources.divide(rc, clusterResource,
            q.guaranteed, activeCap);
      }
    }
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
      List<TempQueue> queues, Resource clusterResource) {

    Map<ApplicationAttemptId,Set<RMContainer>> preemptMap =
        new HashMap<ApplicationAttemptId,Set<RMContainer>>();
    List<RMContainer> skippedAMContainerlist = new ArrayList<RMContainer>();

    for (TempQueue qT : queues) {
      // we act only if we are violating balance by more than
      // maxIgnoredOverCapacity
      if (Resources.greaterThan(rc, clusterResource, qT.current,
          Resources.multiply(qT.guaranteed, 1.0 + maxIgnoredOverCapacity))) {
        // we introduce a dampening factor naturalTerminationFactor that
        // accounts for natural termination of containers
        Resource resToObtain =
          Resources.multiply(qT.toBePreempted, naturalTerminationFactor);
        Resource skippedAMSize = Resource.newInstance(0, 0);

        // lock the leafqueue while we scan applications and unreserve
        synchronized (qT.leafQueue) {
          NavigableSet<FiCaSchedulerApp> ns = 
              (NavigableSet<FiCaSchedulerApp>) qT.leafQueue.getApplications();
          Iterator<FiCaSchedulerApp> desc = ns.descendingIterator();
          qT.actuallyPreempted = Resources.clone(resToObtain);
          while (desc.hasNext()) {
            FiCaSchedulerApp fc = desc.next();
            if (Resources.lessThanOrEqual(rc, clusterResource, resToObtain,
                Resources.none())) {
              break;
            }
            preemptMap.put(
                fc.getApplicationAttemptId(),
                preemptFrom(fc, clusterResource, resToObtain,
                    skippedAMContainerlist, skippedAMSize));
          }
          Resource maxAMCapacityForThisQueue = Resources.multiply(
              Resources.multiply(clusterResource,
                  qT.leafQueue.getAbsoluteCapacity()),
              qT.leafQueue.getMaxAMResourcePerQueuePercent());

          // Can try preempting AMContainers (still saving atmost
          // maxAMCapacityForThisQueue AMResource's) if more resources are
          // required to be preempted from this Queue.
          preemptAMContainers(clusterResource, preemptMap,
              skippedAMContainerlist, resToObtain, skippedAMSize,
              maxAMCapacityForThisQueue);
        }
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
      List<RMContainer> skippedAMContainerlist, Resource resToObtain,
      Resource skippedAMSize, Resource maxAMCapacityForThisQueue) {
    for (RMContainer c : skippedAMContainerlist) {
      // Got required amount of resources for preemption, can stop now
      if (Resources.lessThanOrEqual(rc, clusterResource, resToObtain,
          Resources.none())) {
        break;
      }
      // Once skippedAMSize reaches down to maxAMCapacityForThisQueue,
      // container selection iteration for preemption will be stopped. 
      if (Resources.lessThanOrEqual(rc, clusterResource, skippedAMSize,
          maxAMCapacityForThisQueue)) {
        break;
      }
      Set<RMContainer> contToPrempt = preemptMap.get(c
          .getApplicationAttemptId());
      if (null == contToPrempt) {
        contToPrempt = new HashSet<RMContainer>();
        preemptMap.put(c.getApplicationAttemptId(), contToPrempt);
      }
      contToPrempt.add(c);
      
      Resources.subtractFrom(resToObtain, c.getContainer().getResource());
      Resources.subtractFrom(skippedAMSize, c.getContainer()
          .getResource());
    }
    skippedAMContainerlist.clear();
  }

  /**
   * Given a target preemption for a specific application, select containers
   * to preempt (after unreserving all reservation for that app).
   *
   * @param app
   * @param clusterResource
   * @param rsrcPreempt
   * @return
   */
  private Set<RMContainer> preemptFrom(FiCaSchedulerApp app,
      Resource clusterResource, Resource rsrcPreempt,
      List<RMContainer> skippedAMContainerlist, Resource skippedAMSize) {
    Set<RMContainer> ret = new HashSet<RMContainer>();
    ApplicationAttemptId appId = app.getApplicationAttemptId();

    // first drop reserved containers towards rsrcPreempt
    List<RMContainer> reservations =
        new ArrayList<RMContainer>(app.getReservedContainers());
    for (RMContainer c : reservations) {
      if (Resources.lessThanOrEqual(rc, clusterResource,
          rsrcPreempt, Resources.none())) {
        return ret;
      }
      if (!observeOnly) {
        dispatcher.handle(new ContainerPreemptEvent(appId, c,
            ContainerPreemptEventType.DROP_RESERVATION));
      }
      Resources.subtractFrom(rsrcPreempt, c.getContainer().getResource());
    }

    // if more resources are to be freed go through all live containers in
    // reverse priority and reverse allocation order and mark them for
    // preemption
    List<RMContainer> containers =
      new ArrayList<RMContainer>(app.getLiveContainers());

    sortContainers(containers);

    for (RMContainer c : containers) {
      if (Resources.lessThanOrEqual(rc, clusterResource,
            rsrcPreempt, Resources.none())) {
        return ret;
      }
      // Skip AM Container from preemption for now.
      if (c.isAMContainer()) {
        skippedAMContainerlist.add(c);
        Resources.addTo(skippedAMSize, c.getContainer().getResource());
        continue;
      }
      ret.add(c);
      Resources.subtractFrom(rsrcPreempt, c.getContainer().getResource());
    }

    return ret;
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
   * @param root the root of the CapacityScheduler queue hierarchy
   * @param clusterResources the total amount of resources in the cluster
   * @return the root of the cloned queue hierarchy
   */
  private TempQueue cloneQueues(CSQueue root, Resource clusterResources) {
    TempQueue ret;
    synchronized (root) {
      String queueName = root.getQueueName();
      float absUsed = root.getAbsoluteUsedCapacity();
      float absCap = root.getAbsoluteCapacity();
      float absMaxCap = root.getAbsoluteMaximumCapacity();

      Resource current = Resources.multiply(clusterResources, absUsed);
      Resource guaranteed = Resources.multiply(clusterResources, absCap);
      Resource maxCapacity = Resources.multiply(clusterResources, absMaxCap);
      if (root instanceof LeafQueue) {
        LeafQueue l = (LeafQueue) root;
        Resource pending = l.getTotalResourcePending();
        ret = new TempQueue(queueName, current, pending, guaranteed,
            maxCapacity);

        ret.setLeafQueue(l);
      } else {
        Resource pending = Resource.newInstance(0, 0);
        ret = new TempQueue(root.getQueueName(), current, pending, guaranteed,
            maxCapacity);
        for (CSQueue c : root.getChildQueues()) {
          ret.addChild(cloneQueues(c, clusterResources));
        }
      }
    }
    return ret;
  }

  // simple printout function that reports internal queue state (useful for
  // plotting)
  private void logToCSV(List<TempQueue> unorderedqueues){
    List<TempQueue> queues = new ArrayList<TempQueue>(unorderedqueues);
    Collections.sort(queues, new Comparator<TempQueue>(){
      @Override
      public int compare(TempQueue o1, TempQueue o2) {
        return o1.queueName.compareTo(o2.queueName);
      }});
    String queueState = " QUEUESTATE: " + clock.getTime();
    StringBuilder sb = new StringBuilder();
    sb.append(queueState);
    for (TempQueue tq : queues) {
      sb.append(", ");
      tq.appendLogString(sb);
    }
    LOG.debug(sb.toString());
  }

  /**
   * Temporary data-structure tracking resource availability, pending resource
   * need, current utilization. Used to clone {@link CSQueue}.
   */
  static class TempQueue {
    final String queueName;
    final Resource current;
    final Resource pending;
    final Resource guaranteed;
    final Resource maxCapacity;
    Resource idealAssigned;
    Resource toBePreempted;
    Resource actuallyPreempted;

    double normalizedGuarantee;

    final ArrayList<TempQueue> children;
    LeafQueue leafQueue;

    TempQueue(String queueName, Resource current, Resource pending,
        Resource guaranteed, Resource maxCapacity) {
      this.queueName = queueName;
      this.current = current;
      this.pending = pending;
      this.guaranteed = guaranteed;
      this.maxCapacity = maxCapacity;
      this.idealAssigned = Resource.newInstance(0, 0);
      this.actuallyPreempted = Resource.newInstance(0, 0);
      this.toBePreempted = Resource.newInstance(0, 0);
      this.normalizedGuarantee = Float.NaN;
      this.children = new ArrayList<TempQueue>();
    }

    public void setLeafQueue(LeafQueue l){
      assert children.size() == 0;
      this.leafQueue = l;
    }

    /**
     * When adding a child we also aggregate its pending resource needs.
     * @param q the child queue to add to this queue
     */
    public void addChild(TempQueue q) {
      assert leafQueue == null;
      children.add(q);
      Resources.addTo(pending, q.pending);
    }

    public void addChildren(ArrayList<TempQueue> queues) {
      assert leafQueue == null;
      children.addAll(queues);
    }


    public ArrayList<TempQueue> getChildren(){
      return children;
    }

    // This function "accepts" all the resources it can (pending) and return
    // the unused ones
    Resource offer(Resource avail, ResourceCalculator rc,
        Resource clusterResource) {
      // remain = avail - min(avail, (max - assigned), (current + pending - assigned))
      Resource accepted = 
          Resources.min(rc, clusterResource, 
              Resources.subtract(maxCapacity, idealAssigned),
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
        .append("\n");

      return sb.toString();
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

}

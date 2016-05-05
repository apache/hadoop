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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class FifoCandidatesSelector
    extends PreemptionCandidatesSelector {
  private static final Log LOG =
      LogFactory.getLog(FifoCandidatesSelector.class);
  private PreemptableResourceCalculator preemptableAmountCalculator;

  FifoCandidatesSelector(
      CapacitySchedulerPreemptionContext preemptionContext) {
    super(preemptionContext);

    preemptableAmountCalculator = new PreemptableResourceCalculator(
        preemptionContext, false);
  }

  @Override
  public Map<ApplicationAttemptId, Set<RMContainer>> selectCandidates(
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Resource clusterResource, Resource totalPreemptionAllowed) {
    // Calculate how much resources we need to preempt
    preemptableAmountCalculator.computeIdealAllocation(clusterResource,
        totalPreemptionAllowed);

    // Previous selectors (with higher priority) could have already
    // selected containers. We need to deduct preemptable resources
    // based on already selected candidates.
    CapacitySchedulerPreemptionUtils
        .deductPreemptableResourcesBasedSelectedCandidates(preemptionContext,
            selectedCandidates);

    List<RMContainer> skippedAMContainerlist = new ArrayList<>();

    // Loop all leaf queues
    for (String queueName : preemptionContext.getLeafQueueNames()) {
      // check if preemption disabled for the queue
      if (preemptionContext.getQueueByPartition(queueName,
          RMNodeLabelsManager.NO_LABEL).preemptionDisabled) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("skipping from queue=" + queueName
              + " because it's a non-preemptable queue");
        }
        continue;
      }

      // compute resToObtainByPartition considered inter-queue preemption
      LeafQueue leafQueue = preemptionContext.getQueueByPartition(queueName,
          RMNodeLabelsManager.NO_LABEL).leafQueue;

      Map<String, Resource> resToObtainByPartition =
          CapacitySchedulerPreemptionUtils
              .getResToObtainByPartitionForLeafQueue(preemptionContext,
                  queueName, clusterResource);

      synchronized (leafQueue) {
        // go through all ignore-partition-exclusivity containers first to make
        // sure such containers will be preemptionCandidates first
        Map<String, TreeSet<RMContainer>> ignorePartitionExclusivityContainers =
            leafQueue.getIgnoreExclusivityRMContainers();
        for (String partition : resToObtainByPartition.keySet()) {
          if (ignorePartitionExclusivityContainers.containsKey(partition)) {
            TreeSet<RMContainer> rmContainers =
                ignorePartitionExclusivityContainers.get(partition);
            // We will check container from reverse order, so latter submitted
            // application's containers will be preemptionCandidates first.
            for (RMContainer c : rmContainers.descendingSet()) {
              if (CapacitySchedulerPreemptionUtils.isContainerAlreadySelected(c,
                  selectedCandidates)) {
                // Skip already selected containers
                continue;
              }
              boolean preempted = tryPreemptContainerAndDeductResToObtain(
                  resToObtainByPartition, c, clusterResource, selectedCandidates,
                  totalPreemptionAllowed);
              if (!preempted) {
                continue;
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
              skippedAMContainerlist, skippedAMSize, selectedCandidates,
              totalPreemptionAllowed);
        }

        // Can try preempting AMContainers (still saving atmost
        // maxAMCapacityForThisQueue AMResource's) if more resources are
        // required to be preemptionCandidates from this Queue.
        Resource maxAMCapacityForThisQueue = Resources.multiply(
            Resources.multiply(clusterResource,
                leafQueue.getAbsoluteCapacity()),
            leafQueue.getMaxAMResourcePerQueuePercent());

        preemptAMContainers(clusterResource, selectedCandidates, skippedAMContainerlist,
            resToObtainByPartition, skippedAMSize, maxAMCapacityForThisQueue,
            totalPreemptionAllowed);
      }
    }

    return selectedCandidates;
  }

  /**
   * As more resources are needed for preemption, saved AMContainers has to be
   * rescanned. Such AMContainers can be preemptionCandidates based on resToObtain, but
   * maxAMCapacityForThisQueue resources will be still retained.
   *
   * @param clusterResource
   * @param preemptMap
   * @param skippedAMContainerlist
   * @param skippedAMSize
   * @param maxAMCapacityForThisQueue
   */
  private void preemptAMContainers(Resource clusterResource,
      Map<ApplicationAttemptId, Set<RMContainer>> preemptMap,
      List<RMContainer> skippedAMContainerlist,
      Map<String, Resource> resToObtainByPartition, Resource skippedAMSize,
      Resource maxAMCapacityForThisQueue, Resource totalPreemptionAllowed) {
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
              clusterResource, preemptMap, totalPreemptionAllowed);
      if (preempted) {
        Resources.subtractFrom(skippedAMSize, c.getAllocatedResource());
      }
    }
    skippedAMContainerlist.clear();
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

  /**
   * Return should we preempt rmContainer. If we should, deduct from
   * <code>resourceToObtainByPartition</code>
   */
  private boolean tryPreemptContainerAndDeductResToObtain(
      Map<String, Resource> resourceToObtainByPartitions,
      RMContainer rmContainer, Resource clusterResource,
      Map<ApplicationAttemptId, Set<RMContainer>> preemptMap,
      Resource totalPreemptionAllowed) {
    ApplicationAttemptId attemptId = rmContainer.getApplicationAttemptId();

    // We will not account resource of a container twice or more
    if (preemptMapContains(preemptMap, attemptId, rmContainer)) {
      return false;
    }

    String nodePartition = getPartitionByNodeId(rmContainer.getAllocatedNode());
    Resource toObtainByPartition =
        resourceToObtainByPartitions.get(nodePartition);

    if (null != toObtainByPartition && Resources.greaterThan(rc,
        clusterResource, toObtainByPartition, Resources.none()) && Resources
        .fitsIn(rc, clusterResource, rmContainer.getAllocatedResource(),
            totalPreemptionAllowed)) {
      Resources.subtractFrom(toObtainByPartition,
          rmContainer.getAllocatedResource());
      Resources.subtractFrom(totalPreemptionAllowed,
          rmContainer.getAllocatedResource());

      // When we have no more resource need to obtain, remove from map.
      if (Resources.lessThanOrEqual(rc, clusterResource, toObtainByPartition,
          Resources.none())) {
        resourceToObtainByPartitions.remove(nodePartition);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(this.getClass().getName() + " Marked container=" + rmContainer
            .getContainerId() + " from partition=" + nodePartition + " queue="
            + rmContainer.getQueueName() + " to be preemption candidates");
      }
      // Add to preemptMap
      addToPreemptMap(preemptMap, attemptId, rmContainer);
      return true;
    }

    return false;
  }

  private String getPartitionByNodeId(NodeId nodeId) {
    return preemptionContext.getScheduler().getSchedulerNode(nodeId)
        .getPartition();
  }

  /**
   * Given a target preemption for a specific application, select containers
   * to preempt (after unreserving all reservation for that app).
   */
  @SuppressWarnings("unchecked")
  private void preemptFrom(FiCaSchedulerApp app,
      Resource clusterResource, Map<String, Resource> resToObtainByPartition,
      List<RMContainer> skippedAMContainerlist, Resource skippedAMSize,
      Map<ApplicationAttemptId, Set<RMContainer>> selectedContainers,
      Resource totalPreemptionAllowed) {
    ApplicationAttemptId appId = app.getApplicationAttemptId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Looking at application=" + app.getApplicationAttemptId()
          + " resourceToObtain=" + resToObtainByPartition);
    }

    // first drop reserved containers towards rsrcPreempt
    List<RMContainer> reservedContainers =
        new ArrayList<>(app.getReservedContainers());
    for (RMContainer c : reservedContainers) {
      if (CapacitySchedulerPreemptionUtils.isContainerAlreadySelected(c,
          selectedContainers)) {
        continue;
      }
      if (resToObtainByPartition.isEmpty()) {
        return;
      }

      // Try to preempt this container
      tryPreemptContainerAndDeductResToObtain(resToObtainByPartition, c,
          clusterResource, selectedContainers, totalPreemptionAllowed);

      if (!preemptionContext.isObserveOnly()) {
        preemptionContext.getRMContext().getDispatcher().getEventHandler()
            .handle(new ContainerPreemptEvent(appId, c,
                SchedulerEventType.KILL_RESERVED_CONTAINER));
      }
    }

    // if more resources are to be freed go through all live containers in
    // reverse priority and reverse allocation order and mark them for
    // preemption
    List<RMContainer> liveContainers =
        new ArrayList<>(app.getLiveContainers());

    sortContainers(liveContainers);

    for (RMContainer c : liveContainers) {
      if (resToObtainByPartition.isEmpty()) {
        return;
      }

      if (CapacitySchedulerPreemptionUtils.isContainerAlreadySelected(c,
          selectedContainers)) {
        continue;
      }

      // Skip already marked to killable containers
      if (null != preemptionContext.getKillableContainers() && preemptionContext
          .getKillableContainers().contains(c.getContainerId())) {
        continue;
      }

      // Skip AM Container from preemption for now.
      if (c.isAMContainer()) {
        skippedAMContainerlist.add(c);
        Resources.addTo(skippedAMSize, c.getAllocatedResource());
        continue;
      }

      // Try to preempt this container
      tryPreemptContainerAndDeductResToObtain(resToObtainByPartition, c,
          clusterResource, selectedContainers, totalPreemptionAllowed);
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

  private void addToPreemptMap(
      Map<ApplicationAttemptId, Set<RMContainer>> preemptMap,
      ApplicationAttemptId appAttemptId, RMContainer containerToPreempt) {
    Set<RMContainer> set;
    if (null == (set = preemptMap.get(appAttemptId))) {
      set = new HashSet<>();
      preemptMap.put(appAttemptId, set);
    }
    set.add(containerToPreempt);
  }
}

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;
import java.util.HashMap;
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
  private boolean allowQueuesBalanceAfterAllQueuesSatisfied;

  FifoCandidatesSelector(CapacitySchedulerPreemptionContext preemptionContext,
      boolean includeReservedResource,
      boolean allowQueuesBalanceAfterAllQueuesSatisfied) {
    super(preemptionContext);

    this.allowQueuesBalanceAfterAllQueuesSatisfied =
        allowQueuesBalanceAfterAllQueuesSatisfied;
    preemptableAmountCalculator = new PreemptableResourceCalculator(
        preemptionContext, includeReservedResource,
        allowQueuesBalanceAfterAllQueuesSatisfied);
  }

  @Override
  public Map<ApplicationAttemptId, Set<RMContainer>> selectCandidates(
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Resource clusterResource, Resource totalPreemptionAllowed) {
    Map<ApplicationAttemptId, Set<RMContainer>> curCandidates = new HashMap<>();
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

      try {
        leafQueue.getReadLock().lock();
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
              boolean preempted = CapacitySchedulerPreemptionUtils
                  .tryPreemptContainerAndDeductResToObtain(rc,
                      preemptionContext, resToObtainByPartition, c,
                      clusterResource, selectedCandidates, curCandidates,
                      totalPreemptionAllowed, false);
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
              curCandidates, totalPreemptionAllowed);
        }

        // Can try preempting AMContainers (still saving atmost
        // maxAMCapacityForThisQueue AMResource's) if more resources are
        // required to be preemptionCandidates from this Queue.
        Resource maxAMCapacityForThisQueue = Resources
            .multiply(
                leafQueue.getEffectiveCapacity(RMNodeLabelsManager.NO_LABEL),
                leafQueue.getMaxAMResourcePerQueuePercent());

        preemptAMContainers(clusterResource, selectedCandidates, curCandidates,
            skippedAMContainerlist, resToObtainByPartition, skippedAMSize,
            maxAMCapacityForThisQueue, totalPreemptionAllowed);
      } finally {
        leafQueue.getReadLock().unlock();
      }
    }

    return curCandidates;
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
      Map<ApplicationAttemptId, Set<RMContainer>> curCandidates,
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

      boolean preempted = CapacitySchedulerPreemptionUtils
          .tryPreemptContainerAndDeductResToObtain(rc, preemptionContext,
              resToObtainByPartition, c, clusterResource, preemptMap,
              curCandidates, totalPreemptionAllowed, false);
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
  private void preemptFrom(FiCaSchedulerApp app,
      Resource clusterResource, Map<String, Resource> resToObtainByPartition,
      List<RMContainer> skippedAMContainerlist, Resource skippedAMSize,
      Map<ApplicationAttemptId, Set<RMContainer>> selectedContainers,
      Map<ApplicationAttemptId, Set<RMContainer>> curCandidates,
      Resource totalPreemptionAllowed) {
    ApplicationAttemptId appId = app.getApplicationAttemptId();

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
     CapacitySchedulerPreemptionUtils
          .tryPreemptContainerAndDeductResToObtain(rc, preemptionContext,
              resToObtainByPartition, c, clusterResource, selectedContainers,
              curCandidates, totalPreemptionAllowed, false);

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
      CapacitySchedulerPreemptionUtils
          .tryPreemptContainerAndDeductResToObtain(rc, preemptionContext,
              resToObtainByPartition, c, clusterResource, selectedContainers,
              curCandidates, totalPreemptionAllowed, false);
    }
  }

  public boolean getAllowQueuesBalanceAfterAllQueuesSatisfied() {
    return allowQueuesBalanceAfterAllQueuesSatisfied;
  }
}

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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CapacitySchedulerPreemptionUtils {
  public static Map<String, Resource> getResToObtainByPartitionForLeafQueue(
      CapacitySchedulerPreemptionContext context, String queueName,
      Resource clusterResource) {
    Map<String, Resource> resToObtainByPartition = new HashMap<>();
    // compute resToObtainByPartition considered inter-queue preemption
    for (TempQueuePerPartition qT : context.getQueuePartitions(queueName)) {
      if (qT.preemptionDisabled) {
        continue;
      }

      // Only add resToObtainByPartition when actuallyToBePreempted resource >=
      // 0
      if (Resources.greaterThan(context.getResourceCalculator(),
          clusterResource, qT.getActuallyToBePreempted(), Resources.none())) {
        resToObtainByPartition.put(qT.partition,
            Resources.clone(qT.getActuallyToBePreempted()));
      }
    }

    return resToObtainByPartition;
  }

  public static boolean isContainerAlreadySelected(RMContainer container,
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates) {
    if (null == selectedCandidates) {
      return false;
    }

    Set<RMContainer> containers = selectedCandidates
        .get(container.getApplicationAttemptId());
    if (containers == null) {
      return false;
    }
    return containers.contains(container);
  }

  public static void deductPreemptableResourcesBasedSelectedCandidates(
      CapacitySchedulerPreemptionContext context,
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates) {
    for (Set<RMContainer> containers : selectedCandidates.values()) {
      for (RMContainer c : containers) {
        SchedulerNode schedulerNode = context.getScheduler()
            .getSchedulerNode(c.getAllocatedNode());
        if (null == schedulerNode) {
          continue;
        }

        String partition = schedulerNode.getPartition();
        String queue = c.getQueueName();
        TempQueuePerPartition tq = context.getQueueByPartition(queue,
            partition);

        Resource res = c.getReservedResource();
        if (null == res) {
          res = c.getAllocatedResource();
        }

        if (null != res) {
          tq.deductActuallyToBePreempted(context.getResourceCalculator(),
              tq.totalPartitionResource, res);
          Collection<TempAppPerPartition> tas = tq.getApps();
          if (null == tas || tas.isEmpty()) {
            continue;
          }

          deductPreemptableResourcePerApp(context, tq.totalPartitionResource,
              tas, res);
        }
      }
    }
  }

  private static void deductPreemptableResourcePerApp(
      CapacitySchedulerPreemptionContext context,
      Resource totalPartitionResource, Collection<TempAppPerPartition> tas,
      Resource res) {
    for (TempAppPerPartition ta : tas) {
      ta.deductActuallyToBePreempted(context.getResourceCalculator(),
          totalPartitionResource, res);
    }
  }

  /**
   * Invoke this method to preempt container based on resToObtain.
   *
   * @param rc
   *          resource calculator
   * @param context
   *          preemption context
   * @param resourceToObtainByPartitions
   *          map to hold resource to obtain per partition
   * @param rmContainer
   *          container
   * @param clusterResource
   *          total resource
   * @param preemptMap
   *          map to hold preempted containers
   * @param totalPreemptionAllowed
   *          total preemption allowed per round
   * @param conservativeDRF
   *          should we do conservativeDRF preemption or not.
   *          When true:
   *            stop preempt container when any major resource type
   *            {@literal <=} 0 for to-preempt.
   *            This is default preemption behavior of intra-queue preemption
   *          When false:
   *            stop preempt container when: all major resource type
   *            {@literal <=} 0 for to-preempt.
   *            This is default preemption behavior of inter-queue preemption
   * @return should we preempt rmContainer. If we should, deduct from
   *         <code>resourceToObtainByPartition</code>
   */
  public static boolean tryPreemptContainerAndDeductResToObtain(
      ResourceCalculator rc, CapacitySchedulerPreemptionContext context,
      Map<String, Resource> resourceToObtainByPartitions,
      RMContainer rmContainer, Resource clusterResource,
      Map<ApplicationAttemptId, Set<RMContainer>> preemptMap,
      Map<ApplicationAttemptId, Set<RMContainer>> curCandidates,
      Resource totalPreemptionAllowed, boolean conservativeDRF) {
    ApplicationAttemptId attemptId = rmContainer.getApplicationAttemptId();

    // We will not account resource of a container twice or more
    if (preemptMapContains(preemptMap, attemptId, rmContainer)) {
      return false;
    }

    String nodePartition = getPartitionByNodeId(context,
        rmContainer.getAllocatedNode());
    Resource toObtainByPartition = resourceToObtainByPartitions
        .get(nodePartition);
    if (null == toObtainByPartition) {
      return false;
    }

    // If a toObtain resource type == 0, set it to -1 to avoid 0 resource
    // type affect following doPreemption check: isAnyMajorResourceZero
    for (ResourceInformation ri : toObtainByPartition.getResources()) {
      if (ri.getValue() == 0) {
        ri.setValue(-1);
      }
    }

    if (rc.isAnyMajorResourceAboveZero(toObtainByPartition) && Resources.fitsIn(
        rc, rmContainer.getAllocatedResource(), totalPreemptionAllowed)) {
      boolean doPreempt;

      // How much resource left after preemption happen.
      Resource toObtainAfterPreemption = Resources.subtract(toObtainByPartition,
          rmContainer.getAllocatedResource());

      if (conservativeDRF) {
        doPreempt = !rc.isAnyMajorResourceZeroOrNegative(toObtainByPartition);
      } else {
        // When we want to do more aggressive preemption, we will do preemption
        // only if:
        // - The preempt of the container makes positive contribution to the
        //   to-obtain resource. Positive contribution means any positive
        //   resource type decreases.
        //
        //   This is example of positive contribution:
        //     * before: <30, 10, 5>, after <20, 10, -10>
        //   But this not positive contribution:
        //     * before: <30, 10, 0>, after <30, 10, -15>
        doPreempt = Resources.lessThan(rc, clusterResource,
            Resources
                .componentwiseMax(toObtainAfterPreemption, Resources.none()),
            Resources.componentwiseMax(toObtainByPartition, Resources.none()));
      }

      if (!doPreempt) {
        return false;
      }

      Resources.subtractFrom(toObtainByPartition,
          rmContainer.getAllocatedResource());
      Resources.subtractFrom(totalPreemptionAllowed,
          rmContainer.getAllocatedResource());

      // When we have no more resource need to obtain, remove from map.
      if (Resources.lessThanOrEqual(rc, clusterResource, toObtainByPartition,
          Resources.none())) {
        resourceToObtainByPartitions.remove(nodePartition);
      }

      // Add to preemptMap
      addToPreemptMap(preemptMap, curCandidates, attemptId, rmContainer);
      return true;
    }

    return false;
  }

  private static String getPartitionByNodeId(
      CapacitySchedulerPreemptionContext context, NodeId nodeId) {
    return context.getScheduler().getSchedulerNode(nodeId).getPartition();
  }

  protected static void addToPreemptMap(
      Map<ApplicationAttemptId, Set<RMContainer>> preemptMap,
      Map<ApplicationAttemptId, Set<RMContainer>> curCandidates,
      ApplicationAttemptId appAttemptId, RMContainer containerToPreempt) {
    Set<RMContainer> setForToPreempt = preemptMap.get(appAttemptId);
    Set<RMContainer> setForCurCandidates = curCandidates.get(appAttemptId);
    if (null == setForToPreempt) {
      setForToPreempt = new HashSet<>();
      preemptMap.put(appAttemptId, setForToPreempt);
    }
    setForToPreempt.add(containerToPreempt);

    if (null == setForCurCandidates) {
      setForCurCandidates = new HashSet<>();
      curCandidates.put(appAttemptId, setForCurCandidates);
    }
    setForCurCandidates.add(containerToPreempt);
  }

  private static boolean preemptMapContains(
      Map<ApplicationAttemptId, Set<RMContainer>> preemptMap,
      ApplicationAttemptId attemptId, RMContainer rmContainer) {
    Set<RMContainer> rmContainers = preemptMap.get(attemptId);
    if (null == rmContainers) {
      return false;
    }
    return rmContainers.contains(rmContainer);
  }
}

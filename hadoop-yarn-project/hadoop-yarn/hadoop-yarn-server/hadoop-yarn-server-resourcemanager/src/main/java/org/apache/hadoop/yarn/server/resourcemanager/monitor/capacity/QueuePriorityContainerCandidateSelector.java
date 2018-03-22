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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class QueuePriorityContainerCandidateSelector
    extends PreemptionCandidatesSelector {
  private static final Log LOG =
      LogFactory.getLog(QueuePriorityContainerCandidateSelector.class);

  // Configured timeout before doing reserved container preemption
  private long minTimeout;

  // Allow move reservation around for better placement?
  private boolean allowMoveReservation;

  // All the reserved containers of the system which could possible preempt from
  // queue with lower priorities
  private List<RMContainer> reservedContainers;

  // From -> To
  // A digraph to represent if one queue has higher priority than another.
  // For example, a->b means queue=a has higher priority than queue=b
  private Table<String, String, Boolean> priorityDigraph =
      HashBasedTable.create();

  private Resource clusterResource;
  private Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates;
  private Resource totalPreemptionAllowed;

  // A cached scheduler node map, will be refreshed each round.
  private Map<NodeId, TempSchedulerNode> tempSchedulerNodeMap = new HashMap<>();

  // Have we touched (make any changes to the node) for this round
  // Once a node is touched, we will not try to move reservations to the node
  private Set<NodeId> touchedNodes;

  // Resource which marked to preempt from other queues.
  // <Queue, Partition, Resource-marked-to-be-preempted-from-other-queue>
  private Table<String, String, Resource> toPreemptedFromOtherQueues =
      HashBasedTable.create();

  private final Comparator<RMContainer>
      CONTAINER_CREATION_TIME_COMPARATOR = new Comparator<RMContainer>() {
    @Override
    public int compare(RMContainer o1, RMContainer o2) {
      if (preemptionAllowed(o1.getQueueName(), o2.getQueueName())) {
        return -1;
      } else if (preemptionAllowed(o2.getQueueName(), o1.getQueueName())) {
        return 1;
      }

      // If two queues cannot preempt each other, compare creation time.
      return Long.compare(o1.getCreationTime(), o2.getCreationTime());
    }
  };

  QueuePriorityContainerCandidateSelector(
      CapacitySchedulerPreemptionContext preemptionContext) {
    super(preemptionContext);

    // Initialize parameters
    CapacitySchedulerConfiguration csc =
        preemptionContext.getScheduler().getConfiguration();

    minTimeout = csc.getPUOrderingPolicyUnderUtilizedPreemptionDelay();
    allowMoveReservation =
        csc.getPUOrderingPolicyUnderUtilizedPreemptionMoveReservation();
  }

  private List<TempQueuePerPartition> getPathToRoot(TempQueuePerPartition tq) {
    List<TempQueuePerPartition> list = new ArrayList<>();
    while (tq != null) {
      list.add(tq);
      tq = tq.parent;
    }
    return list;
  }

  private void initializePriorityDigraph() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Initializing priority preemption directed graph:");
    }
    // Make sure we iterate all leaf queue combinations
    for (String q1 : preemptionContext.getLeafQueueNames()) {
      for (String q2 : preemptionContext.getLeafQueueNames()) {
        // Make sure we only calculate each combination once instead of all
        // permutations
        if (q1.compareTo(q2) < 0) {
          TempQueuePerPartition tq1 = preemptionContext.getQueueByPartition(q1,
              RMNodeLabelsManager.NO_LABEL);
          TempQueuePerPartition tq2 = preemptionContext.getQueueByPartition(q2,
              RMNodeLabelsManager.NO_LABEL);

          List<TempQueuePerPartition> path1 = getPathToRoot(tq1);
          List<TempQueuePerPartition> path2 = getPathToRoot(tq2);

          // Get direct ancestor below LCA (Lowest common ancestor)
          int i = path1.size() - 1;
          int j = path2.size() - 1;
          while (path1.get(i).queueName.equals(path2.get(j).queueName)) {
            i--;
            j--;
          }

          // compare priority of path1[i] and path2[j]
          int p1 = path1.get(i).relativePriority;
          int p2 = path2.get(j).relativePriority;
          if (p1 < p2) {
            priorityDigraph.put(q2, q1, true);
            if (LOG.isDebugEnabled()) {
              LOG.debug("- Added priority ordering edge: " + q2 + " >> " + q1);
            }
          } else if (p2 < p1) {
            priorityDigraph.put(q1, q2, true);
            if (LOG.isDebugEnabled()) {
              LOG.debug("- Added priority ordering edge: " + q1 + " >> " + q2);
            }
          }
        }
      }
    }
  }

  /**
   * Do we allow demandingQueue preempt resource from toBePreemptedQueue
   *
   * @param demandingQueue demandingQueue
   * @param toBePreemptedQueue toBePreemptedQueue
   * @return can/cannot
   */
  private boolean preemptionAllowed(String demandingQueue,
      String toBePreemptedQueue) {
    return priorityDigraph.contains(demandingQueue,
        toBePreemptedQueue);
  }

  /**
   * Can we preempt enough resource for given:
   *
   * @param requiredResource askedResource
   * @param demandingQueue demandingQueue
   * @param schedulerNode node
   * @param lookingForNewReservationPlacement Are we trying to look for move
   *        reservation to the node
   * @param newlySelectedContainers newly selected containers, will be set when
   *        we can preempt enough resources from the node.
   *
   * @return can/cannot
   */
  private boolean canPreemptEnoughResourceForAsked(Resource requiredResource,
      String demandingQueue, FiCaSchedulerNode schedulerNode,
      boolean lookingForNewReservationPlacement,
      List<RMContainer> newlySelectedContainers) {
    // Do not check touched nodes again.
    if (touchedNodes.contains(schedulerNode.getNodeID())) {
      return false;
    }

    TempSchedulerNode node = tempSchedulerNodeMap.get(schedulerNode.getNodeID());
    if (null == node) {
      node = TempSchedulerNode.fromSchedulerNode(schedulerNode);
      tempSchedulerNodeMap.put(schedulerNode.getNodeID(), node);
    }

    if (null != schedulerNode.getReservedContainer()
        && lookingForNewReservationPlacement) {
      // Node reserved by the others, skip this node
      // We will not try to move the reservation to node which reserved already.
      return false;
    }

    // Need to preemption = asked - (node.total - node.allocated)
    Resource lacking = Resources.subtract(requiredResource, Resources
        .subtract(node.getTotalResource(), node.getAllocatedResource()));

    // On each host, simply check if we could preempt containers from
    // lower-prioritized queues or not
    List<RMContainer> runningContainers = node.getRunningContainers();
    Collections.sort(runningContainers, CONTAINER_CREATION_TIME_COMPARATOR);

    // First of all, consider already selected containers
    for (RMContainer runningContainer : runningContainers) {
      if (CapacitySchedulerPreemptionUtils.isContainerAlreadySelected(
          runningContainer, selectedCandidates)) {
        Resources.subtractFrom(lacking,
            runningContainer.getAllocatedResource());
      }
    }

    // If we already can allocate the reserved container after preemption,
    // skip following steps
    if (Resources.fitsIn(rc, lacking, Resources.none())) {
      return true;
    }

    Resource allowed = Resources.clone(totalPreemptionAllowed);
    Resource selected = Resources.createResource(0);

    for (RMContainer runningContainer : runningContainers) {
      if (CapacitySchedulerPreemptionUtils.isContainerAlreadySelected(
          runningContainer, selectedCandidates)) {
        // ignore selected containers
        continue;
      }

      // Only preempt resource from queue with lower priority
      if (!preemptionAllowed(demandingQueue,
          runningContainer.getQueueName())) {
        continue;
      }

      // Don't preempt AM container
      if (runningContainer.isAMContainer()) {
        continue;
      }

      // Not allow to preempt more than limit
      if (Resources.greaterThanOrEqual(rc, clusterResource, allowed,
          runningContainer.getAllocatedResource())) {
        Resources.subtractFrom(allowed,
            runningContainer.getAllocatedResource());
        Resources.subtractFrom(lacking,
            runningContainer.getAllocatedResource());
        Resources.addTo(selected, runningContainer.getAllocatedResource());

        if (null != newlySelectedContainers) {
          newlySelectedContainers.add(runningContainer);
        }
      }

      // Lacking <= 0 means we can allocate the reserved container
      if (Resources.fitsIn(rc, lacking, Resources.none())) {
        return true;
      }
    }

    return false;
  }

  private boolean preChecksForMovingReservedContainerToNode(
      RMContainer reservedContainer, FiCaSchedulerNode newNode) {
    // Don't do this if it has hard-locality preferences
    if (reservedContainer.getReservedSchedulerKey().getContainerToUpdate()
        != null) {
      // This means a container update request (like increase / promote)
      return false;
    }

    // For normal requests
    FiCaSchedulerApp app =
        preemptionContext.getScheduler().getApplicationAttempt(
            reservedContainer.getApplicationAttemptId());
    if (!app.getAppSchedulingInfo().canDelayTo(
        reservedContainer.getAllocatedSchedulerKey(), ResourceRequest.ANY)) {
      // This is a hard locality request
      return false;
    }

    // Check if newNode's partition matches requested partition
    if (!StringUtils.equals(reservedContainer.getNodeLabelExpression(),
        newNode.getPartition())) {
      return false;
    }

    return true;
  }

  private void tryToMakeBetterReservationPlacement(
      RMContainer reservedContainer,
      List<FiCaSchedulerNode> allSchedulerNodes) {
    for (FiCaSchedulerNode targetNode : allSchedulerNodes) {
      // Precheck if we can move the rmContainer to the new targetNode
      if (!preChecksForMovingReservedContainerToNode(reservedContainer,
          targetNode)) {
        continue;
      }

      if (canPreemptEnoughResourceForAsked(
          reservedContainer.getReservedResource(),
          reservedContainer.getQueueName(), targetNode, true, null)) {
        NodeId fromNode = reservedContainer.getNodeId();

        // We can place container to this targetNode, so just go ahead and notify
        // scheduler
        if (preemptionContext.getScheduler().moveReservedContainer(
            reservedContainer, targetNode)) {
          LOG.info("Successfully moved reserved container=" + reservedContainer
              .getContainerId() + " from targetNode=" + fromNode
              + " to targetNode=" + targetNode.getNodeID());
          touchedNodes.add(targetNode.getNodeID());
        }
      }
    }
  }

  /**
   * Do we allow the demanding queue preempt resource from other queues?
   * A satisfied queue is not allowed to preempt resource from other queues.
   * @param demandingQueue
   * @return allowed/not
   */
  private boolean isQueueSatisfied(String demandingQueue,
      String partition) {
    TempQueuePerPartition tq = preemptionContext.getQueueByPartition(
        demandingQueue, partition);
    if (null == tq) {
      return false;
    }

    Resource guaranteed = tq.getGuaranteed();
    Resource usedDeductReservd = Resources.subtract(tq.getUsed(),
        tq.getReserved());
    Resource markedToPreemptFromOtherQueue = toPreemptedFromOtherQueues.get(
        demandingQueue, partition);
    if (null == markedToPreemptFromOtherQueue) {
      markedToPreemptFromOtherQueue = Resources.none();
    }

    // return Used - reserved + to-preempt-from-other-queue >= guaranteed
    boolean flag = Resources.greaterThanOrEqual(rc, clusterResource,
        Resources.add(usedDeductReservd, markedToPreemptFromOtherQueue),
        guaranteed);
    return flag;
  }

  private void incToPreempt(String queue, String partition,
      Resource allocated) {
    Resource total = toPreemptedFromOtherQueues.get(queue, partition);
    if (null == total) {
      total = Resources.createResource(0);
      toPreemptedFromOtherQueues.put(queue, partition, total);
    }

    Resources.addTo(total, allocated);
  }

  @Override
  public Map<ApplicationAttemptId, Set<RMContainer>> selectCandidates(
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Resource clusterResource,
      Resource totalPreemptedResourceAllowed) {
    // Initialize digraph from queues
    // TODO (wangda): only do this when queue refreshed.
    priorityDigraph.clear();
    initializePriorityDigraph();

    // When all queues are set to same priority, or priority is not respected,
    // direct return.
    if (priorityDigraph.isEmpty()) {
      return selectedCandidates;
    }

    // Save parameters to be shared by other methods
    this.selectedCandidates = selectedCandidates;
    this.clusterResource = clusterResource;
    this.totalPreemptionAllowed = totalPreemptedResourceAllowed;

    toPreemptedFromOtherQueues.clear();

    reservedContainers = new ArrayList<>();

    // Clear temp-scheduler-node-map every time when doing selection of
    // containers.
    tempSchedulerNodeMap.clear();
    touchedNodes = new HashSet<>();

    // Add all reserved containers for analysis
    List<FiCaSchedulerNode> allSchedulerNodes =
        preemptionContext.getScheduler().getAllNodes();
    for (FiCaSchedulerNode node : allSchedulerNodes) {
      RMContainer reservedContainer = node.getReservedContainer();
      if (null != reservedContainer) {
        // Add to reservedContainers list if the queue that the reserved
        // container belongs to has high priority than at least one queue
        if (priorityDigraph.containsRow(
            reservedContainer.getQueueName())) {
          reservedContainers.add(reservedContainer);
        }
      }
    }

    // Sort reserved container by creation time
    Collections.sort(reservedContainers, CONTAINER_CREATION_TIME_COMPARATOR);

    long currentTime = System.currentTimeMillis();

    // From the beginning of the list
    for (RMContainer reservedContainer : reservedContainers) {
      // Only try to preempt reserved container after reserved container created
      // and cannot be allocated after minTimeout
      if (currentTime - reservedContainer.getCreationTime() < minTimeout) {
        continue;
      }

      FiCaSchedulerNode node = preemptionContext.getScheduler().getNode(
          reservedContainer.getReservedNode());
      if (null == node) {
        // Something is wrong, ignore
        continue;
      }

      List<RMContainer> newlySelectedToBePreemptContainers = new ArrayList<>();

      // Check if we can preempt for this queue
      // We will skip if the demanding queue is already satisfied.
      String demandingQueueName = reservedContainer.getQueueName();
      boolean demandingQueueSatisfied = isQueueSatisfied(demandingQueueName,
          node.getPartition());

      // We will continue check if it is possible to preempt reserved container
      // from the node.
      boolean canPreempt = false;
      if (!demandingQueueSatisfied) {
        canPreempt = canPreemptEnoughResourceForAsked(
            reservedContainer.getReservedResource(), demandingQueueName, node,
            false, newlySelectedToBePreemptContainers);
      }

      // Add selected container if we can allocate reserved container by
      // preemption others
      if (canPreempt) {
        touchedNodes.add(node.getNodeID());

        if (LOG.isDebugEnabled()) {
          LOG.debug("Trying to preempt following containers to make reserved "
              + "container=" + reservedContainer.getContainerId() + " on node="
              + node.getNodeID() + " can be allocated:");
        }

        // Update to-be-preempt
        incToPreempt(demandingQueueName, node.getPartition(),
            reservedContainer.getReservedResource());

        for (RMContainer c : newlySelectedToBePreemptContainers) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(" --container=" + c.getContainerId() + " resource=" + c
                .getReservedResource());
          }

          Set<RMContainer> containers = selectedCandidates.get(
              c.getApplicationAttemptId());
          if (null == containers) {
            containers = new HashSet<>();
            selectedCandidates.put(c.getApplicationAttemptId(), containers);
          }
          containers.add(c);

          // Update totalPreemptionResourceAllowed
          Resources.subtractFrom(totalPreemptedResourceAllowed,
              c.getAllocatedResource());
        }
      } else if (!demandingQueueSatisfied) {
        // We failed to get enough resource to allocate the container
        // This typically happens when the reserved node is proper, will
        // try to see if we can reserve the container on a better host.
        // Only do this if the demanding queue is not satisfied.
        //
        // TODO (wangda): do more tests before making it usable
        //
        if (allowMoveReservation) {
          tryToMakeBetterReservationPlacement(reservedContainer,
              allSchedulerNodes);
        }
      }
    }

    return selectedCandidates;
  }
}

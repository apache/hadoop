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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
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

public class ReservedContainerCandidatesSelector
    extends PreemptionCandidatesSelector {
  private static final Log LOG =
      LogFactory.getLog(ReservedContainerCandidatesSelector.class);

  private PreemptableResourceCalculator preemptableAmountCalculator;

  /**
   * A temporary data structure to remember what to preempt on a node
   */
  private static class NodeForPreemption {
    private float preemptionCost;
    private FiCaSchedulerNode schedulerNode;
    private List<RMContainer> selectedContainers;

    public NodeForPreemption(float preemptionCost,
        FiCaSchedulerNode schedulerNode, List<RMContainer> selectedContainers) {
      this.preemptionCost = preemptionCost;
      this.schedulerNode = schedulerNode;
      this.selectedContainers = selectedContainers;
    }
  }

  ReservedContainerCandidatesSelector(
      CapacitySchedulerPreemptionContext preemptionContext) {
    super(preemptionContext);
    preemptableAmountCalculator = new PreemptableResourceCalculator(
        preemptionContext, true);
  }

  @Override
  public Map<ApplicationAttemptId, Set<RMContainer>> selectCandidates(
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Resource clusterResource,
      Resource totalPreemptedResourceAllowed) {
    // Calculate how much resources we need to preempt
    preemptableAmountCalculator.computeIdealAllocation(clusterResource,
        totalPreemptedResourceAllowed);

    // Get queue to preemptable resource by partition
    Map<String, Map<String, Resource>> queueToPreemptableResourceByPartition =
        new HashMap<>();
    for (String leafQueue : preemptionContext.getLeafQueueNames()) {
      queueToPreemptableResourceByPartition.put(leafQueue,
          CapacitySchedulerPreemptionUtils
              .getResToObtainByPartitionForLeafQueue(preemptionContext,
                  leafQueue, clusterResource));
    }

    // Get list of nodes for preemption, ordered by preemption cost
    List<NodeForPreemption> nodesForPreemption = getNodesForPreemption(
        clusterResource, queueToPreemptableResourceByPartition,
        selectedCandidates, totalPreemptedResourceAllowed);

    for (NodeForPreemption nfp : nodesForPreemption) {
      RMContainer reservedContainer = nfp.schedulerNode.getReservedContainer();
      if (null == reservedContainer) {
        continue;
      }

      NodeForPreemption preemptionResult = getPreemptionCandidatesOnNode(
          nfp.schedulerNode, clusterResource,
          queueToPreemptableResourceByPartition, selectedCandidates,
          totalPreemptedResourceAllowed, false);
      if (null != preemptionResult) {
        for (RMContainer c : preemptionResult.selectedContainers) {
          ApplicationAttemptId appId = c.getApplicationAttemptId();
          Set<RMContainer> containers = selectedCandidates.get(appId);
          if (null == containers) {
            containers = new HashSet<>();
            selectedCandidates.put(appId, containers);
          }

          containers.add(c);
          if (LOG.isDebugEnabled()) {
            LOG.debug(this.getClass().getName() + " Marked container=" + c
                .getContainerId() + " from queue=" + c.getQueueName()
                + " to be preemption candidates");
          }
        }
      }
    }

    return selectedCandidates;
  }

  private Resource getPreemptableResource(String queueName,
      String partitionName,
      Map<String, Map<String, Resource>> queueToPreemptableResourceByPartition) {
    Map<String, Resource> partitionToPreemptable =
        queueToPreemptableResourceByPartition.get(queueName);
    if (null == partitionToPreemptable) {
      return null;
    }

    Resource preemptable = partitionToPreemptable.get(partitionName);
    return preemptable;
  }

  private boolean tryToPreemptFromQueue(Resource cluster, String queueName,
      String partitionName,
      Map<String, Map<String, Resource>> queueToPreemptableResourceByPartition,
      Resource required, Resource totalPreemptionAllowed, boolean readOnly) {
    Resource preemptable = getPreemptableResource(queueName, partitionName,
        queueToPreemptableResourceByPartition);
    if (null == preemptable) {
      return false;
    }

    if (!Resources.fitsIn(rc, cluster, required, preemptable)) {
      return false;
    }

    if (!Resources.fitsIn(rc, cluster, required, totalPreemptionAllowed)) {
      return false;
    }

    if (!readOnly) {
      Resources.subtractFrom(preemptable, required);
      Resources.subtractFrom(totalPreemptionAllowed, required);
    }
    return true;
  }



  /**
   * Try to check if we can preempt resources for reserved container in given node
   * @param node
   * @param cluster
   * @param queueToPreemptableResourceByPartition it's a map of
   *                 <queueName, <partition, preemptable-resource>>
   * @param readOnly do we want to modify preemptable resource after we selected
   *                 candidates
   * @return NodeForPreemption if it's possible to preempt containers on the node
   * to satisfy reserved resource
   */
  private NodeForPreemption getPreemptionCandidatesOnNode(
      FiCaSchedulerNode node, Resource cluster,
      Map<String, Map<String, Resource>> queueToPreemptableResourceByPartition,
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Resource totalPreemptionAllowed, boolean readOnly) {
    RMContainer reservedContainer = node.getReservedContainer();
    Resource available = Resources.clone(node.getUnallocatedResource());
    Resource totalSelected = Resources.createResource(0);
    List<RMContainer> sortedRunningContainers =
        node.getCopiedListOfRunningContainers();
    List<RMContainer> selectedContainers = new ArrayList<>();
    Map<ContainerId, RMContainer> killableContainers =
        node.getKillableContainers();

    // Sort running container by launch time, we preferred to preempt recent
    // launched preempt container
    Collections.sort(sortedRunningContainers, new Comparator<RMContainer>() {
      @Override public int compare(RMContainer o1, RMContainer o2) {
        return -1 * o1.getContainerId().compareTo(o2.getContainerId());
      }
    });

    // First check: can we preempt containers to allocate the
    // reservedContainer?
    boolean canAllocateReservedContainer = false;

    // At least, we can get available + killable resources from this node
    Resource cur = Resources.add(available, node.getTotalKillableResources());
    String partition = node.getPartition();

    // Avoid preempt any container if required <= available + killable
    if (Resources.fitsIn(rc, cluster, reservedContainer.getReservedResource(),
        cur)) {
      return null;
    }

    // Extra cost of am container preemption
    float amPreemptionCost = 0f;

    for (RMContainer c : sortedRunningContainers) {
      String containerQueueName = c.getQueueName();

      // Skip container if it is already marked killable
      if (killableContainers.containsKey(c.getContainerId())) {
        continue;
      }

      // An alternative approach is add a "penalty cost" if AM container is
      // selected. Here for safety, avoid preempt AM container in any cases
      if (c.isAMContainer()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skip selecting AM container on host=" + node.getNodeID()
              + " AM container=" + c.getContainerId());
        }
        continue;
      }

      // Can we preempt container c?
      // Check if we have quota to preempt this container
      boolean canPreempt = tryToPreemptFromQueue(cluster, containerQueueName,
          partition, queueToPreemptableResourceByPartition,
          c.getAllocatedResource(), totalPreemptionAllowed, readOnly);

      // If we can, add to selected container, and change resource accordingly.
      if (canPreempt) {
        if (!CapacitySchedulerPreemptionUtils.isContainerAlreadySelected(c,
            selectedCandidates)) {
          if (!readOnly) {
            selectedContainers.add(c);
          }
          Resources.addTo(totalSelected, c.getAllocatedResource());
        }
        Resources.addTo(cur, c.getAllocatedResource());
        if (Resources.fitsIn(rc, cluster,
            reservedContainer.getReservedResource(), cur)) {
          canAllocateReservedContainer = true;
          break;
        }
      }
    }

    if (!canAllocateReservedContainer) {
      if (!readOnly) {
        // Revert queue preemption quotas
        for (RMContainer c : selectedContainers) {
          Resource res = getPreemptableResource(c.getQueueName(), partition,
              queueToPreemptableResourceByPartition);
          if (null == res) {
            // This shouldn't happen in normal cases, one possible cause is
            // container moved to different queue while executing preemption logic.
            // Ignore such failures.
            continue;
          }
          Resources.addTo(res, c.getAllocatedResource());
        }
      }
      return null;
    }

    float ratio = Resources.ratio(rc, totalSelected,
        reservedContainer.getReservedResource());

    // Compute preemption score
    NodeForPreemption nfp = new NodeForPreemption(ratio + amPreemptionCost,
        node, selectedContainers);

    return nfp;
  }

  private List<NodeForPreemption> getNodesForPreemption(Resource cluster,
      Map<String, Map<String, Resource>> queueToPreemptableResourceByPartition,
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Resource totalPreemptionAllowed) {
    List<NodeForPreemption> nfps = new ArrayList<>();

    // get nodes have reserved container
    for (FiCaSchedulerNode node : preemptionContext.getScheduler()
        .getAllNodes()) {
      if (node.getReservedContainer() != null) {
        NodeForPreemption nfp = getPreemptionCandidatesOnNode(node, cluster,
            queueToPreemptableResourceByPartition, selectedCandidates,
            totalPreemptionAllowed, true);
        if (null != nfp) {
          // Null means we cannot preempt containers on the node to satisfy
          // reserved container
          nfps.add(nfp);
        }
      }
    }

    // Return sorted node-for-preemptions (by cost)
    Collections.sort(nfps, new Comparator<NodeForPreemption>() {
      @Override
      public int compare(NodeForPreemption o1, NodeForPreemption o2) {
        return Float.compare(o1.preemptionCost, o2.preemptionCost);
      }
    });

    return nfps;
  }
}
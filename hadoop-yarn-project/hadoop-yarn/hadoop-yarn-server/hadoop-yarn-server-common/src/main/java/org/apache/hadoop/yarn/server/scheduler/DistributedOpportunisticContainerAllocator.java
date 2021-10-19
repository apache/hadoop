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

package org.apache.hadoop.yarn.server.scheduler;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;

import org.apache.hadoop.yarn.server.api.protocolrecords.RemoteNode;
import org.apache.hadoop.yarn.server.metrics.OpportunisticSchedulerMetrics;
import org.apache.hadoop.yarn.server.security.BaseContainerTokenSecretManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <p>
 * The DistributedOpportunisticContainerAllocator allocates containers on a
 * given list of nodes, after modifying the container sizes to respect the
 * limits set by the ResourceManager. It tries to distribute the containers
 * as evenly as possible.
 * </p>
 */
public class DistributedOpportunisticContainerAllocator
    extends OpportunisticContainerAllocator {

  private static final int NODE_LOCAL_LOOP = 0;
  private static final int RACK_LOCAL_LOOP = 1;
  private static final int OFF_SWITCH_LOOP = 2;

  private static final Logger LOG =
      LoggerFactory.getLogger(DistributedOpportunisticContainerAllocator.class);

  /**
   * Create a new Opportunistic Container Allocator.
   * @param tokenSecretManager TokenSecretManager
   */
  public DistributedOpportunisticContainerAllocator(
      BaseContainerTokenSecretManager tokenSecretManager) {
    super(tokenSecretManager);
  }

  /**
   * Create a new Opportunistic Container Allocator.
   * @param tokenSecretManager TokenSecretManager
   * @param maxAllocationsPerAMHeartbeat max number of containers to be
   *                                     allocated in one AM heartbeat
   */
  public DistributedOpportunisticContainerAllocator(
      BaseContainerTokenSecretManager tokenSecretManager,
      int maxAllocationsPerAMHeartbeat) {
    super(tokenSecretManager, maxAllocationsPerAMHeartbeat);
  }

  @Override
  public List<Container> allocateContainers(ResourceBlacklistRequest blackList,
      List<ResourceRequest> oppResourceReqs,
      ApplicationAttemptId applicationAttemptId,
      OpportunisticContainerContext opportContext, long rmIdentifier,
      String appSubmitter) throws YarnException {

    // Update black list.
    updateBlacklist(blackList, opportContext);

    // Add OPPORTUNISTIC requests to the outstanding ones.
    opportContext.addToOutstandingReqs(oppResourceReqs);
    Set<String> nodeBlackList = new HashSet<>(opportContext.getBlacklist());
    Set<String> allocatedNodes = new HashSet<>();
    List<Container> allocatedContainers = new ArrayList<>();

    // Satisfy the outstanding OPPORTUNISTIC requests.
    boolean continueLoop = true;
    while (continueLoop) {
      continueLoop = false;
      List<Map<Resource, List<Allocation>>> allocations = new ArrayList<>();
      for (SchedulerRequestKey schedulerKey :
          opportContext.getOutstandingOpReqs().descendingKeySet()) {
        // Allocated containers :
        //  Key = Requested Capability,
        //  Value = List of Containers of given cap (the actual container size
        //          might be different than what is requested, which is why
        //          we need the requested capability (key) to match against
        //          the outstanding reqs)
        int remAllocs = -1;
        int maxAllocationsPerAMHeartbeat = getMaxAllocationsPerAMHeartbeat();
        if (maxAllocationsPerAMHeartbeat > 0) {
          remAllocs =
              maxAllocationsPerAMHeartbeat - allocatedContainers.size()
                  - getTotalAllocations(allocations);
          if (remAllocs <= 0) {
            LOG.info("Not allocating more containers as we have reached max "
                    + "allocations per AM heartbeat {}",
                maxAllocationsPerAMHeartbeat);
            break;
          }
        }
        Map<Resource, List<Allocation>> allocation = allocate(
            rmIdentifier, opportContext, schedulerKey, applicationAttemptId,
            appSubmitter, nodeBlackList, allocatedNodes, remAllocs);
        if (allocation.size() > 0) {
          allocations.add(allocation);
          continueLoop = true;
        }
      }
      matchAllocation(allocations, allocatedContainers, opportContext);
    }

    return allocatedContainers;
  }

  private Map<Resource, List<Allocation>> allocate(long rmIdentifier,
      OpportunisticContainerContext appContext, SchedulerRequestKey schedKey,
      ApplicationAttemptId appAttId, String userName, Set<String> blackList,
      Set<String> allocatedNodes, int maxAllocations)
      throws YarnException {
    Map<Resource, List<Allocation>> containers = new HashMap<>();
    for (EnrichedResourceRequest enrichedAsk :
        appContext.getOutstandingOpReqs().get(schedKey).values()) {
      int remainingAllocs = -1;
      if (maxAllocations > 0) {
        int totalAllocated = 0;
        for (List<Allocation> allocs : containers.values()) {
          totalAllocated += allocs.size();
        }
        remainingAllocs = maxAllocations - totalAllocated;
        if (remainingAllocs <= 0) {
          LOG.info("Not allocating more containers as max allocations per AM "
              + "heartbeat {} has reached", getMaxAllocationsPerAMHeartbeat());
          break;
        }
      }
      allocateContainersInternal(rmIdentifier, appContext.getAppParams(),
          appContext.getContainerIdGenerator(), blackList, allocatedNodes,
          appAttId, appContext.getNodeMap(), userName, containers, enrichedAsk,
          remainingAllocs);
      ResourceRequest anyAsk = enrichedAsk.getRequest();
      if (!containers.isEmpty()) {
        LOG.info("Opportunistic allocation requested for [priority={}, "
                + "allocationRequestId={}, num_containers={}, capability={}] "
                + "allocated = {}", anyAsk.getPriority(),
            anyAsk.getAllocationRequestId(), anyAsk.getNumContainers(),
            anyAsk.getCapability(), containers.keySet());
      }
    }
    return containers;
  }

  private void allocateContainersInternal(long rmIdentifier,
      AllocationParams appParams, ContainerIdGenerator idCounter,
      Set<String> blacklist, Set<String> allocatedNodes,
      ApplicationAttemptId id, Map<String, RemoteNode> allNodes,
      String userName, Map<Resource, List<Allocation>> allocations,
      EnrichedResourceRequest enrichedAsk, int maxAllocations)
      throws YarnException {
    if (allNodes.size() == 0) {
      LOG.info("No nodes currently available to " +
          "allocate OPPORTUNISTIC containers.");
      return;
    }
    ResourceRequest anyAsk = enrichedAsk.getRequest();
    int toAllocate = anyAsk.getNumContainers()
        - (allocations.isEmpty() ? 0 :
        allocations.get(anyAsk.getCapability()).size());
    toAllocate = Math.min(toAllocate,
        appParams.getMaxAllocationsPerSchedulerKeyPerRound());
    if (maxAllocations >= 0) {
      toAllocate = Math.min(maxAllocations, toAllocate);
    }
    int numAllocated = 0;
    // Node Candidates are selected as follows:
    // * Node local candidates selected in loop == 0
    // * Rack local candidates selected in loop == 1
    // * From loop == 2 onwards, we revert to off switch allocations.
    int loopIndex = OFF_SWITCH_LOOP;
    if (enrichedAsk.getNodeMap().size() > 0) {
      loopIndex = NODE_LOCAL_LOOP;
    }
    while (numAllocated < toAllocate) {
      Collection<RemoteNode> nodeCandidates =
          findNodeCandidates(loopIndex, allNodes, blacklist, allocatedNodes,
              enrichedAsk);
      for (RemoteNode rNode : nodeCandidates) {
        String rNodeHost = rNode.getNodeId().getHost();
        // Ignore black list
        if (blacklist.contains(rNodeHost)) {
          LOG.info("Nodes for scheduling has a blacklisted node" +
              " [" + rNodeHost + "]..");
          continue;
        }
        String location = ResourceRequest.ANY;
        if (loopIndex == NODE_LOCAL_LOOP) {
          if (enrichedAsk.getNodeMap().containsKey(rNodeHost)) {
            location = rNodeHost;
          } else {
            continue;
          }
        } else if (allocatedNodes.contains(rNodeHost)) {
          LOG.info("Opportunistic container has already been allocated on {}.",
              rNodeHost);
          continue;
        }
        if (loopIndex == RACK_LOCAL_LOOP) {
          if (enrichedAsk.getRackMap().containsKey(
              rNode.getRackName())) {
            location = rNode.getRackName();
          } else {
            continue;
          }
        }
        Container container = createContainer(rmIdentifier, appParams,
            idCounter, id, userName, allocations, location,
            anyAsk, rNode);
        numAllocated++;
        updateMetrics(loopIndex);
        allocatedNodes.add(rNodeHost);
        LOG.info("Allocated [" + container.getId() + "] as opportunistic at " +
            "location [" + location + "]");
        if (numAllocated >= toAllocate) {
          break;
        }
      }
      if (loopIndex == NODE_LOCAL_LOOP &&
          enrichedAsk.getRackMap().size() > 0) {
        loopIndex = RACK_LOCAL_LOOP;
      } else {
        loopIndex++;
      }
      // Handle case where there are no nodes remaining after blacklist is
      // considered.
      if (loopIndex > OFF_SWITCH_LOOP && numAllocated == 0) {
        LOG.warn("Unable to allocate any opportunistic containers.");
        break;
      }
    }
  }



  private void updateMetrics(int loopIndex) {
    OpportunisticSchedulerMetrics metrics =
        OpportunisticSchedulerMetrics.getMetrics();
    if (loopIndex == NODE_LOCAL_LOOP) {
      metrics.incrNodeLocalOppContainers();
    } else if (loopIndex == RACK_LOCAL_LOOP) {
      metrics.incrRackLocalOppContainers();
    } else {
      metrics.incrOffSwitchOppContainers();
    }
  }

  private Collection<RemoteNode> findNodeCandidates(int loopIndex,
      Map<String, RemoteNode> allNodes, Set<String> blackList,
      Set<String> allocatedNodes, EnrichedResourceRequest enrichedRR) {
    LinkedList<RemoteNode> retList = new LinkedList<>();
    String partition = getRequestPartition(enrichedRR);
    if (loopIndex > 1) {
      for (RemoteNode remoteNode : allNodes.values()) {
        if (StringUtils.equals(partition, getRemoteNodePartition(remoteNode))) {
          retList.add(remoteNode);
        }
      }
      return retList;
    } else {

      int numContainers = enrichedRR.getRequest().getNumContainers();
      while (numContainers > 0) {
        if (loopIndex == 0) {
          // Node local candidates
          numContainers = collectNodeLocalCandidates(
              allNodes, enrichedRR, retList, numContainers);
        } else {
          // Rack local candidates
          numContainers =
              collectRackLocalCandidates(allNodes, enrichedRR, retList,
                  blackList, allocatedNodes, numContainers);
        }
        if (numContainers == enrichedRR.getRequest().getNumContainers()) {
          // If there is no change in numContainers, then there is no point
          // in looping again.
          break;
        }
      }
      return retList;
    }
  }

  private int collectRackLocalCandidates(Map<String, RemoteNode> allNodes,
      EnrichedResourceRequest enrichedRR, LinkedList<RemoteNode> retList,
      Set<String> blackList, Set<String> allocatedNodes, int numContainers) {
    String partition = getRequestPartition(enrichedRR);
    for (RemoteNode rNode : allNodes.values()) {
      if (StringUtils.equals(partition, getRemoteNodePartition(rNode)) &&
          enrichedRR.getRackMap().containsKey(rNode.getRackName())) {
        String rHost = rNode.getNodeId().getHost();
        if (blackList.contains(rHost)) {
          continue;
        }
        if (allocatedNodes.contains(rHost)) {
          retList.addLast(rNode);
        } else {
          retList.addFirst(rNode);
          numContainers--;
        }
      }
      if (numContainers == 0) {
        break;
      }
    }
    return numContainers;
  }

  private int collectNodeLocalCandidates(Map<String, RemoteNode> allNodes,
      EnrichedResourceRequest enrichedRR, List<RemoteNode> retList,
      int numContainers) {
    String partition = getRequestPartition(enrichedRR);
    for (String nodeName : enrichedRR.getNodeMap().keySet()) {
      RemoteNode remoteNode = allNodes.get(nodeName);
      if (remoteNode != null &&
          StringUtils.equals(partition, getRemoteNodePartition(remoteNode))) {
        retList.add(remoteNode);
        numContainers--;
      }
      if (numContainers == 0) {
        break;
      }
    }
    return numContainers;
  }
}

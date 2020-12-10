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


package org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoteNode;
import org.apache.hadoop.yarn.server.metrics.OpportunisticSchedulerMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerAllocator;
import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerContext;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.security.BaseContainerTokenSecretManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 * The CentralizedOpportunisticContainerAllocator allocates opportunistic
 * containers by considering all the nodes present in the cluster, after
 * modifying the container sizes to respect the limits set by the
 * ResourceManager. It tries to distribute the containers as evenly as
 * possible.
 * </p>
 */
public class CentralizedOpportunisticContainerAllocator extends
    OpportunisticContainerAllocator {

  private static final Logger LOG =
      LoggerFactory.getLogger(CentralizedOpportunisticContainerAllocator.class);

  private NodeQueueLoadMonitor nodeQueueLoadMonitor;
  private OpportunisticSchedulerMetrics metrics =
      OpportunisticSchedulerMetrics.getMetrics();

  /**
   * Create a new Centralized Opportunistic Container Allocator.
   * @param tokenSecretManager TokenSecretManager
   */
  public CentralizedOpportunisticContainerAllocator(
      BaseContainerTokenSecretManager tokenSecretManager) {
    super(tokenSecretManager);
  }

  /**
   * Create a new Centralized Opportunistic Container Allocator.
   * @param tokenSecretManager TokenSecretManager
   * @param maxAllocationsPerAMHeartbeat max number of containers to be
   *                                     allocated in one AM heartbeat
   */
  public CentralizedOpportunisticContainerAllocator(
      BaseContainerTokenSecretManager tokenSecretManager,
      int maxAllocationsPerAMHeartbeat,
      NodeQueueLoadMonitor nodeQueueLoadMonitor) {
    super(tokenSecretManager, maxAllocationsPerAMHeartbeat);
    this.nodeQueueLoadMonitor = nodeQueueLoadMonitor;
  }

  @VisibleForTesting
  void setNodeQueueLoadMonitor(NodeQueueLoadMonitor nodeQueueLoadMonitor) {
    this.nodeQueueLoadMonitor = nodeQueueLoadMonitor;
  }

  @Override
  public List<Container> allocateContainers(
      ResourceBlacklistRequest blackList, List<ResourceRequest> oppResourceReqs,
      ApplicationAttemptId applicationAttemptId,
      OpportunisticContainerContext opportContext, long rmIdentifier,
      String appSubmitter) throws YarnException {

    updateBlacklist(blackList, opportContext);

    // Add OPPORTUNISTIC requests to the outstanding ones.
    opportContext.addToOutstandingReqs(oppResourceReqs);

    Set<String> nodeBlackList = new HashSet<>(opportContext.getBlacklist());
    List<Container> allocatedContainers = new ArrayList<>();
    int maxAllocationsPerAMHeartbeat = getMaxAllocationsPerAMHeartbeat();
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
      if (maxAllocationsPerAMHeartbeat > 0) {
        remAllocs =
            maxAllocationsPerAMHeartbeat - getTotalAllocations(allocations);
        if (remAllocs <= 0) {
          LOG.info("Not allocating more containers as we have reached max "
                  + "allocations per AM heartbeat {}",
              maxAllocationsPerAMHeartbeat);
          break;
        }
      }
      Map<Resource, List<Allocation>> allocation = allocatePerSchedulerKey(
          rmIdentifier, opportContext, schedulerKey, applicationAttemptId,
          appSubmitter, nodeBlackList, remAllocs);
      if (allocation.size() > 0) {
        allocations.add(allocation);
      }
    }
    matchAllocation(allocations, allocatedContainers, opportContext);
    return allocatedContainers;
  }

  private Map<Resource, List<Allocation>> allocatePerSchedulerKey(
      long rmIdentifier, OpportunisticContainerContext appContext,
      SchedulerRequestKey schedKey, ApplicationAttemptId appAttId,
      String userName, Set<String> blackList, int maxAllocations)
      throws YarnException {
    Map<Resource, List<Allocation>> allocations = new HashMap<>();
    int totalAllocated = 0;
    for (EnrichedResourceRequest enrichedAsk :
        appContext.getOutstandingOpReqs().get(schedKey).values()) {
      int remainingAllocs = -1;
      if (maxAllocations > 0) {
        remainingAllocs = maxAllocations - totalAllocated;
        if (remainingAllocs <= 0) {
          LOG.info("Not allocating more containers as max allocations per AM "
              + "heartbeat {} has reached", getMaxAllocationsPerAMHeartbeat());
          break;
        }
      }

      totalAllocated += allocateContainersPerRequest(rmIdentifier,
          appContext.getAppParams(),
          appContext.getContainerIdGenerator(), blackList,
          appAttId, userName, allocations, enrichedAsk,
          remainingAllocs);
      ResourceRequest anyAsk = enrichedAsk.getRequest();
      if (!allocations.isEmpty()) {
        LOG.info("Opportunistic allocation requested for [priority={}, "
                + "allocationRequestId={}, num_containers={}, capability={}] "
                + "allocated = {}", anyAsk.getPriority(),
            anyAsk.getAllocationRequestId(), anyAsk.getNumContainers(),
            anyAsk.getCapability(), allocations.keySet());
      }
    }
    return allocations;
  }

  @SuppressWarnings("checkstyle:parameternumber")
  private int allocateContainersPerRequest(long rmIdentifier,
      AllocationParams appParams, ContainerIdGenerator idCounter,
      Set<String> blacklist,
      ApplicationAttemptId id,
      String userName, Map<Resource, List<Allocation>> allocations,
      EnrichedResourceRequest enrichedAsk, int maxAllocations)
      throws YarnException {
    ResourceRequest anyAsk = enrichedAsk.getRequest();
    int totalAllocated = 0;
    int maxToAllocate = anyAsk.getNumContainers()
        - (allocations.isEmpty() ? 0 :
        allocations.get(anyAsk.getCapability()).size());
    if (maxAllocations >= 0) {
      maxToAllocate = Math.min(maxAllocations, maxToAllocate);
    }

    // allocate node local
    if (maxToAllocate > 0) {
      Map<String, AtomicInteger> nodeLocations = enrichedAsk.getNodeMap();
      for (Map.Entry<String, AtomicInteger> nodeLocation :
          nodeLocations.entrySet()) {
        int numContainers = nodeLocation.getValue().get();
        numContainers = Math.min(numContainers, maxToAllocate);
        List<Container> allocatedContainers =
            allocateNodeLocal(enrichedAsk, nodeLocation.getKey(),
                numContainers, rmIdentifier, appParams, idCounter, blacklist,
                id, userName, allocations);
        totalAllocated += allocatedContainers.size();
        maxToAllocate -= allocatedContainers.size();
        // no more containers to allocate
        if (maxToAllocate <= 0) {
          break;
        }
      }
    }

    // if still left, allocate rack local
    if (maxToAllocate > 0) {
      Map<String, AtomicInteger> rackLocations = enrichedAsk.getRackMap();
      for (Map.Entry<String, AtomicInteger> rack : rackLocations.entrySet()) {
        int numContainers = rack.getValue().get();
        numContainers = Math.min(numContainers, maxToAllocate);
        List<Container> allocatedContainers =
            allocateRackLocal(enrichedAsk, rack.getKey(), numContainers,
                rmIdentifier, appParams, idCounter, blacklist, id,
                userName, allocations);
        totalAllocated += allocatedContainers.size();
        maxToAllocate -= allocatedContainers.size();
        // no more containers to allocate
        if (maxToAllocate <= 0) {
          break;
        }
      }
    }

    // if still left, try on ANY
    if (maxToAllocate > 0) {
      List<Container> allocatedContainers = allocateAny(enrichedAsk,
          maxToAllocate, rmIdentifier, appParams, idCounter, blacklist,
          id, userName, allocations);
      totalAllocated += allocatedContainers.size();
    }
    return totalAllocated;
  }

  @SuppressWarnings("checkstyle:parameternumber")
  private List<Container> allocateNodeLocal(
      EnrichedResourceRequest enrichedAsk,
      String nodeLocation,
      int toAllocate, long rmIdentifier,
      AllocationParams appParams, ContainerIdGenerator idCounter,
      Set<String> blacklist,
      ApplicationAttemptId id,
      String userName, Map<Resource, List<Allocation>> allocations)
      throws YarnException {
    List<Container> allocatedContainers = new ArrayList<>();
    while (toAllocate > 0) {
      RMNode node = nodeQueueLoadMonitor.selectLocalNode(nodeLocation,
          blacklist);
      if (node != null) {
        toAllocate--;
        Container container = createContainer(rmIdentifier, appParams,
            idCounter, id, userName, allocations, nodeLocation,
            enrichedAsk.getRequest(), convertToRemoteNode(node));
        allocatedContainers.add(container);
        LOG.info("Allocated [{}] as opportunistic at location [{}]",
            container.getId(), nodeLocation);
        metrics.incrNodeLocalOppContainers();
      } else {
        // we couldn't allocate any - break the loop.
        break;
      }
    }
    return allocatedContainers;
  }

  @SuppressWarnings("checkstyle:parameternumber")
  private List<Container> allocateRackLocal(EnrichedResourceRequest enrichedAsk,
      String rackLocation, int toAllocate, long rmIdentifier,
      AllocationParams appParams, ContainerIdGenerator idCounter,
      Set<String> blacklist,
      ApplicationAttemptId id,
      String userName, Map<Resource, List<Allocation>> allocations)
      throws YarnException {
    List<Container> allocatedContainers = new ArrayList<>();
    while (toAllocate > 0) {
      RMNode node = nodeQueueLoadMonitor.selectRackLocalNode(rackLocation,
          blacklist);
      if (node != null) {
        toAllocate--;
        Container container = createContainer(rmIdentifier, appParams,
            idCounter, id, userName, allocations, rackLocation,
            enrichedAsk.getRequest(), convertToRemoteNode(node));
        allocatedContainers.add(container);
        metrics.incrRackLocalOppContainers();
        LOG.info("Allocated [{}] as opportunistic at location [{}]",
            container.getId(), rackLocation);
      } else {
        // we couldn't allocate any - break the loop.
        break;
      }
    }
    return allocatedContainers;
  }

  @SuppressWarnings("checkstyle:parameternumber")
  private List<Container> allocateAny(EnrichedResourceRequest enrichedAsk,
      int toAllocate, long rmIdentifier,
      AllocationParams appParams, ContainerIdGenerator idCounter,
      Set<String> blacklist,
      ApplicationAttemptId id,
      String userName, Map<Resource, List<Allocation>> allocations)
      throws YarnException {
    List<Container> allocatedContainers = new ArrayList<>();
    while (toAllocate > 0) {
      RMNode node = nodeQueueLoadMonitor.selectAnyNode(blacklist);
      if (node != null) {
        toAllocate--;
        Container container = createContainer(rmIdentifier, appParams,
            idCounter, id, userName, allocations, ResourceRequest.ANY,
            enrichedAsk.getRequest(), convertToRemoteNode(node));
        allocatedContainers.add(container);
        metrics.incrOffSwitchOppContainers();
        LOG.info("Allocated [{}] as opportunistic at location [{}]",
            container.getId(), ResourceRequest.ANY);
      } else {
        // we couldn't allocate any - break the loop.
        break;
      }
    }
    return allocatedContainers;
  }

  private RemoteNode convertToRemoteNode(RMNode rmNode) {
    if (rmNode != null) {
      RemoteNode rNode = RemoteNode.newInstance(rmNode.getNodeID(),
          rmNode.getHttpAddress());
      rNode.setRackName(rmNode.getRackName());
      return rNode;
    }
    return null;
  }
}

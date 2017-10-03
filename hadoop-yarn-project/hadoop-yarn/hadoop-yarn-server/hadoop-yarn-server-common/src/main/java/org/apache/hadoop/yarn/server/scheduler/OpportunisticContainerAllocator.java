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

import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ContainerType;

import org.apache.hadoop.yarn.server.api.protocolrecords.RemoteNode;
import org.apache.hadoop.yarn.server.security.BaseContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>
 * The OpportunisticContainerAllocator allocates containers on a given list of
 * nodes, after modifying the container sizes to respect the limits set by the
 * ResourceManager. It tries to distribute the containers as evenly as possible.
 * </p>
 */
public class OpportunisticContainerAllocator {

  private static final int NODE_LOCAL_LOOP = 0;
  private static final int RACK_LOCAL_LOOP = 1;
  private static final int OFF_SWITCH_LOOP = 2;

  /**
   * This class encapsulates application specific parameters used to build a
   * Container.
   */
  public static class AllocationParams {
    private Resource maxResource;
    private Resource minResource;
    private Resource incrementResource;
    private int containerTokenExpiryInterval;
    private int maxAllocationsPerSchedulerKeyPerRound = 1;

    /**
     * Return Max Resource.
     * @return Resource
     */
    public Resource getMaxResource() {
      return maxResource;
    }

    /**
     * Set Max Resource.
     * @param maxResource Resource
     */
    public void setMaxResource(Resource maxResource) {
      this.maxResource = maxResource;
    }

    /**
     * Get Min Resource.
     * @return Resource
     */
    public Resource getMinResource() {
      return minResource;
    }

    /**
     * Set Min Resource.
     * @param minResource Resource
     */
    public void setMinResource(Resource minResource) {
      this.minResource = minResource;
    }

    /**
     * Get Incremental Resource.
     * @return Incremental Resource
     */
    public Resource getIncrementResource() {
      return incrementResource;
    }

    /**
     * Set Incremental resource.
     * @param incrementResource Resource
     */
    public void setIncrementResource(Resource incrementResource) {
      this.incrementResource = incrementResource;
    }

    /**
     * Get Container Token Expiry interval.
     * @return Container Token Expiry interval
     */
    public int getContainerTokenExpiryInterval() {
      return containerTokenExpiryInterval;
    }

    /**
     * Set Container Token Expiry time in ms.
     * @param containerTokenExpiryInterval Container Token Expiry in ms
     */
    public void setContainerTokenExpiryInterval(
        int containerTokenExpiryInterval) {
      this.containerTokenExpiryInterval = containerTokenExpiryInterval;
    }

    /**
     * Get the Max Allocations per Scheduler Key per allocation round.
     * @return maxAllocationsPerSchedulerKeyPerRound.
     */
    public int getMaxAllocationsPerSchedulerKeyPerRound() {
      return maxAllocationsPerSchedulerKeyPerRound;
    }

    /**
     * Set the Max Allocations per Scheduler Key per allocation round.
     * @param maxAllocationsPerSchedulerKeyPerRound val.
     */
    public void setMaxAllocationsPerSchedulerKeyPerRound(
        int maxAllocationsPerSchedulerKeyPerRound) {
      this.maxAllocationsPerSchedulerKeyPerRound =
          maxAllocationsPerSchedulerKeyPerRound;
    }
  }

  /**
   * A Container Id Generator.
   */
  public static class ContainerIdGenerator {

    protected volatile AtomicLong containerIdCounter = new AtomicLong(1);

    /**
     * This method can reset the generator to a specific value.
     * @param containerIdStart containerId
     */
    public void resetContainerIdCounter(long containerIdStart) {
      this.containerIdCounter.set(containerIdStart);
    }

    /**
     * Generates a new long value. Default implementation increments the
     * underlying AtomicLong. Sub classes are encouraged to over-ride this
     * behaviour.
     * @return Counter.
     */
    public long generateContainerId() {
      return this.containerIdCounter.incrementAndGet();
    }
  }

  /**
   * Class that includes two lists of {@link ResourceRequest}s: one for
   * GUARANTEED and one for OPPORTUNISTIC {@link ResourceRequest}s.
   */
  public static class PartitionedResourceRequests {
    private List<ResourceRequest> guaranteed = new ArrayList<>();
    private List<ResourceRequest> opportunistic = new ArrayList<>();

    public List<ResourceRequest> getGuaranteed() {
      return guaranteed;
    }

    public List<ResourceRequest> getOpportunistic() {
      return opportunistic;
    }
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(OpportunisticContainerAllocator.class);

  private static final ResourceCalculator RESOURCE_CALCULATOR =
      new DominantResourceCalculator();

  private final BaseContainerTokenSecretManager tokenSecretManager;

  static class Allocation {
    private final Container container;
    private final String resourceName;

    Allocation(Container container, String resourceName) {
      this.container = container;
      this.resourceName = resourceName;
    }

    Container getContainer() {
      return container;
    }

    String getResourceName() {
      return resourceName;
    }
  }

  static class EnrichedResourceRequest {
    private final Map<String, AtomicInteger> nodeLocations = new HashMap<>();
    private final Map<String, AtomicInteger> rackLocations = new HashMap<>();
    private final ResourceRequest request;

    EnrichedResourceRequest(ResourceRequest request) {
      this.request = request;
    }

    ResourceRequest getRequest() {
      return request;
    }

    void addLocation(String location, int count) {
      Map<String, AtomicInteger> m = rackLocations;
      if (!location.startsWith("/")) {
        m = nodeLocations;
      }
      if (count == 0) {
        m.remove(location);
      } else {
        m.put(location, new AtomicInteger(count));
      }
    }

    void removeLocation(String location) {
      Map<String, AtomicInteger> m = rackLocations;
      AtomicInteger count = m.get(location);
      if (count == null) {
        m = nodeLocations;
        count = m.get(location);
      }

      if (count != null) {
        if (count.decrementAndGet() == 0) {
          m.remove(location);
        }
      }
    }

    Set<String> getNodeLocations() {
      return nodeLocations.keySet();
    }

    Set<String> getRackLocations() {
      return rackLocations.keySet();
    }
  }
  /**
   * Create a new Opportunistic Container Allocator.
   * @param tokenSecretManager TokenSecretManager
   */
  public OpportunisticContainerAllocator(
      BaseContainerTokenSecretManager tokenSecretManager) {
    this.tokenSecretManager = tokenSecretManager;
  }

  /**
   * Allocate OPPORTUNISTIC containers.
   * @param blackList Resource BlackList Request
   * @param oppResourceReqs Opportunistic Resource Requests
   * @param applicationAttemptId ApplicationAttemptId
   * @param opportContext App specific OpportunisticContainerContext
   * @param rmIdentifier RM Identifier
   * @param appSubmitter App Submitter
   * @return List of Containers.
   * @throws YarnException YarnException
   */
  public List<Container> allocateContainers(ResourceBlacklistRequest blackList,
      List<ResourceRequest> oppResourceReqs,
      ApplicationAttemptId applicationAttemptId,
      OpportunisticContainerContext opportContext, long rmIdentifier,
      String appSubmitter) throws YarnException {

    // Update black list.
    if (blackList != null) {
      opportContext.getBlacklist().removeAll(blackList.getBlacklistRemovals());
      opportContext.getBlacklist().addAll(blackList.getBlacklistAdditions());
    }

    // Add OPPORTUNISTIC requests to the outstanding ones.
    opportContext.addToOutstandingReqs(oppResourceReqs);

    Set<String> nodeBlackList = new HashSet<>(opportContext.getBlacklist());
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
        Map<Resource, List<Allocation>> allocation = allocate(
            rmIdentifier, opportContext, schedulerKey, applicationAttemptId,
            appSubmitter, nodeBlackList);
        if (allocation.size() > 0) {
          allocations.add(allocation);
          continueLoop = true;
        }
      }
      for (Map<Resource, List<Allocation>> allocation : allocations) {
        for (Map.Entry<Resource, List<Allocation>> e : allocation.entrySet()) {
          opportContext.matchAllocationToOutstandingRequest(
              e.getKey(), e.getValue());
          for (Allocation alloc : e.getValue()) {
            allocatedContainers.add(alloc.getContainer());
          }
        }
      }
    }

    return allocatedContainers;
  }

  private Map<Resource, List<Allocation>> allocate(long rmIdentifier,
      OpportunisticContainerContext appContext, SchedulerRequestKey schedKey,
      ApplicationAttemptId appAttId, String userName, Set<String> blackList)
      throws YarnException {
    Map<Resource, List<Allocation>> containers = new HashMap<>();
    for (EnrichedResourceRequest enrichedAsk :
        appContext.getOutstandingOpReqs().get(schedKey).values()) {
      allocateContainersInternal(rmIdentifier, appContext.getAppParams(),
          appContext.getContainerIdGenerator(), blackList, appAttId,
          appContext.getNodeMap(), userName, containers, enrichedAsk);
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
      Set<String> blacklist, ApplicationAttemptId id,
      Map<String, RemoteNode> allNodes, String userName,
      Map<Resource, List<Allocation>> allocations,
      EnrichedResourceRequest enrichedAsk)
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
    int numAllocated = 0;
    // Node Candidates are selected as follows:
    // * Node local candidates selected in loop == 0
    // * Rack local candidates selected in loop == 1
    // * From loop == 2 onwards, we revert to off switch allocations.
    int loopIndex = OFF_SWITCH_LOOP;
    if (enrichedAsk.getNodeLocations().size() > 0) {
      loopIndex = NODE_LOCAL_LOOP;
    }
    while (numAllocated < toAllocate) {
      Collection<RemoteNode> nodeCandidates =
          findNodeCandidates(loopIndex, allNodes, blacklist, enrichedAsk);
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
          if (enrichedAsk.getNodeLocations().contains(rNodeHost)) {
            location = rNodeHost;
          } else {
            continue;
          }
        }
        if (loopIndex == RACK_LOCAL_LOOP) {
          if (enrichedAsk.getRackLocations().contains(rNode.getRackName())) {
            location = rNode.getRackName();
          } else {
            continue;
          }
        }
        Container container = createContainer(rmIdentifier, appParams,
            idCounter, id, userName, allocations, location,
            anyAsk, rNode);
        numAllocated++;
        // Try to spread the allocations across the nodes.
        // But don't add if it is a node local request.
        if (loopIndex != NODE_LOCAL_LOOP) {
          blacklist.add(rNode.getNodeId().getHost());
        }
        LOG.info("Allocated [" + container.getId() + "] as opportunistic at " +
            "location [" + location + "]");
        if (numAllocated >= toAllocate) {
          break;
        }
      }
      if (loopIndex == NODE_LOCAL_LOOP &&
          enrichedAsk.getRackLocations().size() > 0) {
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

  private Collection<RemoteNode> findNodeCandidates(int loopIndex,
      Map<String, RemoteNode> allNodes, Set<String> blackList,
      EnrichedResourceRequest enrichedRR) {
    if (loopIndex > 1) {
      return allNodes.values();
    } else {
      LinkedList<RemoteNode> retList = new LinkedList<>();
      int numContainers = enrichedRR.getRequest().getNumContainers();
      while (numContainers > 0) {
        if (loopIndex == 0) {
          // Node local candidates
          numContainers = collectNodeLocalCandidates(
              allNodes, enrichedRR, retList, numContainers);
        } else {
          // Rack local candidates
          numContainers = collectRackLocalCandidates(
              allNodes, enrichedRR, retList, blackList, numContainers);
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
      Set<String> blackList, int numContainers) {
    for (RemoteNode rNode : allNodes.values()) {
      if (enrichedRR.getRackLocations().contains(rNode.getRackName())) {
        if (blackList.contains(rNode.getNodeId().getHost())) {
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
    for (String nodeName : enrichedRR.getNodeLocations()) {
      RemoteNode remoteNode = allNodes.get(nodeName);
      if (remoteNode != null) {
        retList.add(remoteNode);
        numContainers--;
      }
      if (numContainers == 0) {
        break;
      }
    }
    return numContainers;
  }

  private Container createContainer(long rmIdentifier,
      AllocationParams appParams, ContainerIdGenerator idCounter,
      ApplicationAttemptId id, String userName,
      Map<Resource, List<Allocation>> allocations, String location,
      ResourceRequest anyAsk, RemoteNode rNode) throws YarnException {
    Container container = buildContainer(rmIdentifier, appParams,
        idCounter, anyAsk, id, userName, rNode);
    List<Allocation> allocList = allocations.get(anyAsk.getCapability());
    if (allocList == null) {
      allocList = new ArrayList<>();
      allocations.put(anyAsk.getCapability(), allocList);
    }
    allocList.add(new Allocation(container, location));
    return container;
  }

  private Container buildContainer(long rmIdentifier,
      AllocationParams appParams, ContainerIdGenerator idCounter,
      ResourceRequest rr, ApplicationAttemptId id, String userName,
      RemoteNode node) throws YarnException {
    ContainerId cId =
        ContainerId.newContainerId(id, idCounter.generateContainerId());

    // Normalize the resource asks (Similar to what the the RM scheduler does
    // before accepting an ask)
    Resource capability = normalizeCapability(appParams, rr);

    return createContainer(
        rmIdentifier, appParams.getContainerTokenExpiryInterval(),
        SchedulerRequestKey.create(rr), userName, node, cId, capability);
  }

  private Container createContainer(long rmIdentifier, long tokenExpiry,
      SchedulerRequestKey schedulerKey, String userName, RemoteNode node,
      ContainerId cId, Resource capability) {
    long currTime = System.currentTimeMillis();
    ContainerTokenIdentifier containerTokenIdentifier =
        new ContainerTokenIdentifier(
            cId, 0, node.getNodeId().toString(), userName,
            capability, currTime + tokenExpiry,
            tokenSecretManager.getCurrentKey().getKeyId(), rmIdentifier,
            schedulerKey.getPriority(), currTime,
            null, CommonNodeLabelsManager.NO_LABEL, ContainerType.TASK,
            ExecutionType.OPPORTUNISTIC);
    byte[] pwd =
        tokenSecretManager.createPassword(containerTokenIdentifier);
    Token containerToken = newContainerToken(node.getNodeId(), pwd,
        containerTokenIdentifier);
    Container container = BuilderUtils.newContainer(
        cId, node.getNodeId(), node.getHttpAddress(),
        capability, schedulerKey.getPriority(), containerToken,
        containerTokenIdentifier.getExecutionType(),
        schedulerKey.getAllocationRequestId());
    return container;
  }

  private Resource normalizeCapability(AllocationParams appParams,
      ResourceRequest ask) {
    return Resources.normalize(RESOURCE_CALCULATOR,
        ask.getCapability(), appParams.minResource, appParams.maxResource,
        appParams.incrementResource);
  }

  private static Token newContainerToken(NodeId nodeId, byte[] password,
      ContainerTokenIdentifier tokenIdentifier) {
    // RPC layer client expects ip:port as service for tokens
    InetSocketAddress addr = NetUtils.createSocketAddrForHost(nodeId.getHost(),
        nodeId.getPort());
    // NOTE: use SecurityUtil.setTokenService if this becomes a "real" token
    Token containerToken = Token.newInstance(tokenIdentifier.getBytes(),
        ContainerTokenIdentifier.KIND.toString(), password, SecurityUtil
            .buildTokenService(addr).toString());
    return containerToken;
  }

  /**
   * Partitions a list of ResourceRequest to two separate lists, one for
   * GUARANTEED and one for OPPORTUNISTIC ResourceRequests.
   * @param askList the list of ResourceRequests to be partitioned
   * @return the partitioned ResourceRequests
   */
  public PartitionedResourceRequests partitionAskList(
      List<ResourceRequest> askList) {
    PartitionedResourceRequests partitionedRequests =
        new PartitionedResourceRequests();
    for (ResourceRequest rr : askList) {
      if (rr.getExecutionTypeRequest().getExecutionType() ==
          ExecutionType.OPPORTUNISTIC) {
        partitionedRequests.getOpportunistic().add(rr);
      } else {
        partitionedRequests.getGuaranteed().add(rr);
      }
    }
    return partitionedRequests;
  }
}

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
import org.apache.hadoop.util.Time;
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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>
 * Base abstract class for Opportunistic container allocations, that provides
 * common functions required for Opportunistic container allocation.
 * </p>
 */
public abstract class OpportunisticContainerAllocator {

  private int maxAllocationsPerAMHeartbeat = -1;

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

  private static final ResourceCalculator RESOURCE_CALCULATOR =
      new DominantResourceCalculator();

  private final BaseContainerTokenSecretManager tokenSecretManager;

  /**
   * This class encapsulates container and resourceName for an allocation.
   */
  public static class Allocation {
    private final Container container;
    private final String resourceName;

    /**
     * Creates an instance of Allocation.
     * @param container allocated container.
     * @param resourceName location where it got allocated.
     */
    public Allocation(Container container, String resourceName) {
      this.container = container;
      this.resourceName = resourceName;
    }

    /**
     * Get container of the allocation.
     * @return container of the allocation.
     */
    public Container getContainer() {
      return container;
    }

    /**
     * Get resource name of the allocation.
     * @return resource name of the allocation.
     */
    public String getResourceName() {
      return resourceName;
    }
  }

  /**
   * This class encapsulates Resource Request and provides requests per
   * node and rack.
   */
  public static class EnrichedResourceRequest {
    private final Map<String, AtomicInteger> nodeLocations = new HashMap<>();
    private final Map<String, AtomicInteger> rackLocations = new HashMap<>();
    private final ResourceRequest request;
    private final long timestamp;

    public EnrichedResourceRequest(ResourceRequest request) {
      this.request = request;
      timestamp = Time.monotonicNow();
    }

    public long getTimestamp() {
      return timestamp;
    }

    public ResourceRequest getRequest() {
      return request;
    }

    public void addLocation(String location, int count) {
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

    public void removeLocation(String location) {
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

    public Map<String, AtomicInteger> getNodeMap() {
      return nodeLocations;
    }

    public Map<String, AtomicInteger> getRackMap() {
      return rackLocations;
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
   * Create a new Opportunistic Container Allocator.
   * @param tokenSecretManager TokenSecretManager
   * @param maxAllocationsPerAMHeartbeat max number of containers to be
   *                                     allocated in one AM heartbeat
   */
  public OpportunisticContainerAllocator(
      BaseContainerTokenSecretManager tokenSecretManager,
      int maxAllocationsPerAMHeartbeat) {
    this.tokenSecretManager = tokenSecretManager;
    this.maxAllocationsPerAMHeartbeat = maxAllocationsPerAMHeartbeat;
  }

  public void setMaxAllocationsPerAMHeartbeat(
      int maxAllocationsPerAMHeartbeat) {
    this.maxAllocationsPerAMHeartbeat = maxAllocationsPerAMHeartbeat;
  }

  /**
   * Get the Max Allocations per AM heartbeat.
   * @return maxAllocationsPerAMHeartbeat.
   */
  public int getMaxAllocationsPerAMHeartbeat() {
    return this.maxAllocationsPerAMHeartbeat;
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
  public abstract List<Container> allocateContainers(
      ResourceBlacklistRequest blackList,
      List<ResourceRequest> oppResourceReqs,
      ApplicationAttemptId applicationAttemptId,
      OpportunisticContainerContext opportContext, long rmIdentifier,
      String appSubmitter) throws YarnException;


  protected void updateBlacklist(ResourceBlacklistRequest blackList,
      OpportunisticContainerContext oppContext) {
    if (blackList != null) {
      oppContext.getBlacklist().removeAll(blackList.getBlacklistRemovals());
      oppContext.getBlacklist().addAll(blackList.getBlacklistAdditions());
    }
  }

  protected void matchAllocation(List<Map<Resource,
      List<Allocation>>> allocations, List<Container> allocatedContainers,
      OpportunisticContainerContext oppContext) {
    for (Map<Resource, List<Allocation>> allocation : allocations) {
      for (Map.Entry<Resource, List<Allocation>> e : allocation.entrySet()) {
        oppContext.matchAllocationToOutstandingRequest(
            e.getKey(), e.getValue());
        for (Allocation alloc : e.getValue()) {
          allocatedContainers.add(alloc.getContainer());
        }
      }
    }
  }

  protected int getTotalAllocations(
      List<Map<Resource, List<Allocation>>> allocations) {
    int totalAllocs = 0;
    for (Map<Resource, List<Allocation>> allocation : allocations) {
      for (List<Allocation> allocs : allocation.values()) {
        totalAllocs += allocs.size();
      }
    }
    return totalAllocs;
  }

  @SuppressWarnings("checkstyle:parameternumber")
  protected Container createContainer(long rmIdentifier,
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

  @SuppressWarnings("checkstyle:parameternumber")
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
            null, getRemoteNodePartition(node), ContainerType.TASK,
            ExecutionType.OPPORTUNISTIC, schedulerKey.getAllocationRequestId());
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

  protected String getRequestPartition(EnrichedResourceRequest enrichedRR) {
    String partition = enrichedRR.getRequest().getNodeLabelExpression();
    if (partition == null) {
      partition = CommonNodeLabelsManager.NO_LABEL;
    }
    return partition;
  }

  protected String getRemoteNodePartition(RemoteNode node) {
    String partition = node.getNodePartition();
    if (partition == null) {
      partition = CommonNodeLabelsManager.NO_LABEL;
    }
    return partition;
  }
}

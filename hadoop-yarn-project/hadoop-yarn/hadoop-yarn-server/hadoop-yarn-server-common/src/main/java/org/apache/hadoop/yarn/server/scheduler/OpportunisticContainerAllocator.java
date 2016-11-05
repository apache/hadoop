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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.records.*;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>
 * The OpportunisticContainerAllocator allocates containers on a given list of
 * nodes, after modifying the container sizes to respect the limits set by the
 * ResourceManager. It tries to distribute the containers as evenly as possible.
 * </p>
 */
public class OpportunisticContainerAllocator {

  /**
   * This class encapsulates application specific parameters used to build a
   * Container.
   */
  public static class AllocationParams {
    private Resource maxResource;
    private Resource minResource;
    private Resource incrementResource;
    private int containerTokenExpiryInterval;

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

  static class PartitionedResourceRequests {
    private List<ResourceRequest> guaranteed = new ArrayList<>();
    private List<ResourceRequest> opportunistic = new ArrayList<>();
    public List<ResourceRequest> getGuaranteed() {
      return guaranteed;
    }
    public List<ResourceRequest> getOpportunistic() {
      return opportunistic;
    }
  }

  private static final Log LOG =
      LogFactory.getLog(OpportunisticContainerAllocator.class);

  private static final ResourceCalculator RESOURCE_CALCULATOR =
      new DominantResourceCalculator();

  private final BaseContainerTokenSecretManager tokenSecretManager;

  /**
   * Create a new Opportunistic Container Allocator.
   * @param tokenSecretManager TokenSecretManager
   */
  public OpportunisticContainerAllocator(
      BaseContainerTokenSecretManager tokenSecretManager) {
    this.tokenSecretManager = tokenSecretManager;
  }

  /**
   * Entry point into the Opportunistic Container Allocator.
   * @param request AllocateRequest
   * @param applicationAttemptId ApplicationAttemptId
   * @param appContext App Specific OpportunisticContainerContext
   * @param rmIdentifier RM Identifier
   * @param appSubmitter App Submitter
   * @return List of Containers.
   * @throws YarnException YarnException
   */
  public List<Container> allocateContainers(
      AllocateRequest request, ApplicationAttemptId applicationAttemptId,
      OpportunisticContainerContext appContext, long rmIdentifier,
      String appSubmitter) throws YarnException {
    // Partition requests into GUARANTEED and OPPORTUNISTIC reqs
    PartitionedResourceRequests partitionedAsks =
        partitionAskList(request.getAskList());

    if (partitionedAsks.getOpportunistic().isEmpty()) {
      return Collections.emptyList();
    }

    List<ContainerId> releasedContainers = request.getReleaseList();
    int numReleasedContainers = releasedContainers.size();
    if (numReleasedContainers > 0) {
      LOG.info("AttemptID: " + applicationAttemptId + " released: "
          + numReleasedContainers);
      appContext.getContainersAllocated().removeAll(releasedContainers);
    }

    // Also, update black list
    ResourceBlacklistRequest rbr = request.getResourceBlacklistRequest();
    if (rbr != null) {
      appContext.getBlacklist().removeAll(rbr.getBlacklistRemovals());
      appContext.getBlacklist().addAll(rbr.getBlacklistAdditions());
    }

    // Add OPPORTUNISTIC reqs to the outstanding reqs
    appContext.addToOutstandingReqs(partitionedAsks.getOpportunistic());

    List<Container> allocatedContainers = new ArrayList<>();
    for (Priority priority :
        appContext.getOutstandingOpReqs().descendingKeySet()) {
      // Allocated containers :
      //  Key = Requested Capability,
      //  Value = List of Containers of given cap (the actual container size
      //          might be different than what is requested, which is why
      //          we need the requested capability (key) to match against
      //          the outstanding reqs)
      Map<Resource, List<Container>> allocated = allocate(rmIdentifier,
          appContext, priority, applicationAttemptId, appSubmitter);
      for (Map.Entry<Resource, List<Container>> e : allocated.entrySet()) {
        appContext.matchAllocationToOutstandingRequest(
            e.getKey(), e.getValue());
        allocatedContainers.addAll(e.getValue());
      }
    }

    // Send all the GUARANTEED Reqs to RM
    request.setAskList(partitionedAsks.getGuaranteed());
    return allocatedContainers;
  }

  private Map<Resource, List<Container>> allocate(long rmIdentifier,
      OpportunisticContainerContext appContext, Priority priority,
      ApplicationAttemptId appAttId, String userName) throws YarnException {
    Map<Resource, List<Container>> containers = new HashMap<>();
    for (ResourceRequest anyAsk :
        appContext.getOutstandingOpReqs().get(priority).values()) {
      allocateContainersInternal(rmIdentifier, appContext.getAppParams(),
          appContext.getContainerIdGenerator(), appContext.getBlacklist(),
          appAttId, appContext.getNodeMap(), userName, containers, anyAsk);
      LOG.info("Opportunistic allocation requested for ["
          + "priority=" + anyAsk.getPriority()
          + ", num_containers=" + anyAsk.getNumContainers()
          + ", capability=" + anyAsk.getCapability() + "]"
          + " allocated = " + containers.get(anyAsk.getCapability()).size());
    }
    return containers;
  }

  private void allocateContainersInternal(long rmIdentifier,
      AllocationParams appParams, ContainerIdGenerator idCounter,
      Set<String> blacklist, ApplicationAttemptId id,
      Map<String, RemoteNode> allNodes, String userName,
      Map<Resource, List<Container>> containers, ResourceRequest anyAsk)
      throws YarnException {
    int toAllocate = anyAsk.getNumContainers()
        - (containers.isEmpty() ? 0 :
            containers.get(anyAsk.getCapability()).size());

    List<RemoteNode> nodesForScheduling = new ArrayList<>();
    for (Entry<String, RemoteNode> nodeEntry : allNodes.entrySet()) {
      // Do not use blacklisted nodes for scheduling.
      if (blacklist.contains(nodeEntry.getKey())) {
        continue;
      }
      nodesForScheduling.add(nodeEntry.getValue());
    }
    if (nodesForScheduling.isEmpty()) {
      LOG.warn("No nodes available for allocating opportunistic containers.");
      return;
    }
    int numAllocated = 0;
    int nextNodeToSchedule = 0;
    for (int numCont = 0; numCont < toAllocate; numCont++) {
      nextNodeToSchedule++;
      nextNodeToSchedule %= nodesForScheduling.size();
      RemoteNode node = nodesForScheduling.get(nextNodeToSchedule);
      Container container = buildContainer(rmIdentifier, appParams, idCounter,
          anyAsk, id, userName, node);
      List<Container> cList = containers.get(anyAsk.getCapability());
      if (cList == null) {
        cList = new ArrayList<>();
        containers.put(anyAsk.getCapability(), cList);
      }
      cList.add(container);
      numAllocated++;
      LOG.info("Allocated [" + container.getId() + "] as opportunistic.");
    }
    LOG.info("Allocated " + numAllocated + " opportunistic containers.");
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

    long currTime = System.currentTimeMillis();
    ContainerTokenIdentifier containerTokenIdentifier =
        new ContainerTokenIdentifier(
            cId, 0, node.getNodeId().toString(), userName,
            capability, currTime + appParams.containerTokenExpiryInterval,
            tokenSecretManager.getCurrentKey().getKeyId(), rmIdentifier,
            rr.getPriority(), currTime,
            null, CommonNodeLabelsManager.NO_LABEL, ContainerType.TASK,
            ExecutionType.OPPORTUNISTIC);
    byte[] pwd =
        tokenSecretManager.createPassword(containerTokenIdentifier);
    Token containerToken = newContainerToken(node.getNodeId(), pwd,
        containerTokenIdentifier);
    Container container = BuilderUtils.newContainer(
        cId, node.getNodeId(), node.getHttpAddress(),
        capability, rr.getPriority(), containerToken,
        containerTokenIdentifier.getExecutionType(),
        rr.getAllocationRequestId());
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

  private PartitionedResourceRequests partitionAskList(List<ResourceRequest>
      askList) {
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

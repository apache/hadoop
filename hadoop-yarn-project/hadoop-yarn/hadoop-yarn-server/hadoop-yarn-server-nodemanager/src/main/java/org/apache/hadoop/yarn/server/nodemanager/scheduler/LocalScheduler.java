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

package org.apache.hadoop.yarn.server.nodemanager.scheduler;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistSchedAllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistSchedRegisterResponse;
import org.apache.hadoop.yarn.api.protocolrecords
    .FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords
    .FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords
    .RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords
    .RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.amrmproxy
    .AMRMProxyApplicationContext;
import org.apache.hadoop.yarn.server.nodemanager.amrmproxy.AbstractRequestInterceptor;



import org.apache.hadoop.yarn.server.nodemanager.security
    .NMTokenSecretManagerInNM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * <p>The LocalScheduler runs on the NodeManager and is modelled as an
 * <code>AMRMProxy</code> request interceptor. It is responsible for the
 * following :</p>
 * <ul>
 *   <li>Intercept <code>ApplicationMasterProtocol</code> calls and unwrap the
 *   response objects to extract instructions from the
 *   <code>ClusterManager</code> running on the ResourceManager to aid in making
 *   Scheduling scheduling decisions</li>
 *   <li>Call the <code>OpportunisticContainerAllocator</code> to allocate
 *   containers for the opportunistic resource outstandingOpReqs</li>
 * </ul>
 */
public final class LocalScheduler extends AbstractRequestInterceptor {

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

  static class DistSchedulerParams {
    Resource maxResource;
    Resource minResource;
    Resource incrementResource;
    int containerTokenExpiryInterval;
  }

  private static final Logger LOG = LoggerFactory
      .getLogger(LocalScheduler.class);

  // Currently just used to keep track of allocated Containers
  // Can be used for reporting stats later
  private Set<ContainerId> containersAllocated = new HashSet<>();

  private DistSchedulerParams appParams = new DistSchedulerParams();
  private final OpportunisticContainerAllocator.ContainerIdCounter containerIdCounter =
      new OpportunisticContainerAllocator.ContainerIdCounter();
  private Map<String, NodeId> nodeList = new HashMap<>();

  // Mapping of NodeId to NodeTokens. Populated either from RM response or
  // generated locally if required.
  private Map<NodeId, NMToken> nodeTokens = new HashMap<>();
  final Set<String> blacklist = new HashSet<>();

  // This maintains a map of outstanding OPPORTUNISTIC Reqs. Key-ed by Priority,
  // Resource Name (Host/rack/any) and capability. This mapping is required
  // to match a received Container to an outstanding OPPORTUNISTIC
  // ResourceRequests (ask)
  final TreeMap<Priority, Map<Resource, ResourceRequest>>
      outstandingOpReqs = new TreeMap<>();

  private ApplicationAttemptId applicationAttemptId;
  private OpportunisticContainerAllocator containerAllocator;
  private NMTokenSecretManagerInNM nmSecretManager;
  private String appSubmitter;

  public void init(AMRMProxyApplicationContext appContext) {
    super.init(appContext);
    initLocal(appContext.getApplicationAttemptId(),
        appContext.getNMCotext().getContainerAllocator(),
        appContext.getNMCotext().getNMTokenSecretManager(),
        appContext.getUser());
  }

  @VisibleForTesting
  void initLocal(ApplicationAttemptId applicationAttemptId,
      OpportunisticContainerAllocator containerAllocator,
      NMTokenSecretManagerInNM nmSecretManager, String appSubmitter) {
    this.applicationAttemptId = applicationAttemptId;
    this.containerAllocator = containerAllocator;
    this.nmSecretManager = nmSecretManager;
    this.appSubmitter = appSubmitter;
  }

  /**
   * Route register call to the corresponding distributed scheduling method viz.
   * registerApplicationMasterForDistributedScheduling, and return response to
   * the caller after stripping away Distributed Scheduling information.
   *
   * @param request
   *          registration request
   * @return Allocate Response
   * @throws YarnException
   * @throws IOException
   */
  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster
      (RegisterApplicationMasterRequest request) throws YarnException,
      IOException {
    return registerApplicationMasterForDistributedScheduling(request)
        .getRegisterResponse();
  }

  /**
   * Route allocate call to the allocateForDistributedScheduling method and
   * return response to the caller after stripping away Distributed Scheduling
   * information.
   *
   * @param request
   *          allocation request
   * @return Allocate Response
   * @throws YarnException
   * @throws IOException
   */
  @Override
  public AllocateResponse allocate(AllocateRequest request) throws
      YarnException, IOException {
    return allocateForDistributedScheduling(request).getAllocateResponse();
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster
      (FinishApplicationMasterRequest request) throws YarnException,
      IOException {
    return getNextInterceptor().finishApplicationMaster(request);
  }

  /**
   * Check if we already have a NMToken. if Not, generate the Token and
   * add it to the response
   * @param response
   * @param nmTokens
   * @param allocatedContainers
   */
  private void updateResponseWithNMTokens(AllocateResponse response,
      List<NMToken> nmTokens, List<Container> allocatedContainers) {
    List<NMToken> newTokens = new ArrayList<>();
    if (allocatedContainers.size() > 0) {
      response.getAllocatedContainers().addAll(allocatedContainers);
      for (Container alloc : allocatedContainers) {
        if (!nodeTokens.containsKey(alloc.getNodeId())) {
          newTokens.add(nmSecretManager.generateNMToken(appSubmitter, alloc));
        }
      }
      List<NMToken> retTokens = new ArrayList<>(nmTokens);
      retTokens.addAll(newTokens);
      response.setNMTokens(retTokens);
    }
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

  private void updateParameters(
      DistSchedRegisterResponse registerResponse) {
    appParams.minResource = registerResponse.getMinAllocatableCapabilty();
    appParams.maxResource = registerResponse.getMaxAllocatableCapabilty();
    appParams.incrementResource =
        registerResponse.getIncrAllocatableCapabilty();
    if (appParams.incrementResource == null) {
      appParams.incrementResource = appParams.minResource;
    }
    appParams.containerTokenExpiryInterval = registerResponse
        .getContainerTokenExpiryInterval();

    containerIdCounter
        .resetContainerIdCounter(registerResponse.getContainerIdStart());
    setNodeList(registerResponse.getNodesForScheduling());
  }

  /**
   * Takes a list of ResourceRequests (asks), extracts the key information viz.
   * (Priority, ResourceName, Capability) and adds it the outstanding
   * OPPORTUNISTIC outstandingOpReqs map. The nested map is required to enforce
   * the current YARN constraint that only a single ResourceRequest can exist at
   * a give Priority and Capability
   * @param resourceAsks
   */
  public void addToOutstandingReqs(List<ResourceRequest> resourceAsks) {
    for (ResourceRequest request : resourceAsks) {
      Priority priority = request.getPriority();

      // TODO: Extend for Node/Rack locality. We only handle ANY requests now
      if (!ResourceRequest.isAnyLocation(request.getResourceName())) {
        continue;
      }

      if (request.getNumContainers() == 0) {
        continue;
      }

      Map<Resource, ResourceRequest> reqMap =
          this.outstandingOpReqs.get(priority);
      if (reqMap == null) {
        reqMap = new HashMap<>();
        this.outstandingOpReqs.put(priority, reqMap);
      }

      ResourceRequest resourceRequest = reqMap.get(request.getCapability());
      if (resourceRequest == null) {
        resourceRequest = request;
        reqMap.put(request.getCapability(), request);
      } else {
        resourceRequest.setNumContainers(
            resourceRequest.getNumContainers() + request.getNumContainers());
      }
      if (ResourceRequest.isAnyLocation(request.getResourceName())) {
        LOG.info("# of outstandingOpReqs in ANY (at priority = " + priority
            + ", with capability = " + request.getCapability() + " ) : "
            + resourceRequest.getNumContainers());
      }
    }
  }

  /**
   * This method matches a returned list of Container Allocations to any
   * outstanding OPPORTUNISTIC ResourceRequest
   * @param capability
   * @param allocatedContainers
   */
  public void matchAllocationToOutstandingRequest(Resource capability,
      List<Container> allocatedContainers) {
    for (Container c : allocatedContainers) {
      containersAllocated.add(c.getId());
      Map<Resource, ResourceRequest> asks =
          outstandingOpReqs.get(c.getPriority());

      if (asks == null)
        continue;

      ResourceRequest rr = asks.get(capability);
      if (rr != null) {
        rr.setNumContainers(rr.getNumContainers() - 1);
        if (rr.getNumContainers() == 0) {
          asks.remove(capability);
        }
      }
    }
  }

  private void setNodeList(List<NodeId> nodeList) {
    this.nodeList.clear();
    addToNodeList(nodeList);
  }

  private void addToNodeList(List<NodeId> nodes) {
    for (NodeId n : nodes) {
      this.nodeList.put(n.getHost(), n);
    }
  }

  @Override
  public DistSchedRegisterResponse
  registerApplicationMasterForDistributedScheduling
      (RegisterApplicationMasterRequest request) throws YarnException,
      IOException {
    LOG.info("Forwarding registration request to the" +
        "Distributed Scheduler Service on YARN RM");
    DistSchedRegisterResponse dsResp = getNextInterceptor()
        .registerApplicationMasterForDistributedScheduling(request);
    updateParameters(dsResp);
    return dsResp;
  }

  @Override
  public DistSchedAllocateResponse allocateForDistributedScheduling
      (AllocateRequest request) throws YarnException, IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Forwarding allocate request to the" +
          "Distributed Scheduler Service on YARN RM");
    }
    // Partition requests into GUARANTEED and OPPORTUNISTIC reqs
    PartitionedResourceRequests partitionedAsks = partitionAskList(request
        .getAskList());

    List<ContainerId> releasedContainers = request.getReleaseList();
    int numReleasedContainers = releasedContainers.size();
    if (numReleasedContainers > 0) {
      LOG.info("AttemptID: " + applicationAttemptId + " released: "
          + numReleasedContainers);
      containersAllocated.removeAll(releasedContainers);
    }

    // Also, update black list
    ResourceBlacklistRequest rbr = request.getResourceBlacklistRequest();
    if (rbr != null) {
      blacklist.removeAll(rbr.getBlacklistRemovals());
      blacklist.addAll(rbr.getBlacklistAdditions());
    }

    // Add OPPORTUNISTIC reqs to the outstanding reqs
    addToOutstandingReqs(partitionedAsks.getOpportunistic());

    List<Container> allocatedContainers = new ArrayList<>();
    for (Priority priority : outstandingOpReqs.descendingKeySet()) {
      // Allocated containers :
      //  Key = Requested Capability,
      //  Value = List of Containers of given Cap (The actual container size
      //          might be different than what is requested.. which is why
      //          we need the requested capability (key) to match against
      //          the outstanding reqs)
      Map<Resource, List<Container>> allocated =
          containerAllocator.allocate(this.appParams, containerIdCounter,
              outstandingOpReqs.get(priority).values(), blacklist,
              applicationAttemptId, nodeList, appSubmitter);
      for (Map.Entry<Resource, List<Container>> e : allocated.entrySet()) {
        matchAllocationToOutstandingRequest(e.getKey(), e.getValue());
        allocatedContainers.addAll(e.getValue());
      }
    }

    // Send all the GUARANTEED Reqs to RM
    request.setAskList(partitionedAsks.getGuaranteed());
    DistSchedAllocateResponse dsResp =
        getNextInterceptor().allocateForDistributedScheduling(request);

    // Update host to nodeId mapping
    setNodeList(dsResp.getNodesForScheduling());
    List<NMToken> nmTokens = dsResp.getAllocateResponse().getNMTokens();
    for (NMToken nmToken : nmTokens) {
      nodeTokens.put(nmToken.getNodeId(), nmToken);
    }

    List<ContainerStatus> completedContainers =
        dsResp.getAllocateResponse().getCompletedContainersStatuses();

    // Only account for opportunistic containers
    for (ContainerStatus cs : completedContainers) {
      if (cs.getExecutionType() == ExecutionType.OPPORTUNISTIC) {
        containersAllocated.remove(cs.getContainerId());
      }
    }

    // Check if we have NM tokens for all the allocated containers. If not
    // generate one and update the response.
    updateResponseWithNMTokens(
        dsResp.getAllocateResponse(), nmTokens, allocatedContainers);

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Number of opportunistic containers currently allocated by" +
              "application: " + containersAllocated.size());
    }
    return dsResp;
  }
}

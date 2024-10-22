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

package org.apache.hadoop.yarn.client.api.impl;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.AbstractMap.SimpleEntry;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.client.AMRMClientUtils;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.InvalidContainerRequestException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.RackResolver;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Private
@Unstable
public class AMRMClientImpl<T extends ContainerRequest> extends AMRMClient<T> {

  private static final Logger LOG =
          LoggerFactory.getLogger(AMRMClientImpl.class);
  private static final List<String> ANY_LIST =
      Collections.singletonList(ResourceRequest.ANY);
  
  private int lastResponseId = 0;

  protected String appHostName;
  protected int appHostPort;
  protected String appTrackingUrl;
  protected String newTrackingUrl;

  protected ApplicationMasterProtocol rmClient;
  protected Resource clusterAvailableResources;
  protected int clusterNodeCount;
  
  // blacklistedNodes is required for keeping history of blacklisted nodes that
  // are sent to RM. On RESYNC command from RM, blacklistedNodes are used to get
  // current blacklisted nodes and send back to RM.
  protected final Set<String> blacklistedNodes = new HashSet<String>();
  protected final Set<String> blacklistAdditions = new HashSet<String>();
  protected final Set<String> blacklistRemovals = new HashSet<String>();
  private Map<Set<String>, PlacementConstraint> placementConstraints =
      new HashMap<>();

  protected Map<String, Resource> resourceProfilesMap;

  static class ResourceRequestInfo<T> {
    ResourceRequest remoteRequest;
    LinkedHashSet<T> containerRequests;

    ResourceRequestInfo(Long allocationRequestId, Priority priority,
        String resourceName, Resource capability, boolean relaxLocality) {
      remoteRequest = ResourceRequest.newBuilder().priority(priority)
          .resourceName(resourceName).capability(capability).numContainers(0)
          .allocationRequestId(allocationRequestId).relaxLocality(relaxLocality)
          .build();
      containerRequests = new LinkedHashSet<T>();
    }
  }

  /**
   * Class compares Resource by memory, then cpu and then the remaining resource
   * types in reverse order.
   */
  static class ResourceReverseComparator<T extends Resource>
      implements Comparator<T>, Serializable {
    public int compare(Resource res0, Resource res1) {
      return res1.compareTo(res0);
    }
  }

  private final Map<Long, RemoteRequestsTable<T>> remoteRequests =
      new HashMap<>();

  protected final Set<ResourceRequest> ask = new TreeSet<ResourceRequest>(
      new org.apache.hadoop.yarn.api.records.ResourceRequest.ResourceRequestComparator());
  protected final Set<ContainerId> release = new TreeSet<ContainerId>();
  // pendingRelease holds history of release requests.
  // request is removed only if RM sends completedContainer.
  // How it different from release? --> release is for per allocate() request.
  protected Set<ContainerId> pendingRelease = new TreeSet<ContainerId>();
  // change map holds container resource change requests between two allocate()
  // calls, and are cleared after each successful allocate() call.
  protected final Map<ContainerId,
      SimpleEntry<Container, UpdateContainerRequest>> change = new HashMap<>();
  // pendingChange map holds history of container resource change requests in
  // case AM needs to reregister with the ResourceManager.
  // Change requests are removed from this map if RM confirms the change
  // through allocate response, or if RM confirms that the container has been
  // completed.
  protected final Map<ContainerId,
      SimpleEntry<Container, UpdateContainerRequest>> pendingChange =
      new HashMap<>();

  private List<SchedulingRequest> schedulingRequests = new ArrayList<>();
  private Map<Set<String>, List<SchedulingRequest>> outstandingSchedRequests =
      new HashMap<>();

  public AMRMClientImpl() {
    super(AMRMClientImpl.class.getName());
  }

  @VisibleForTesting
  AMRMClientImpl(ApplicationMasterProtocol protocol) {
    super(AMRMClientImpl.class.getName());
    this.rmClient = protocol;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    RackResolver.init(conf);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    final YarnConfiguration conf = new YarnConfiguration(getConfig());
    try {
      if (rmClient == null) {
        rmClient = ClientRMProxy.createRMProxy(
            conf, ApplicationMasterProtocol.class);
      }
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.rmClient != null) {
      RPC.stopProxy(this.rmClient);
    }
    super.serviceStop();
  }

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      String appHostName, int appHostPort, String appTrackingUrl)
      throws YarnException, IOException {
    return registerApplicationMaster(appHostName, appHostPort, appTrackingUrl,
        null);
  }

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      String appHostName, int appHostPort, String appTrackingUrl,
      Map<Set<String>, PlacementConstraint> placementConstraintsMap)
      throws YarnException, IOException {
    this.appHostName = appHostName;
    this.appHostPort = appHostPort;
    this.appTrackingUrl = appTrackingUrl;
    if (placementConstraintsMap != null && !placementConstraintsMap.isEmpty()) {
      this.placementConstraints.putAll(placementConstraintsMap);
    }
    Preconditions.checkArgument(appHostName != null,
        "The host name should not be null");
    Preconditions.checkArgument(appHostPort >= -1, "Port number of the host"
        + " should be any integers larger than or equal to -1");

    return registerApplicationMaster();
  }

  @SuppressWarnings("unchecked")
  private RegisterApplicationMasterResponse registerApplicationMaster()
      throws YarnException, IOException {
    RegisterApplicationMasterRequest request =
        RegisterApplicationMasterRequest.newInstance(this.appHostName,
            this.appHostPort, this.appTrackingUrl);
    if (!this.placementConstraints.isEmpty()) {
      request.setPlacementConstraints(this.placementConstraints);
    }
    RegisterApplicationMasterResponse response =
        rmClient.registerApplicationMaster(request);
    synchronized (this) {
      lastResponseId = 0;
      if (!response.getNMTokensFromPreviousAttempts().isEmpty()) {
        populateNMTokens(response.getNMTokensFromPreviousAttempts());
      }
      this.resourceProfilesMap = response.getResourceProfiles();
      List<Container> prevContainers =
          response.getContainersFromPreviousAttempts();
      AMRMClientUtils.removeFromOutstandingSchedulingRequests(prevContainers,
          this.outstandingSchedRequests);
    }
    return response;
  }

  @Override
  public synchronized void addSchedulingRequests(
      Collection<SchedulingRequest> newSchedulingRequests) {
    this.schedulingRequests.addAll(newSchedulingRequests);
    AMRMClientUtils.addToOutstandingSchedulingRequests(newSchedulingRequests,
        this.outstandingSchedRequests);
  }

  @Override
  public AllocateResponse allocate(float progressIndicator) 
      throws YarnException, IOException {
    Preconditions.checkArgument(progressIndicator >= 0,
        "Progress indicator should not be negative");
    AllocateResponse allocateResponse = null;
    List<ResourceRequest> askList = null;
    List<ContainerId> releaseList = null;
    AllocateRequest allocateRequest = null;
    List<String> blacklistToAdd = new ArrayList<String>();
    List<String> blacklistToRemove = new ArrayList<String>();
    Map<ContainerId, SimpleEntry<Container, UpdateContainerRequest>> oldChange =
        new HashMap<>();
    List<SchedulingRequest> schedulingRequestList = new LinkedList<>();

    try {
      synchronized (this) {
        askList = cloneAsks();
        // Save the current change for recovery
        oldChange.putAll(change);
        List<UpdateContainerRequest> updateList = createUpdateList();
        releaseList = new ArrayList<ContainerId>(release);
        schedulingRequestList = new ArrayList<>(schedulingRequests);

        // optimistically clear this collection assuming no RPC failure
        ask.clear();
        release.clear();
        change.clear();
        schedulingRequests.clear();

        blacklistToAdd.addAll(blacklistAdditions);
        blacklistToRemove.addAll(blacklistRemovals);
        
        ResourceBlacklistRequest blacklistRequest =
            ResourceBlacklistRequest.newInstance(blacklistToAdd,
                blacklistToRemove);

        allocateRequest = AllocateRequest.newBuilder()
            .responseId(lastResponseId).progress(progressIndicator)
            .askList(askList).resourceBlacklistRequest(blacklistRequest)
            .releaseList(releaseList).updateRequests(updateList)
            .schedulingRequests(schedulingRequestList).build();

        if (this.newTrackingUrl != null) {
          allocateRequest.setTrackingUrl(this.newTrackingUrl);
          this.appTrackingUrl = this.newTrackingUrl;
          this.newTrackingUrl = null;
        }
        // clear blacklistAdditions and blacklistRemovals before
        // unsynchronized part
        blacklistAdditions.clear();
        blacklistRemovals.clear();
      }

      try {
        allocateResponse = rmClient.allocate(allocateRequest);
      } catch (ApplicationMasterNotRegisteredException e) {
        LOG.warn("ApplicationMaster is out of sync with ResourceManager,"
            + " hence resyncing.");
        synchronized (this) {
          release.addAll(this.pendingRelease);
          blacklistAdditions.addAll(this.blacklistedNodes);
          for (RemoteRequestsTable remoteRequestsTable :
              remoteRequests.values()) {
            @SuppressWarnings("unchecked")
            Iterator<ResourceRequestInfo<T>> reqIter =
                remoteRequestsTable.iterator();
            while (reqIter.hasNext()) {
              addResourceRequestToAsk(reqIter.next().remoteRequest);
            }
          }
          change.putAll(this.pendingChange);
          for (List<SchedulingRequest> schedReqs :
              this.outstandingSchedRequests.values()) {
            this.schedulingRequests.addAll(schedReqs);
          }
        }
        // re register with RM
        registerApplicationMaster();
        allocateResponse = allocate(progressIndicator);
        return allocateResponse;
      }

      synchronized (this) {
        // update these on successful RPC
        clusterNodeCount = allocateResponse.getNumClusterNodes();
        lastResponseId = allocateResponse.getResponseId();
        clusterAvailableResources = allocateResponse.getAvailableResources();
        if (!allocateResponse.getNMTokens().isEmpty()) {
          populateNMTokens(allocateResponse.getNMTokens());
        }
        if (allocateResponse.getAMRMToken() != null) {
          updateAMRMToken(allocateResponse.getAMRMToken());
        }
        if (!pendingRelease.isEmpty()
            && !allocateResponse.getCompletedContainersStatuses().isEmpty()) {
          removePendingReleaseRequests(allocateResponse
              .getCompletedContainersStatuses());
        }
        if (!pendingChange.isEmpty()) {
          List<ContainerStatus> completed =
              allocateResponse.getCompletedContainersStatuses();
          List<UpdatedContainer> changed = new ArrayList<>();
          changed.addAll(allocateResponse.getUpdatedContainers());
          // remove all pending change requests that belong to the completed
          // containers
          for (ContainerStatus status : completed) {
            ContainerId containerId = status.getContainerId();
            pendingChange.remove(containerId);
          }
          // remove all pending change requests that have been satisfied
          if (!changed.isEmpty()) {
            removePendingChangeRequests(changed);
          }
        }
        AMRMClientUtils.removeFromOutstandingSchedulingRequests(
            allocateResponse.getAllocatedContainers(),
            this.outstandingSchedRequests);
        AMRMClientUtils.removeFromOutstandingSchedulingRequests(
            allocateResponse.getContainersFromPreviousAttempts(),
            this.outstandingSchedRequests);
      }
    } finally {
      // TODO how to differentiate remote yarn exception vs error in rpc
      if(allocateResponse == null) {
        // we hit an exception in allocate()
        // preserve ask and release for next call to allocate()
        synchronized (this) {
          release.addAll(releaseList);
          // requests could have been added or deleted during call to allocate
          // If requests were added/removed then there is nothing to do since
          // the ResourceRequest object in ask would have the actual new value.
          // If ask does not have this ResourceRequest then it was unchanged and
          // so we can add the value back safely.
          // This assumes that there will no concurrent calls to allocate() and
          // so we dont have to worry about ask being changed in the
          // synchronized block at the beginning of this method.
          for(ResourceRequest oldAsk : askList) {
            if(!ask.contains(oldAsk)) {
              ask.add(oldAsk);
            }
          }
          // change requests could have been added during the allocate call.
          // Those are the newest requests which take precedence
          // over requests cached in the oldChange map.
          //
          // Only insert entries from the cached oldChange map
          // that do not exist in the current change map:
          for (Map.Entry<ContainerId,
              SimpleEntry<Container, UpdateContainerRequest>> entry :
              oldChange.entrySet()) {
            ContainerId oldContainerId = entry.getKey();
            Container oldContainer = entry.getValue().getKey();
            UpdateContainerRequest oldupdate = entry.getValue().getValue();
            if (change.get(oldContainerId) == null) {
              change.put(
                  oldContainerId, new SimpleEntry<>(oldContainer, oldupdate));
            }
          }
          blacklistAdditions.addAll(blacklistToAdd);
          blacklistRemovals.addAll(blacklistToRemove);

          schedulingRequests.addAll(schedulingRequestList);
        }
      }
    }
    return allocateResponse;
  }

  private List<UpdateContainerRequest> createUpdateList() {
    List<UpdateContainerRequest> updateList = new ArrayList<>();
    for (Map.Entry<ContainerId, SimpleEntry<Container,
        UpdateContainerRequest>> entry : change.entrySet()) {
      Resource targetCapability = entry.getValue().getValue().getCapability();
      ExecutionType targetExecType =
          entry.getValue().getValue().getExecutionType();
      ContainerUpdateType updateType =
          entry.getValue().getValue().getContainerUpdateType();
      int version = entry.getValue().getKey().getVersion();
      updateList.add(
          UpdateContainerRequest.newInstance(version, entry.getKey(),
              updateType, targetCapability, targetExecType));
    }
    return updateList;
  }

  private List<ResourceRequest> cloneAsks() {
    List<ResourceRequest> askList = new ArrayList<ResourceRequest>(ask.size());
    for(ResourceRequest r : ask) {
      // create a copy of ResourceRequest as we might change it while the
      // RPC layer is using it to send info across
      askList.add(ResourceRequest.clone(r));
    }
    return askList;
  }

  protected void removePendingReleaseRequests(
      List<ContainerStatus> completedContainersStatuses) {
    for (ContainerStatus containerStatus : completedContainersStatuses) {
      pendingRelease.remove(containerStatus.getContainerId());
    }
  }

  protected void removePendingChangeRequests(
      List<UpdatedContainer> changedContainers) {
    for (UpdatedContainer changedContainer : changedContainers) {
      ContainerId containerId = changedContainer.getContainer().getId();
      if (pendingChange.get(containerId) == null) {
        continue;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("RM has confirmed changed resource allocation for container {}. " +
            "Current resource allocation:{}. " +
            "Remove pending change request:{}",
            containerId,
            changedContainer.getContainer().getResource(),
            pendingChange.get(containerId).getValue());
      }
      pendingChange.remove(containerId);
    }
  }

  @Private
  @VisibleForTesting
  protected void populateNMTokens(List<NMToken> nmTokens) {
    for (NMToken token : nmTokens) {
      String nodeId = token.getNodeId().toString();
      if (LOG.isDebugEnabled()) {
        if (getNMTokenCache().containsToken(nodeId)) {
          LOG.debug("Replacing token for : {}.", nodeId);
        } else {
          LOG.debug("Received new token for : {}.", nodeId);
        }
      }
      getNMTokenCache().setToken(nodeId, token.getToken());
    }
  }

  @Override
  public void unregisterApplicationMaster(FinalApplicationStatus appStatus,
      String appMessage, String appTrackingUrl) throws YarnException,
      IOException {
    Preconditions.checkArgument(appStatus != null,
      "AppStatus should not be null.");
    FinishApplicationMasterRequest request =
        FinishApplicationMasterRequest.newInstance(appStatus, appMessage,
          appTrackingUrl);
    try {
      while (true) {
        FinishApplicationMasterResponse response =
            rmClient.finishApplicationMaster(request);
        if (response.getIsUnregistered()) {
          break;
        }
        LOG.info("Waiting for application to be successfully unregistered.");
        Thread.sleep(100);
      }
    } catch (InterruptedException e) {
      LOG.info("Interrupted while waiting for application"
          + " to be removed from RMStateStore");
    } catch (ApplicationMasterNotRegisteredException e) {
      LOG.warn("ApplicationMaster is out of sync with ResourceManager,"
          + " hence resyncing.");
      // re register with RM
      registerApplicationMaster();
      unregisterApplicationMaster(appStatus, appMessage, appTrackingUrl);
    }
  }
  
  @Override
  public synchronized void addContainerRequest(T req) {
    Preconditions.checkArgument(req != null,
        "Resource request can not be null.");
    Set<String> dedupedRacks = new HashSet<String>();
    if (req.getRacks() != null) {
      dedupedRacks.addAll(req.getRacks());
      if(req.getRacks().size() != dedupedRacks.size()) {
        Joiner joiner = Joiner.on(',');
        LOG.warn("ContainerRequest has duplicate racks: {}.", joiner.join(req.getRacks()));
      }
    }
    Set<String> inferredRacks = resolveRacks(req.getNodes());
    inferredRacks.removeAll(dedupedRacks);

    Resource resource = checkAndGetResourceProfile(req.getResourceProfile(),
        req.getCapability());

    // check that specific and non-specific requests cannot be mixed within a
    // priority
    checkLocalityRelaxationConflict(req.getAllocationRequestId(),
        req.getPriority(), ANY_LIST, req.getRelaxLocality());
    // check that specific rack cannot be mixed with specific node within a 
    // priority. If node and its rack are both specified then they must be 
    // in the same request.
    // For explicitly requested racks, we set locality relaxation to true
    checkLocalityRelaxationConflict(req.getAllocationRequestId(),
        req.getPriority(), dedupedRacks, true);
    checkLocalityRelaxationConflict(req.getAllocationRequestId(),
        req.getPriority(), inferredRacks, req.getRelaxLocality());
    // check if the node label expression specified is valid
    checkNodeLabelExpression(req);

    if (req.getNodes() != null) {
      HashSet<String> dedupedNodes = new HashSet<String>(req.getNodes());
      if(dedupedNodes.size() != req.getNodes().size()) {
        Joiner joiner = Joiner.on(',');
        LOG.warn("ContainerRequest has duplicate nodes: {}.", joiner.join(req.getNodes()));
      }
      for (String node : dedupedNodes) {
        addResourceRequest(req.getPriority(), node,
            req.getExecutionTypeRequest(), resource, req, true,
            req.getNodeLabelExpression());
      }
    }

    for (String rack : dedupedRacks) {
      addResourceRequest(req.getPriority(), rack, req.getExecutionTypeRequest(),
          resource, req, true, req.getNodeLabelExpression());
    }

    // Ensure node requests are accompanied by requests for
    // corresponding rack
    for (String rack : inferredRacks) {
      addResourceRequest(req.getPriority(), rack, req.getExecutionTypeRequest(),
          resource, req, req.getRelaxLocality(),
          req.getNodeLabelExpression());
    }
    // Off-switch
    addResourceRequest(req.getPriority(), ResourceRequest.ANY,
        req.getExecutionTypeRequest(), resource, req,
        req.getRelaxLocality(), req.getNodeLabelExpression());
  }

  @Override
  public synchronized void removeContainerRequest(T req) {
    Preconditions.checkArgument(req != null,
        "Resource request can not be null.");
    Resource resource = checkAndGetResourceProfile(req.getResourceProfile(),
        req.getCapability());
    Set<String> allRacks = new HashSet<String>();
    if (req.getRacks() != null) {
      allRacks.addAll(req.getRacks());
    }
    allRacks.addAll(resolveRacks(req.getNodes()));

    // Update resource requests
    if (req.getNodes() != null) {
      for (String node : new HashSet<String>(req.getNodes())) {
        decResourceRequest(req.getPriority(), node,
            req.getExecutionTypeRequest(), resource, req);
      }
    }

    for (String rack : allRacks) {
      decResourceRequest(req.getPriority(), rack,
          req.getExecutionTypeRequest(), resource, req);
    }

    decResourceRequest(req.getPriority(), ResourceRequest.ANY,
        req.getExecutionTypeRequest(), resource, req);
  }

  @Override
  public synchronized void requestContainerUpdate(
      Container container, UpdateContainerRequest updateContainerRequest) {
    Preconditions.checkNotNull(container, "Container cannot be null!!");
    Preconditions.checkNotNull(updateContainerRequest,
        "UpdateContainerRequest cannot be null!!");
    LOG.info("Requesting Container update : container={}, updateType={}," +
        " targetCapability={}, targetExecType={}",
        container,
        updateContainerRequest.getContainerUpdateType(),
        updateContainerRequest.getCapability(),
        updateContainerRequest.getExecutionType());
    if (updateContainerRequest.getCapability() != null &&
        updateContainerRequest.getExecutionType() == null) {
      validateContainerResourceChangeRequest(
          updateContainerRequest.getContainerUpdateType(),
          container.getId(), container.getResource(),
          updateContainerRequest.getCapability());
    } else if (updateContainerRequest.getExecutionType() != null &&
        updateContainerRequest.getCapability() == null) {
      validateContainerExecTypeChangeRequest(
          updateContainerRequest.getContainerUpdateType(),
          container.getId(), container.getExecutionType(),
          updateContainerRequest.getExecutionType());
    } else if (updateContainerRequest.getExecutionType() == null &&
        updateContainerRequest.getCapability() == null) {
      throw new IllegalArgumentException("Both target Capability and" +
          "target Execution Type are null");
    } else {
      throw new IllegalArgumentException("Support currently exists only for" +
          " EITHER update of Capability OR update of Execution Type NOT both");
    }
    if (change.get(container.getId()) == null) {
      change.put(container.getId(),
          new SimpleEntry<>(container, updateContainerRequest));
    } else {
      change.get(container.getId()).setValue(updateContainerRequest);
    }
    if (pendingChange.get(container.getId()) == null) {
      pendingChange.put(container.getId(),
          new SimpleEntry<>(container, updateContainerRequest));
    } else {
      pendingChange.get(container.getId()).setValue(updateContainerRequest);
    }
  }

  @Override
  public synchronized void releaseAssignedContainer(ContainerId containerId) {
    Preconditions.checkArgument(containerId != null,
        "ContainerId can not be null.");
    pendingRelease.add(containerId);
    release.add(containerId);
    pendingChange.remove(containerId);
  }
  
  @Override
  public synchronized Resource getAvailableResources() {
    return clusterAvailableResources;
  }
  
  @Override
  public synchronized int getClusterNodeCount() {
    return clusterNodeCount;
  }


  @Override
  @SuppressWarnings("unchecked")
  public Collection<T> getMatchingRequests(long allocationRequestId) {
    RemoteRequestsTable remoteRequestsTable = getTable(allocationRequestId);
    LinkedHashSet<T> list = new LinkedHashSet<>();

    if (remoteRequestsTable != null) {
      Iterator<ResourceRequestInfo<T>> reqIter =
          remoteRequestsTable.iterator();
      while (reqIter.hasNext()) {
        ResourceRequestInfo<T> resReqInfo = reqIter.next();
        list.addAll(resReqInfo.containerRequests);
      }
    }
    return list;
  }

  @Override
  public synchronized List<? extends Collection<T>> getMatchingRequests(
      Priority priority,
      String resourceName,
      Resource capability) {
    return getMatchingRequests(priority, resourceName,
        ExecutionType.GUARANTEED, capability);
  }

  @Override
  public List<? extends Collection<T>> getMatchingRequests(Priority priority,
      String resourceName, ExecutionType executionType,
      Resource capability, String profile) {
    capability = checkAndGetResourceProfile(profile, capability);
    return getMatchingRequests(priority, resourceName, executionType, capability);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized List<? extends Collection<T>> getMatchingRequests(
      Priority priority, String resourceName, ExecutionType executionType,
      Resource capability) {
    Preconditions.checkArgument(capability != null,
        "The Resource to be requested should not be null ");
    Preconditions.checkArgument(priority != null,
        "The priority at which to request containers should not be null ");
    List<LinkedHashSet<T>> list = new LinkedList<>();

    RemoteRequestsTable remoteRequestsTable = getTable(0);

    if (null != remoteRequestsTable) {
      List<ResourceRequestInfo<T>> matchingRequests = remoteRequestsTable
          .getMatchingRequests(priority, resourceName, executionType,
              capability);
      if (null != matchingRequests) {
        // If no exact match. Container may be larger than what was requested.
        // get all resources <= capability. map is reverse sorted.
        for (ResourceRequestInfo<T> resReqInfo : matchingRequests) {
          if (Resources.fitsIn(resReqInfo.remoteRequest.getCapability(),
              capability) && !resReqInfo.containerRequests.isEmpty()) {
            list.add(resReqInfo.containerRequests);
          }
        }
      }
    }
    // no match found
    return list;
  }
  
  private Set<String> resolveRacks(List<String> nodes) {
    Set<String> racks = new HashSet<String>();    
    if (nodes != null) {
      List<Node> tmpList = RackResolver.resolve(nodes);
      for (Node node : tmpList) {
        String rack = node.getNetworkLocation();
        // Ensure node requests are accompanied by requests for
        // corresponding rack
        if (rack == null) {
          LOG.warn("Failed to resolve rack for node {}.", node);
        } else {
          racks.add(rack);
        }
      }
    }
    return racks;
  }

  /**
   * ContainerRequests with locality relaxation cannot be made at the same
   * priority as ContainerRequests without locality relaxation.
   */
  private void checkLocalityRelaxationConflict(Long allocationReqId,
      Priority priority, Collection<String> locations, boolean relaxLocality) {
    // Locality relaxation will be set to relaxLocality for all implicitly
    // requested racks. Make sure that existing rack requests match this.

    RemoteRequestsTable<T> remoteRequestsTable = getTable(allocationReqId);
    if (remoteRequestsTable != null) {
      @SuppressWarnings("unchecked")
      List<ResourceRequestInfo> allCapabilityMaps =
          remoteRequestsTable.getAllResourceRequestInfos(priority, locations);
      for (ResourceRequestInfo reqs : allCapabilityMaps) {
        ResourceRequest remoteRequest = reqs.remoteRequest;
        boolean existingRelaxLocality = remoteRequest.getRelaxLocality();
        if (relaxLocality != existingRelaxLocality) {
          throw new InvalidContainerRequestException("Cannot submit a "
              + "ContainerRequest asking for location "
              + remoteRequest.getResourceName() + " with locality relaxation "
              + relaxLocality + " when it has already been requested"
              + "with locality relaxation " + existingRelaxLocality);
        }
      }
    }
  }

  // When profile and override resource are specified at the same time, override
  // predefined resource value in profile if any resource type has a positive
  // value.
  private Resource checkAndGetResourceProfile(String profile,
      Resource overrideResource) {
    Resource returnResource = overrideResource;

    // if application requested a non-empty/null profile, and the
    if (profile != null && !profile.isEmpty()) {
      if (resourceProfilesMap == null || (!resourceProfilesMap.containsKey(
          profile))) {
        throw new InvalidContainerRequestException(
            "Invalid profile name specified=" + profile + (
                resourceProfilesMap == null ?
                    "" :
                    (", valid profile names are " + resourceProfilesMap
                        .keySet())));
      }
      returnResource = Resources.clone(resourceProfilesMap.get(profile));
      for (ResourceInformation info : overrideResource
          .getAllResourcesListCopy()) {
        if (info.getValue() > 0) {
          returnResource.setResourceInformation(info.getName(), info);
        }
      }
    }

    return returnResource;
  }
  
  /**
   * Valid if a node label expression specified on container request is valid or
   * not
   * 
   * @param containerRequest
   */
  private void checkNodeLabelExpression(T containerRequest) {
    String exp = containerRequest.getNodeLabelExpression();
    
    if (null == exp || exp.isEmpty()) {
      return;
    }

    // Don't support specifying > 1 node labels in a node label expression now
    if (exp.contains("&&") || exp.contains("||")) {
      throw new InvalidContainerRequestException(
          "Cannot specify more than one node label"
              + " in a single node label expression");
    }
  }

  private void validateContainerResourceChangeRequest(
      ContainerUpdateType updateType, ContainerId containerId,
      Resource original, Resource target) {
    Preconditions.checkArgument(containerId != null,
        "ContainerId cannot be null");
    Preconditions.checkArgument(original != null,
        "Original resource capability cannot be null");
    Preconditions.checkArgument(!Resources.equals(Resources.none(), original)
            && Resources.fitsIn(Resources.none(), original),
        "Original resource capability must be greater than 0");
    Preconditions.checkArgument(target != null,
        "Target resource capability cannot be null");
    Preconditions.checkArgument(!Resources.equals(Resources.none(), target)
            && Resources.fitsIn(Resources.none(), target),
        "Target resource capability must be greater than 0");
    if (ContainerUpdateType.DECREASE_RESOURCE == updateType) {
      Preconditions.checkArgument(Resources.fitsIn(target, original),
          "Target resource capability must fit in Original capability");
    } else {
      Preconditions.checkArgument(Resources.fitsIn(original, target),
          "Target resource capability must be more than Original capability");

    }
  }

  private void validateContainerExecTypeChangeRequest(
      ContainerUpdateType updateType, ContainerId containerId,
      ExecutionType original, ExecutionType target) {
    Preconditions.checkArgument(containerId != null,
        "ContainerId cannot be null");
    Preconditions.checkArgument(original != null,
        "Original Execution Type cannot be null");
    Preconditions.checkArgument(target != null,
        "Target Execution Type cannot be null");
    if (ContainerUpdateType.DEMOTE_EXECUTION_TYPE == updateType) {
      Preconditions.checkArgument(target == ExecutionType.OPPORTUNISTIC
              && original == ExecutionType.GUARANTEED,
          "Incorrect Container update request, target should be" +
              " OPPORTUNISTIC and original should be GUARANTEED");
    } else {
      Preconditions.checkArgument(target == ExecutionType.GUARANTEED
                  && original == ExecutionType.OPPORTUNISTIC,
          "Incorrect Container update request, target should be" +
              " GUARANTEED and original should be OPPORTUNISTIC");
    }
  }

  private void addResourceRequestToAsk(ResourceRequest remoteRequest) {
    // This code looks weird but is needed because of the following scenario.
    // A ResourceRequest is removed from the remoteRequestTable. A 0 container 
    // request is added to 'ask' to notify the RM about not needing it any more.
    // Before the call to allocate, the user now requests more containers. If 
    // the locations of the 0 size request and the new request are the same
    // (with the difference being only container count), then the set comparator
    // will consider both to be the same and not add the new request to ask. So 
    // we need to check for the "same" request being present and remove it and 
    // then add it back. The comparator is container count agnostic.
    // This should happen only rarely but we do need to guard against it.
    if(ask.contains(remoteRequest)) {
      ask.remove(remoteRequest);
    }
    ask.add(remoteRequest);
  }

  private void addResourceRequest(Priority priority, String resourceName,
      ExecutionTypeRequest execTypeReq, Resource capability, T req,
      boolean relaxLocality, String labelExpression) {
    RemoteRequestsTable<T> remoteRequestsTable =
        getTable(req.getAllocationRequestId());
    if (remoteRequestsTable == null) {
      remoteRequestsTable = new RemoteRequestsTable<>();
      putTable(req.getAllocationRequestId(), remoteRequestsTable);
    }
    @SuppressWarnings("unchecked")
    ResourceRequestInfo resourceRequestInfo = remoteRequestsTable
        .addResourceRequest(req.getAllocationRequestId(), priority,
            resourceName, execTypeReq, capability, req, relaxLocality,
            labelExpression);

    // Note this down for next interaction with ResourceManager
    addResourceRequestToAsk(resourceRequestInfo.remoteRequest);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding request to ask {}", resourceRequestInfo.remoteRequest);
      LOG.debug("addResourceRequest: allocationRequestId={} priority={}" +
          " resourceName={} numContainers={} #asks={}",
          resourceRequestInfo.remoteRequest.getAllocationRequestId(),
          priority.getPriority(),
          resourceName,
          resourceRequestInfo.remoteRequest.getNumContainers(),
          ask.size());
    }
  }

  private void decResourceRequest(Priority priority, String resourceName,
      ExecutionTypeRequest execTypeReq, Resource capability, T req) {
    RemoteRequestsTable<T> remoteRequestsTable =
        getTable(req.getAllocationRequestId());
    if (remoteRequestsTable != null) {
      @SuppressWarnings("unchecked")
      ResourceRequestInfo resourceRequestInfo =
          remoteRequestsTable.decResourceRequest(priority, resourceName,
              execTypeReq, capability, req);
      // send the ResourceRequest to RM even if is 0 because it needs to
      // override a previously sent value. If ResourceRequest was not sent
      // previously then sending 0 aught to be a no-op on RM
      if (resourceRequestInfo != null) {
        addResourceRequestToAsk(resourceRequestInfo.remoteRequest);

        // delete entry from map if no longer needed
        if (resourceRequestInfo.remoteRequest.getNumContainers() == 0) {
          remoteRequestsTable.remove(priority, resourceName,
              execTypeReq.getExecutionType(), capability);
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("AFTER decResourceRequest: allocationRequestId={} " +
              "priority={} resourceName={} numContainers={} #asks={}",
              req.getAllocationRequestId(), priority.getPriority(),
              resourceName,
              resourceRequestInfo.remoteRequest.getNumContainers(), ask.size());
        }
      }
    } else {
      LOG.info("No remoteRequestTable found with allocationRequestId={}",
          req.getAllocationRequestId());
    }
  }

  @Override
  public synchronized void updateBlacklist(List<String> blacklistAdditions,
      List<String> blacklistRemovals) {
    
    if (blacklistAdditions != null) {
      this.blacklistAdditions.addAll(blacklistAdditions);
      this.blacklistedNodes.addAll(blacklistAdditions);
      // if some resources are also in blacklistRemovals updated before, we 
      // should remove them here.
      this.blacklistRemovals.removeAll(blacklistAdditions);
    }
    
    if (blacklistRemovals != null) {
      this.blacklistRemovals.addAll(blacklistRemovals);
      this.blacklistedNodes.removeAll(blacklistRemovals);
      // if some resources are in blacklistAdditions before, we should remove
      // them here.
      this.blacklistAdditions.removeAll(blacklistRemovals);
    }
    
    if (blacklistAdditions != null && blacklistRemovals != null
        && blacklistAdditions.removeAll(blacklistRemovals)) {
      // we allow resources to appear in addition list and removal list in the
      // same invocation of updateBlacklist(), but should get a warn here.
      LOG.warn("The same resources appear in both blacklistAdditions and " +
          "blacklistRemovals in updateBlacklist.");
    }
  }

  @Override
  public synchronized void updateTrackingUrl(String trackingUrl) {
    this.newTrackingUrl = trackingUrl;
  }

  private void updateAMRMToken(Token token) throws IOException {
    org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> amrmToken =
        new org.apache.hadoop.security.token.Token<AMRMTokenIdentifier>(token
          .getIdentifier().array(), token.getPassword().array(), new Text(
          token.getKind()), new Text(token.getService()));
    // Preserve the token service sent by the RM when adding the token
    // to ensure we replace the previous token setup by the RM.
    // Afterwards we can update the service address for the RPC layer.
    UserGroupInformation currentUGI = UserGroupInformation.getCurrentUser();
    currentUGI.addToken(amrmToken);
    amrmToken.setService(ClientRMProxy.getAMRMTokenService(getConfig()));
  }

  @VisibleForTesting
  RemoteRequestsTable<T> getTable(long allocationRequestId) {
    return remoteRequests.get(Long.valueOf(allocationRequestId));
  }

  @VisibleForTesting
  Map<Set<String>, List<SchedulingRequest>> getOutstandingSchedRequests() {
    return outstandingSchedRequests;
  }

  RemoteRequestsTable<T> putTable(long allocationRequestId,
      RemoteRequestsTable<T> table) {
    return remoteRequests.put(Long.valueOf(allocationRequestId), table);
  }
}

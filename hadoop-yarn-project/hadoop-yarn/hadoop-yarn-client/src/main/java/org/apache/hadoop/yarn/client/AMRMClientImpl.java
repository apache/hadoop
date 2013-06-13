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

package org.apache.hadoop.yarn.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.YarnRuntimeException;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.service.AbstractService;

// TODO check inputs for null etc. YARN-654

@Unstable
public class AMRMClientImpl<T extends ContainerRequest> 
                          extends AbstractService implements AMRMClient<T> {

  private static final Log LOG = LogFactory.getLog(AMRMClientImpl.class);
  
  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  
  private int lastResponseId = 0;

  protected AMRMProtocol rmClient;
  protected final ApplicationAttemptId appAttemptId;  
  protected Resource clusterAvailableResources;
  protected int clusterNodeCount;
  
  class ResourceRequestInfo {
    ResourceRequest remoteRequest;
    LinkedHashSet<T> containerRequests;
    
    ResourceRequestInfo(Priority priority, String resourceName,
        Resource capability) {
      remoteRequest = ResourceRequest.newInstance(priority, resourceName,
          capability, 0);
      containerRequests = new LinkedHashSet<T>();
    }
  }
  
  
  /**
   * Class compares Resource by memory then cpu in reverse order
   */
  class ResourceReverseMemoryThenCpuComparator implements Comparator<Resource> {
    @Override
    public int compare(Resource arg0, Resource arg1) {
      int mem0 = arg0.getMemory();
      int mem1 = arg1.getMemory();
      int cpu0 = arg0.getVirtualCores();
      int cpu1 = arg1.getVirtualCores();
      if(mem0 == mem1) {
        if(cpu0 == cpu1) {
          return 0;
        }
        if(cpu0 < cpu1) {
          return 1;
        }
        return -1;
      }
      if(mem0 < mem1) { 
        return 1;
      }
      return -1;
    }    
  }
  
  static boolean canFit(Resource arg0, Resource arg1) {
    int mem0 = arg0.getMemory();
    int mem1 = arg1.getMemory();
    int cpu0 = arg0.getVirtualCores();
    int cpu1 = arg1.getVirtualCores();
    
    if(mem0 <= mem1 && cpu0 <= cpu1) { 
      return true;
    }
    return false; 
  }
  
  //Key -> Priority
  //Value -> Map
  //Key->ResourceName (e.g., hostname, rackname, *)
  //Value->Map
  //Key->Resource Capability
  //Value->ResourceRequest
  protected final 
  Map<Priority, Map<String, TreeMap<Resource, ResourceRequestInfo>>>
    remoteRequestsTable =
    new TreeMap<Priority, Map<String, TreeMap<Resource, ResourceRequestInfo>>>();

  protected final Set<ResourceRequest> ask = new TreeSet<ResourceRequest>(
      new org.apache.hadoop.yarn.api.records.ResourceRequest.ResourceRequestComparator());
  protected final Set<ContainerId> release = new TreeSet<ContainerId>();
  
  public AMRMClientImpl(ApplicationAttemptId appAttemptId) {
    super(AMRMClientImpl.class.getName());
    this.appAttemptId = appAttemptId;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    final YarnConfiguration conf = new YarnConfiguration(getConfig());
    final YarnRPC rpc = YarnRPC.create(conf);
    final InetSocketAddress rmAddress = conf.getSocketAddr(
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);

    UserGroupInformation currentUser;
    try {
      currentUser = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }

    // CurrentUser should already have AMToken loaded.
    rmClient = currentUser.doAs(new PrivilegedAction<AMRMProtocol>() {
      @Override
      public AMRMProtocol run() {
        return (AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rmAddress,
            conf);
      }
    });
    LOG.debug("Connecting to ResourceManager at " + rmAddress);
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
    // do this only once ???
    RegisterApplicationMasterRequest request = recordFactory
        .newRecordInstance(RegisterApplicationMasterRequest.class);
    synchronized (this) {
      request.setApplicationAttemptId(appAttemptId);      
    }
    request.setHost(appHostName);
    request.setRpcPort(appHostPort);
    if(appTrackingUrl != null) {
      request.setTrackingUrl(appTrackingUrl);
    }
    RegisterApplicationMasterResponse response = rmClient
        .registerApplicationMaster(request);
    return response;
  }

  @Override
  public AllocateResponse allocate(float progressIndicator) 
      throws YarnException, IOException {
    AllocateResponse allocateResponse = null;
    ArrayList<ResourceRequest> askList = null;
    ArrayList<ContainerId> releaseList = null;
    AllocateRequest allocateRequest = null;
    
    try {
      synchronized (this) {
        askList = new ArrayList<ResourceRequest>(ask);
        releaseList = new ArrayList<ContainerId>(release);
        // optimistically clear this collection assuming no RPC failure
        ask.clear();
        release.clear();
        allocateRequest =
            AllocateRequest.newInstance(appAttemptId, lastResponseId,
              progressIndicator, askList, releaseList, null);
      }

      allocateResponse = rmClient.allocate(allocateRequest);

      synchronized (this) {
        // update these on successful RPC
        clusterNodeCount = allocateResponse.getNumClusterNodes();
        lastResponseId = allocateResponse.getResponseId();
        clusterAvailableResources = allocateResponse.getAvailableResources();
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
        }
      }
    }
    return allocateResponse;
  }

  @Override
  public void unregisterApplicationMaster(FinalApplicationStatus appStatus,
      String appMessage, String appTrackingUrl) throws YarnException,
      IOException {
    FinishApplicationMasterRequest request = recordFactory
                  .newRecordInstance(FinishApplicationMasterRequest.class);
    request.setAppAttemptId(appAttemptId);
    request.setFinishApplicationStatus(appStatus);
    if(appMessage != null) {
      request.setDiagnostics(appMessage);
    }
    if(appTrackingUrl != null) {
      request.setTrackingUrl(appTrackingUrl);
    }
    rmClient.finishApplicationMaster(request);
  }
  
  @Override
  public synchronized void addContainerRequest(T req) {
    // Create resource requests
    // add check for dup locations
    if (req.hosts != null) {
      for (String host : req.hosts) {
        addResourceRequest(req.priority, host, req.capability,
            req.containerCount, req);
      }
    }

    if (req.racks != null) {
      for (String rack : req.racks) {
        addResourceRequest(req.priority, rack, req.capability,
            req.containerCount, req);
      }
    }

    // Off-switch
    addResourceRequest(req.priority, ResourceRequest.ANY, req.capability,
        req.containerCount, req);
  }

  @Override
  public synchronized void removeContainerRequest(T req) {
    // Update resource requests
    if (req.hosts != null) {
      for (String hostName : req.hosts) {
        decResourceRequest(req.priority, hostName, req.capability,
            req.containerCount, req);
      }
    }

    if (req.racks != null) {
      for (String rack : req.racks) {
        decResourceRequest(req.priority, rack, req.capability,
            req.containerCount, req);
      }
    }

    decResourceRequest(req.priority, ResourceRequest.ANY, req.capability,
        req.containerCount, req);
  }

  @Override
  public synchronized void releaseAssignedContainer(ContainerId containerId) {
    release.add(containerId);
  }
  
  @Override
  public synchronized Resource getClusterAvailableResources() {
    return clusterAvailableResources;
  }
  
  @Override
  public synchronized int getClusterNodeCount() {
    return clusterNodeCount;
  }
  
  @Override
  public synchronized List<? extends Collection<T>> getMatchingRequests(
                                          Priority priority, 
                                          String resourceName, 
                                          Resource capability) {
    List<LinkedHashSet<T>> list = new LinkedList<LinkedHashSet<T>>();
    Map<String, TreeMap<Resource, ResourceRequestInfo>> remoteRequests = 
        this.remoteRequestsTable.get(priority);
    if (remoteRequests == null) {
      return list;
    }
    TreeMap<Resource, ResourceRequestInfo> reqMap = remoteRequests
        .get(resourceName);
    if (reqMap == null) {
      return list;
    }

    ResourceRequestInfo resourceRequestInfo = reqMap.get(capability);
    if (resourceRequestInfo != null) {
      list.add(resourceRequestInfo.containerRequests);
      return list;
    }
    
    // no exact match. Container may be larger than what was requested.
    // get all resources <= capability. map is reverse sorted. 
    SortedMap<Resource, ResourceRequestInfo> tailMap = 
                                                  reqMap.tailMap(capability);
    for(Map.Entry<Resource, ResourceRequestInfo> entry : tailMap.entrySet()) {
      if(canFit(entry.getKey(), capability)) {
        // match found that fits in the larger resource
        list.add(entry.getValue().containerRequests);
      }
    }
    
    // no match found
    return list;          
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
      Resource capability, int containerCount, T req) {
    Map<String, TreeMap<Resource, ResourceRequestInfo>> remoteRequests =
      this.remoteRequestsTable.get(priority);
    if (remoteRequests == null) {
      remoteRequests = 
          new HashMap<String, TreeMap<Resource, ResourceRequestInfo>>();
      this.remoteRequestsTable.put(priority, remoteRequests);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Added priority=" + priority);
      }
    }
    TreeMap<Resource, ResourceRequestInfo> reqMap = 
                                          remoteRequests.get(resourceName);
    if (reqMap == null) {
      // capabilities are stored in reverse sorted order. smallest last.
      reqMap = new TreeMap<Resource, ResourceRequestInfo>(
          new ResourceReverseMemoryThenCpuComparator());
      remoteRequests.put(resourceName, reqMap);
    }
    ResourceRequestInfo resourceRequestInfo = reqMap.get(capability);
    if (resourceRequestInfo == null) {
      resourceRequestInfo =
          new ResourceRequestInfo(priority, resourceName, capability);
      reqMap.put(capability, resourceRequestInfo);
    }
    
    resourceRequestInfo.remoteRequest.setNumContainers(
         resourceRequestInfo.remoteRequest.getNumContainers() + containerCount);

    if(req instanceof StoredContainerRequest) {
      resourceRequestInfo.containerRequests.add(req);
    }

    // Note this down for next interaction with ResourceManager
    addResourceRequestToAsk(resourceRequestInfo.remoteRequest);

    if (LOG.isDebugEnabled()) {
      LOG.debug("addResourceRequest:" + " applicationId="
          + appAttemptId + " priority=" + priority.getPriority()
          + " resourceName=" + resourceName + " numContainers="
          + resourceRequestInfo.remoteRequest.getNumContainers() 
          + " #asks=" + ask.size());
    }
  }

  private void decResourceRequest(Priority priority, 
                                   String resourceName,
                                   Resource capability, 
                                   int containerCount, 
                                   T req) {
    Map<String, TreeMap<Resource, ResourceRequestInfo>> remoteRequests =
      this.remoteRequestsTable.get(priority);
    
    if(remoteRequests == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not decrementing resource as priority " + priority 
            + " is not present in request table");
      }
      return;
    }
    
    Map<Resource, ResourceRequestInfo> reqMap = remoteRequests.get(resourceName);
    if (reqMap == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not decrementing resource as " + resourceName
            + " is not present in request table");
      }
      return;
    }
    ResourceRequestInfo resourceRequestInfo = reqMap.get(capability);

    if (LOG.isDebugEnabled()) {
      LOG.debug("BEFORE decResourceRequest:" + " applicationId="
          + appAttemptId + " priority=" + priority.getPriority()
          + " resourceName=" + resourceName + " numContainers="
          + resourceRequestInfo.remoteRequest.getNumContainers() 
          + " #asks=" + ask.size());
    }

    resourceRequestInfo.remoteRequest.setNumContainers(
        resourceRequestInfo.remoteRequest.getNumContainers() - containerCount);

    if(req instanceof StoredContainerRequest) {
      resourceRequestInfo.containerRequests.remove(req);
    }
    
    if(resourceRequestInfo.remoteRequest.getNumContainers() < 0) {
      // guard against spurious removals
      resourceRequestInfo.remoteRequest.setNumContainers(0);
    }
    // send the ResourceRequest to RM even if is 0 because it needs to override
    // a previously sent value. If ResourceRequest was not sent previously then
    // sending 0 aught to be a no-op on RM
    addResourceRequestToAsk(resourceRequestInfo.remoteRequest);

    // delete entries from map if no longer needed
    if (resourceRequestInfo.remoteRequest.getNumContainers() == 0) {
      reqMap.remove(capability);
      if (reqMap.size() == 0) {
        remoteRequests.remove(resourceName);
      }
      if (remoteRequests.size() == 0) {
        remoteRequestsTable.remove(priority);
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.info("AFTER decResourceRequest:" + " applicationId="
          + appAttemptId + " priority=" + priority.getPriority()
          + " resourceName=" + resourceName + " numContainers="
          + resourceRequestInfo.remoteRequest.getNumContainers() 
          + " #asks=" + ask.size());
    }
  }

}

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

package org.apache.hadoop.mapreduce.v2.app.rm;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.BuilderUtils;

/**
 * Keeps the data structures to send container requests to RM.
 */
public abstract class RMContainerRequestor extends RMCommunicator {
  
  private static final Log LOG = LogFactory.getLog(RMContainerRequestor.class);
  static final String ANY = "*";

  private int lastResponseID;
  private Resource availableResources;

  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  //Key -> Priority
  //Value -> Map
  //Key->ResourceName (e.g., hostname, rackname, *)
  //Value->Map
  //Key->Resource Capability
  //Value->ResourceRequest
  private final Map<Priority, Map<String, Map<Resource, ResourceRequest>>>
  remoteRequestsTable =
      new TreeMap<Priority, Map<String, Map<Resource, ResourceRequest>>>();

  private final Set<ResourceRequest> ask = new TreeSet<ResourceRequest>();
  private final Set<ContainerId> release = new TreeSet<ContainerId>(); 

  private boolean nodeBlacklistingEnabled;
  private int maxTaskFailuresPerNode;
  private final Map<String, Integer> nodeFailures = new HashMap<String, Integer>();
  private final Set<String> blacklistedNodes = new HashSet<String>();

  public RMContainerRequestor(ClientService clientService, AppContext context) {
    super(clientService, context);
  }

  static class ContainerRequest {
    final TaskAttemptId attemptID;
    final Resource capability;
    final String[] hosts;
    final String[] racks;
    //final boolean earlierAttemptFailed;
    final Priority priority;
    
    public ContainerRequest(ContainerRequestEvent event, Priority priority) {
      this(event.getAttemptID(), event.getCapability(), event.getHosts(),
          event.getRacks(), priority);
    }
    
    public ContainerRequest(TaskAttemptId attemptID,
        Resource capability, String[] hosts, String[] racks, 
        Priority priority) {
      this.attemptID = attemptID;
      this.capability = capability;
      this.hosts = hosts;
      this.racks = racks;
      this.priority = priority;
    }
    
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
    nodeBlacklistingEnabled = 
      conf.getBoolean(MRJobConfig.MR_AM_JOB_NODE_BLACKLISTING_ENABLE, true);
    LOG.info("nodeBlacklistingEnabled:" + nodeBlacklistingEnabled);
    maxTaskFailuresPerNode = 
      conf.getInt(MRJobConfig.MAX_TASK_FAILURES_PER_TRACKER, 3);
    LOG.info("maxTaskFailuresPerNode is " + maxTaskFailuresPerNode);
  }

  protected AMResponse makeRemoteRequest() throws YarnRemoteException {
    AllocateRequest allocateRequest = BuilderUtils.newAllocateRequest(
        applicationAttemptId, lastResponseID, super.getApplicationProgress(),
        new ArrayList<ResourceRequest>(ask), new ArrayList<ContainerId>(
            release));
    AllocateResponse allocateResponse = scheduler.allocate(allocateRequest);
    AMResponse response = allocateResponse.getAMResponse();
    lastResponseID = response.getResponseId();
    availableResources = response.getAvailableResources();

    LOG.info("getResources() for " + applicationId + ":" + " ask="
        + ask.size() + " release= " + release.size() + 
        " newContainers=" + response.getAllocatedContainers().size() + 
        " finishedContainers=" + 
        response.getCompletedContainersStatuses().size() + 
        " resourcelimit=" + availableResources);

    ask.clear();
    release.clear();
    return response;
  }

  protected void containerFailedOnHost(String hostName) {
    if (!nodeBlacklistingEnabled) {
      return;
    }
    if (blacklistedNodes.contains(hostName)) {
      LOG.info("Host " + hostName + " is already blacklisted.");
      return; //already blacklisted
    }
    Integer failures = nodeFailures.remove(hostName);
    failures = failures == null ? 0 : failures;
    failures++;
    LOG.info(failures + " failures on node " + hostName);
    if (failures >= maxTaskFailuresPerNode) {
      blacklistedNodes.add(hostName);
      LOG.info("Blacklisted host " + hostName);
      
      //remove all the requests corresponding to this hostname
      for (Map<String, Map<Resource, ResourceRequest>> remoteRequests 
          : remoteRequestsTable.values()){
        //remove from host if no pending allocations
        boolean foundAll = true;
        Map<Resource, ResourceRequest> reqMap = remoteRequests.get(hostName);
        if (reqMap != null) {
          for (ResourceRequest req : reqMap.values()) {
            if (!ask.remove(req)) {
              foundAll = false;
            }
            else {
              // if ask already sent to RM, we can try and overwrite it if possible.
              // send a new ask to RM with numContainers
              // specified for the blacklisted host to be 0.
              ResourceRequest zeroedRequest = BuilderUtils.newResourceRequest(req);
              zeroedRequest.setNumContainers(0);
              // to be sent to RM on next heartbeat
              ask.add(zeroedRequest);
            }
          }
          // if all requests were still in ask queue
          // we can remove this request
          if (foundAll) {
            remoteRequests.remove(hostName);
          }     
        }
        // TODO handling of rack blacklisting
        // Removing from rack should be dependent on no. of failures within the rack 
        // Blacklisting a rack on the basis of a single node's blacklisting 
        // may be overly aggressive. 
        // Node failures could be co-related with other failures on the same rack 
        // but we probably need a better approach at trying to decide how and when 
        // to blacklist a rack
      }
    } else {
      nodeFailures.put(hostName, failures);
    }
  }

  protected Resource getAvailableResources() {
    return availableResources;
  }
  
  protected void addContainerReq(ContainerRequest req) {
    // Create resource requests
    for (String host : req.hosts) {
      // Data-local
      if (!isNodeBlacklisted(host)) {
        addResourceRequest(req.priority, host, req.capability);
      }      
    }

    // Nothing Rack-local for now
    for (String rack : req.racks) {
      addResourceRequest(req.priority, rack, req.capability);
    }

    // Off-switch
    addResourceRequest(req.priority, ANY, req.capability); 
  }

  protected void decContainerReq(ContainerRequest req) {
    // Update resource requests
    for (String hostName : req.hosts) {
      decResourceRequest(req.priority, hostName, req.capability);
    }
    
    for (String rack : req.racks) {
      decResourceRequest(req.priority, rack, req.capability);
    }
   
    decResourceRequest(req.priority, ANY, req.capability);
  }

  private void addResourceRequest(Priority priority, String resourceName,
      Resource capability) {
    Map<String, Map<Resource, ResourceRequest>> remoteRequests =
      this.remoteRequestsTable.get(priority);
    if (remoteRequests == null) {
      remoteRequests = new HashMap<String, Map<Resource, ResourceRequest>>();
      this.remoteRequestsTable.put(priority, remoteRequests);
      LOG.info("Added priority=" + priority);
    }
    Map<Resource, ResourceRequest> reqMap = remoteRequests.get(resourceName);
    if (reqMap == null) {
      reqMap = new HashMap<Resource, ResourceRequest>();
      remoteRequests.put(resourceName, reqMap);
    }
    ResourceRequest remoteRequest = reqMap.get(capability);
    if (remoteRequest == null) {
      remoteRequest = recordFactory.newRecordInstance(ResourceRequest.class);
      remoteRequest.setPriority(priority);
      remoteRequest.setHostName(resourceName);
      remoteRequest.setCapability(capability);
      remoteRequest.setNumContainers(0);
      reqMap.put(capability, remoteRequest);
    }
    remoteRequest.setNumContainers(remoteRequest.getNumContainers() + 1);

    // Note this down for next interaction with ResourceManager
    ask.add(remoteRequest);
    LOG.info("addResourceRequest:" + " applicationId=" + applicationId.getId()
        + " priority=" + priority.getPriority() + " resourceName=" + resourceName
        + " numContainers=" + remoteRequest.getNumContainers() + " #asks="
        + ask.size());
  }

  private void decResourceRequest(Priority priority, String resourceName,
      Resource capability) {
    Map<String, Map<Resource, ResourceRequest>> remoteRequests =
      this.remoteRequestsTable.get(priority);
    Map<Resource, ResourceRequest> reqMap = remoteRequests.get(resourceName);
    if (reqMap == null) {
      // as we modify the resource requests by filtering out blacklisted hosts 
      // when they are added, this value may be null when being 
      // decremented
      LOG.debug("Not decrementing resource as " + resourceName
          + " is not present in request table");
      return;
    }
    ResourceRequest remoteRequest = reqMap.get(capability);

    LOG.info("BEFORE decResourceRequest:" + " applicationId=" + applicationId.getId()
        + " priority=" + priority.getPriority() + " resourceName=" + resourceName
        + " numContainers=" + remoteRequest.getNumContainers() + " #asks="
        + ask.size());

    remoteRequest.setNumContainers(remoteRequest.getNumContainers() -1);
    if (remoteRequest.getNumContainers() == 0) {
      reqMap.remove(capability);
      if (reqMap.size() == 0) {
        remoteRequests.remove(resourceName);
      }
      if (remoteRequests.size() == 0) {
        remoteRequestsTable.remove(priority);
      }
      //remove from ask if it may have
      ask.remove(remoteRequest);
    } else {
      ask.add(remoteRequest);//this will override the request if ask doesn't
      //already have it.
    }

    LOG.info("AFTER decResourceRequest:" + " applicationId="
             + applicationId.getId() + " priority=" + priority.getPriority()
             + " resourceName=" + resourceName + " numContainers="
             + remoteRequest.getNumContainers() + " #asks=" + ask.size());
  }

  protected void release(ContainerId containerId) {
    release.add(containerId);
  }
  
  protected boolean isNodeBlacklisted(String hostname) {
    if (!nodeBlacklistingEnabled) {
      return false;
    }
    return blacklistedNodes.contains(hostname);
  }
  
  protected ContainerRequest getFilteredContainerRequest(ContainerRequest orig) {
    ArrayList<String> newHosts = new ArrayList<String>();
    for (String host : orig.hosts) {
      if (!isNodeBlacklisted(host)) {
        newHosts.add(host);      
      }
    }
    String[] hosts = newHosts.toArray(new String[newHosts.size()]);
    ContainerRequest newReq = new ContainerRequest(orig.attemptID, orig.capability,
        hosts, orig.racks, orig.priority); 
    return newReq;
  }
}

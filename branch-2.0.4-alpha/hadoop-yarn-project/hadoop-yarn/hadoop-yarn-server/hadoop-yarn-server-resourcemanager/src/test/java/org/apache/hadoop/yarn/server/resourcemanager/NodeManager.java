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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.BuilderUtils;

@Private
public class NodeManager implements ContainerManager {
  private static final Log LOG = LogFactory.getLog(NodeManager.class);
  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  
  final private String containerManagerAddress;
  final private String nodeHttpAddress;
  final private String rackName;
  final private NodeId nodeId;
  final private Resource capability;
  Resource available = recordFactory.newRecordInstance(Resource.class);
  Resource used = recordFactory.newRecordInstance(Resource.class);

  final ResourceTrackerService resourceTrackerService;
  final FiCaSchedulerNode schedulerNode;
  final Map<ApplicationId, List<Container>> containers = 
    new HashMap<ApplicationId, List<Container>>();
  
  public NodeManager(String hostName, int containerManagerPort, int httpPort,
      String rackName, Resource capability,
      ResourceTrackerService resourceTrackerService, RMContext rmContext)
      throws IOException {
    this.containerManagerAddress = hostName + ":" + containerManagerPort;
    this.nodeHttpAddress = hostName + ":" + httpPort;
    this.rackName = rackName;
    this.resourceTrackerService = resourceTrackerService;
    this.capability = capability;
    Resources.addTo(available, capability);

    this.nodeId = recordFactory.newRecordInstance(NodeId.class);
    this.nodeId.setHost(hostName);
    this.nodeId.setPort(containerManagerPort);
    RegisterNodeManagerRequest request = recordFactory
        .newRecordInstance(RegisterNodeManagerRequest.class);
    request.setHttpPort(httpPort);
    request.setNodeId(this.nodeId);
    request.setResource(capability);
    request.setNodeId(this.nodeId);
    resourceTrackerService.registerNodeManager(request)
        .getRegistrationResponse();
    this.schedulerNode = new FiCaSchedulerNode(rmContext.getRMNodes().get(
        this.nodeId));
   
    // Sanity check
    Assert.assertEquals(capability.getMemory(), 
       schedulerNode.getAvailableResource().getMemory());
    Assert.assertEquals(capability.getVirtualCores(), 
        schedulerNode.getAvailableResource().getVirtualCores());
  }
  
  public String getHostName() {
    return containerManagerAddress;
  }

  public String getRackName() {
    return rackName;
  }

  public NodeId getNodeId() {
    return nodeId;
  }

  public Resource getCapability() {
    return capability;
  }

  public Resource getAvailable() {
    return available;
  }
  
  public Resource getUsed() {
    return used;
  }
  
  int responseID = 0;
  
  private List<ContainerStatus> getContainerStatuses(Map<ApplicationId, List<Container>> containers) {
    List<ContainerStatus> containerStatuses = new ArrayList<ContainerStatus>();
    for (List<Container> appContainers : containers.values()) {
      for (Container container : appContainers) {
        containerStatuses.add(container.getContainerStatus());
      }
    }
    return containerStatuses;
  }
  public void heartbeat() throws IOException {
    NodeStatus nodeStatus = 
      org.apache.hadoop.yarn.server.resourcemanager.NodeManager.createNodeStatus(
          nodeId, getContainerStatuses(containers));
    nodeStatus.setResponseId(responseID);
    NodeHeartbeatRequest request = recordFactory
        .newRecordInstance(NodeHeartbeatRequest.class);
    request.setNodeStatus(nodeStatus);
    HeartbeatResponse response = resourceTrackerService
        .nodeHeartbeat(request).getHeartbeatResponse();
    responseID = response.getResponseId();
  }

  @Override
  synchronized public StartContainerResponse startContainer(
      StartContainerRequest request) 
  throws YarnRemoteException {
    ContainerLaunchContext containerLaunchContext = 
        request.getContainerLaunchContext();
    
    ApplicationId applicationId = 
        containerLaunchContext.getContainerId().getApplicationAttemptId().
        getApplicationId();

    List<Container> applicationContainers = containers.get(applicationId);
    if (applicationContainers == null) {
      applicationContainers = new ArrayList<Container>();
      containers.put(applicationId, applicationContainers);
    }
    
    // Sanity check
    for (Container container : applicationContainers) {
      if (container.getId().compareTo(containerLaunchContext.getContainerId()) 
          == 0) {
        throw new IllegalStateException(
            "Container " + containerLaunchContext.getContainerId() + 
            " already setup on node " + containerManagerAddress);
      }
    }

    Container container =
        BuilderUtils.newContainer(containerLaunchContext.getContainerId(),
            this.nodeId, nodeHttpAddress,
            containerLaunchContext.getResource(), 
            null, null                                 // DKDC - Doesn't matter
            );

    applicationContainers.add(container);
    
    Resources.subtractFrom(available, containerLaunchContext.getResource());
    Resources.addTo(used, containerLaunchContext.getResource());
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("startContainer:" + " node=" + containerManagerAddress
        + " application=" + applicationId + " container=" + container
        + " available=" + available + " used=" + used);
    }

    StartContainerResponse response = recordFactory.newRecordInstance(StartContainerResponse.class);
    return response;
  }

  synchronized public void checkResourceUsage() {
    LOG.info("Checking resource usage for " + containerManagerAddress);
    Assert.assertEquals(available.getMemory(), 
        schedulerNode.getAvailableResource().getMemory());
    Assert.assertEquals(used.getMemory(), 
        schedulerNode.getUsedResource().getMemory());
  }
  
  @Override
  synchronized public StopContainerResponse stopContainer(StopContainerRequest request) 
  throws YarnRemoteException {
    ContainerId containerID = request.getContainerId();
    String applicationId = String.valueOf(
        containerID.getApplicationAttemptId().getApplicationId().getId());
    
    // Mark the container as COMPLETE
    List<Container> applicationContainers = containers.get(applicationId);
    for (Container c : applicationContainers) {
      if (c.getId().compareTo(containerID) == 0) {
        c.setState(ContainerState.COMPLETE);
      }
    }
    
    // Send a heartbeat
    try {
      heartbeat();
    } catch (IOException ioe) {
      throw RPCUtil.getRemoteException(ioe);
    }
    
    // Remove container and update status
    int ctr = 0;
    Container container = null;
    for (Iterator<Container> i=applicationContainers.iterator(); i.hasNext();) {
      container = i.next();
      if (container.getId().compareTo(containerID) == 0) {
        i.remove();
        ++ctr;
      }
    }
    
    if (ctr != 1) {
      throw new IllegalStateException("Container " + containerID + 
          " stopped " + ctr + " times!");
    }
    
    Resources.addTo(available, container.getResource());
    Resources.subtractFrom(used, container.getResource());

    if(LOG.isDebugEnabled()) {
      LOG.debug("stopContainer:" + " node=" + containerManagerAddress
        + " application=" + applicationId + " container=" + containerID
        + " available=" + available + " used=" + used);
    }

    StopContainerResponse response = recordFactory.newRecordInstance(StopContainerResponse.class);
    return response;
  }

  @Override
  synchronized public GetContainerStatusResponse getContainerStatus(GetContainerStatusRequest request) throws YarnRemoteException {
    ContainerId containerId = request.getContainerId();
    List<Container> appContainers = 
        containers.get(
            containerId.getApplicationAttemptId().getApplicationId());
    Container container = null;
    for (Container c : appContainers) {
      if (c.getId().equals(containerId)) {
        container = c;
      }
    }
    GetContainerStatusResponse response = 
        recordFactory.newRecordInstance(GetContainerStatusResponse.class);
    if (container != null && container.getContainerStatus() != null) {
      response.setStatus(container.getContainerStatus());
    }
    return response;
  }

  public static org.apache.hadoop.yarn.server.api.records.NodeStatus 
  createNodeStatus(NodeId nodeId, List<ContainerStatus> containers) {
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    org.apache.hadoop.yarn.server.api.records.NodeStatus nodeStatus = 
        recordFactory.newRecordInstance(org.apache.hadoop.yarn.server.api.records.NodeStatus.class);
    nodeStatus.setNodeId(nodeId);
    nodeStatus.setContainersStatuses(containers);
    NodeHealthStatus nodeHealthStatus = 
      recordFactory.newRecordInstance(NodeHealthStatus.class);
    nodeHealthStatus.setIsNodeHealthy(true);
    nodeStatus.setNodeHealthStatus(nodeHealthStatus);
    return nodeStatus;
  }
}

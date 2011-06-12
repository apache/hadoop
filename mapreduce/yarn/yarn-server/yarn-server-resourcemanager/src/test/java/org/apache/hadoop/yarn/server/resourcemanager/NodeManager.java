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
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.RegistrationResponse;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.RMResourceTrackerImpl;
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

  final RMResourceTrackerImpl resourceTracker;
  final NodeInfo nodeInfo;
  final Map<String, List<Container>> containers = 
    new HashMap<String, List<Container>>();
  
  public NodeManager(String hostName, int containerManagerPort, int httpPort,
      String rackName, int memory, RMResourceTrackerImpl resourceTracker)
      throws IOException {
    this.containerManagerAddress = hostName + ":" + containerManagerPort;
    this.nodeHttpAddress = hostName + ":" + httpPort;
    this.rackName = rackName;
    this.resourceTracker = resourceTracker;
    this.capability = Resources.createResource(memory);
    Resources.addTo(available, capability);

    RegistrationResponse response =
        resourceTracker.registerNodeManager(hostName, containerManagerPort, 
            httpPort, capability);
    this.nodeId = response.getNodeId();
    this.nodeInfo = resourceTracker.getNodeManager(nodeId);
   
    // Sanity check
    Assert.assertEquals(memory, 
       nodeInfo.getAvailableResource().getMemory());
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
  
  public void heartbeat() throws IOException {
    NodeStatus nodeStatus = 
      org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeStatus.createNodeStatus(
          nodeId, containers);
    nodeStatus.setResponseId(responseID);
    HeartbeatResponse response = resourceTracker.nodeHeartbeat(nodeStatus);
    responseID = response.getResponseId();
  }

  @Override
  synchronized public StartContainerResponse startContainer(StartContainerRequest request) throws YarnRemoteException {
    ContainerLaunchContext containerLaunchContext = request.getContainerLaunchContext();
    
    String applicationId = String.valueOf(containerLaunchContext.getContainerId().getAppId().getId());

    List<Container> applicationContainers = containers.get(applicationId);
    if (applicationContainers == null) {
      applicationContainers = new ArrayList<Container>();
      containers.put(applicationId, applicationContainers);
    }
    
    // Sanity check
    for (Container container : applicationContainers) {
      if (container.getId().compareTo(containerLaunchContext.getContainerId()) == 0) {
        throw new IllegalStateException(
            "Container " + containerLaunchContext.getContainerId() + 
            " already setup on node " + containerManagerAddress);
      }
    }

    Container container =
        BuilderUtils.newContainer(containerLaunchContext.getContainerId(),
            containerManagerAddress, nodeHttpAddress,
            containerLaunchContext.getResource());

    applicationContainers.add(container);
    
    Resources.subtractFrom(available, containerLaunchContext.getResource());
    Resources.addTo(used, containerLaunchContext.getResource());
    
    LOG.info("DEBUG --- startContainer:" +
        " node=" + containerManagerAddress +
        " application=" + applicationId + 
        " container=" + container +
        " available=" + available +
        " used=" + used);

    StartContainerResponse response = recordFactory.newRecordInstance(StartContainerResponse.class);
    return response;
  }

  synchronized public void checkResourceUsage() {
    LOG.info("Checking resource usage for " + containerManagerAddress);
    Assert.assertEquals(available.getMemory(), 
        nodeInfo.getAvailableResource().getMemory());
    Assert.assertEquals(used.getMemory(), 
        nodeInfo.getUsedResource().getMemory());
  }
  
  @Override
  synchronized public StopContainerResponse stopContainer(StopContainerRequest request) 
  throws YarnRemoteException {
    ContainerId containerID = request.getContainerId();
    String applicationId = String.valueOf(containerID.getAppId().getId());
    
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

    LOG.info("DEBUG --- stopContainer:" +
        " node=" + containerManagerAddress +
        " application=" + applicationId + 
        " container=" + containerID +
        " available=" + available +
        " used=" + used);

    StopContainerResponse response = recordFactory.newRecordInstance(StopContainerResponse.class);
    return response;
  }

  @Override
  synchronized public GetContainerStatusResponse getContainerStatus(GetContainerStatusRequest request) throws YarnRemoteException {
    ContainerId containerID = request.getContainerId();
    GetContainerStatusResponse response = recordFactory.newRecordInstance(GetContainerStatusResponse.class);
    return response;
  }
}

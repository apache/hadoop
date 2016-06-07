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

import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceResponse;
import org.junit.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.util.resource.Resources;

@Private
public class NodeManager implements ContainerManagementProtocol {
  private static final Log LOG = LogFactory.getLog(NodeManager.class);
  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  
  final private String containerManagerAddress;
  final private String nodeHttpAddress;
  final private String rackName;
  final private NodeId nodeId;
  final private Resource capability;
  final private ResourceManager resourceManager;
  Resource available = recordFactory.newRecordInstance(Resource.class);
  Resource used = recordFactory.newRecordInstance(Resource.class);

  final ResourceTrackerService resourceTrackerService;
  final Map<ApplicationId, List<Container>> containers = 
    new HashMap<ApplicationId, List<Container>>();
  
  final Map<Container, ContainerStatus> containerStatusMap =
      new HashMap<Container, ContainerStatus>();
  
  public NodeManager(String hostName, int containerManagerPort, int httpPort,
      String rackName, Resource capability,
      ResourceManager resourceManager)
      throws IOException, YarnException {
    this.containerManagerAddress = hostName + ":" + containerManagerPort;
    this.nodeHttpAddress = hostName + ":" + httpPort;
    this.rackName = rackName;
    this.resourceTrackerService = resourceManager.getResourceTrackerService();
    this.capability = capability;
    Resources.addTo(available, capability);
    this.nodeId = NodeId.newInstance(hostName, containerManagerPort);
    RegisterNodeManagerRequest request = recordFactory
        .newRecordInstance(RegisterNodeManagerRequest.class);
    request.setHttpPort(httpPort);
    request.setResource(capability);
    request.setNodeId(this.nodeId);
    request.setNMVersion(YarnVersionInfo.getVersion());
    resourceTrackerService.registerNodeManager(request);
    this.resourceManager = resourceManager;
    resourceManager.getResourceScheduler().getNodeReport(this.nodeId);
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
        containerStatuses.add(containerStatusMap.get(container));
      }
    }
    return containerStatuses;
  }
  public void heartbeat() throws IOException, YarnException {
    NodeStatus nodeStatus = 
      org.apache.hadoop.yarn.server.resourcemanager.NodeManager.createNodeStatus(
          nodeId, getContainerStatuses(containers));
    nodeStatus.setResponseId(responseID);
    NodeHeartbeatRequest request = recordFactory
        .newRecordInstance(NodeHeartbeatRequest.class);
    request.setNodeStatus(nodeStatus);
    NodeHeartbeatResponse response = resourceTrackerService
        .nodeHeartbeat(request);
    responseID = response.getResponseId();
  }

  @Override
  synchronized public StartContainersResponse startContainers(
      StartContainersRequest requests) 
  throws YarnException {

    for (StartContainerRequest request : requests.getStartContainerRequests()) {
      Token containerToken = request.getContainerToken();
      ContainerTokenIdentifier tokenId = null;

      try {
        tokenId = BuilderUtils.newContainerTokenIdentifier(containerToken);
      } catch (IOException e) {
        throw RPCUtil.getRemoteException(e);
      }

      ContainerId containerID = tokenId.getContainerID();
      ApplicationId applicationId =
          containerID.getApplicationAttemptId().getApplicationId();

      List<Container> applicationContainers = containers.get(applicationId);
      if (applicationContainers == null) {
        applicationContainers = new ArrayList<Container>();
        containers.put(applicationId, applicationContainers);
      }

      // Sanity check
      for (Container container : applicationContainers) {
        if (container.getId().compareTo(containerID) == 0) {
          throw new IllegalStateException("Container " + containerID
              + " already setup on node " + containerManagerAddress);
        }
      }

      Container container =
          BuilderUtils.newContainer(containerID, this.nodeId, nodeHttpAddress,
            tokenId.getResource(), null, null // DKDC - Doesn't matter
            );

      ContainerStatus containerStatus =
          BuilderUtils.newContainerStatus(container.getId(),
            ContainerState.NEW, "", -1000, container.getResource());
      applicationContainers.add(container);
      containerStatusMap.put(container, containerStatus);
      Resources.subtractFrom(available, tokenId.getResource());
      Resources.addTo(used, tokenId.getResource());

      if (LOG.isDebugEnabled()) {
        LOG.debug("startContainer:" + " node=" + containerManagerAddress
            + " application=" + applicationId + " container=" + container
            + " available=" + available + " used=" + used);
      }

    }
    StartContainersResponse response =
        StartContainersResponse.newInstance(null, null, null);
    return response;
  }

  synchronized public void checkResourceUsage() {
    LOG.info("Checking resource usage for " + containerManagerAddress);
    Assert.assertEquals(available.getMemorySize(),
        resourceManager.getResourceScheduler().getNodeReport(
            this.nodeId).getAvailableResource().getMemorySize());
    Assert.assertEquals(used.getMemorySize(),
        resourceManager.getResourceScheduler().getNodeReport(
            this.nodeId).getUsedResource().getMemorySize());
  }
  
  @Override
  synchronized public StopContainersResponse stopContainers(StopContainersRequest request) 
  throws YarnException {
    for (ContainerId containerID : request.getContainerIds()) {
      String applicationId =
          String.valueOf(containerID.getApplicationAttemptId()
            .getApplicationId().getId());
      // Mark the container as COMPLETE
      List<Container> applicationContainers = containers.get(containerID.getApplicationAttemptId()
              .getApplicationId());
      for (Container c : applicationContainers) {
        if (c.getId().compareTo(containerID) == 0) {
          ContainerStatus containerStatus = containerStatusMap.get(c);
          containerStatus.setState(ContainerState.COMPLETE);
          containerStatusMap.put(c, containerStatus);
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
      for (Iterator<Container> i = applicationContainers.iterator(); i
        .hasNext();) {
        container = i.next();
        if (container.getId().compareTo(containerID) == 0) {
          i.remove();
          ++ctr;
        }
      }

      if (ctr != 1) {
        throw new IllegalStateException("Container " + containerID
            + " stopped " + ctr + " times!");
      }

      Resources.addTo(available, container.getResource());
      Resources.subtractFrom(used, container.getResource());

      if (LOG.isDebugEnabled()) {
        LOG.debug("stopContainer:" + " node=" + containerManagerAddress
            + " application=" + applicationId + " container=" + containerID
            + " available=" + available + " used=" + used);
      }
    }
    return StopContainersResponse.newInstance(null,null);
  }

  @Override
  synchronized public GetContainerStatusesResponse getContainerStatuses(
      GetContainerStatusesRequest request) throws YarnException {
    List<ContainerStatus> statuses = new ArrayList<ContainerStatus>();
    for (ContainerId containerId : request.getContainerIds()) {
      List<Container> appContainers =
          containers.get(containerId.getApplicationAttemptId()
            .getApplicationId());
      Container container = null;
      for (Container c : appContainers) {
        if (c.getId().equals(containerId)) {
          container = c;
        }
      }
      if (container != null
          && containerStatusMap.get(container).getState() != null) {
        statuses.add(containerStatusMap.get(container));
      }
    }
    return GetContainerStatusesResponse.newInstance(statuses, null);
  }

  @Override
  public IncreaseContainersResourceResponse increaseContainersResource(
      IncreaseContainersResourceRequest request)
          throws YarnException, IOException {
    return null;
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

  @Override
  public synchronized SignalContainerResponse signalToContainer(
      SignalContainerRequest request) throws YarnException, IOException {
    throw new YarnException("Not supported yet!");
  }
}

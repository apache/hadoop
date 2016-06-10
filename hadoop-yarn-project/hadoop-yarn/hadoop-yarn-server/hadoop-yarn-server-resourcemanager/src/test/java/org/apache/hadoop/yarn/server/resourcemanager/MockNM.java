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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.mortbay.log.Log;

public class MockNM {

  private int responseId;
  private NodeId nodeId;
  private long memory;
  private int vCores;
  private ResourceTrackerService resourceTracker;
  private int httpPort = 2;
  private MasterKey currentContainerTokenMasterKey;
  private MasterKey currentNMTokenMasterKey;
  private String version;

  public MockNM(String nodeIdStr, int memory, ResourceTrackerService resourceTracker) {
    // scale vcores based on the requested memory
    this(nodeIdStr, memory,
        Math.max(1, (memory * YarnConfiguration.DEFAULT_NM_VCORES) /
            YarnConfiguration.DEFAULT_NM_PMEM_MB),
        resourceTracker);
  }

  public MockNM(String nodeIdStr, int memory, int vcores,
      ResourceTrackerService resourceTracker) {
    this(nodeIdStr, memory, vcores, resourceTracker, YarnVersionInfo.getVersion());
  }

  public MockNM(String nodeIdStr, int memory, int vcores,
      ResourceTrackerService resourceTracker, String version) {
    this.memory = memory;
    this.vCores = vcores;
    this.resourceTracker = resourceTracker;
    this.version = version;
    String[] splits = nodeIdStr.split(":");
    nodeId = BuilderUtils.newNodeId(splits[0], Integer.parseInt(splits[1]));
  }

  public NodeId getNodeId() {
    return nodeId;
  }

  public int getHttpPort() {
    return httpPort;
  }
  
  public void setHttpPort(int port) {
    httpPort = port;
  }

  public void setResourceTrackerService(ResourceTrackerService resourceTracker) {
    this.resourceTracker = resourceTracker;
  }

  public void containerStatus(ContainerStatus containerStatus) throws Exception {
    Map<ApplicationId, List<ContainerStatus>> conts = 
        new HashMap<ApplicationId, List<ContainerStatus>>();
    conts.put(containerStatus.getContainerId().getApplicationAttemptId().getApplicationId(),
        Arrays.asList(new ContainerStatus[] { containerStatus }));
    nodeHeartbeat(conts, true);
  }

  public void containerIncreaseStatus(Container container) throws Exception {
    Map<ApplicationId, List<ContainerStatus>> conts = new HashMap<>();
    ContainerStatus containerStatus = BuilderUtils.newContainerStatus(
        container.getId(), ContainerState.RUNNING, "Success", 0,
            container.getResource());
    conts.put(container.getId().getApplicationAttemptId().getApplicationId(),
        Collections.singletonList(containerStatus));
    List<Container> increasedConts = Collections.singletonList(container);
    nodeHeartbeat(conts, increasedConts, true, ++responseId);
  }

  public RegisterNodeManagerResponse registerNode() throws Exception {
    return registerNode(null, null);
  }
  
  public RegisterNodeManagerResponse registerNode(
      List<ApplicationId> runningApplications) throws Exception {
    return registerNode(null, runningApplications);
  }

  public RegisterNodeManagerResponse registerNode(
      List<NMContainerStatus> containerReports,
      List<ApplicationId> runningApplications) throws Exception {
    RegisterNodeManagerRequest req = Records.newRecord(
        RegisterNodeManagerRequest.class);
    req.setNodeId(nodeId);
    req.setHttpPort(httpPort);
    Resource resource = BuilderUtils.newResource(memory, vCores);
    req.setResource(resource);
    req.setContainerStatuses(containerReports);
    req.setNMVersion(version);
    req.setRunningApplications(runningApplications);
    RegisterNodeManagerResponse registrationResponse =
        resourceTracker.registerNodeManager(req);
    this.currentContainerTokenMasterKey =
        registrationResponse.getContainerTokenMasterKey();
    this.currentNMTokenMasterKey = registrationResponse.getNMTokenMasterKey();
    Resource newResource = registrationResponse.getResource();
    if (newResource != null) {
      memory = (int) newResource.getMemorySize();
      vCores = newResource.getVirtualCores();
    }
    return registrationResponse;
  }

  public NodeHeartbeatResponse nodeHeartbeat(boolean isHealthy) throws Exception {
    return nodeHeartbeat(new HashMap<ApplicationId, List<ContainerStatus>>(),
        isHealthy, ++responseId);
  }

  public NodeHeartbeatResponse nodeHeartbeat(ApplicationAttemptId attemptId,
      long containerId, ContainerState containerState) throws Exception {
    HashMap<ApplicationId, List<ContainerStatus>> nodeUpdate =
        new HashMap<ApplicationId, List<ContainerStatus>>(1);
    ContainerStatus containerStatus = BuilderUtils.newContainerStatus(
        BuilderUtils.newContainerId(attemptId, containerId), containerState,
        "Success", 0, BuilderUtils.newResource(memory, vCores));
    ArrayList<ContainerStatus> containerStatusList =
        new ArrayList<ContainerStatus>(1);
    containerStatusList.add(containerStatus);
    Log.info("ContainerStatus: " + containerStatus);
    nodeUpdate.put(attemptId.getApplicationId(), containerStatusList);
    return nodeHeartbeat(nodeUpdate, true);
  }

  public NodeHeartbeatResponse nodeHeartbeat(Map<ApplicationId,
      List<ContainerStatus>> conts, boolean isHealthy) throws Exception {
    return nodeHeartbeat(conts, isHealthy, ++responseId);
  }

  public NodeHeartbeatResponse nodeHeartbeat(Map<ApplicationId,
      List<ContainerStatus>> conts, boolean isHealthy, int resId) throws Exception {
    return nodeHeartbeat(conts, new ArrayList<Container>(), isHealthy, resId);
  }

  public NodeHeartbeatResponse nodeHeartbeat(Map<ApplicationId,
      List<ContainerStatus>> conts, List<Container> increasedConts,
          boolean isHealthy, int resId) throws Exception {
    NodeHeartbeatRequest req = Records.newRecord(NodeHeartbeatRequest.class);
    NodeStatus status = Records.newRecord(NodeStatus.class);
    status.setResponseId(resId);
    status.setNodeId(nodeId);
    for (Map.Entry<ApplicationId, List<ContainerStatus>> entry : conts.entrySet()) {
      Log.info("entry.getValue() " + entry.getValue());
      status.setContainersStatuses(entry.getValue());
    }
    status.setIncreasedContainers(increasedConts);
    NodeHealthStatus healthStatus = Records.newRecord(NodeHealthStatus.class);
    healthStatus.setHealthReport("");
    healthStatus.setIsNodeHealthy(isHealthy);
    healthStatus.setLastHealthReportTime(1);
    status.setNodeHealthStatus(healthStatus);
    req.setNodeStatus(status);
    req.setLastKnownContainerTokenMasterKey(this.currentContainerTokenMasterKey);
    req.setLastKnownNMTokenMasterKey(this.currentNMTokenMasterKey);
    NodeHeartbeatResponse heartbeatResponse =
        resourceTracker.nodeHeartbeat(req);
    
    MasterKey masterKeyFromRM = heartbeatResponse.getContainerTokenMasterKey();
    if (masterKeyFromRM != null
        && masterKeyFromRM.getKeyId() != this.currentContainerTokenMasterKey
            .getKeyId()) {
      this.currentContainerTokenMasterKey = masterKeyFromRM;
    }

    masterKeyFromRM = heartbeatResponse.getNMTokenMasterKey();
    if (masterKeyFromRM != null
        && masterKeyFromRM.getKeyId() != this.currentNMTokenMasterKey
            .getKeyId()) {
      this.currentNMTokenMasterKey = masterKeyFromRM;
    }

    Resource newResource = heartbeatResponse.getResource();
    if (newResource != null) {
      memory = newResource.getMemorySize();
      vCores = newResource.getVirtualCores();
    }

    return heartbeatResponse;
  }

  public long getMemory() {
    return memory;
  }

  public int getvCores() {
    return vCores;
  }
}

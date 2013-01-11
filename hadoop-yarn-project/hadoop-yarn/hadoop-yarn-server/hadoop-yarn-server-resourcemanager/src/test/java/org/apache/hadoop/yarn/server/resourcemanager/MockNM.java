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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.RegistrationResponse;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;

public class MockNM {

  private int responseId;
  private NodeId nodeId;
  private final int memory;
  private ResourceTrackerService resourceTracker;
  private final int httpPort = 2;
  private MasterKey currentMasterKey;

  public MockNM(String nodeIdStr, int memory, ResourceTrackerService resourceTracker) {
    this.memory = memory;
    this.resourceTracker = resourceTracker;
    String[] splits = nodeIdStr.split(":");
    nodeId = Records.newRecord(NodeId.class);
    nodeId.setHost(splits[0]);
    nodeId.setPort(Integer.parseInt(splits[1]));
  }

  public NodeId getNodeId() {
    return nodeId;
  }

  public int getHttpPort() {
    return httpPort;
  }
  
  void setResourceTrackerService(ResourceTrackerService resourceTracker) {
    this.resourceTracker = resourceTracker;
  }

  public void containerStatus(Container container) throws Exception {
    Map<ApplicationId, List<ContainerStatus>> conts = 
        new HashMap<ApplicationId, List<ContainerStatus>>();
    conts.put(container.getId().getApplicationAttemptId().getApplicationId(), 
        Arrays.asList(new ContainerStatus[] { container.getContainerStatus() }));
    nodeHeartbeat(conts, true);
  }

  public RegistrationResponse registerNode() throws Exception {
    RegisterNodeManagerRequest req = Records.newRecord(
        RegisterNodeManagerRequest.class);
    req.setNodeId(nodeId);
    req.setHttpPort(httpPort);
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemory(memory);
    req.setResource(resource);
    RegistrationResponse registrationResponse =
        resourceTracker.registerNodeManager(req).getRegistrationResponse();
    this.currentMasterKey = registrationResponse.getMasterKey();
    return registrationResponse;
  }

  public HeartbeatResponse nodeHeartbeat(boolean isHealthy) throws Exception {
    return nodeHeartbeat(new HashMap<ApplicationId, List<ContainerStatus>>(),
        isHealthy, ++responseId);
  }

  public HeartbeatResponse nodeHeartbeat(ApplicationAttemptId attemptId,
      int containerId, ContainerState containerState) throws Exception {
    HashMap<ApplicationId, List<ContainerStatus>> nodeUpdate =
        new HashMap<ApplicationId, List<ContainerStatus>>(1);
    ContainerStatus amContainerStatus = BuilderUtils.newContainerStatus(
        BuilderUtils.newContainerId(attemptId, 1),
        ContainerState.COMPLETE, "Success", 0);
    ArrayList<ContainerStatus> containerStatusList =
        new ArrayList<ContainerStatus>(1);
    containerStatusList.add(amContainerStatus);
    nodeUpdate.put(attemptId.getApplicationId(), containerStatusList);
    return nodeHeartbeat(nodeUpdate, true);
  }

  public HeartbeatResponse nodeHeartbeat(Map<ApplicationId, 
      List<ContainerStatus>> conts, boolean isHealthy) throws Exception {
    return nodeHeartbeat(conts, isHealthy, ++responseId);
  }

  public HeartbeatResponse nodeHeartbeat(Map<ApplicationId, 
      List<ContainerStatus>> conts, boolean isHealthy, int resId) throws Exception {
    NodeHeartbeatRequest req = Records.newRecord(NodeHeartbeatRequest.class);
    NodeStatus status = Records.newRecord(NodeStatus.class);
    status.setResponseId(resId);
    status.setNodeId(nodeId);
    for (Map.Entry<ApplicationId, List<ContainerStatus>> entry : conts.entrySet()) {
      status.setContainersStatuses(entry.getValue());
    }
    NodeHealthStatus healthStatus = Records.newRecord(NodeHealthStatus.class);
    healthStatus.setHealthReport("");
    healthStatus.setIsNodeHealthy(isHealthy);
    healthStatus.setLastHealthReportTime(1);
    status.setNodeHealthStatus(healthStatus);
    req.setNodeStatus(status);
    req.setLastKnownMasterKey(this.currentMasterKey);
    HeartbeatResponse heartbeatResponse =
        resourceTracker.nodeHeartbeat(req).getHeartbeatResponse();
    MasterKey masterKeyFromRM = heartbeatResponse.getMasterKey();
    this.currentMasterKey =
        (masterKeyFromRM != null
            && masterKeyFromRM.getKeyId() != this.currentMasterKey.getKeyId()
            ? masterKeyFromRM : this.currentMasterKey);
    return heartbeatResponse;
  }

}

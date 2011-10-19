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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.util.Records;

public class MockNM {

  private int responseId;
  private NodeId nodeId;
  private final String nodeIdStr;
  private final int memory;
  private final ResourceTrackerService resourceTracker;
  private final int httpPort = 2;

  MockNM(String nodeIdStr, int memory, ResourceTrackerService resourceTracker) {
    this.nodeIdStr = nodeIdStr;
    this.memory = memory;
    this.resourceTracker = resourceTracker;
  }

  public NodeId getNodeId() {
    return nodeId;
  }

  public String getHttpAddress() {
    return nodeId.getHost() + ":" + String.valueOf(httpPort);
  }

  public void containerStatus(Container container) throws Exception {
    Map<ApplicationId, List<ContainerStatus>> conts = 
        new HashMap<ApplicationId, List<ContainerStatus>>();
    conts.put(container.getId().getApplicationAttemptId().getApplicationId(), 
        Arrays.asList(new ContainerStatus[] { container.getContainerStatus() }));
    nodeHeartbeat(conts, true);
  }

  public NodeId registerNode() throws Exception {
    String[] splits = nodeIdStr.split(":");
    nodeId = Records.newRecord(NodeId.class);
    nodeId.setHost(splits[0]);
    nodeId.setPort(Integer.parseInt(splits[1]));
    RegisterNodeManagerRequest req = Records.newRecord(
        RegisterNodeManagerRequest.class);
    req.setNodeId(nodeId);
    req.setHttpPort(httpPort);
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemory(memory);
    req.setResource(resource);
    resourceTracker.registerNodeManager(req);
    return nodeId;
  }

  public HeartbeatResponse nodeHeartbeat(boolean b) throws Exception {
    return nodeHeartbeat(new HashMap<ApplicationId, List<ContainerStatus>>(), b);
  }

  public HeartbeatResponse nodeHeartbeat(Map<ApplicationId, 
      List<ContainerStatus>> conts, boolean isHealthy) throws Exception {
    NodeHeartbeatRequest req = Records.newRecord(NodeHeartbeatRequest.class);
    NodeStatus status = Records.newRecord(NodeStatus.class);
    status.setNodeId(nodeId);
    for (Map.Entry<ApplicationId, List<ContainerStatus>> entry : conts.entrySet()) {
      status.setContainersStatuses(entry.getValue());
    }
    NodeHealthStatus healthStatus = Records.newRecord(NodeHealthStatus.class);
    healthStatus.setHealthReport("");
    healthStatus.setIsNodeHealthy(isHealthy);
    healthStatus.setLastHealthReportTime(1);
    status.setNodeHealthStatus(healthStatus);
    status.setResponseId(++responseId);
    req.setNodeStatus(status);
    return resourceTracker.nodeHeartbeat(req).getHeartbeatResponse();
  }

}

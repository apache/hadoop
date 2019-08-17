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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.api.records.NodeAttribute;

public abstract class NodeHeartbeatRequest {
  
  public static NodeHeartbeatRequest newInstance(NodeStatus nodeStatus,
      MasterKey lastKnownContainerTokenMasterKey,
      MasterKey lastKnownNMTokenMasterKey, Set<NodeLabel> nodeLabels) {
    NodeHeartbeatRequest nodeHeartbeatRequest =
        Records.newRecord(NodeHeartbeatRequest.class);
    nodeHeartbeatRequest.setNodeStatus(nodeStatus);
    nodeHeartbeatRequest
        .setLastKnownContainerTokenMasterKey(lastKnownContainerTokenMasterKey);
    nodeHeartbeatRequest
        .setLastKnownNMTokenMasterKey(lastKnownNMTokenMasterKey);
    nodeHeartbeatRequest.setNodeLabels(nodeLabels);
    return nodeHeartbeatRequest;
  }

  public static NodeHeartbeatRequest newInstance(NodeStatus nodeStatus,
      MasterKey lastKnownContainerTokenMasterKey,
      MasterKey lastKnownNMTokenMasterKey, Set<NodeLabel> nodeLabels,
      Map<ApplicationId, AppCollectorData> registeringCollectors) {
    NodeHeartbeatRequest nodeHeartbeatRequest =
        Records.newRecord(NodeHeartbeatRequest.class);
    nodeHeartbeatRequest.setNodeStatus(nodeStatus);
    nodeHeartbeatRequest
        .setLastKnownContainerTokenMasterKey(lastKnownContainerTokenMasterKey);
    nodeHeartbeatRequest
        .setLastKnownNMTokenMasterKey(lastKnownNMTokenMasterKey);
    nodeHeartbeatRequest.setNodeLabels(nodeLabels);
    nodeHeartbeatRequest.setRegisteringCollectors(registeringCollectors);
    return nodeHeartbeatRequest;
  }

  public static NodeHeartbeatRequest newInstance(NodeStatus nodeStatus,
      MasterKey lastKnownContainerTokenMasterKey,
      MasterKey lastKnownNMTokenMasterKey, Set<NodeLabel> nodeLabels,
      Set<NodeAttribute> nodeAttributes,
      Map<ApplicationId, AppCollectorData> registeringCollectors) {
    NodeHeartbeatRequest request = NodeHeartbeatRequest
        .newInstance(nodeStatus, lastKnownContainerTokenMasterKey,
            lastKnownNMTokenMasterKey, nodeLabels, registeringCollectors);
    request.setNodeAttributes(nodeAttributes);
    return request;
  }

  public abstract NodeStatus getNodeStatus();
  public abstract void setNodeStatus(NodeStatus status);

  public abstract MasterKey getLastKnownContainerTokenMasterKey();
  public abstract void setLastKnownContainerTokenMasterKey(MasterKey secretKey);
  
  public abstract MasterKey getLastKnownNMTokenMasterKey();
  public abstract void setLastKnownNMTokenMasterKey(MasterKey secretKey);
  
  public abstract Set<NodeLabel> getNodeLabels();
  public abstract void setNodeLabels(Set<NodeLabel> nodeLabels);

  public abstract List<LogAggregationReport>
      getLogAggregationReportsForApps();

  public abstract void setLogAggregationReportsForApps(
      List<LogAggregationReport> logAggregationReportsForApps);

  // This tells RM registered collectors' address info on this node
  public abstract Map<ApplicationId, AppCollectorData>
      getRegisteringCollectors();

  public abstract void setRegisteringCollectors(Map<ApplicationId,
      AppCollectorData> appCollectorsMap);

  public abstract Set<NodeAttribute> getNodeAttributes();

  public abstract void setNodeAttributes(Set<NodeAttribute> nodeAttributes);

  public abstract void setTokenSequenceNo(long tokenSequenceNo);

  public abstract long getTokenSequenceNo();
}

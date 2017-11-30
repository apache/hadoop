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

package org.apache.hadoop.yarn.sls.scheduler;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode
        .UpdatedContainerInfo;

import java.util.Collections;
import java.util.List;
import java.util.Set;

@Private
@Unstable
public class RMNodeWrapper implements RMNode {
  private RMNode node;
  private List<UpdatedContainerInfo> updates;
  private boolean pulled = false;
  
  public RMNodeWrapper(RMNode node) {
    this.node = node;
    updates = node.pullContainerUpdates();
  }
  
  @Override
  public NodeId getNodeID() {
    return node.getNodeID();
  }

  @Override
  public String getHostName() {
    return node.getHostName();
  }

  @Override
  public int getCommandPort() {
    return node.getCommandPort();
  }

  @Override
  public int getHttpPort() {
    return node.getHttpPort();
  }

  @Override
  public String getNodeAddress() {
    return node.getNodeAddress();
  }

  @Override
  public String getHttpAddress() {
    return node.getHttpAddress();
  }

  @Override
  public String getHealthReport() {
    return node.getHealthReport();
  }

  @Override
  public long getLastHealthReportTime() {
    return node.getLastHealthReportTime();
  }

  @Override
  public Resource getTotalCapability() {
    return node.getTotalCapability();
  }

  @Override
  public String getRackName() {
    return node.getRackName();
  }

  @Override
  public Node getNode() {
    return node.getNode();
  }

  @Override
  public NodeState getState() {
    return node.getState();
  }

  @Override
  public List<ContainerId> getContainersToCleanUp() {
    return node.getContainersToCleanUp();
  }

  @Override
  public List<ApplicationId> getAppsToCleanup() {
    return node.getAppsToCleanup();
  }

  @Override
  public List<ApplicationId> getRunningApps() {
    return node.getRunningApps();
  }

  @Override
  public void updateNodeHeartbeatResponseForCleanup(
          NodeHeartbeatResponse nodeHeartbeatResponse) {
    node.updateNodeHeartbeatResponseForCleanup(nodeHeartbeatResponse);
  }

  @Override
  public NodeHeartbeatResponse getLastNodeHeartBeatResponse() {
    return node.getLastNodeHeartBeatResponse();
  }

  @Override
  public void resetLastNodeHeartBeatResponse() {
    node.getLastNodeHeartBeatResponse().setResponseId(0);
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<UpdatedContainerInfo> pullContainerUpdates() {
    List<UpdatedContainerInfo> list = Collections.EMPTY_LIST;
    if (! pulled) {
      list = updates;
      pulled = true;
    }
    return list;    
  }
  
  List<UpdatedContainerInfo> getContainerUpdates() {
    return updates;
  }

  @Override
  public String getNodeManagerVersion() {
    return node.getNodeManagerVersion();
  }

  @Override
  public Set<String> getNodeLabels() {
    return RMNodeLabelsManager.EMPTY_STRING_SET;
  }

  @Override
  public void updateNodeHeartbeatResponseForUpdatedContainers(
      NodeHeartbeatResponse response) {
    // TODO Auto-generated method stub
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<Container> pullNewlyIncreasedContainers() {
    // TODO Auto-generated method stub
    return Collections.EMPTY_LIST;
  }

  public OpportunisticContainersStatus getOpportunisticContainersStatus() {
    return null;
  }

  @Override
  public ResourceUtilization getAggregatedContainersUtilization() {
    return node.getAggregatedContainersUtilization();
  }

  @Override
  public ResourceUtilization getNodeUtilization() {
    return node.getNodeUtilization();
  }

  @Override
  public long getUntrackedTimeStamp() {
    return 0;
  }

  @Override
  public void setUntrackedTimeStamp(long timeStamp) {
  }

  @Override
  public Integer getDecommissioningTimeout() {
    return null;
  }

  @Override
  public Resource getPhysicalResource() {
    return null;
  }
}

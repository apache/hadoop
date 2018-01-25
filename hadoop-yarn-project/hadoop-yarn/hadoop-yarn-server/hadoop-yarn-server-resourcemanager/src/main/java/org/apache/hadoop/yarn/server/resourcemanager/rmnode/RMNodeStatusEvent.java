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

package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;

public class RMNodeStatusEvent extends RMNodeEvent {

  private final NodeStatus nodeStatus;
  private List<LogAggregationReport> logAggregationReportsForApps;

  public RMNodeStatusEvent(NodeId nodeId, NodeStatus nodeStatus) {
    this(nodeId, nodeStatus, null);
  }

  public RMNodeStatusEvent(NodeId nodeId, NodeStatus nodeStatus,
      List<LogAggregationReport> logAggregationReportsForApps) {
    super(nodeId, RMNodeEventType.STATUS_UPDATE);
    this.nodeStatus = nodeStatus;
    this.logAggregationReportsForApps = logAggregationReportsForApps;
  }

  public NodeHealthStatus getNodeHealthStatus() {
    return this.nodeStatus.getNodeHealthStatus();
  }

  public List<ContainerStatus> getContainers() {
    return this.nodeStatus.getContainersStatuses();
  }

  public List<ApplicationId> getKeepAliveAppIds() {
    return this.nodeStatus.getKeepAliveApplications();
  }

  public ResourceUtilization getAggregatedContainersUtilization() {
    return this.nodeStatus.getContainersUtilization();
  }

  public ResourceUtilization getNodeUtilization() {
    return this.nodeStatus.getNodeUtilization();
  }

  public List<LogAggregationReport> getLogAggregationReportsForApps() {
    return this.logAggregationReportsForApps;
  }

  public OpportunisticContainersStatus getOpportunisticContainersStatus() {
    return this.nodeStatus.getOpportunisticContainersStatus();
  }

  public void setLogAggregationReportsForApps(
      List<LogAggregationReport> logAggregationReportsForApps) {
    this.logAggregationReportsForApps = logAggregationReportsForApps;
  }
  
  @SuppressWarnings("unchecked")
  public List<Container> getNMReportedIncreasedContainers() {
    return this.nodeStatus.getIncreasedContainers() == null ?
        Collections.EMPTY_LIST : this.nodeStatus.getIncreasedContainers();
  }


}
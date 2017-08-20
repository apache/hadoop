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


import java.util.List;
import java.util.Set;

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

/**
 * Node managers information on available resources 
 * and other static information.
 *
 */
public interface RMNode {

  /**
   * the node id of of this node.
   * @return the node id of this node.
   */
  public NodeId getNodeID();
  
  /**
   * the hostname of this node
   * @return hostname of this node
   */
  public String getHostName();
  
  /**
   * the command port for this node
   * @return command port for this node
   */
  public int getCommandPort();
  
  /**
   * the http port for this node
   * @return http port for this node
   */
  public int getHttpPort();


  /**
   * the ContainerManager address for this node.
   * @return the ContainerManager address for this node.
   */
  public String getNodeAddress();
  
  /**
   * the http-Address for this node.
   * @return the http-url address for this node
   */
  public String getHttpAddress();
  
  /**
   * the latest health report received from this node.
   * @return the latest health report received from this node.
   */
  public String getHealthReport();
  
  /**
   * the time of the latest health report received from this node.
   * @return the time of the latest health report received from this node.
   */
  public long getLastHealthReportTime();

  /**
   * the node manager version of the node received as part of the
   * registration with the resource manager
   */
  public String getNodeManagerVersion();

  /**
   * the total available resource.
   * @return the total available resource.
   */
  public Resource getTotalCapability();

  /**
   * the aggregated resource utilization of the containers.
   * @return the aggregated resource utilization of the containers.
   */
  public ResourceUtilization getAggregatedContainersUtilization();

  /**
   * the total resource utilization of the node.
   * @return the total resource utilization of the node.
   */
  public ResourceUtilization getNodeUtilization();

  /**
   * the physical resources in the node.
   * @return the physical resources in the node.
   */
  Resource getPhysicalResource();

  /**
   * The rack name for this node manager.
   * @return the rack name.
   */
  public String getRackName();
  
  /**
   * the {@link Node} information for this node.
   * @return {@link Node} information for this node.
   */
  public Node getNode();
  
  public NodeState getState();

  public List<ContainerId> getContainersToCleanUp();

  public List<ApplicationId> getAppsToCleanup();

  List<ApplicationId> getRunningApps();

  /**
   * Update a {@link NodeHeartbeatResponse} with the list of containers and
   * applications to clean up for this node.
   * @param response the {@link NodeHeartbeatResponse} to update
   */
  void updateNodeHeartbeatResponseForCleanup(NodeHeartbeatResponse response);

  public NodeHeartbeatResponse getLastNodeHeartBeatResponse();

  /**
   * Reset lastNodeHeartbeatResponse's ID to 0.
   */
  void resetLastNodeHeartBeatResponse();

  /**
   * Get and clear the list of containerUpdates accumulated across NM
   * heartbeats.
   * 
   * @return containerUpdates accumulated across NM heartbeats.
   */
  public List<UpdatedContainerInfo> pullContainerUpdates();
  
  /**
   * Get set of labels in this node
   * 
   * @return labels in this node
   */
  public Set<String> getNodeLabels();
  
  /**
   * Update containers to be updated
   */
  void updateNodeHeartbeatResponseForUpdatedContainers(
      NodeHeartbeatResponse response);
  
  public List<Container> pullNewlyIncreasedContainers();

  OpportunisticContainersStatus getOpportunisticContainersStatus();

  long getUntrackedTimeStamp();

  void setUntrackedTimeStamp(long timeStamp);
  /*
   * Optional decommissioning timeout in second
   * (null indicates default timeout).
   * @return the decommissioning timeout in second.
   */
  Integer getDecommissioningTimeout();
}

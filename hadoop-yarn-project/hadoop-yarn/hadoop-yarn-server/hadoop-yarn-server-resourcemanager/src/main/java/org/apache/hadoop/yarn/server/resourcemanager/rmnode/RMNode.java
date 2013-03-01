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

import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;

/**
 * Node managers information on available resources 
 * and other static information.
 *
 */
public interface RMNode {

  public static final String ANY = "*";

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
   * the health-status for this node
   * @return the health-status for this node.
   */
  public NodeHealthStatus getNodeHealthStatus();
  
  /**
   * the total available resource.
   * @return the total available resource.
   */
  public org.apache.hadoop.yarn.api.records.Resource getTotalCapability();
  
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

  /**
   * Update a {@link HeartbeatResponse} with the list of containers and
   * applications to clean up for this node.
   * @param response the {@link HeartbeatResponse} to update
   */
  public void updateHeartbeatResponseForCleanup(HeartbeatResponse response);

  public HeartbeatResponse getLastHeartBeatResponse();
  
  /**
   * Get and clear the list of containerUpdates accumulated across NM
   * heartbeats.
   * 
   * @return containerUpdates accumulated across NM heartbeats.
   */
  public List<UpdatedContainerInfo> pullContainerUpdates();
  
}

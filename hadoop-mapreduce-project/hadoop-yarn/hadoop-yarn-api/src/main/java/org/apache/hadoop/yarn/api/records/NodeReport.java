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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ClientRMProtocol;

/**
 * <p><code>NodeReport</code> is a summary of runtime information of a 
 * node in the cluster.</p>
 * 
 * <p>It includes details such as:
 *   <ul>
 *     <li>{@link NodeId} of the node.</li>
 *     <li>HTTP Tracking URL of the node.</li>
 *     <li>Rack name for the node.</li>
 *     <li>Used {@link Resource} on the node.</li>
 *     <li>Total available {@link Resource} of the node.</li>
 *     <li>Number of running containers on the node.</li>
 *     <li>{@link NodeHealthStatus} of the node.</li>
 *   </ul>
 * </p>
 *
 * @see NodeHealthStatus
 * @see ClientRMProtocol#getClusterNodes(org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest)
 */
@Public
@Stable
public interface NodeReport {
  /**
   * Get the <code>NodeId</code> of the node.
   * @return <code>NodeId</code> of the node
   */
  NodeId getNodeId();
  
  @Private
  @Unstable
  void setNodeId(NodeId nodeId);
  
  /**
   * Get the <em>http address</em> of the node.
   * @return <em>http address</em> of the node
   */
  @Public
  @Stable
  String getHttpAddress();
  
  @Private
  @Unstable
  void setHttpAddress(String httpAddress);
  
  /**
   * Get the <em>rack name</em> for the node.
   * @return <em>rack name</em> for the node
   */
  @Public
  @Stable
  String getRackName();
  
  @Private
  @Unstable
  void setRackName(String rackName);
  
  /**
   * Get <em>used</em> <code>Resource</code> on the node.
   * @return <em>used</em> <code>Resource</code> on the node
   */
  @Public
  @Stable
  Resource getUsed();        
  
  @Private
  @Unstable
  void setUsed(Resource used);
  
  /**
   * Get the <em>total</em> <code>Resource</code> on the node.
   * @return <em>total</em> <code>Resource</code> on the node
   */
  @Public
  @Stable
  Resource getCapability();
  
  @Private
  @Unstable
  void setCapability(Resource capability);
  
  /**
   * Get the <em>number of running containers</em> on the node.
   * @return <em>number of running containers</em> on the node
   */
  @Public
  @Stable
  int getNumContainers();
  
  @Private
  @Unstable
  void setNumContainers(int numContainers);
  
  /**
   * Get the <code>NodeHealthStatus</code> of the node. 
   * @return <code>NodeHealthStatus</code> of the node
   */
  @Public
  @Stable
  NodeHealthStatus getNodeHealthStatus();
  
  @Private
  @Unstable
  void setNodeHealthStatus(NodeHealthStatus nodeHealthStatus);
}

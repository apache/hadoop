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

import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.util.Records;

/**
 * {@code NodeReport} is a summary of runtime information of a node
 * in the cluster.
 * <p>
 * It includes details such as:
 * <ul>
 *   <li>{@link NodeId} of the node.</li>
 *   <li>HTTP Tracking URL of the node.</li>
 *   <li>Rack name for the node.</li>
 *   <li>Used {@link Resource} on the node.</li>
 *   <li>Total available {@link Resource} of the node.</li>
 *   <li>Number of running containers on the node.</li>
 * </ul>
 *
 * @see ApplicationClientProtocol#getClusterNodes(org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest)
 */
@Public
@Stable
public abstract class NodeReport {
  
  @Private
  @Unstable
  public static NodeReport newInstance(NodeId nodeId, NodeState nodeState,
      String httpAddress, String rackName, Resource used, Resource capability,
      int numContainers, String healthReport, long lastHealthReportTime) {
    return newInstance(nodeId, nodeState, httpAddress, rackName, used,
        capability, numContainers, healthReport, lastHealthReportTime, null);
  }

  @Private
  @Unstable
  public static NodeReport newInstance(NodeId nodeId, NodeState nodeState,
      String httpAddress, String rackName, Resource used, Resource capability,
      int numContainers, String healthReport, long lastHealthReportTime,
      Set<String> nodeLabels) {
    NodeReport nodeReport = Records.newRecord(NodeReport.class);
    nodeReport.setNodeId(nodeId);
    nodeReport.setNodeState(nodeState);
    nodeReport.setHttpAddress(httpAddress);
    nodeReport.setRackName(rackName);
    nodeReport.setUsed(used);
    nodeReport.setCapability(capability);
    nodeReport.setNumContainers(numContainers);
    nodeReport.setHealthReport(healthReport);
    nodeReport.setLastHealthReportTime(lastHealthReportTime);
    nodeReport.setNodeLabels(nodeLabels);
    return nodeReport;
  }

  /**
   * Get the <code>NodeId</code> of the node.
   * @return <code>NodeId</code> of the node
   */
  @Public
  @Stable
  public abstract NodeId getNodeId();
  
  @Private
  @Unstable
  public abstract void setNodeId(NodeId nodeId);
  
  /**
   * Get the <code>NodeState</code> of the node.
   * @return <code>NodeState</code> of the node
   */
  @Public
  @Stable
  public abstract NodeState getNodeState();
  
  @Private
  @Unstable
  public abstract void setNodeState(NodeState nodeState);
  
  /**
   * Get the <em>http address</em> of the node.
   * @return <em>http address</em> of the node
   */
  @Public
  @Stable
  public abstract String getHttpAddress();
  
  @Private
  @Unstable
  public abstract void setHttpAddress(String httpAddress);
  
  /**
   * Get the <em>rack name</em> for the node.
   * @return <em>rack name</em> for the node
   */
  @Public
  @Stable
  public abstract String getRackName();
  
  @Private
  @Unstable
  public abstract void setRackName(String rackName);
  
  /**
   * Get <em>used</em> <code>Resource</code> on the node.
   * @return <em>used</em> <code>Resource</code> on the node
   */
  @Public
  @Stable
  public abstract Resource getUsed();
  
  @Private
  @Unstable
  public abstract void setUsed(Resource used);
  
  /**
   * Get the <em>total</em> <code>Resource</code> on the node.
   * @return <em>total</em> <code>Resource</code> on the node
   */
  @Public
  @Stable
  public abstract Resource getCapability();
  
  @Private
  @Unstable
  public abstract void setCapability(Resource capability);
  
  /**
   * Get the <em>number of allocated containers</em> on the node.
   * @return <em>number of allocated containers</em> on the node
   */
  @Private
  @Unstable
  public abstract int getNumContainers();
  
  @Private
  @Unstable
  public abstract void setNumContainers(int numContainers);
  

  /** 
   * Get the <em>diagnostic health report</em> of the node.
   * @return <em>diagnostic health report</em> of the node
   */
  @Public
  @Stable
  public abstract String getHealthReport();

  @Private
  @Unstable
  public abstract void setHealthReport(String healthReport);

  /**
   * Get the <em>last timestamp</em> at which the health report was received.
   * @return <em>last timestamp</em> at which the health report was received
   */
  @Public
  @Stable
  public abstract long getLastHealthReportTime();

  @Private
  @Unstable
  public abstract void setLastHealthReportTime(long lastHealthReport);
  
  /**
   * Get labels of this node
   * @return labels of this node
   */
  @Public
  @Stable
  public abstract Set<String> getNodeLabels();
  
  @Private
  @Unstable
  public abstract void setNodeLabels(Set<String> nodeLabels);
}

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
package org.apache.hadoop.yarn.server.api.records;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.util.Records;

/**
 * {@code NodeStatus} is a summary of the status of the node.
 * <p>
 * It includes information such as:
 * <ul>
 *   <li>Node information and status..</li>
 *   <li>Container status.</li>
 * </ul>
 */
public abstract class NodeStatus {

  /**
   * Create a new {@code NodeStatus}.
   * @param nodeId Identifier for this node.
   * @param responseId Identifier for the response.
   * @param containerStatuses Status of the containers running in this node.
   * @param keepAliveApplications Applications to keep alive.
   * @param nodeHealthStatus Health status of the node.
   * @param containersUtilization Utilization of the containers in this node.
   * @param nodeUtilization Utilization of the node.
   * @param increasedContainers Containers whose resource has been increased.
   * @return New {@code NodeStatus} with the provided information.
   */
  public static NodeStatus newInstance(NodeId nodeId, int responseId,
      List<ContainerStatus> containerStatuses,
      List<ApplicationId> keepAliveApplications,
      NodeHealthStatus nodeHealthStatus,
      ResourceUtilization containersUtilization,
      ResourceUtilization nodeUtilization,
      List<Container> increasedContainers) {
    NodeStatus nodeStatus = Records.newRecord(NodeStatus.class);
    nodeStatus.setResponseId(responseId);
    nodeStatus.setNodeId(nodeId);
    nodeStatus.setContainersStatuses(containerStatuses);
    nodeStatus.setKeepAliveApplications(keepAliveApplications);
    nodeStatus.setNodeHealthStatus(nodeHealthStatus);
    nodeStatus.setContainersUtilization(containersUtilization);
    nodeStatus.setNodeUtilization(nodeUtilization);
    nodeStatus.setIncreasedContainers(increasedContainers);
    return nodeStatus;
  }

  public abstract NodeId getNodeId();
  public abstract int getResponseId();
  
  public abstract List<ContainerStatus> getContainersStatuses();
  public abstract void setContainersStatuses(
      List<ContainerStatus> containersStatuses);

  public abstract List<ApplicationId> getKeepAliveApplications();
  public abstract void setKeepAliveApplications(List<ApplicationId> appIds);
  
  public abstract NodeHealthStatus getNodeHealthStatus();
  public abstract void setNodeHealthStatus(NodeHealthStatus healthStatus);

  public abstract void setNodeId(NodeId nodeId);
  public abstract void setResponseId(int responseId);

  /**
   * Get the <em>resource utilization</em> of the containers.
   * @return <em>resource utilization</em> of the containers
   */
  @Public
  @Stable
  public abstract ResourceUtilization getContainersUtilization();

  @Private
  @Unstable
  public abstract void setContainersUtilization(
      ResourceUtilization containersUtilization);

  /**
   * Get the <em>resource utilization</em> of the node.
   * @return <em>resource utilization</em> of the node
   */
  @Public
  @Stable
  public abstract ResourceUtilization getNodeUtilization();

  @Private
  @Unstable
  public abstract void setNodeUtilization(
      ResourceUtilization nodeUtilization);

  @Public
  @Unstable
  public abstract List<Container> getIncreasedContainers();

  @Private
  @Unstable
  public abstract void setIncreasedContainers(
      List<Container> increasedContainers);

  @Private
  @Unstable
  public abstract QueuedContainersStatus getQueuedContainersStatus();

  @Private
  @Unstable
  public abstract void setQueuedContainersStatus(
      QueuedContainersStatus queuedContainersStatus);
}

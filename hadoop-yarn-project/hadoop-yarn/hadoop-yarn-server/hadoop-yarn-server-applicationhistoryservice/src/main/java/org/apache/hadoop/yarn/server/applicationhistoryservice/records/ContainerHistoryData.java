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

package org.apache.hadoop.yarn.server.applicationhistoryservice.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.List;
import java.util.Map;

/**
 * The class contains all the fields that are stored persistently for
 * <code>RMContainer</code>.
 */
@Public
@Unstable
public class ContainerHistoryData {

  private ContainerId containerId;

  private Resource allocatedResource;

  private NodeId assignedNode;

  private Priority priority;

  private long startTime;

  private long finishTime;

  private String diagnosticsInfo;

  private int containerExitStatus;

  private ContainerState containerState;

  private Map<String, List<Map<String, String>>> exposedPorts;

  @Public
  @Unstable
  public static ContainerHistoryData newInstance(ContainerId containerId,
      Resource allocatedResource, NodeId assignedNode, Priority priority,
      long startTime, long finishTime, String diagnosticsInfo,
      int containerExitCode, ContainerState containerState) {
    ContainerHistoryData containerHD = new ContainerHistoryData();
    containerHD.setContainerId(containerId);
    containerHD.setAllocatedResource(allocatedResource);
    containerHD.setAssignedNode(assignedNode);
    containerHD.setPriority(priority);
    containerHD.setStartTime(startTime);
    containerHD.setFinishTime(finishTime);
    containerHD.setDiagnosticsInfo(diagnosticsInfo);
    containerHD.setContainerExitStatus(containerExitCode);
    containerHD.setContainerState(containerState);

    return containerHD;
  }

  @Public
  @Unstable
  public ContainerId getContainerId() {
    return containerId;
  }

  @Public
  @Unstable
  public void setContainerId(ContainerId containerId) {
    this.containerId = containerId;
  }

  @Public
  @Unstable
  public Resource getAllocatedResource() {
    return allocatedResource;
  }

  @Public
  @Unstable
  public void setAllocatedResource(Resource resource) {
    this.allocatedResource = resource;
  }

  @Public
  @Unstable
  public NodeId getAssignedNode() {
    return assignedNode;
  }

  @Public
  @Unstable
  public void setAssignedNode(NodeId nodeId) {
    this.assignedNode = nodeId;
  }

  @Public
  @Unstable
  public Priority getPriority() {
    return priority;
  }

  @Public
  @Unstable
  public void setPriority(Priority priority) {
    this.priority = priority;
  }

  @Public
  @Unstable
  public long getStartTime() {
    return startTime;
  }

  @Public
  @Unstable
  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  @Public
  @Unstable
  public long getFinishTime() {
    return finishTime;
  }

  @Public
  @Unstable
  public void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  @Public
  @Unstable
  public String getDiagnosticsInfo() {
    return diagnosticsInfo;
  }

  @Public
  @Unstable
  public void setDiagnosticsInfo(String diagnosticsInfo) {
    this.diagnosticsInfo = diagnosticsInfo;
  }

  @Public
  @Unstable
  public int getContainerExitStatus() {
    return containerExitStatus;
  }

  @Public
  @Unstable
  public void setContainerExitStatus(int containerExitStatus) {
    this.containerExitStatus = containerExitStatus;
  }

  @Public
  @Unstable
  public ContainerState getContainerState() {
    return containerState;
  }

  @Public
  @Unstable
  public void setContainerState(ContainerState containerState) {
    this.containerState = containerState;
  }

  public Map<String, List<Map<String, String>>> getExposedPorts() {
    return exposedPorts;
  }

  public void setExposedPorts(Map<String, List<Map<String, String>>> ports) {
    this.exposedPorts = ports;
  }
}

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
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;
import java.util.Map;

/**
 * {@code ContainerReport} is a report of an container.
 * <p>
 * It includes details such as:
 * <ul>
 *   <li>{@link ContainerId} of the container.</li>
 *   <li>Allocated Resources to the container.</li>
 *   <li>Assigned Node id.</li>
 *   <li>Assigned Priority.</li>
 *   <li>Creation Time.</li>
 *   <li>Finish Time.</li>
 *   <li>Container Exit Status.</li>
 *   <li>{@link ContainerState} of the container.</li>
 *   <li>Diagnostic information in case of errors.</li>
 *   <li>Log URL.</li>
 *   <li>nodeHttpAddress</li>
 * </ul>
 */

@Public
@Unstable
public abstract class ContainerReport {
  @Private
  @Unstable
  public static ContainerReport newInstance(ContainerId containerId,
      Resource allocatedResource, NodeId assignedNode, Priority priority,
      long creationTime, long finishTime, String diagnosticInfo, String logUrl,
      int containerExitStatus, ContainerState containerState,
      String nodeHttpAddress) {
    return newInstance(containerId, allocatedResource, assignedNode, priority,
        creationTime, finishTime, diagnosticInfo, logUrl, containerExitStatus,
        containerState, nodeHttpAddress, ExecutionType.GUARANTEED);
  }

  @Private
  @Unstable
  public static ContainerReport newInstance(ContainerId containerId,
      Resource allocatedResource, NodeId assignedNode, Priority priority,
      long creationTime, long finishTime, String diagnosticInfo, String logUrl,
      int containerExitStatus, ContainerState containerState,
      String nodeHttpAddress, ExecutionType executionType) {
    ContainerReport report = Records.newRecord(ContainerReport.class);
    report.setContainerId(containerId);
    report.setAllocatedResource(allocatedResource);
    report.setAssignedNode(assignedNode);
    report.setPriority(priority);
    report.setCreationTime(creationTime);
    report.setFinishTime(finishTime);
    report.setDiagnosticsInfo(diagnosticInfo);
    report.setLogUrl(logUrl);
    report.setContainerExitStatus(containerExitStatus);
    report.setContainerState(containerState);
    report.setNodeHttpAddress(nodeHttpAddress);
    report.setExecutionType(executionType);

    return report;
  }

  /**
   * Get the <code>ContainerId</code> of the container.
   * 
   * @return <code>ContainerId</code> of the container.
   */
  @Public
  @Unstable
  public abstract ContainerId getContainerId();

  @Public
  @Unstable
  public abstract void setContainerId(ContainerId containerId);

  /**
   * Get the allocated <code>Resource</code> of the container.
   * 
   * @return allocated <code>Resource</code> of the container.
   */
  @Public
  @Unstable
  public abstract Resource getAllocatedResource();

  @Public
  @Unstable
  public abstract void setAllocatedResource(Resource resource);

  /**
   * Get the allocated <code>NodeId</code> where container is running.
   * 
   * @return allocated <code>NodeId</code> where container is running.
   */
  @Public
  @Unstable
  public abstract NodeId getAssignedNode();

  @Public
  @Unstable
  public abstract void setAssignedNode(NodeId nodeId);

  /**
   * Get the allocated <code>Priority</code> of the container.
   * 
   * @return allocated <code>Priority</code> of the container.
   */
  @Public
  @Unstable
  public abstract Priority getPriority();

  @Public
  @Unstable
  public abstract void setPriority(Priority priority);

  /**
   * Get the creation time of the container.
   * 
   * @return creation time of the container
   */
  @Public
  @Unstable
  public abstract long getCreationTime();

  @Public
  @Unstable
  public abstract void setCreationTime(long creationTime);

  /**
   * Get the Finish time of the container.
   * 
   * @return Finish time of the container
   */
  @Public
  @Unstable
  public abstract long getFinishTime();

  @Public
  @Unstable
  public abstract void setFinishTime(long finishTime);

  /**
   * Get the DiagnosticsInfo of the container.
   * 
   * @return DiagnosticsInfo of the container
   */
  @Public
  @Unstable
  public abstract String getDiagnosticsInfo();

  @Public
  @Unstable
  public abstract void setDiagnosticsInfo(String diagnosticsInfo);

  /**
   * Get the LogURL of the container.
   * 
   * @return LogURL of the container
   */
  @Public
  @Unstable
  public abstract String getLogUrl();

  @Public
  @Unstable
  public abstract void setLogUrl(String logUrl);

  /**
   * Get the final <code>ContainerState</code> of the container.
   * 
   * @return final <code>ContainerState</code> of the container.
   */
  @Public
  @Unstable
  public abstract ContainerState getContainerState();

  @Public
  @Unstable
  public abstract void setContainerState(ContainerState containerState);

  /**
   * Get the final <code>exit status</code> of the container.
   * 
   * @return final <code>exit status</code> of the container.
   */
  @Public
  @Unstable
  public abstract int getContainerExitStatus();

  @Public
  @Unstable
  public abstract void setContainerExitStatus(int containerExitStatus);

  /**
   * Get exposed ports of the container.
   * 
   * @return the node exposed ports of the container
   */
  @Public
  @Unstable
  public abstract String getExposedPorts();

  @Private
  @Unstable
  public abstract void setExposedPorts(
      Map<String, List<Map<String, String>>> ports);

  /**
   * Get the Node Http address of the container.
   *
   * @return the node http address of the container
   */
  @Public
  @Unstable
  public abstract String getNodeHttpAddress();

  @Private
  @Unstable
  public abstract void setNodeHttpAddress(String nodeHttpAddress);

  /**
   * Get the execution type of the container.
   *
   * @return the execution type of the container
   */
  @Public
  @Unstable
  public abstract ExecutionType getExecutionType();

  @Private
  @Unstable
  public abstract void setExecutionType(ExecutionType executionType);
}

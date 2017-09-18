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

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.util.Records;

/**
 * NMContainerStatus includes the current information of a container. This
 * record is used by YARN only, whereas {@link ContainerStatus} is used both
 * inside YARN and by end-users.
 */
public abstract class NMContainerStatus {
  
  // Used by tests only
  public static NMContainerStatus newInstance(ContainerId containerId,
      int version, ContainerState containerState, Resource allocatedResource,
      String diagnostics, int containerExitStatus, Priority priority,
      long creationTime) {
    return newInstance(containerId, version, containerState, allocatedResource,
        diagnostics, containerExitStatus, priority, creationTime,
        CommonNodeLabelsManager.NO_LABEL, ExecutionType.GUARANTEED);
  }

  public static NMContainerStatus newInstance(ContainerId containerId,
      int version, ContainerState containerState, Resource allocatedResource,
      String diagnostics, int containerExitStatus, Priority priority,
      long creationTime, String nodeLabelExpression,
      ExecutionType executionType) {
    NMContainerStatus status =
        Records.newRecord(NMContainerStatus.class);
    status.setContainerId(containerId);
    status.setVersion(version);
    status.setContainerState(containerState);
    status.setAllocatedResource(allocatedResource);
    status.setDiagnostics(diagnostics);
    status.setContainerExitStatus(containerExitStatus);
    status.setPriority(priority);
    status.setCreationTime(creationTime);
    status.setNodeLabelExpression(nodeLabelExpression);
    status.setExecutionType(executionType);
    return status;
  }

  /**
   * Get the <code>ContainerId</code> of the container.
   * 
   * @return <code>ContainerId</code> of the container.
   */
  public abstract ContainerId getContainerId();

  public abstract void setContainerId(ContainerId containerId);

  /**
   * Get the allocated <code>Resource</code> of the container.
   * 
   * @return allocated <code>Resource</code> of the container.
   */
  public abstract Resource getAllocatedResource();


  public abstract void setAllocatedResource(Resource resource);

  /**
   * Get the DiagnosticsInfo of the container.
   * 
   * @return DiagnosticsInfo of the container
   */
  public abstract String getDiagnostics();

  public abstract void setDiagnostics(String diagnostics);


  public abstract ContainerState getContainerState();

  public abstract void setContainerState(ContainerState containerState);

  /**
   * Get the final <code>exit status</code> of the container.
   * 
   * @return final <code>exit status</code> of the container.
   */
  public abstract int getContainerExitStatus();


  public abstract void setContainerExitStatus(int containerExitStatus);

  /**
   * Get the <code>Priority</code> of the request.
   * @return <code>Priority</code> of the request
   */
  public abstract Priority getPriority();

  public abstract void setPriority(Priority priority);

  /**
   * Get the time when the container is created
   */
  public abstract long getCreationTime();

  public abstract void setCreationTime(long creationTime);
  
  /**
   * Get the node-label-expression in the original ResourceRequest
   */
  public abstract String getNodeLabelExpression();

  public abstract void setNodeLabelExpression(
      String nodeLabelExpression);

  public int getVersion() {
    return 0;
  }

  public void setVersion(int version) {

  }

  /**
   * Get the <code>ExecutionType</code> of the container.
   * @return <code>ExecutionType</code> of the container
   */
  public ExecutionType getExecutionType() {
    return ExecutionType.GUARANTEED;
  }

  public void setExecutionType(ExecutionType executionType) { }
}

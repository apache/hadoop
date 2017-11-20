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

package org.apache.hadoop.yarn.server.webapp.dao;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.util.Times;

@Public
@Evolving
@XmlRootElement(name = "container")
@XmlAccessorType(XmlAccessType.FIELD)
public class ContainerInfo {

  protected String containerId;
  protected long allocatedMB;
  protected long allocatedVCores;
  protected String assignedNodeId;
  protected int priority;
  protected long startedTime;
  protected long finishedTime;
  protected long elapsedTime;
  protected String diagnosticsInfo;
  protected String logUrl;
  protected int containerExitStatus;
  protected ContainerState containerState;
  protected String nodeHttpAddress;
  protected String nodeId;
  protected Map<String, Long> allocatedResources;

  public ContainerInfo() {
    // JAXB needs this
  }

  public ContainerInfo(ContainerReport container) {
    if (container.getAssignedNode() != null) {
      assignedNodeId = container.getAssignedNode().toString();
    }

    containerId = container.getContainerId().toString();
    priority = container.getPriority().getPriority();
    startedTime = container.getCreationTime();
    finishedTime = container.getFinishTime();
    elapsedTime = Times.elapsed(startedTime, finishedTime);
    diagnosticsInfo = container.getDiagnosticsInfo();
    logUrl = container.getLogUrl();
    containerExitStatus = container.getContainerExitStatus();
    containerState = container.getContainerState();
    nodeHttpAddress = container.getNodeHttpAddress();
    nodeId = container.getAssignedNode().toString();

    Resource allocated = container.getAllocatedResource();

    if (allocated != null) {
      allocatedMB = allocated.getMemorySize();
      allocatedVCores = allocated.getVirtualCores();

      // Now populate the allocated resources. This map will include memory
      // and CPU, because it's where they belong. We still keep allocatedMB
      // and allocatedVCores so that we don't break the API.
      allocatedResources = new HashMap<>();

      for (ResourceInformation info : allocated.getResources()) {
        allocatedResources.put(info.getName(), info.getValue());
      }
    }
  }

  public String getContainerId() {
    return containerId;
  }

  public long getAllocatedMB() {
    return allocatedMB;
  }

  public long getAllocatedVCores() {
    return allocatedVCores;
  }

  public String getAssignedNodeId() {
    return assignedNodeId;
  }

  public int getPriority() {
    return priority;
  }

  public long getStartedTime() {
    return startedTime;
  }

  public long getFinishedTime() {
    return finishedTime;
  }

  public long getElapsedTime() {
    return elapsedTime;
  }

  public String getDiagnosticsInfo() {
    return diagnosticsInfo;
  }

  public String getLogUrl() {
    return logUrl;
  }

  public int getContainerExitStatus() {
    return containerExitStatus;
  }

  public ContainerState getContainerState() {
    return containerState;
  }

  public String getNodeHttpAddress() {
    return nodeHttpAddress;
  }

  public String getNodeId() {
    return nodeId;
  }

  /**
   * Return a map of the allocated resources. The map key is the resource name,
   * and the value is the resource value.
   *
   * @return the allocated resources map
   */
  public Map<String, Long> getAllocatedResources() {
    return Collections.unmodifiableMap(allocatedResources);
  }
}

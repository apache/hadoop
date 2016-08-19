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

package org.apache.hadoop.yarn.logaggregation;

import java.util.Set;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerState;

public class ContainerLogsRequest {
  private ApplicationId appId;
  private String containerId;
  private String nodeId;
  private String nodeHttpAddress;
  private String appOwner;
  private boolean appFinished;
  private String outputLocalDir;
  private Set<String> logTypes;
  private long bytes;
  private ContainerState containerState;

  public ContainerLogsRequest() {}

  public ContainerLogsRequest(ContainerLogsRequest request) {
    this.setAppId(request.getAppId());
    this.setAppFinished(request.isAppFinished());
    this.setAppOwner(request.getAppOwner());
    this.setNodeId(request.getNodeId());
    this.setNodeHttpAddress(request.getNodeHttpAddress());
    this.setContainerId(request.getContainerId());
    this.setOutputLocalDir(request.getOutputLocalDir());
    this.setLogTypes(request.getLogTypes());
    this.setBytes(request.getBytes());
    this.setContainerState(request.getContainerState());
  }

  public ContainerLogsRequest(ApplicationId applicationId,
      boolean isAppFinished, String owner,
      String address, String httpAddress, String container, String localDir,
      Set<String> logs, long bytes, ContainerState containerState) {
    this.setAppId(applicationId);
    this.setAppFinished(isAppFinished);
    this.setAppOwner(owner);
    this.setNodeId(address);
    this.setNodeHttpAddress(httpAddress);
    this.setContainerId(container);
    this.setOutputLocalDir(localDir);
    this.setLogTypes(logs);
    this.setBytes(bytes);
    this.setContainerState(containerState);
  }

  public ApplicationId getAppId() {
    return appId;
  }

  public void setAppId(ApplicationId appId) {
    this.appId = appId;
  }

  public String getContainerId() {
    return containerId;
  }

  public void setContainerId(String containerId) {
    this.containerId = containerId;
  }

  public String getNodeId() {
    return nodeId;
  }

  public void setNodeId(String nodeAddress) {
    this.nodeId = nodeAddress;
  }

  public String getAppOwner() {
    return appOwner;
  }

  public void setAppOwner(String appOwner) {
    this.appOwner = appOwner;
  }

  public String getNodeHttpAddress() {
    return nodeHttpAddress;
  }

  public void setNodeHttpAddress(String nodeHttpAddress) {
    this.nodeHttpAddress = nodeHttpAddress;
  }

  public boolean isAppFinished() {
    return appFinished;
  }

  public void setAppFinished(boolean appFinished) {
    this.appFinished = appFinished;
  }

  public String getOutputLocalDir() {
    return outputLocalDir;
  }

  public void setOutputLocalDir(String outputLocalDir) {
    this.outputLocalDir = outputLocalDir;
  }

  public Set<String> getLogTypes() {
    return logTypes;
  }

  public void setLogTypes(Set<String> logTypes) {
    this.logTypes = logTypes;
  }

  public long getBytes() {
    return bytes;
  }

  public void setBytes(long bytes) {
    this.bytes = bytes;
  }

  public ContainerState getContainerState() {
    return containerState;
  }

  public void setContainerState(ContainerState containerState) {
    this.containerState = containerState;
  }
}

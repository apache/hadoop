/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.server.appmaster.web.rest.agent;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.Map;

/**
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class CommandReport {

  int exitCode;
  private String role;
  private String actionId;
  private String stdout;
  private String stderr;
  private String structuredOut;
  private String status;
  private String clusterName;
  private String serviceName;
  private long taskId;
  private String roleCommand;
  private Map<String, String> folders;
  private Map<String, String> allocatedPorts;
  private Map<String, Map<String, String>> configurationTags;

  @JsonProperty("taskId")
  public long getTaskId() {
    return taskId;
  }

  @JsonProperty("taskId")
  public void setTaskId(long taskId) {
    this.taskId = taskId;
  }

  @JsonProperty("clusterName")
  public String getClusterName() {
    return this.clusterName;
  }

  @JsonProperty("clusterName")
  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  @JsonProperty("actionId")
  public String getActionId() {
    return this.actionId;
  }

  @JsonProperty("actionId")
  public void setActionId(String actionId) {
    this.actionId = actionId;
  }

  @JsonProperty("stderr")
  public String getStdErr() {
    return this.stderr;
  }

  @JsonProperty("stderr")
  public void setStdErr(String stderr) {
    this.stderr = stderr;
  }

  @JsonProperty("exitcode")
  public int getExitCode() {
    return this.exitCode;
  }

  @JsonProperty("exitcode")
  public void setExitCode(int exitCode) {
    this.exitCode = exitCode;
  }

  @JsonProperty("stdout")
  public String getStdOut() {
    return this.stdout;
  }

  @JsonProperty("stdout")
  public void setStdOut(String stdout) {
    this.stdout = stdout;
  }

  @JsonProperty("structuredOut")
  public String getStructuredOut() {
    return this.structuredOut;
  }

  @JsonProperty("structuredOut")
  public void setStructuredOut(String structuredOut) {
    this.structuredOut = structuredOut;
  }

  @JsonProperty("roleCommand")
  public String getRoleCommand() {
    return this.roleCommand;
  }

  @JsonProperty("roleCommand")
  public void setRoleCommand(String roleCommand) {
    this.roleCommand = roleCommand;
  }

  @JsonProperty("role")
  public String getRole() {
    return role;
  }

  @JsonProperty("role")
  public void setRole(String role) {
    this.role = role;
  }

  @JsonProperty("status")
  public String getStatus() {
    return status;
  }

  @JsonProperty("status")
  public void setStatus(String status) {
    this.status = status;
  }

  @JsonProperty("serviceName")
  public String getServiceName() {
    return serviceName;
  }

  @JsonProperty("serviceName")
  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  /** @return the config tags that match this command, or <code>null</code> if none are present */
  @JsonProperty("configurationTags")
  public Map<String, Map<String, String>> getConfigurationTags() {
    return configurationTags;
  }

  /** @param tags the config tags that match this command */
  @JsonProperty("configurationTags")
  public void setConfigurationTags(Map<String, Map<String, String>> tags) {
    configurationTags = tags;
  }

  /** @return the allocated ports, or <code>null</code> if none are present */
  @JsonProperty("allocatedPorts")
  public Map<String, String> getAllocatedPorts() {
    return allocatedPorts;
  }

  /** @param ports allocated ports */
  @JsonProperty("allocatedPorts")
  public void setAllocatedPorts(Map<String, String> ports) {
    this.allocatedPorts = ports;
  }

  /** @return the folders, or <code>null</code> if none are present */
  @JsonProperty("folders")
  public Map<String, String> getFolders() {
    return folders;
  }

  /** @param folders allocated ports */
  @JsonProperty("folders")
  public void setFolders(Map<String, String> folders) {
    this.folders = folders;
  }

  @Override
  public String toString() {
    return "CommandReport{" +
           "role='" + role + '\'' +
           ", actionId='" + actionId + '\'' +
           ", status='" + status + '\'' +
           ", exitCode=" + exitCode +
           ", clusterName='" + clusterName + '\'' +
           ", serviceName='" + serviceName + '\'' +
           ", taskId=" + taskId +
           ", roleCommand=" + roleCommand +
           ", configurationTags=" + configurationTags +
           '}';
  }
}

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

import java.util.HashMap;
import java.util.Map;

/**
 * Command to report the status of a list of services in roles.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class StatusCommand {
  public static String STATUS_COMMAND = "STATUS";
  public static String GET_CONFIG_COMMAND = "GET_CONFIG";

  AgentCommandType agentCommandType;

  private String clusterName;
  private String serviceName;
  private String componentName;
  private Map<String, Map<String, String>> configurations;
  private Map<String, String> commandParams = new HashMap<String, String>();
  private Map<String, String> hostLevelParams = new HashMap<String, String>();
  private String roleCommand;
  private boolean yarnDockerMode;

  public StatusCommand() {
    this.agentCommandType = AgentCommandType.STATUS_COMMAND;
  }

  @JsonProperty("clusterName")
  public String getClusterName() {
    return clusterName;
  }

  @JsonProperty("clusterName")
  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  @JsonProperty("serviceName")
  public String getServiceName() {
    return serviceName;
  }

  @JsonProperty("serviceName")
  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  @JsonProperty("componentName")
  public String getComponentName() {
    return componentName;
  }

  @JsonProperty("componentName")
  public void setComponentName(String componentName) {
    this.componentName = componentName;
  }

  @JsonProperty("configurations")
  public Map<String, Map<String, String>> getConfigurations() {
    return configurations;
  }

  @JsonProperty("configurations")
  public void setConfigurations(Map<String, Map<String, String>> configurations) {
    this.configurations = configurations;
  }

  @JsonProperty("hostLevelParams")
  public Map<String, String> getHostLevelParams() {
    return hostLevelParams;
  }

  @JsonProperty("hostLevelParams")
  public void setHostLevelParams(Map<String, String> params) {
    this.hostLevelParams = params;
  }

  @JsonProperty("commandParams")
  public Map<String, String> getCommandParams() {
    return commandParams;
  }

  @JsonProperty("commandParams")
  public void setCommandParams(Map<String, String> commandParams) {
    this.commandParams = commandParams;
  }

  @JsonProperty("commandType")
  public AgentCommandType getCommandType() {
    return agentCommandType;
  }

  @JsonProperty("commandType")
  public void setCommandType(AgentCommandType commandType) {
    this.agentCommandType = commandType;
  }

  @JsonProperty("roleCommand")
  public String getRoleCommand() {
    return roleCommand;
  }

  @JsonProperty("roleCommand")
  public void setRoleCommand(String roleCommand) {
    this.roleCommand = roleCommand;
  }
  
  @JsonProperty("yarnDockerMode")
  public boolean isYarnDockerMode() {
    return yarnDockerMode;
  }

  @JsonProperty("yarnDockerMode")
  public void setYarnDockerMode(boolean yarnDockerMode) {
    this.yarnDockerMode = yarnDockerMode;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("StatusCommand [agentCommandType=").append(agentCommandType)
        .append(", clusterName=").append(clusterName).append(", serviceName=")
        .append(serviceName).append(", componentName=").append(componentName)
        .append(", configurations=").append(configurations)
        .append(", commandParams=").append(commandParams)
        .append(", hostLevelParams=").append(hostLevelParams)
        .append(", roleCommand=").append(roleCommand).append("]");
    return builder.toString();
  }
}

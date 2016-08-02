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

import org.apache.slider.providers.agent.application.metadata.Component;
import org.apache.slider.providers.agent.application.metadata.DockerContainer;
import org.apache.slider.providers.agent.application.metadata.DockerContainerInputFile;
import org.apache.slider.providers.agent.application.metadata.DockerContainerMount;
import org.apache.slider.providers.agent.application.metadata.DockerContainerPort;
import org.apache.slider.providers.agent.application.metadata.Metainfo;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class ExecutionCommand {
  protected static final Logger log =
      LoggerFactory.getLogger(ExecutionCommand.class);
  private AgentCommandType commandType = AgentCommandType.EXECUTION_COMMAND;
  private String clusterName;
  private long taskId;
  private String commandId;
  //TODO Remove hostname from being set in the command
  private String hostname;
  private String role;
  private Map<String, String> hostLevelParams = new HashMap<String, String>();
  private Map<String, String> roleParams = null;
  private String roleCommand;
  private Map<String, Map<String, String>> configurations;
  private Map<String, Map<String, String>> componentConfigurations;
  private Map<String, String> commandParams;
  private String serviceName;
  private String componentName;
  private String componentType;
  private List<DockerContainer> containers = new ArrayList<>();
  private String pkg;
  private boolean yarnDockerMode = false;

  public ExecutionCommand(AgentCommandType commandType) {
    this.commandType = commandType;
  }

  @JsonProperty("commandType")
  public AgentCommandType getCommandType() {
    return commandType;
  }

  @JsonProperty("commandType")
  public void setCommandType(AgentCommandType commandType) {
    this.commandType = commandType;
  }

  @JsonProperty("commandId")
  public String getCommandId() {
    return this.commandId;
  }

  @JsonProperty("commandId")
  public void setCommandId(String commandId) {
    this.commandId = commandId;
  }

  @JsonProperty("taskId")
  public long getTaskId() {
    return taskId;
  }

  @JsonProperty("taskId")
  public void setTaskId(long taskId) {
    this.taskId = taskId;
  }

  @JsonProperty("role")
  public String getRole() {
    return role;
  }

  @JsonProperty("role")
  public void setRole(String role) {
    this.role = role;
  }

  @JsonProperty("roleParams")
  public Map<String, String> getRoleParams() {
    return roleParams;
  }

  @JsonProperty("roleParams")
  public void setRoleParams(Map<String, String> roleParams) {
    this.roleParams = roleParams;
  }

  @JsonProperty("roleCommand")
  public String getRoleCommand() {
    return roleCommand;
  }

  @JsonProperty("roleCommand")
  public void setRoleCommand(String cmd) {
    this.roleCommand = cmd;
  }

  @JsonProperty("clusterName")
  public String getClusterName() {
    return clusterName;
  }

  @JsonProperty("clusterName")
  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  @JsonProperty("componentType")
  public String getComponentType() {
    return componentType;
  }

  @JsonProperty("componentType")
  public void setComponentType(String componentType) {
    this.componentType = componentType;
  }

  @JsonProperty("hostname")
  public String getHostname() {
    return hostname;
  }

  @JsonProperty("hostname")
  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  @JsonProperty("hostLevelParams")
  public Map<String, String> getHostLevelParams() {
    return hostLevelParams;
  }

  @JsonProperty("hostLevelParams")
  public void setHostLevelParams(Map<String, String> params) {
    this.hostLevelParams = params;
  }

  @JsonProperty("configurations")
  public Map<String, Map<String, String>> getConfigurations() {
    return configurations;
  }

  @JsonProperty("configurations")
  public void setConfigurations(Map<String, Map<String, String>> configurations) {
    this.configurations = configurations;
  }

  @JsonProperty("commandParams")
  public Map<String, String> getCommandParams() {
    return commandParams;
  }

  @JsonProperty("commandParams")
  public void setCommandParams(Map<String, String> commandParams) {
    this.commandParams = commandParams;
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

  @JsonProperty("package")
  public String getPkg() {
    return pkg;
  }

  @JsonProperty("package")
  public void setPkg(String pkg) {
    this.pkg = pkg;
  }

  @JsonProperty("componentConfig")
  public Map<String, Map<String, String>> getComponentConfigurations() {
    return componentConfigurations;
  }

  @JsonProperty("componentConfig")
  public void setComponentConfigurations(
      Map<String, Map<String, String>> componentConfigurations) {
    this.componentConfigurations = componentConfigurations;
  }

  @JsonProperty("containers")
  public List<DockerContainer> getContainers() {
    return containers;
  }

  @JsonProperty("yarnDockerMode")
  public boolean isYarnDockerMode() {
    return yarnDockerMode ;
  }

  @JsonProperty("yarnDockerMode")
  public void setYarnDockerMode(boolean yarnDockerMode) {
    this.yarnDockerMode = yarnDockerMode;
  }
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("ExecutionCommand [commandType=").append(commandType)
        .append(", clusterName=").append(clusterName).append(", taskId=")
        .append(taskId).append(", commandId=").append(commandId)
        .append(", hostname=").append(hostname).append(", role=").append(role)
        .append(", hostLevelParams=").append(hostLevelParams)
        .append(", roleParams=").append(roleParams).append(", roleCommand=")
        .append(roleCommand).append(", configurations=").append(configurations)
        .append(", commandParams=").append(commandParams)
        .append(", serviceName=").append(serviceName)
        .append(", componentName=").append(componentName)
        .append(", componentType=").append(componentType)
        .append(", yarnDockerMode=").append(yarnDockerMode).append(", pkg=")
        .append(pkg).append("]");
    return builder.toString();
  }
  
  public void addContainerDetails(String componentGroup, Metainfo metaInfo) {
    Component component = metaInfo.getApplicationComponent(componentGroup);
    this.setComponentType(component.getType());
    log.info("Adding container details for {}", componentGroup, " from ",
        metaInfo.toString());
    for (DockerContainer metaContainer : component.getDockerContainers()) {
      DockerContainer container = new DockerContainer();
      container.setImage(metaContainer.getImage());
      container.setNetwork(metaContainer.getNetwork());
      container.setUseNetworkScript(metaContainer.getUseNetworkScript());
      container.setName(metaContainer.getName());
      container.setOptions(metaContainer.getOptions());
      container.setAdditionalParam(metaContainer.getAdditionalParam());
      container.setCommandPath(metaContainer.getAdditionalParam());
      container.setStatusCommand(metaContainer.getStatusCommand());
      container.setStartCommand(metaContainer.getStartCommand());
      if (metaContainer.getMounts().size() > 0) {
        for (DockerContainerMount metaContMount : metaContainer.getMounts()) {
          DockerContainerMount contMnt = new DockerContainerMount();
          contMnt.setContainerMount(metaContMount.getContainerMount());
          contMnt.setHostMount(metaContMount.getHostMount());
          container.getMounts().add(contMnt);
        }
      }
      if (metaContainer.getPorts().size() > 0) {
        for (DockerContainerPort metaCntPort : metaContainer.getPorts()) {
          DockerContainerPort cntPort = new DockerContainerPort();
          cntPort.setContainerPort(metaCntPort.getContainerPort());
          cntPort.setHostPort(metaCntPort.getHostPort());
          container.getPorts().add(cntPort);
        }
      }
      if (metaContainer.getInputFiles().size() > 0) {
        for (DockerContainerInputFile metaInpFile : metaContainer
            .getInputFiles()) {
          DockerContainerInputFile inpFile = new DockerContainerInputFile();
          inpFile.setContainerMount(metaInpFile.getContainerMount());
          inpFile.setFileLocalPath(metaInpFile.getFileLocalPath());
          container.getInputFiles().add(inpFile);
        }
      }
      if (metaContainer.getConfigFiles() != null) {
        container.setConfigFiles(metaContainer.getConfigFiles());
      }
      log.info("Docker container meta info ready: " + container.toString());
      this.getContainers().add(container);
    }
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.providers.agent.application.metadata;

import org.apache.slider.core.exceptions.SliderException;
import org.codehaus.jackson.annotate.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a docker container
 */
public class DockerContainer implements Validate {
  protected static final Logger
      log = LoggerFactory.getLogger(DockerContainer.class);

  private String name;
  private String image;
  private String network;
  private String useNetworkScript;
  private String options;
  private List<DockerContainerMount> mounts = new ArrayList<>();
  private List<DockerContainerPort> ports = new ArrayList<>();
  private String statusCommand;
  private String startCommand;
  private String commandPath;
  private String additionalParam;
  private String runPrivilegedContainer;
  private List<DockerContainerInputFile> inputFiles = new ArrayList<>();
  private List<ConfigFile> configFiles = new ArrayList<>();

  public DockerContainer() {
  }

  @JsonProperty("mounts")
  public List<DockerContainerMount> getMounts() { return this.mounts; }

  @JsonProperty("ports")
  public List<DockerContainerPort> getPorts() {
    return this.ports;
  }

  @JsonProperty("inputFiles")
  public List<DockerContainerInputFile> getInputFiles() {
    return this.inputFiles;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getImage() {
    return image;
  }

  public void setImage(String image) {
    this.image = image;
  }

  public String getNetwork() {
    return network;
  }

  public void setNetwork(String network) {
    this.network = network;
  }

  public String getUseNetworkScript() {
    return useNetworkScript;
  }

  public void setUseNetworkScript(String useNetworkScript) {
    this.useNetworkScript = useNetworkScript;
  }

  public String getOptions() {
    return options;
  }

  public void setOptions(String options) {
    this.options = options;
  }

  @Override
  public void validate(String version) throws SliderException {
    Metainfo.checkNonNull(getName(), "name", "dockerContainer");
    Metainfo.checkNonNull(getImage(), "image", "dockerContainer");
    for (DockerContainerMount dcm : getMounts()) {
      dcm.validate(version);
    }
    for (DockerContainerPort dcp : getPorts()) {
      dcp.validate(version);
    }
  }

  @JsonProperty("statusCommand")
  public String getStatusCommand() {
    return statusCommand;
  }

  @JsonProperty("statusCommand")
  public void setStatusCommand(String statusCommand) {
    this.statusCommand = statusCommand;
  }

  public String getCommandPath() {
    return commandPath;
  }

  public void setCommandPath(String commandPath) {
    this.commandPath = commandPath;
  }

  public String getAdditionalParam() {
    return additionalParam;
  }

  public void setAdditionalParam(String additionalParam) {
    this.additionalParam = additionalParam;
  }

  @JsonProperty("startCommand")
  public String getStartCommand() {
    return startCommand;
  }

  @JsonProperty("startCommand")
  public void setStartCommand(String startCommand) {
    this.startCommand = startCommand;
  }

  @JsonProperty("runPrivilegedContainer")
  public String getRunPrivilegedContainer() {
    return runPrivilegedContainer;
  }

  @JsonProperty("runPrivilegedContainer")
  public void setRunPrivilegedContainer(String runPrivilegedContainer) {
    this.runPrivilegedContainer = runPrivilegedContainer;
  }

  public List<ConfigFile> getConfigFiles() {
    return configFiles;
  }

  public void setConfigFiles(List<ConfigFile> configFiles) {
    this.configFiles = configFiles;
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder("DockerContainer [name=")
        .append(name).append(", image=").append(image).append(", options=")
        .append(options).append(", mounts=").append(mounts).append(", ports=")
        .append(ports).append(", statusCommand=").append(statusCommand)
        .append(", commandPath=").append(commandPath)
        .append(", additionalParam=").append(additionalParam)
        .append(", inputFiles=").append(inputFiles).append(", startCommand=")
        .append(startCommand).append(", runPriviledgedContainer=")
        .append(runPrivilegedContainer).append(", configFiles=")
        .append(configFiles).append("]");
    return result.toString();
  }
}
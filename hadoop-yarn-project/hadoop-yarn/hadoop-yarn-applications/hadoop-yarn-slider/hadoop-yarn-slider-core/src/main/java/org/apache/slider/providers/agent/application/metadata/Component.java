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
package org.apache.slider.providers.agent.application.metadata;

import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.exceptions.SliderException;
import org.codehaus.jackson.annotate.JsonProperty;
import java.util.ArrayList;
import java.util.List;

/**
 * Component defined in master package metainfo.json
 */
public class Component extends AbstractComponent {

  String category = CATEGORY_MASTER;
  String publishConfig = Boolean.FALSE.toString();
  String minInstanceCount = "0";
  String maxInstanceCount;
  String autoStartOnFailure = Boolean.FALSE.toString();
  String appExports;
  String compExports;
  String type = TYPE_STANDARD;
  List<ComponentExport> componentExports = new ArrayList<>();
  List<DockerContainer> dockerContainers = new ArrayList<>();
  List<ConfigFile> configFiles = new ArrayList<>();

  public Component() {
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getCategory() {
    return category;
  }

  public void setCategory(String category) {
    this.category = category;
  }

  public String getPublishConfig() {
    return publishConfig;
  }

  public void setPublishConfig(String publishConfig) {
    this.publishConfig = publishConfig;
  }

  public String getAutoStartOnFailure() {
    return autoStartOnFailure;
  }

  public void setAutoStartOnFailure(String autoStartOnFailure) {
    this.autoStartOnFailure = autoStartOnFailure;
  }

  public String getAppExports() {
    return appExports;
  }

  public void setAppExports(String appExports) {
    this.appExports = appExports;
  }

  public String getCompExports() {
    return compExports;
  }

  public void setCompExports(String compExports) {
    this.compExports = compExports;
  }

  public String getMinInstanceCount() {
    return minInstanceCount;
  }
  
  @JsonProperty("dockerContainers")
  public List<DockerContainer> getDockerContainers() {
     return this.dockerContainers;
  }
  
  public Boolean getAutoStartOnFailureBoolean() {
    if (SliderUtils.isUnset(getAutoStartOnFailure())) {
      return Boolean.FALSE;
    }

    return Boolean.parseBoolean(getAutoStartOnFailure());
  }

  public int getMinInstanceCountInt() throws BadConfigException {
    if (SliderUtils.isUnset(minInstanceCount)) {
      return 0;
    }

    try {
      return Integer.parseInt(minInstanceCount);
    } catch (NumberFormatException nfe) {
      throw new BadConfigException(nfe, "Invalid value for minInstanceCount for %s", name);
    }
  }

  public int getMaxInstanceCountInt() throws BadConfigException {
    if (SliderUtils.isUnset(maxInstanceCount)) {
      return Integer.MAX_VALUE;
    }

    try {
      return Integer.parseInt(maxInstanceCount);
    } catch (NumberFormatException nfe) {
      throw new BadConfigException(nfe, "Invalid value for maxInstanceCount for %s", name);
    }
  }

  public void setMinInstanceCount(String minInstanceCount) {
    this.minInstanceCount = minInstanceCount;
  }

  public String getMaxInstanceCount() {
    return maxInstanceCount;
  }

  public void setMaxInstanceCount(String maxInstanceCount) {
    this.maxInstanceCount = maxInstanceCount;
  }

  public void addComponentExport(ComponentExport export) {
    componentExports.add(export);
  }

  public List<ComponentExport> getComponentExports() {
    return componentExports;
  }

  public Boolean getRequiresAutoRestart() {
    return Boolean.parseBoolean(this.autoStartOnFailure);
  }

  public void addConfigFile(ConfigFile configFile) {
    this.configFiles.add(configFile);
  }

  @JsonProperty("configFiles")
  public List<ConfigFile> getConfigFiles() {
    return configFiles;
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder("{");
    sb.append("\n\"name\": ").append(name);
    sb.append(",\n\"category\": ").append(category);
    sb.append(",\n\"commandScript\" :").append(commandScript);
    for(DockerContainer dc : dockerContainers){
      sb.append(",\n\"container\" :").append(dc.toString());
    }    
    sb.append('}');
    return sb.toString();
  }

  public void validate(String version) throws SliderException {
    Metainfo.checkNonNull(getName(), "name", "component");
    Metainfo.checkNonNull(getCategory(), "category", "component");
    if (!getCategory().equals(CATEGORY_MASTER)
        && !getCategory().equals(CATEGORY_SLAVE)
        && !getCategory().equals(CATEGORY_CLIENT)) {
      throw new SliderException("Invalid category for the component " + getCategory());
    }

    Metainfo.checkNonNull(getType(), "type", "component");
    if (!getType().equals(TYPE_DOCKER)
        && !getType().equals(TYPE_STANDARD)) {
      throw new SliderException("Invalid type for the component " + getType());
    }

    if (version.equals(Metainfo.VERSION_TWO_ZERO)) {
      if (getType().equals(TYPE_DOCKER)) {
        throw new SliderException(TYPE_DOCKER + " is not supported in version " + Metainfo.VERSION_TWO_ZERO);
      }

      if (getCommands().size() > 0) {
        throw new SliderException("commands are not supported in version " + Metainfo.VERSION_TWO_ZERO);
      }
    }

    if (commandScript != null) {
      commandScript.validate(version);
    }

    if (version.equals(Metainfo.VERSION_TWO_ONE)) {
      for (ComponentCommand cc : getCommands()) {
        cc.validate(version);
      }
    }
  }
}

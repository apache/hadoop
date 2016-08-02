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
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.SliderException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * Application type defined in the metainfo
 */
public class Application extends AbstractMetainfoSchema {
  String exportedConfigs;
  List<ExportGroup> exportGroups = new ArrayList<>();
  List<OSSpecific> osSpecifics = new ArrayList<>();
  List<CommandOrder> commandOrders = new ArrayList<>();
  List<Package> packages = new ArrayList<>();
  private List<Component> components = new ArrayList<>();

  public Application() {
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getExportedConfigs() {
    return exportedConfigs;
  }

  public void setExportedConfigs(String exportedConfigs) {
    this.exportedConfigs = exportedConfigs;
  }

  public void addConfigFile(ConfigFile configFile) {
    this.configFiles.add(configFile);
  }

  @JsonProperty("configFiles")
  public List<ConfigFile> getConfigFiles() {
    return configFiles;
  }

  public void addComponent(Component component) {
    components.add(component);
  }

  @JsonProperty("components")
  public List<Component> getComponents() {
    return components;
  }

  public void addExportGroup(ExportGroup exportGroup) {
    exportGroups.add(exportGroup);
  }

  @JsonProperty("exportGroups")
  public List<ExportGroup> getExportGroups() {
    return exportGroups;
  }

  public void addOSSpecific(OSSpecific osSpecific) {
    osSpecifics.add(osSpecific);
  }

  @JsonIgnore
  public List<OSSpecific> getOSSpecifics() {
    return osSpecifics;
  }

  public void addCommandOrder(CommandOrder commandOrder) {
    commandOrders.add(commandOrder);
  }

  @JsonProperty("commandOrders")
  public List<CommandOrder> getCommandOrders() {
    return commandOrders;
  }

  public void addPackage(Package pkg) {
    packages.add(pkg);
  }

  @JsonProperty("packages")
  public List<Package> getPackages() {
    return packages;
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder("{");
    sb.append(",\n\"name\": ").append(name);
    sb.append(",\n\"comment\": ").append(comment);
    sb.append(",\n\"version\" :").append(version);
    sb.append(",\n\"components\" : {");
    for (Component component : components) {
      sb.append("\n").append(component.toString());
    }
    sb.append("\n},");
    sb.append('}');
    return sb.toString();
  }

  public void validate(String version) throws SliderException {
    if(SliderUtils.isUnset(version)) {
      throw new BadCommandArgumentsException("schema version cannot be null");
    }

    Metainfo.checkNonNull(getName(), "name", "application");

    Metainfo.checkNonNull(getVersion(), "version", "application");

    if(getComponents().size() == 0) {
      throw new SliderException("application must contain at least one component");
    }

    if(version.equals(Metainfo.VERSION_TWO_ZERO)) {
      if(getPackages().size() > 0) {
        throw new SliderException("packages is not supported in version " + version);
      }
    }

    if(version.equals(Metainfo.VERSION_TWO_ONE)) {
      if(getOSSpecifics().size() > 0) {
        throw new SliderException("osSpecifics is not supported in version " + version);
      }
    }

    for(CommandOrder co : getCommandOrders()) {
      co.validate(version);
    }

    for(Component comp : getComponents()) {
      comp.validate(version);
    }

    for(ConfigFile cf : getConfigFiles()) {
      cf.validate(version);
    }

    for(ExportGroup eg : getExportGroups()) {
      eg.validate(version);
    }

    for(Package pkg : getPackages()) {
      pkg.validate(version);
    }

    for(OSSpecific os : getOSSpecifics()) {
      os.validate(version);
    }
  }
}

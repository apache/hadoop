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

package org.apache.hadoop.yarn.services.resource;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One or more components of the application. If the application is HBase say,
 * then the component can be a simple role like master or regionserver. If the
 * application is a complex business webapp then a component can be other
 * applications say Kafka or Storm. Thereby it opens up the support for complex
 * and nested applications.
 **/

@ApiModel(description = "One or more components of the application. If the application is HBase say, then the component can be a simple role like master or regionserver. If the application is a complex business webapp then a component can be other applications say Kafka or Storm. Thereby it opens up the support for complex and nested applications.")
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaClientCodegen", date = "2016-06-02T08:15:05.615-07:00")
@XmlRootElement
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Component {

  private String name = null;
  private List<String> dependencies = new ArrayList<String>();
  private ReadinessCheck readinessCheck = null;
  private Artifact artifact = null;
  private String launchCommand = null;
  private Resource resource = null;
  private Long numberOfContainers = null;
  private Boolean uniqueComponentSupport = null;
  private Boolean runPrivilegedContainer = null;
  private PlacementPolicy placementPolicy = null;
  private Configuration configuration = null;
  private List<String> quicklinks = new ArrayList<String>();

  /**
   * Name of the application component (mandatory).
   **/
  public Component name(String name) {
    this.name = name;
    return this;
  }

  @ApiModelProperty(example = "null", required = true, value = "Name of the application component (mandatory).")
  @JsonProperty("name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  /**
   * An array of application components which should be in READY state (as
   * defined by readiness check), before this component can be started. The
   * dependencies across all components of an application should be represented
   * as a DAG.
   **/
  public Component dependencies(List<String> dependencies) {
    this.dependencies = dependencies;
    return this;
  }

  @ApiModelProperty(example = "null", value = "An array of application components which should be in READY state (as defined by readiness check), before this component can be started. The dependencies across all components of an application should be represented as a DAG.")
  @JsonProperty("dependencies")
  public List<String> getDependencies() {
    return dependencies;
  }

  public void setDependencies(List<String> dependencies) {
    this.dependencies = dependencies;
  }

  /**
   * Readiness check for this app-component.
   **/
  public Component readinessCheck(ReadinessCheck readinessCheck) {
    this.readinessCheck = readinessCheck;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Readiness check for this app-component.")
  @JsonProperty("readiness_check")
  public ReadinessCheck getReadinessCheck() {
    return readinessCheck;
  }

  @XmlElement(name = "readiness_check")
  public void setReadinessCheck(ReadinessCheck readinessCheck) {
    this.readinessCheck = readinessCheck;
  }

  /**
   * Artifact of the component (optional). If not specified, the application
   * level global artifact takes effect.
   **/
  public Component artifact(Artifact artifact) {
    this.artifact = artifact;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Artifact of the component (optional). If not specified, the application level global artifact takes effect.")
  @JsonProperty("artifact")
  public Artifact getArtifact() {
    return artifact;
  }

  public void setArtifact(Artifact artifact) {
    this.artifact = artifact;
  }

  /**
   * The custom launch command of this component (optional). When specified at
   * the component level, it overrides the value specified at the global level
   * (if any).
   **/
  public Component launchCommand(String launchCommand) {
    this.launchCommand = launchCommand;
    return this;
  }

  @ApiModelProperty(example = "null", value = "The custom launch command of this component (optional). When specified at the component level, it overrides the value specified at the global level (if any).")
  @JsonProperty("launch_command")
  public String getLaunchCommand() {
    return launchCommand;
  }

  @XmlElement(name = "launch_command")
  public void setLaunchCommand(String launchCommand) {
    this.launchCommand = launchCommand;
  }

  /**
   * Resource of this component (optional). If not specified, the application
   * level global resource takes effect.
   **/
  public Component resource(Resource resource) {
    this.resource = resource;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Resource of this component (optional). If not specified, the application level global resource takes effect.")
  @JsonProperty("resource")
  public Resource getResource() {
    return resource;
  }

  public void setResource(Resource resource) {
    this.resource = resource;
  }

  /**
   * Number of containers for this app-component (optional). If not specified,
   * the application level global number_of_containers takes effect.
   **/
  public Component numberOfContainers(Long numberOfContainers) {
    this.numberOfContainers = numberOfContainers;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Number of containers for this app-component (optional). If not specified, the application level global number_of_containers takes effect.")
  @JsonProperty("number_of_containers")
  public Long getNumberOfContainers() {
    return numberOfContainers;
  }

  @XmlElement(name = "number_of_containers")
  public void setNumberOfContainers(Long numberOfContainers) {
    this.numberOfContainers = numberOfContainers;
  }

  /**
   * Certain applications need to define multiple components using the same
   * artifact and resource profile, differing only in configurations. In such
   * cases, this field helps app owners to avoid creating multiple component
   * definitions with repeated information. The number_of_containers field
   * dictates the initial number of components created. Component names
   * typically differ with a trailing id, but assumptions should not be made on
   * that, as the algorithm can change at any time. Configurations section will
   * be able to use placeholders like ${APP_COMPONENT_NAME} to get its component
   * name at runtime, and thereby differing in value at runtime. The best part
   * of this feature is that when the component is flexed up, entirely new
   * components (with new trailing ids) are created.
   **/
  public Component uniqueComponentSupport(Boolean uniqueComponentSupport) {
    this.uniqueComponentSupport = uniqueComponentSupport;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Certain applications need to define multiple components using the same artifact and resource profile, differing only in configurations. In such cases, this field helps app owners to avoid creating multiple component definitions with repeated information. The number_of_containers field dictates the initial number of components created. Component names typically differ with a trailing id, but assumptions should not be made on that, as the algorithm can change at any time. Configurations section will be able to use placeholders like ${APP_COMPONENT_NAME} to get its component name at runtime, and thereby differing in value at runtime. The best part of this feature is that when the component is flexed up, entirely new components (with new trailing ids) are created.")
  @JsonProperty("unique_component_support")
  public Boolean getUniqueComponentSupport() {
    return uniqueComponentSupport;
  }

  @XmlElement(name = "unique_component_support")
  public void setUniqueComponentSupport(Boolean uniqueComponentSupport) {
    this.uniqueComponentSupport = uniqueComponentSupport;
  }

  /**
   * Run all containers of this component in privileged mode (YARN-4262).
   **/
  public Component runPrivilegedContainer(Boolean runPrivilegedContainer) {
    this.runPrivilegedContainer = runPrivilegedContainer;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Run all containers of this component in privileged mode (YARN-4262).")
  @JsonProperty("run_privileged_container")
  public Boolean getRunPrivilegedContainer() {
    return runPrivilegedContainer;
  }

  @XmlElement(name = "run_privileged_container")
  public void setRunPrivilegedContainer(Boolean runPrivilegedContainer) {
    this.runPrivilegedContainer = runPrivilegedContainer;
  }

  /**
   * Advanced scheduling and placement policies for all containers of this
   * component (optional). If not specified, the app level placement_policy
   * takes effect. Refer to the description at the global level for more
   * details.
   **/
  public Component placementPolicy(PlacementPolicy placementPolicy) {
    this.placementPolicy = placementPolicy;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Advanced scheduling and placement policies for all containers of this component (optional). If not specified, the app level placement_policy takes effect. Refer to the description at the global level for more details.")
  @JsonProperty("placement_policy")
  public PlacementPolicy getPlacementPolicy() {
    return placementPolicy;
  }

  @XmlElement(name = "placement_policy")
  public void setPlacementPolicy(PlacementPolicy placementPolicy) {
    this.placementPolicy = placementPolicy;
  }

  /**
   * Config properties for this app-component.
   **/
  public Component configuration(Configuration configuration) {
    this.configuration = configuration;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Config properties for this app-component.")
  @JsonProperty("configuration")
  public Configuration getConfiguration() {
    return configuration;
  }

  public void setConfiguration(Configuration configuration) {
    this.configuration = configuration;
  }

  /**
   * A list of quicklink keys defined at the application level, and to be
   * resolved by this component.
   **/
  public Component quicklinks(List<String> quicklinks) {
    this.quicklinks = quicklinks;
    return this;
  }

  @ApiModelProperty(example = "null", value = "A list of quicklink keys defined at the application level, and to be resolved by this component.")
  @JsonProperty("quicklinks")
  public List<String> getQuicklinks() {
    return quicklinks;
  }

  public void setQuicklinks(List<String> quicklinks) {
    this.quicklinks = quicklinks;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Component component = (Component) o;
    return Objects.equals(this.name, component.name)
        && Objects.equals(this.dependencies, component.dependencies)
        && Objects.equals(this.readinessCheck, component.readinessCheck)
        && Objects.equals(this.artifact, component.artifact)
        && Objects.equals(this.launchCommand, component.launchCommand)
        && Objects.equals(this.resource, component.resource)
        && Objects
            .equals(this.numberOfContainers, component.numberOfContainers)
        && Objects.equals(this.uniqueComponentSupport,
            component.uniqueComponentSupport)
        && Objects.equals(this.runPrivilegedContainer,
            component.runPrivilegedContainer)
        && Objects.equals(this.placementPolicy, component.placementPolicy)
        && Objects.equals(this.configuration, component.configuration)
        && Objects.equals(this.quicklinks, component.quicklinks);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, dependencies, readinessCheck, artifact,
        launchCommand, resource, numberOfContainers, uniqueComponentSupport,
        runPrivilegedContainer, placementPolicy, configuration, quicklinks);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Component {\n");

    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    dependencies: ").append(toIndentedString(dependencies))
        .append("\n");
    sb.append("    readinessCheck: ").append(toIndentedString(readinessCheck))
        .append("\n");
    sb.append("    artifact: ").append(toIndentedString(artifact)).append("\n");
    sb.append("    launchCommand: ").append(toIndentedString(launchCommand))
        .append("\n");
    sb.append("    resource: ").append(toIndentedString(resource)).append("\n");
    sb.append("    numberOfContainers: ")
        .append(toIndentedString(numberOfContainers)).append("\n");
    sb.append("    uniqueComponentSupport: ")
        .append(toIndentedString(uniqueComponentSupport)).append("\n");
    sb.append("    runPrivilegedContainer: ")
        .append(toIndentedString(runPrivilegedContainer)).append("\n");
    sb.append("    placementPolicy: ")
        .append(toIndentedString(placementPolicy)).append("\n");
    sb.append("    configuration: ").append(toIndentedString(configuration))
        .append("\n");
    sb.append("    quicklinks: ").append(toIndentedString(quicklinks))
        .append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}

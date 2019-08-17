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

package org.apache.hadoop.yarn.service.api.records;

import com.fasterxml.jackson.annotation.JsonValue;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * One or more components of the service. If the service is HBase say,
 * then the component can be a simple role like master or regionserver. If the
 * service is a complex business webapp then a component can be other
 * services say Kafka or Storm. Thereby it opens up the support for complex
 * and nested services.
 **/
@InterfaceAudience.Public
@InterfaceStability.Unstable
@ApiModel(description = "One or more components of the service. If the service is HBase say, then the component can be a simple role like master or regionserver. If the service is a complex business webapp then a component can be other services say Kafka or Storm. Thereby it opens up the support for complex and nested services.")
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Component implements Serializable {
  private static final long serialVersionUID = -8430058381509087805L;

  @JsonProperty("name")
  private String name = null;

  @JsonProperty("dependencies")
  private List<String> dependencies = new ArrayList<String>();

  @JsonProperty("readiness_check")
  @XmlElement(name = "readiness_check")
  private ReadinessCheck readinessCheck = null;

  @JsonProperty("artifact")
  private Artifact artifact = null;

  @JsonProperty("launch_command")
  @XmlElement(name = "launch_command")
  private String launchCommand = null;

  @JsonProperty("resource")
  private Resource resource = null;

  @JsonProperty("number_of_containers")
  @XmlElement(name = "number_of_containers")
  private Long numberOfContainers = null;

  @JsonProperty("decommissioned_instances")
  @XmlElement(name = "decommissioned_instances")
  private List<String> decommissionedInstances = new ArrayList<>();

  @JsonProperty("run_privileged_container")
  @XmlElement(name = "run_privileged_container")
  private Boolean runPrivilegedContainer = false;

  @JsonProperty("placement_policy")
  @XmlElement(name = "placement_policy")
  private PlacementPolicy placementPolicy = null;

  @JsonProperty("state")
  private ComponentState state = ComponentState.FLEXING;

  @JsonProperty("configuration")
  private Configuration configuration = new Configuration();

  @JsonProperty("quicklinks")
  private List<String> quicklinks = new ArrayList<String>();

  @JsonProperty("containers")
  private List<Container> containers =
      Collections.synchronizedList(new ArrayList<Container>());


  @JsonProperty("restart_policy")
  @XmlElement(name = "restart_policy")
  private RestartPolicyEnum restartPolicy = RestartPolicyEnum.ALWAYS;

  /**
   * Policy of restart component. Including ALWAYS - Long lived components
   * (Always restart component instance even if instance exit code &#x3D; 0.);
   *
   * ON_FAILURE (Only restart component instance if instance exit code !&#x3D;
   * 0);
   * NEVER (Do not restart in any cases)
   *
   * @return restartPolicy
   **/
  @XmlType(name = "restart_policy")
  @XmlEnum
  public enum RestartPolicyEnum {
    ALWAYS("ALWAYS"),

    ON_FAILURE("ON_FAILURE"),

    NEVER("NEVER");
    private String value;

    RestartPolicyEnum(String value) {
      this.value = value;
    }

    @Override
    @JsonValue
    public String toString() {
      return value;
    }
  }

  public Component restartPolicy(RestartPolicyEnum restartPolicyEnumVal) {
    this.restartPolicy = restartPolicyEnumVal;
    return this;
  }

  /**
   * Policy of restart component.
   *
   * Including
   * ALWAYS (Always restart component instance even if instance exit
   * code &#x3D; 0);
   *
   * ON_FAILURE (Only restart component instance if instance exit code !&#x3D;
   * 0);
   *
   * NEVER (Do not restart in any cases)
   *
   * @return restartPolicy
   **/
  @ApiModelProperty(value = "Policy of restart component. Including ALWAYS "
      + "(Always restart component even if instance exit code = 0); "
      + "ON_FAILURE (Only restart component if instance exit code != 0); "
      + "NEVER (Do not restart in any cases)")
  public RestartPolicyEnum getRestartPolicy() {
    return restartPolicy;
  }

  public void setRestartPolicy(RestartPolicyEnum restartPolicy) {
    this.restartPolicy = restartPolicy;
  }


  /**
   * Name of the service component (mandatory).
   **/
  public Component name(String name) {
    this.name = name;
    return this;
  }

  @ApiModelProperty(example = "null", required = true, value = "Name of the service component (mandatory).")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  /**
   * An array of service components which should be in READY state (as
   * defined by readiness check), before this component can be started. The
   * dependencies across all components of a service should be represented
   * as a DAG.
   **/
  public Component dependencies(List<String> dependencies) {
    this.dependencies = dependencies;
    return this;
  }

  @ApiModelProperty(example = "null", value = "An array of service components which should be in READY state (as defined by readiness check), before this component can be started. The dependencies across all components of an service should be represented as a DAG.")
  public List<String> getDependencies() {
    return dependencies;
  }

  public void setDependencies(List<String> dependencies) {
    this.dependencies = dependencies;
  }

  /**
   * Readiness check for this component.
   **/
  public Component readinessCheck(ReadinessCheck readinessCheck) {
    this.readinessCheck = readinessCheck;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Readiness check for this component.")
  public ReadinessCheck getReadinessCheck() {
    return readinessCheck;
  }

  public void setReadinessCheck(ReadinessCheck readinessCheck) {
    this.readinessCheck = readinessCheck;
  }

  /**
   * Artifact of the component (optional). If not specified, the service
   * level global artifact takes effect.
   **/
  public Component artifact(Artifact artifact) {
    this.artifact = artifact;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Artifact of the component (optional). If not specified, the service level global artifact takes effect.")
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
  public String getLaunchCommand() {
    return launchCommand;
  }

  public void setLaunchCommand(String launchCommand) {
    this.launchCommand = launchCommand;
  }

  /**
   * Resource of this component (optional). If not specified, the service
   * level global resource takes effect.
   **/
  public Component resource(Resource resource) {
    this.resource = resource;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Resource of this component (optional). If not specified, the service level global resource takes effect.")
  public Resource getResource() {
    return resource;
  }

  public void setResource(Resource resource) {
    this.resource = resource;
  }

  /**
   * Number of containers for this component (optional). If not specified,
   * the service level global number_of_containers takes effect.
   **/
  public Component numberOfContainers(Long numberOfContainers) {
    this.numberOfContainers = numberOfContainers;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Number of containers for this component (optional). If not specified, the service level global number_of_containers takes effect.")
  public Long getNumberOfContainers() {
    return numberOfContainers;
  }

  public void setNumberOfContainers(Long numberOfContainers) {
    this.numberOfContainers = numberOfContainers;
  }

  /**
   * A list of decommissioned component instances.
   **/
  public Component decommissionedInstances(List<String>
      decommissionedInstances) {
    this.decommissionedInstances = decommissionedInstances;
    return this;
  }

  @ApiModelProperty(example = "null", value = "A list of decommissioned component instances.")
  public List<String> getDecommissionedInstances() {
    return decommissionedInstances;
  }

  public void setDecommissionedInstances(List<String> decommissionedInstances) {
    this.decommissionedInstances = decommissionedInstances;
  }

  public void addDecommissionedInstance(String componentInstanceName) {
    this.decommissionedInstances.add(componentInstanceName);
  }

  @ApiModelProperty(example = "null", value = "Containers of a started component. Specifying a value for this attribute for the POST payload raises a validation error. This blob is available only in the GET response of a started service.")
  public List<Container> getContainers() {
    return containers;
  }

  public void setContainers(List<Container> containers) {
    this.containers = containers;
  }

  public void addContainer(Container container) {
    this.containers.add(container);
  }

  public void removeContainer(Container container) {
    containers.remove(container);
  }
  public Container getContainer(String id) {
    for (Container container : containers) {
      if (container.getId().equals(id)) {
        return container;
      }
    }
    return null;
  }

  public Container getComponentInstance(String compInstanceName) {
    for (Container container : containers) {
      if (compInstanceName.equals(container.getComponentInstanceName())) {
        return container;
      }
    }
    return null;
  }

  /**
   * Run all containers of this component in privileged mode (YARN-4262).
   **/
  public Component runPrivilegedContainer(Boolean runPrivilegedContainer) {
    this.runPrivilegedContainer = runPrivilegedContainer;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Run all containers of this component in privileged mode (YARN-4262).")
  public Boolean getRunPrivilegedContainer() {
    return runPrivilegedContainer;
  }

  public void setRunPrivilegedContainer(Boolean runPrivilegedContainer) {
    this.runPrivilegedContainer = runPrivilegedContainer;
  }

  /**
   * Advanced scheduling and placement policies for all containers of this
   * component.
   **/
  public Component placementPolicy(PlacementPolicy placementPolicy) {
    this.placementPolicy = placementPolicy;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Advanced scheduling and "
      + "placement policies for all containers of this component.")
  public PlacementPolicy getPlacementPolicy() {
    return placementPolicy;
  }

  public void setPlacementPolicy(PlacementPolicy placementPolicy) {
    this.placementPolicy = placementPolicy;
  }

  /**
   * Config properties for this component.
   **/
  public Component configuration(Configuration configuration) {
    this.configuration = configuration;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Config properties for this component.")
  public Configuration getConfiguration() {
    return configuration;
  }

  public void setConfiguration(Configuration configuration) {
    this.configuration = configuration;
  }

  /**
   * A list of quicklink keys defined at the service level, and to be
   * resolved by this component.
   **/
  public Component quicklinks(List<String> quicklinks) {
    this.quicklinks = quicklinks;
    return this;
  }

  @ApiModelProperty(example = "null", value = "A list of quicklink keys defined at the service level, and to be resolved by this component.")
  public List<String> getQuicklinks() {
    return quicklinks;
  }

  public void setQuicklinks(List<String> quicklinks) {
    this.quicklinks = quicklinks;
  }

  public Component state(ComponentState state) {
    this.state = state;
    return this;
  }

  @ApiModelProperty(example = "null", value = "State of the component.")
  public ComponentState getState() {
    return state;
  }

  public void setState(ComponentState state) {
    this.state = state;
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
        && Objects.equals(this.numberOfContainers, component.numberOfContainers)
        && Objects.equals(this.runPrivilegedContainer,
            component.runPrivilegedContainer)
        && Objects.equals(this.placementPolicy, component.placementPolicy)
        && Objects.equals(this.configuration, component.configuration)
        && Objects.equals(this.quicklinks, component.quicklinks)
        && Objects.equals(this.state, component.state);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, dependencies, readinessCheck, artifact,
        launchCommand, resource, numberOfContainers,
        runPrivilegedContainer, placementPolicy, configuration, quicklinks, state);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Component {\n")
        .append("    name: ").append(toIndentedString(name)).append("\n")
        .append("    state: ").append(toIndentedString(state)).append("\n")
        .append("    dependencies: ").append(toIndentedString(dependencies))
        .append("\n")
        .append("    readinessCheck: ").append(toIndentedString(readinessCheck))
        .append("\n")
        .append("    artifact: ").append(toIndentedString(artifact))
        .append("\n")
        .append("    launchCommand: ").append(toIndentedString(launchCommand))
        .append("\n")
        .append("    resource: ").append(toIndentedString(resource))
        .append("\n")
        .append("    numberOfContainers: ")
        .append(toIndentedString(numberOfContainers)).append("\n")
        .append("    containers: ").append(toIndentedString(containers))
        .append("\n")
        .append("    runPrivilegedContainer: ")
        .append(toIndentedString(runPrivilegedContainer)).append("\n")
        .append("    placementPolicy: ")
        .append(toIndentedString(placementPolicy))
        .append("\n")
        .append("    configuration: ").append(toIndentedString(configuration))
        .append("\n")
        .append("    quicklinks: ").append(toIndentedString(quicklinks))
        .append("\n")
        .append("}");
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

  /**
   * Merge from another component into this component without overwriting.
   */
  public void mergeFrom(Component that) {
    if (this.getArtifact() == null) {
      this.setArtifact(that.getArtifact());
    }
    if (this.getResource() == null) {
      this.setResource(that.getResource());
    }
    if (this.getNumberOfContainers() == null) {
      this.setNumberOfContainers(that.getNumberOfContainers());
    }
    if (this.getLaunchCommand() == null) {
      this.setLaunchCommand(that.getLaunchCommand());
    }
    this.getConfiguration().mergeFrom(that.getConfiguration());
    if (this.getQuicklinks() == null) {
      this.setQuicklinks(that.getQuicklinks());
    }
    if (this.getRunPrivilegedContainer() == null) {
      this.setRunPrivilegedContainer(that.getRunPrivilegedContainer());
    }
    if (this.getDependencies() == null) {
      this.setDependencies(that.getDependencies());
    }
    if (this.getPlacementPolicy() == null) {
      this.setPlacementPolicy(that.getPlacementPolicy());
    }
    if (this.getReadinessCheck() == null) {
      this.setReadinessCheck(that.getReadinessCheck());
    }
  }

  public void overwrite(Component that) {
    setArtifact(that.getArtifact());
    setResource(that.resource);
    setNumberOfContainers(that.getNumberOfContainers());
    setLaunchCommand(that.getLaunchCommand());
    setConfiguration(that.configuration);
    setRunPrivilegedContainer(that.getRunPrivilegedContainer());
    setDependencies(that.getDependencies());
    setPlacementPolicy(that.getPlacementPolicy());
    setReadinessCheck(that.getReadinessCheck());
  }
}

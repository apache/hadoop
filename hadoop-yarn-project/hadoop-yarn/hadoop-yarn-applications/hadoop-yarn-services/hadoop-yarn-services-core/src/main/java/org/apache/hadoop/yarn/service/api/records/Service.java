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

package org.apache.hadoop.yarn.service.api.records;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An Service resource has the following attributes.
 **/
@InterfaceAudience.Public
@InterfaceStability.Unstable
@ApiModel(description = "An Service resource has the following attributes.")
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaClientCodegen", date = "2016-06-02T08:15:05.615-07:00")
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "name", "version", "description", "state", "resource",
    "number_of_containers", "lifetime", "containers" })
public class Service extends BaseResource {
  private static final long serialVersionUID = -4491694636566094885L;

  private String name = null;
  private String id = null;
  private Artifact artifact = null;
  private Resource resource = null;
  @JsonProperty("launch_time")
  @XmlElement(name = "launch_time")
  private Date launchTime = null;
  @JsonProperty("number_of_running_containers")
  @XmlElement(name = "number_of_running_containers")
  private Long numberOfRunningContainers = null;
  private Long lifetime = null;
  @JsonProperty("placement_policy")
  @XmlElement(name = "placement_policy")
  private PlacementPolicy placementPolicy = null;
  private List<Component> components = new ArrayList<>();
  private Configuration configuration = new Configuration();
  private ServiceState state = null;
  private Map<String, String> quicklinks = new HashMap<>();
  private String queue = null;
  @JsonProperty("kerberos_principal")
  @XmlElement(name = "kerberos_principal")
  private KerberosPrincipal kerberosPrincipal = new KerberosPrincipal();
  private String version = null;
  private String description = null;

  /**
   * A unique service name.
   **/
  public Service name(String name) {
    this.name = name;
    return this;
  }

  @ApiModelProperty(example = "null", required = true, value = "A unique service name.")
  @JsonProperty("name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  /**
   * A unique service id.
   **/
  public Service id(String id) {
    this.id = id;
    return this;
  }

  @ApiModelProperty(example = "null", value = "A unique service id.")
  @JsonProperty("id")
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  @ApiModelProperty(example = "null", required = true,
      value = "Version of the service.")
  @JsonProperty("version")
  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  /**
   * Version of the service.
   */
  public Service version(String version) {
    this.version = version;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Description of the service.")
  @JsonProperty("description")
  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * Description of the service.
   */
  public Service description(String description) {
    this.description = description;
    return this;
  }

  /**
   * Artifact of single-component services. Mandatory if components
   * attribute is not specified.
   **/
  public Service artifact(Artifact artifact) {
    this.artifact = artifact;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Artifact of single-component services. Mandatory if components attribute is not specified.")
  @JsonProperty("artifact")
  public Artifact getArtifact() {
    return artifact;
  }

  public void setArtifact(Artifact artifact) {
    this.artifact = artifact;
  }

  /**
   * Resource of single-component services or the global default for
   * multi-component services. Mandatory if it is a single-component
   * service and if cpus and memory are not specified at the Service
   * level.
   **/
  public Service resource(Resource resource) {
    this.resource = resource;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Resource of single-component services or the global default for multi-component services. Mandatory if it is a single-component service and if cpus and memory are not specified at the Service level.")
  @JsonProperty("resource")
  public Resource getResource() {
    return resource;
  }

  public void setResource(Resource resource) {
    this.resource = resource;
  }

  /**
   * The time when the service was created, e.g. 2016-03-16T01:01:49.000Z.
   **/
  public Service launchTime(Date launchTime) {
    this.launchTime = launchTime == null ? null : (Date) launchTime.clone();
    return this;
  }

  @ApiModelProperty(example = "null", value = "The time when the service was created, e.g. 2016-03-16T01:01:49.000Z.")
  public Date getLaunchTime() {
    return launchTime == null ? null : (Date) launchTime.clone();
  }

  public void setLaunchTime(Date launchTime) {
    this.launchTime = launchTime == null ? null : (Date) launchTime.clone();
  }

  /**
   * In get response this provides the total number of running containers for
   * this service (across all components) at the time of request. Note, a
   * subsequent request can return a different number as and when more
   * containers get allocated until it reaches the total number of containers or
   * if a flex request has been made between the two requests.
   **/
  public Service numberOfRunningContainers(Long numberOfRunningContainers) {
    this.numberOfRunningContainers = numberOfRunningContainers;
    return this;
  }

  @ApiModelProperty(example = "null", value = "In get response this provides the total number of running containers for this service (across all components) at the time of request. Note, a subsequent request can return a different number as and when more containers get allocated until it reaches the total number of containers or if a flex request has been made between the two requests.")
  public Long getNumberOfRunningContainers() {
    return numberOfRunningContainers;
  }

  public void setNumberOfRunningContainers(Long numberOfRunningContainers) {
    this.numberOfRunningContainers = numberOfRunningContainers;
  }

  /**
   * Life time (in seconds) of the service from the time it reaches the
   * RUNNING_BUT_UNREADY state (after which it is automatically destroyed by YARN). For
   * unlimited lifetime do not set a lifetime value.
   **/
  public Service lifetime(Long lifetime) {
    this.lifetime = lifetime;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Life time (in seconds) of the service from the time it reaches the RUNNING_BUT_UNREADY state (after which it is automatically destroyed by YARN). For unlimited lifetime do not set a lifetime value.")
  @JsonProperty("lifetime")
  public Long getLifetime() {
    return lifetime;
  }

  public void setLifetime(Long lifetime) {
    this.lifetime = lifetime;
  }

  /**
   * Advanced scheduling and placement policies (optional). If not specified, it
   * defaults to the default placement policy of the service owner. The design of
   * placement policies are in the works. It is not very clear at this point,
   * how policies in conjunction with labels be exposed to service owners.
   * This is a placeholder for now. The advanced structure of this attribute
   * will be determined by YARN-4902.
   **/
  public Service placementPolicy(PlacementPolicy placementPolicy) {
    this.placementPolicy = placementPolicy;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Advanced scheduling and placement policies (optional). If not specified, it defaults to the default placement policy of the service owner. The design of placement policies are in the works. It is not very clear at this point, how policies in conjunction with labels be exposed to service owners. This is a placeholder for now. The advanced structure of this attribute will be determined by YARN-4902.")
  public PlacementPolicy getPlacementPolicy() {
    return placementPolicy;
  }

  public void setPlacementPolicy(PlacementPolicy placementPolicy) {
    this.placementPolicy = placementPolicy;
  }

  /**
   * Components of an service.
   **/
  public Service components(List<Component> components) {
    this.components = components;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Components of an service.")
  @JsonProperty("components")
  public List<Component> getComponents() {
    return components;
  }

  public void setComponents(List<Component> components) {
    this.components = components;
  }

  public void addComponent(Component component) {
    components.add(component);
  }

  public Component getComponent(String name) {
    for (Component component : components) {
      if (component.getName().equals(name)) {
        return component;
      }
    }
    return null;
  }

  /**
   * Config properties of an service. Configurations provided at the
   * service/global level are available to all the components. Specific
   * properties can be overridden at the component level.
   **/
  public Service configuration(Configuration configuration) {
    this.configuration = configuration;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Config properties of an service. Configurations provided at the service/global level are available to all the components. Specific properties can be overridden at the component level.")
  @JsonProperty("configuration")
  public Configuration getConfiguration() {
    return configuration;
  }

  public void setConfiguration(Configuration configuration) {
    this.configuration = configuration;
  }

  /**
   * State of the service. Specifying a value for this attribute for the
   * POST payload raises a validation error. This attribute is available only in
   * the GET response of a started service.
   **/
  public Service state(ServiceState state) {
    this.state = state;
    return this;
  }

  @ApiModelProperty(example = "null", value = "State of the service. Specifying a value for this attribute for the POST payload raises a validation error. This attribute is available only in the GET response of a started service.")
  @JsonProperty("state")
  public ServiceState getState() {
    return state;
  }

  public void setState(ServiceState state) {
    this.state = state;
  }

  /**
   * A blob of key-value pairs of quicklinks to be exported for an service.
   **/
  public Service quicklinks(Map<String, String> quicklinks) {
    this.quicklinks = quicklinks;
    return this;
  }

  @ApiModelProperty(example = "null", value = "A blob of key-value pairs of quicklinks to be exported for an service.")
  @JsonProperty("quicklinks")
  public Map<String, String> getQuicklinks() {
    return quicklinks;
  }

  public void setQuicklinks(Map<String, String> quicklinks) {
    this.quicklinks = quicklinks;
  }

  /**
   * The YARN queue that this service should be submitted to.
   **/
  public Service queue(String queue) {
    this.queue = queue;
    return this;
  }

  @ApiModelProperty(example = "null", value = "The YARN queue that this service should be submitted to.")
  @JsonProperty("queue")
  public String getQueue() {
    return queue;
  }

  public void setQueue(String queue) {
    this.queue = queue;
  }

  public Service kerberosPrincipal(KerberosPrincipal kerberosPrincipal) {
    this.kerberosPrincipal = kerberosPrincipal;
    return this;
  }

  /**
   * The Kerberos Principal of the service.
   * @return kerberosPrincipal
   **/
  @ApiModelProperty(value = "The Kerberos Principal of the service")
  public KerberosPrincipal getKerberosPrincipal() {
    return kerberosPrincipal;
  }

  public void setKerberosPrincipal(KerberosPrincipal kerberosPrincipal) {
    this.kerberosPrincipal = kerberosPrincipal;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Service service = (Service) o;
    return Objects.equals(this.name, service.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Service {\n");

    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    version: ").append(toIndentedString(version)).append("\n");
    sb.append("    description: ").append(toIndentedString(description))
        .append("\n");
    sb.append("    artifact: ").append(toIndentedString(artifact)).append("\n");
    sb.append("    resource: ").append(toIndentedString(resource)).append("\n");
    sb.append("    launchTime: ").append(toIndentedString(launchTime))
        .append("\n");
    sb.append("    numberOfRunningContainers: ")
        .append(toIndentedString(numberOfRunningContainers)).append("\n");
    sb.append("    lifetime: ").append(toIndentedString(lifetime)).append("\n");
    sb.append("    placementPolicy: ").append(toIndentedString(placementPolicy))
        .append("\n");
    sb.append("    components: ").append(toIndentedString(components))
        .append("\n");
    sb.append("    configuration: ").append(toIndentedString(configuration))
        .append("\n");
    sb.append("    state: ").append(toIndentedString(state)).append("\n");
    sb.append("    quicklinks: ").append(toIndentedString(quicklinks))
        .append("\n");
    sb.append("    queue: ").append(toIndentedString(queue)).append("\n");
    sb.append("    kerberosPrincipal: ")
        .append(toIndentedString(kerberosPrincipal)).append("\n");
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

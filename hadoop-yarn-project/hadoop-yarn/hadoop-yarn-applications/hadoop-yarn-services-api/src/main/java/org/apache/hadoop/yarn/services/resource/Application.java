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

package org.apache.hadoop.yarn.services.resource;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.slider.providers.PlacementPolicy;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * An Application resource has the following attributes.
 **/

@ApiModel(description = "An Application resource has the following attributes.")
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaClientCodegen", date = "2016-06-02T08:15:05.615-07:00")
@XmlRootElement
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ " name, state, resource, numberOfContainers, lifetime, containers " })
public class Application extends BaseResource {
  private static final long serialVersionUID = -4491694636566094885L;

  private String name = null;
  private String id = null;
  private Artifact artifact = null;
  private Resource resource = null;
  private String launchCommand = null;
  private Date launchTime = null;
  private Long numberOfContainers = null;
  private Long numberOfRunningContainers = null;
  private Long lifetime = null;
  private PlacementPolicy placementPolicy = null;
  private List<Component> components = null;
  private Configuration configuration = null;
  private List<Container> containers = new ArrayList<>();
  private ApplicationState state = null;
  private Map<String, String> quicklinks = null;
  private String queue = null;

  /**
   * A unique application name.
   **/
  public Application name(String name) {
    this.name = name;
    return this;
  }

  @ApiModelProperty(example = "null", required = true, value = "A unique application name.")
  @JsonProperty("name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  /**
   * A unique application id.
   **/
  public Application id(String id) {
    this.id = id;
    return this;
  }

  @ApiModelProperty(example = "null", value = "A unique application id.")
  @JsonProperty("id")
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  /**
   * Artifact of single-component applications. Mandatory if components
   * attribute is not specified.
   **/
  public Application artifact(Artifact artifact) {
    this.artifact = artifact;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Artifact of single-component applications. Mandatory if components attribute is not specified.")
  @JsonProperty("artifact")
  public Artifact getArtifact() {
    return artifact;
  }

  public void setArtifact(Artifact artifact) {
    this.artifact = artifact;
  }

  /**
   * Resource of single-component applications or the global default for
   * multi-component applications. Mandatory if it is a single-component
   * application and if cpus and memory are not specified at the Application
   * level.
   **/
  public Application resource(Resource resource) {
    this.resource = resource;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Resource of single-component applications or the global default for multi-component applications. Mandatory if it is a single-component application and if cpus and memory are not specified at the Application level.")
  @JsonProperty("resource")
  public Resource getResource() {
    return resource;
  }

  public void setResource(Resource resource) {
    this.resource = resource;
  }

  /**
   * The custom launch command of an application component (optional). If not
   * specified for applications with docker images say, it will default to the
   * default start command of the image. If there is a single component in this
   * application, you can specify this without the need to have a 'components'
   * section.
   **/
  public Application launchCommand(String launchCommand) {
    this.launchCommand = launchCommand;
    return this;
  }

  @ApiModelProperty(example = "null", value = "The custom launch command of an application component (optional). If not specified for applications with docker images say, it will default to the default start command of the image. If there is a single component in this application, you can specify this without the need to have a 'components' section.")
  @JsonProperty("launch_command")
  public String getLaunchCommand() {
    return launchCommand;
  }

  @XmlElement(name = "launch_command")
  public void setLaunchCommand(String launchCommand) {
    this.launchCommand = launchCommand;
  }

  /**
   * The time when the application was created, e.g. 2016-03-16T01:01:49.000Z.
   **/
  public Application launchTime(Date launchTime) {
    this.launchTime = launchTime == null ? null : (Date) launchTime.clone();
    return this;
  }

  @ApiModelProperty(example = "null", value = "The time when the application was created, e.g. 2016-03-16T01:01:49.000Z.")
  @JsonProperty("launch_time")
  public String getLaunchTime() {
    return launchTime == null ? null : launchTime.toString();
  }

  @XmlElement(name = "launch_time")
  public void setLaunchTime(Date launchTime) {
    this.launchTime = launchTime == null ? null : (Date) launchTime.clone();
  }

  /**
   * Number of containers for each app-component in the application. Each
   * app-component can further override this app-level global default.
   **/
  public Application numberOfContainers(Long numberOfContainers) {
    this.numberOfContainers = numberOfContainers;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Number of containers for each app-component in the application. Each app-component can further override this app-level global default.")
  @JsonProperty("number_of_containers")
  public Long getNumberOfContainers() {
    return numberOfContainers;
  }

  @XmlElement(name = "number_of_containers")
  public void setNumberOfContainers(Long numberOfContainers) {
    this.numberOfContainers = numberOfContainers;
  }

  /**
   * In get response this provides the total number of running containers for
   * this application (across all components) at the time of request. Note, a
   * subsequent request can return a different number as and when more
   * containers get allocated until it reaches the total number of containers or
   * if a flex request has been made between the two requests.
   **/
  public Application numberOfRunningContainers(Long numberOfRunningContainers) {
    this.numberOfRunningContainers = numberOfRunningContainers;
    return this;
  }

  @ApiModelProperty(example = "null", value = "In get response this provides the total number of running containers for this application (across all components) at the time of request. Note, a subsequent request can return a different number as and when more containers get allocated until it reaches the total number of containers or if a flex request has been made between the two requests.")
  @JsonProperty("number_of_running_containers")
  public Long getNumberOfRunningContainers() {
    return numberOfRunningContainers;
  }

  @XmlElement(name = "number_of_running_containers")
  public void setNumberOfRunningContainers(Long numberOfRunningContainers) {
    this.numberOfRunningContainers = numberOfRunningContainers;
  }

  /**
   * Life time (in seconds) of the application from the time it reaches the
   * STARTED state (after which it is automatically destroyed by YARN). For
   * unlimited lifetime do not set a lifetime value.
   **/
  public Application lifetime(Long lifetime) {
    this.lifetime = lifetime;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Life time (in seconds) of the application from the time it reaches the STARTED state (after which it is automatically destroyed by YARN). For unlimited lifetime do not set a lifetime value.")
  @JsonProperty("lifetime")
  public Long getLifetime() {
    return lifetime;
  }

  public void setLifetime(Long lifetime) {
    this.lifetime = lifetime;
  }

  /**
   * Advanced scheduling and placement policies (optional). If not specified, it
   * defaults to the default placement policy of the app owner. The design of
   * placement policies are in the works. It is not very clear at this point,
   * how policies in conjunction with labels be exposed to application owners.
   * This is a placeholder for now. The advanced structure of this attribute
   * will be determined by YARN-4902.
   **/
  public Application placementPolicy(PlacementPolicy placementPolicy) {
    this.placementPolicy = placementPolicy;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Advanced scheduling and placement policies (optional). If not specified, it defaults to the default placement policy of the app owner. The design of placement policies are in the works. It is not very clear at this point, how policies in conjunction with labels be exposed to application owners. This is a placeholder for now. The advanced structure of this attribute will be determined by YARN-4902.")
  @JsonProperty("placement_policy")
  public PlacementPolicy getPlacementPolicy() {
    return placementPolicy;
  }

  @XmlElement(name = "placement_policy")
  public void setPlacementPolicy(PlacementPolicy placementPolicy) {
    this.placementPolicy = placementPolicy;
  }

  /**
   * Components of an application.
   **/
  public Application components(List<Component> components) {
    this.components = components;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Components of an application.")
  @JsonProperty("components")
  public List<Component> getComponents() {
    return components;
  }

  public void setComponents(List<Component> components) {
    this.components = components;
  }

  /**
   * Config properties of an application. Configurations provided at the
   * application/global level are available to all the components. Specific
   * properties can be overridden at the component level.
   **/
  public Application configuration(Configuration configuration) {
    this.configuration = configuration;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Config properties of an application. Configurations provided at the application/global level are available to all the components. Specific properties can be overridden at the component level.")
  @JsonProperty("configuration")
  public Configuration getConfiguration() {
    return configuration;
  }

  public void setConfiguration(Configuration configuration) {
    this.configuration = configuration;
  }

  /**
   * Containers of a started application. Specifying a value for this attribute
   * for the POST payload raises a validation error. This blob is available only
   * in the GET response of a started application.
   **/
  public Application containers(List<Container> containers) {
    this.containers = containers;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Containers of a started application. Specifying a value for this attribute for the POST payload raises a validation error. This blob is available only in the GET response of a started application.")
  @JsonProperty("containers")
  public List<Container> getContainers() {
    return containers;
  }

  public void setContainers(List<Container> containers) {
    this.containers = containers;
  }

  public void addContainer(Container container) {
    this.containers.add(container);
  }

  /**
   * State of the application. Specifying a value for this attribute for the
   * POST payload raises a validation error. This attribute is available only in
   * the GET response of a started application.
   **/
  public Application state(ApplicationState state) {
    this.state = state;
    return this;
  }

  @ApiModelProperty(example = "null", value = "State of the application. Specifying a value for this attribute for the POST payload raises a validation error. This attribute is available only in the GET response of a started application.")
  @JsonProperty("state")
  public ApplicationState getState() {
    return state;
  }

  public void setState(ApplicationState state) {
    this.state = state;
  }

  /**
   * A blob of key-value pairs of quicklinks to be exported for an application.
   **/
  public Application quicklinks(Map<String, String> quicklinks) {
    this.quicklinks = quicklinks;
    return this;
  }

  @ApiModelProperty(example = "null", value = "A blob of key-value pairs of quicklinks to be exported for an application.")
  @JsonProperty("quicklinks")
  public Map<String, String> getQuicklinks() {
    return quicklinks;
  }

  public void setQuicklinks(Map<String, String> quicklinks) {
    this.quicklinks = quicklinks;
  }

  /**
   * The YARN queue that this application should be submitted to.
   **/
  public Application queue(String queue) {
    this.queue = queue;
    return this;
  }

  @ApiModelProperty(example = "null", value = "The YARN queue that this application should be submitted to.")
  @JsonProperty("queue")
  public String getQueue() {
    return queue;
  }

  public void setQueue(String queue) {
    this.queue = queue;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Application application = (Application) o;
    return Objects.equals(this.name, application.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Application {\n");

    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    artifact: ").append(toIndentedString(artifact)).append("\n");
    sb.append("    resource: ").append(toIndentedString(resource)).append("\n");
    sb.append("    launchCommand: ").append(toIndentedString(launchCommand))
        .append("\n");
    sb.append("    launchTime: ").append(toIndentedString(launchTime))
        .append("\n");
    sb.append("    numberOfContainers: ")
        .append(toIndentedString(numberOfContainers)).append("\n");
    sb.append("    numberOfRunningContainers: ")
        .append(toIndentedString(numberOfRunningContainers)).append("\n");
    sb.append("    lifetime: ").append(toIndentedString(lifetime)).append("\n");
    sb.append("    placementPolicy: ").append(toIndentedString(placementPolicy))
        .append("\n");
    sb.append("    components: ").append(toIndentedString(components))
        .append("\n");
    sb.append("    configuration: ").append(toIndentedString(configuration))
        .append("\n");
    sb.append("    containers: ").append(toIndentedString(containers))
        .append("\n");
    sb.append("    state: ").append(toIndentedString(state)).append("\n");
    sb.append("    quicklinks: ").append(toIndentedString(quicklinks))
        .append("\n");
    sb.append("    queue: ").append(toIndentedString(queue)).append("\n");
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

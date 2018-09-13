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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.Date;
import java.util.Objects;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * An instance of a running service container.
 **/
@InterfaceAudience.Public
@InterfaceStability.Unstable
@ApiModel(description = "An instance of a running service container")
@XmlRootElement
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Container extends BaseResource {
  private static final long serialVersionUID = -8955788064529288L;

  private String id = null;
  private Date launchTime = null;
  private String ip = null;
  private String hostname = null;
  private String bareHost = null;
  private ContainerState state = null;
  private String componentInstanceName = null;
  private Resource resource = null;
  private Artifact artifact = null;
  private Boolean privilegedContainer = null;

  /**
   * Unique container id of a running service, e.g.
   * container_e3751_1458061340047_0008_01_000002.
   **/
  public Container id(String id) {
    this.id = id;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Unique container id of a running service, e.g. container_e3751_1458061340047_0008_01_000002.")
  @JsonProperty("id")
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  /**
   * The time when the container was created, e.g. 2016-03-16T01:01:49.000Z.
   * This will most likely be different from cluster launch time.
   **/
  public Container launchTime(Date launchTime) {
    this.launchTime = launchTime == null ? null : (Date) launchTime.clone();
    return this;
  }

  @ApiModelProperty(example = "null", value = "The time when the container was created, e.g. 2016-03-16T01:01:49.000Z. This will most likely be different from cluster launch time.")
  @JsonProperty("launch_time")
  public Date getLaunchTime() {
    return launchTime == null ? null : (Date) launchTime.clone();
  }

  @XmlElement(name = "launch_time")
  public void setLaunchTime(Date launchTime) {
    this.launchTime = launchTime == null ? null : (Date) launchTime.clone();
  }

  /**
   * IP address of a running container, e.g. 172.31.42.141. The IP address and
   * hostname attribute values are dependent on the cluster/docker network setup
   * as per YARN-4007.
   **/
  public Container ip(String ip) {
    this.ip = ip;
    return this;
  }

  @ApiModelProperty(example = "null", value = "IP address of a running container, e.g. 172.31.42.141. The IP address and hostname attribute values are dependent on the cluster/docker network setup as per YARN-4007.")
  @JsonProperty("ip")
  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  /**
   * Fully qualified hostname of a running container, e.g.
   * ctr-e3751-1458061340047-0008-01-000002.examplestg.site. The IP address and
   * hostname attribute values are dependent on the cluster/docker network setup
   * as per YARN-4007.
   **/
  public Container hostname(String hostname) {
    this.hostname = hostname;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Fully qualified hostname of a running container, e.g. ctr-e3751-1458061340047-0008-01-000002.examplestg.site. The IP address and hostname attribute values are dependent on the cluster/docker network setup as per YARN-4007.")
  @JsonProperty("hostname")
  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  /**
   * The bare node or host in which the container is running, e.g.
   * cn008.example.com.
   **/
  public Container bareHost(String bareHost) {
    this.bareHost = bareHost;
    return this;
  }

  @ApiModelProperty(example = "null", value = "The bare node or host in which the container is running, e.g. cn008.example.com.")
  @JsonProperty("bare_host")
  public String getBareHost() {
    return bareHost;
  }

  @XmlElement(name = "bare_host")
  public void setBareHost(String bareHost) {
    this.bareHost = bareHost;
  }

  /**
   * State of the container of an service.
   **/
  public Container state(ContainerState state) {
    this.state = state;
    return this;
  }

  @ApiModelProperty(example = "null", value = "State of the container of an service.")
  @JsonProperty("state")
  public ContainerState getState() {
    return state;
  }

  public void setState(ContainerState state) {
    this.state = state;
  }

  /**
   * Name of the component instance that this container instance belongs to.
   **/
  public Container componentInstanceName(String componentInstanceName) {
    this.componentInstanceName = componentInstanceName;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Name of the component instance that this container instance belongs to.")
  @JsonProperty("component_instance_name")
  public String getComponentInstanceName() {
    return componentInstanceName;
  }

  @XmlElement(name = "component_instance_name")
  public void setComponentInstanceName(String componentInstanceName) {
    this.componentInstanceName = componentInstanceName;
  }

  /**
   * Resource used for this container.
   **/
  public Container resource(Resource resource) {
    this.resource = resource;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Resource used for this container.")
  @JsonProperty("resource")
  public Resource getResource() {
    return resource;
  }

  public void setResource(Resource resource) {
    this.resource = resource;
  }

  /**
   * Artifact used for this container.
   **/
  public Container artifact(Artifact artifact) {
    this.artifact = artifact;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Artifact used for this container.")
  @JsonProperty("artifact")
  public Artifact getArtifact() {
    return artifact;
  }

  public void setArtifact(Artifact artifact) {
    this.artifact = artifact;
  }

  /**
   * Container running in privileged mode or not.
   **/
  public Container privilegedContainer(Boolean privilegedContainer) {
    this.privilegedContainer = privilegedContainer;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Container running in privileged mode or not.")
  @JsonProperty("privileged_container")
  public Boolean getPrivilegedContainer() {
    return privilegedContainer;
  }

  public void setPrivilegedContainer(Boolean privilegedContainer) {
    this.privilegedContainer = privilegedContainer;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Container container = (Container) o;
    return Objects.equals(this.id, container.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Container {\n");

    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    launchTime: ").append(toIndentedString(launchTime))
        .append("\n");
    sb.append("    ip: ").append(toIndentedString(ip)).append("\n");
    sb.append("    hostname: ").append(toIndentedString(hostname)).append("\n");
    sb.append("    bareHost: ").append(toIndentedString(bareHost)).append("\n");
    sb.append("    state: ").append(toIndentedString(state)).append("\n");
    sb.append("    componentInstanceName: ").append(toIndentedString(
        componentInstanceName))
        .append("\n");
    sb.append("    resource: ").append(toIndentedString(resource)).append("\n");
    sb.append("    artifact: ").append(toIndentedString(artifact)).append("\n");
    sb.append("    privilegedContainer: ")
        .append(toIndentedString(privilegedContainer)).append("\n");
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

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

import java.util.Date;
import java.util.Objects;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@ApiModel(description = "An instance of a running application container")
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaClientCodegen", date = "2016-06-02T08:15:05.615-07:00")
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
  private String componentName = null;
  private Resource resource = null;

  /**
   * Unique container id of a running application, e.g.
   * container_e3751_1458061340047_0008_01_000002
   **/
  public Container id(String id) {
    this.id = id;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Unique container id of a running application, e.g. container_e3751_1458061340047_0008_01_000002")
  @JsonProperty("id")
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  /**
   * The time when the container was created, e.g. 2016-03-16T01:01:49.000Z. This will most likely be different from cluster launch time.
   **/
  public Container launchTime(Date launchTime) {
    this.launchTime = launchTime;
    return this;
  }

  @ApiModelProperty(example = "null", value = "The time when the container was created, e.g. 2016-03-16T01:01:49.000Z. This will most likely be different from cluster launch time.")
  @JsonProperty("launch_time")
  public String getLaunchTime() {
    return launchTime.toString();
  }

  @XmlElement(name = "launch_time")
  public void setLaunchTime(Date launchTime) {
    this.launchTime = launchTime;
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
   * cn008.example.com
   **/
  public Container bareHost(String bareHost) {
    this.bareHost = bareHost;
    return this;
  }

  @ApiModelProperty(example = "null", value = "The bare node or host in which the container is running, e.g. cn008.example.com")
  @JsonProperty("bare_host")
  public String getBareHost() {
    return bareHost;
  }

  @XmlElement(name = "bare_host")
  public void setBareHost(String bareHost) {
    this.bareHost = bareHost;
  }

  /**
   * State of the container of an application.
   **/
  public Container state(ContainerState state) {
    this.state = state;
    return this;
  }

  @ApiModelProperty(example = "null", value = "State of the container of an application.")
  @JsonProperty("state")
  public ContainerState getState() {
    return state;
  }

  public void setState(ContainerState state) {
    this.state = state;
  }

  /**
   * Name of the component that this container instance belongs to.
   **/
  public Container componentName(String componentName) {
    this.componentName = componentName;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Name of the component that this container instance belongs to.")
  @JsonProperty("component_name")
  public String getComponentName() {
    return componentName;
  }

  @XmlElement(name = "component_name")
  public void setComponentName(String componentName) {
    this.componentName = componentName;
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

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Container container = (Container) o;
    return Objects.equals(this.id, container.id)
        && Objects.equals(this.launchTime, container.launchTime)
        && Objects.equals(this.ip, container.ip)
        && Objects.equals(this.hostname, container.hostname)
        && Objects.equals(this.bareHost, container.bareHost)
        && Objects.equals(this.state, container.state)
        && Objects.equals(this.componentName, container.componentName)
        && Objects.equals(this.resource, container.resource);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, launchTime, ip, hostname, bareHost, state,
        componentName, resource);
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
    sb.append("    componentName: ").append(toIndentedString(componentName))
        .append("\n");
    sb.append("    resource: ").append(toIndentedString(resource)).append("\n");
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

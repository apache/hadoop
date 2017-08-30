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

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Resource determines the amount of resources (vcores, memory, network, etc.)
 * usable by a container. This field determines the resource to be applied for
 * all the containers of a component or service. The resource specified at
 * the service (or global) level can be overriden at the component level. Only one
 * of profile OR cpu &amp; memory are expected. It raises a validation
 * exception otherwise.
 **/
@InterfaceAudience.Public
@InterfaceStability.Unstable
@ApiModel(description = "Resource determines the amount of resources (vcores, memory, network, etc.) usable by a container. This field determines the resource to be applied for all the containers of a component or service. The resource specified at the service (or global) level can be overriden at the component level. Only one of profile OR cpu & memory are expected. It raises a validation exception otherwise.")
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaClientCodegen", date = "2016-06-02T08:15:05.615-07:00")
public class Resource extends BaseResource implements Cloneable {
  private static final long serialVersionUID = -6431667797380250037L;

  private String profile = null;
  private Integer cpus = 1;
  private String memory = null;

  /**
   * Each resource profile has a unique id which is associated with a
   * cluster-level predefined memory, cpus, etc.
   **/
  public Resource profile(String profile) {
    this.profile = profile;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Each resource profile has a unique id which is associated with a cluster-level predefined memory, cpus, etc.")
  @JsonProperty("profile")
  public String getProfile() {
    return profile;
  }

  public void setProfile(String profile) {
    this.profile = profile;
  }

  /**
   * Amount of vcores allocated to each container (optional but overrides cpus
   * in profile if specified).
   **/
  public Resource cpus(Integer cpus) {
    this.cpus = cpus;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Amount of vcores allocated to each container (optional but overrides cpus in profile if specified).")
  @JsonProperty("cpus")
  public Integer getCpus() {
    return cpus;
  }

  public void setCpus(Integer cpus) {
    this.cpus = cpus;
  }

  /**
   * Amount of memory allocated to each container (optional but overrides memory
   * in profile if specified). Currently accepts only an integer value and
   * default unit is in MB.
   **/
  public Resource memory(String memory) {
    this.memory = memory;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Amount of memory allocated to each container (optional but overrides memory in profile if specified). Currently accepts only an integer value and default unit is in MB.")
  @JsonProperty("memory")
  public String getMemory() {
    return memory;
  }

  public void setMemory(String memory) {
    this.memory = memory;
  }

  @JsonIgnore
  public long getMemoryMB() {
    if (this.memory == null) {
      return 0;
    }
    return Long.parseLong(memory);
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Resource resource = (Resource) o;
    return Objects.equals(this.profile, resource.profile)
        && Objects.equals(this.cpus, resource.cpus)
        && Objects.equals(this.memory, resource.memory);
  }

  @Override
  public int hashCode() {
    return Objects.hash(profile, cpus, memory);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Resource {\n");

    sb.append("    profile: ").append(toIndentedString(profile)).append("\n");
    sb.append("    cpus: ").append(toIndentedString(cpus)).append("\n");
    sb.append("    memory: ").append(toIndentedString(memory)).append("\n");
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

  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }
}

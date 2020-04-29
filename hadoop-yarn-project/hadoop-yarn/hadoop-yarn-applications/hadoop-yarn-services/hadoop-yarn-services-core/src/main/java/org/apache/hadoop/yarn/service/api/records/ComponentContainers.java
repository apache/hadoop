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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.io.Serializable;
import java.util.List;

/**
 * Containers of a component.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
@ApiModel(description = "Containers of a component.")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ComponentContainers implements Serializable {

  private static final long serialVersionUID = -1456748479118874991L;

  @JsonProperty("component_name")
  private String componentName;

  @JsonProperty("containers")
  private List<Container> containers;

  @ApiModelProperty(example = "null", required = true,
      value = "Name of the component.")
  public String getComponentName() {
    return componentName;
  }

  public void setComponentName(String name) {
    this.componentName = name;
  }

  /**
   * Name of the service component.
   **/
  public ComponentContainers name(String name) {
    this.componentName = name;
    return this;
  }

  /**
   * Returns the containers of the component.
   */
  @ApiModelProperty(example = "null", value = "Containers of the component.")
  public List<Container> getContainers() {
    return containers;
  }

  /**
   * Sets the containers.
   * @param containers containers of the component.
   */
  public void setContainers(List<Container> containers) {
    this.containers = containers;
  }

  /**
   * Sets the containers.
   * @param compContainers containers of the component.
   */
  public ComponentContainers containers(List<Container> compContainers) {
    this.containers = compContainers;
    return this;
  }

  /**
   * Add a container.
   * @param container container
   */
  public void addContainer(Container container) {
    containers.add(container);
  }
}

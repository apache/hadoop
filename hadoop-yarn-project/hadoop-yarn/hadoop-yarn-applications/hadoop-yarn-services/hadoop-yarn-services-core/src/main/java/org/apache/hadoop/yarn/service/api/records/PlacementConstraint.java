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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.xml.bind.annotation.XmlElement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * Placement constraint details.
 **/
@InterfaceAudience.Public
@InterfaceStability.Unstable
@ApiModel(description = "Placement constraint details.")
public class PlacementConstraint implements Serializable {
  private static final long serialVersionUID = 1518017165676511762L;

  private String name = null;
  private PlacementType type = null;
  private PlacementScope scope = null;
  @JsonProperty("target_tags")
  @XmlElement(name = "target_tags")
  private List<String> targetTags = new ArrayList<>();
  @JsonProperty("node_attributes")
  @XmlElement(name = "node_attributes")
  private Map<String, List<String>> nodeAttributes = new HashMap<>();
  @JsonProperty("node_partitions")
  @XmlElement(name = "node_partitions")
  private List<String> nodePartitions = new ArrayList<>();
  @JsonProperty("min_cardinality")
  @XmlElement(name = "min_cardinality")
  private Long minCardinality = null;
  @JsonProperty("max_cardinality")
  @XmlElement(name = "max_cardinality")
  private Long maxCardinality = null;

  /**
   * An optional name associated to this constraint.
   **/
  public PlacementConstraint name(String name) {
    this.name = name;
    return this;
  }

  @ApiModelProperty(example = "C1", required = true)
  @JsonProperty("name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  /**
   * The type of placement.
   **/
  public PlacementConstraint type(PlacementType type) {
    this.type = type;
    return this;
  }

  @ApiModelProperty(example = "null", required = true)
  @JsonProperty("type")
  public PlacementType getType() {
    return type;
  }

  public void setType(PlacementType type) {
    this.type = type;
  }

  /**
   * The scope of placement.
   **/
  public PlacementConstraint scope(PlacementScope scope) {
    this.scope = scope;
    return this;
  }

  @ApiModelProperty(example = "null", required = true)
  @JsonProperty("scope")
  public PlacementScope getScope() {
    return scope;
  }

  public void setScope(PlacementScope scope) {
    this.scope = scope;
  }

  /**
   * The name of the components that this component's placement policy is
   * depending upon are added as target tags. So for affinity say, this
   * component's containers are requesting to be placed on hosts where
   * containers of the target tag component(s) are running on. Target tags can
   * also contain the name of this component, in which case it implies that for
   * anti-affinity say, no more than one container of this component can be
   * placed on a host. Similarly, for cardinality, it would mean that containers
   * of this component is requesting to be placed on hosts where at least
   * minCardinality but no more than maxCardinality containers of the target tag
   * component(s) are running.
   **/
  public PlacementConstraint targetTags(List<String> targetTags) {
    this.targetTags = targetTags;
    return this;
  }

  @ApiModelProperty(example = "[\"hbase-regionserver\"]")
  public List<String> getTargetTags() {
    return targetTags;
  }

  public void setTargetTags(List<String> targetTags) {
    this.targetTags = targetTags;
  }

  /**
   * Node attributes are a set of key:value(s) pairs associated with nodes.
   */
  public PlacementConstraint nodeAttributes(
      Map<String, List<String>> nodeAttributes) {
    this.nodeAttributes = nodeAttributes;
    return this;
  }

  @ApiModelProperty(example = "\"JavaVersion\":[\"1.7\", \"1.8\"]")
  public Map<String, List<String>> getNodeAttributes() {
    return nodeAttributes;
  }

  public void setNodeAttributes(Map<String, List<String>> nodeAttributes) {
    this.nodeAttributes = nodeAttributes;
  }

  /**
   * Node partitions where the containers of this component can run.
   */
  public PlacementConstraint nodePartitions(
      List<String> nodePartitions) {
    this.nodePartitions = nodePartitions;
    return this;
  }

  @ApiModelProperty(example = "[\"gpu\", \"fast_disk\"]")
  public List<String> getNodePartitions() {
    return nodePartitions;
  }

  public void setNodePartitions(List<String> nodePartitions) {
    this.nodePartitions = nodePartitions;
  }

  /**
   * When placement type is cardinality, the minimum number of containers of the
   * depending component that a host should have, where containers of this
   * component can be allocated on.
   **/
  public PlacementConstraint minCardinality(Long minCardinality) {
    this.minCardinality = minCardinality;
    return this;
  }

  @ApiModelProperty(example = "2")
  public Long getMinCardinality() {
    return minCardinality;
  }

  public void setMinCardinality(Long minCardinality) {
    this.minCardinality = minCardinality;
  }

  /**
   * When placement type is cardinality, the maximum number of containers of the
   * depending component that a host should have, where containers of this
   * component can be allocated on.
   **/
  public PlacementConstraint maxCardinality(Long maxCardinality) {
    this.maxCardinality = maxCardinality;
    return this;
  }

  @ApiModelProperty(example = "3")
  public Long getMaxCardinality() {
    return maxCardinality;
  }

  public void setMaxCardinality(Long maxCardinality) {
    this.maxCardinality = maxCardinality;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PlacementConstraint placementConstraint = (PlacementConstraint) o;
    return Objects.equals(this.name, placementConstraint.name)
        && Objects.equals(this.type, placementConstraint.type)
        && Objects.equals(this.scope, placementConstraint.scope)
        && Objects.equals(this.targetTags, placementConstraint.targetTags)
        && Objects.equals(this.nodeAttributes,
            placementConstraint.nodeAttributes)
        && Objects.equals(this.nodePartitions,
            placementConstraint.nodePartitions)
        && Objects.equals(this.minCardinality,
            placementConstraint.minCardinality)
        && Objects.equals(this.maxCardinality,
            placementConstraint.maxCardinality);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, scope, targetTags, nodeAttributes,
        nodePartitions, minCardinality, maxCardinality);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class PlacementConstraint {\n")

        .append("    name: ").append(toIndentedString(name)).append("\n")
        .append("    type: ").append(toIndentedString(type)).append("\n")
        .append("    scope: ").append(toIndentedString(scope)).append("\n")
        .append("    targetTags: ").append(toIndentedString(targetTags))
        .append("\n")
        .append("    nodeAttributes: ").append(toIndentedString(nodeAttributes))
        .append("\n")
        .append("    nodePartitions: ").append(toIndentedString(nodePartitions))
        .append("\n")
        .append("    minCardinality: ").append(toIndentedString(minCardinality))
        .append("\n")
        .append("    maxCardinality: ").append(toIndentedString(maxCardinality))
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
}

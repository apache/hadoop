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
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * Advanced placement policy of the components of a service.
 **/
@InterfaceAudience.Public
@InterfaceStability.Unstable
@ApiModel(description = "Advanced placement policy of the components of a "
    + "service.")
@javax.annotation.Generated(
    value = "class io.swagger.codegen.languages.JavaClientCodegen",
    date = "2018-02-16T10:20:12.927-07:00")
public class PlacementPolicy implements Serializable {
  private static final long serialVersionUID = 4341110649551172231L;

  private List<PlacementConstraint> constraints = new ArrayList<>();

  /**
   * Placement constraint details.
   **/
  public PlacementPolicy constraints(List<PlacementConstraint> constraints) {
    this.constraints = constraints;
    return this;
  }

  @ApiModelProperty(example = "null", required = true)
  @JsonProperty("constraints")
  public List<PlacementConstraint> getConstraints() {
    return constraints;
  }

  public void setConstraints(List<PlacementConstraint> constraints) {
    this.constraints = constraints;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PlacementPolicy placementPolicy = (PlacementPolicy) o;
    return Objects.equals(this.constraints, placementPolicy.constraints);
  }

  @Override
  public int hashCode() {
    return Objects.hash(constraints);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class PlacementPolicy {\n");

    sb.append("    constraints: ").append(toIndentedString(constraints))
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

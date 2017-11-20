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

import java.io.Serializable;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Placement policy of an instance of an service. This feature is in the
 * works in YARN-4902.
 **/
@InterfaceAudience.Public
@InterfaceStability.Unstable
@ApiModel(description = "Placement policy of an instance of an service. This feature is in the works in YARN-4902.")
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaClientCodegen", date = "2016-06-02T08:15:05.615-07:00")
public class PlacementPolicy implements Serializable {
  private static final long serialVersionUID = 4341110649551172231L;

  private String label = null;

  /**
   * Assigns a service to a named partition of the cluster where the service
   * desires to run (optional). If not specified all services are submitted to
   * a default label of the service owner. One or more labels can be setup for
   * each service owner account with required constraints like no-preemption,
   * sla-99999, preemption-ok, etc.
   **/
  public PlacementPolicy label(String label) {
    this.label = label;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Assigns a service to a named partition of the cluster where the service desires to run (optional). If not specified all services are submitted to a default label of the service owner. One or more labels can be setup for each service owner account with required constraints like no-preemption, sla-99999, preemption-ok, etc.")
  @JsonProperty("label")
  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
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
    return Objects.equals(this.label, placementPolicy.label);
  }

  @Override
  public int hashCode() {
    return Objects.hash(label);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class PlacementPolicy {\n");

    sb.append("    label: ").append(toIndentedString(label)).append("\n");
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

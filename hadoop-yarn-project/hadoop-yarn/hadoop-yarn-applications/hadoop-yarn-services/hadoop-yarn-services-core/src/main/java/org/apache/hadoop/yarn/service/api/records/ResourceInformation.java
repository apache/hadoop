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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.annotations.SerializedName;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.Objects;

/**
 * ResourceInformation determines unit/name/value of resource types in addition to memory and vcores. It will be part of Resource object
 */
@ApiModel(description = "ResourceInformation determines unit/value of resource types in addition to memory and vcores. It will be part of Resource object")
@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaClientCodegen",
                            date = "2017-11-22T15:15:49.495-08:00")
public class ResourceInformation {
  @SerializedName("value")
  private Long value = null;

  @SerializedName("unit")
  private String unit = null;

  public ResourceInformation value(Long value) {
    this.value = value;
    return this;
  }

  /**
   * Integer value of the resource.
   *
   * @return value
   **/
  @ApiModelProperty(value = "Integer value of the resource.")
  @JsonProperty("value")
  public Long getValue() {
    return value;
  }

  public void setValue(Long value) {
    this.value = value;
  }

  public ResourceInformation unit(String unit) {
    this.unit = unit;
    return this;
  }

  /**
   * @return unit
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("unit")
  public String getUnit() {
    return unit == null ? "" : unit;
  }

  public void setUnit(String unit) {
    this.unit = unit;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ResourceInformation resourceInformation = (ResourceInformation) o;
    return Objects
        .equals(this.value, resourceInformation.value) && Objects.equals(
        this.unit, resourceInformation.unit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, unit);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ResourceInformation {\n");
    sb.append("    value: ").append(toIndentedString(value)).append("\n");
    sb.append("    unit: ").append(toIndentedString(unit)).append("\n");
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


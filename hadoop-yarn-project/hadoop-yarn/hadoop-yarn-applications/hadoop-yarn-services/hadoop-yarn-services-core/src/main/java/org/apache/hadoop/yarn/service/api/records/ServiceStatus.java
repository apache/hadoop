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

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * The current status of a submitted service, returned as a response to the
 * GET API.
 **/
@InterfaceAudience.Public
@InterfaceStability.Unstable
@ApiModel(description = "The current status of a submitted service, returned as a response to the GET API.")
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaClientCodegen", date = "2016-06-02T08:15:05.615-07:00")
@XmlRootElement
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ServiceStatus extends BaseResource {
  private static final long serialVersionUID = -3469885905347851034L;

  private String diagnostics = null;
  private ServiceState state = null;
  private Integer code = null;

  /**
   * Diagnostic information (if any) for the reason of the current state of the
   * service. It typically has a non-null value, if the service is in a
   * non-running state.
   **/
  public ServiceStatus diagnostics(String diagnostics) {
    this.diagnostics = diagnostics;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Diagnostic information (if any) for the reason of the current state of the service. It typically has a non-null value, if the service is in a non-running state.")
  @JsonProperty("diagnostics")
  public String getDiagnostics() {
    return diagnostics;
  }

  public void setDiagnostics(String diagnostics) {
    this.diagnostics = diagnostics;
  }

  /**
   * Service state.
   **/
  public ServiceStatus state(ServiceState state) {
    this.state = state;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Service state.")
  @JsonProperty("state")
  public ServiceState getState() {
    return state;
  }

  public void setState(ServiceState state) {
    this.state = state;
  }

  /**
   * An error code specific to a scenario which service owners should be able to use
   * to understand the failure in addition to the diagnostic information.
   **/
  public ServiceStatus code(Integer code) {
    this.code = code;
    return this;
  }

  @ApiModelProperty(example = "null", value = "An error code specific to a scenario which service owners should be able to use to understand the failure in addition to the diagnostic information.")
  @JsonProperty("code")
  public Integer getCode() {
    return code;
  }

  public void setCode(Integer code) {
    this.code = code;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ServiceStatus serviceStatus = (ServiceStatus) o;
    return Objects.equals(this.diagnostics, serviceStatus.diagnostics)
        && Objects.equals(this.state, serviceStatus.state)
        && Objects.equals(this.code, serviceStatus.code);
  }

  @Override
  public int hashCode() {
    return Objects.hash(diagnostics, state, code);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ServiceStatus {\n");

    sb.append("    diagnostics: ").append(toIndentedString(diagnostics))
        .append("\n");
    sb.append("    state: ").append(toIndentedString(state)).append("\n");
    sb.append("    code: ").append(toIndentedString(code)).append("\n");
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

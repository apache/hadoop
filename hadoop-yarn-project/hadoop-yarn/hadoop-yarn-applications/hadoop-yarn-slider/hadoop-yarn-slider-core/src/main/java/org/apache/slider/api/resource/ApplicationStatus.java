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

package org.apache.slider.api.resource;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.Objects;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The current status of a submitted application, returned as a response to the
 * GET API.
 **/

@ApiModel(description = "The current status of a submitted application, returned as a response to the GET API.")
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaClientCodegen", date = "2016-06-02T08:15:05.615-07:00")
@XmlRootElement
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApplicationStatus extends BaseResource {
  private static final long serialVersionUID = -3469885905347851034L;

  private String diagnostics = null;
  private ApplicationState state = null;
  private Integer code = null;

  /**
   * Diagnostic information (if any) for the reason of the current state of the
   * application. It typically has a non-null value, if the application is in a
   * non-running state.
   **/
  public ApplicationStatus diagnostics(String diagnostics) {
    this.diagnostics = diagnostics;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Diagnostic information (if any) for the reason of the current state of the application. It typically has a non-null value, if the application is in a non-running state.")
  @JsonProperty("diagnostics")
  public String getDiagnostics() {
    return diagnostics;
  }

  public void setDiagnostics(String diagnostics) {
    this.diagnostics = diagnostics;
  }

  /**
   * Application state.
   **/
  public ApplicationStatus state(ApplicationState state) {
    this.state = state;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Application state.")
  @JsonProperty("state")
  public ApplicationState getState() {
    return state;
  }

  public void setState(ApplicationState state) {
    this.state = state;
  }

  /**
   * An error code specific to a scenario which app owners should be able to use
   * to understand the failure in addition to the diagnostic information.
   **/
  public ApplicationStatus code(Integer code) {
    this.code = code;
    return this;
  }

  @ApiModelProperty(example = "null", value = "An error code specific to a scenario which app owners should be able to use to understand the failure in addition to the diagnostic information.")
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
    ApplicationStatus applicationStatus = (ApplicationStatus) o;
    return Objects.equals(this.diagnostics, applicationStatus.diagnostics)
        && Objects.equals(this.state, applicationStatus.state)
        && Objects.equals(this.code, applicationStatus.code);
  }

  @Override
  public int hashCode() {
    return Objects.hash(diagnostics, state, code);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ApplicationStatus {\n");

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

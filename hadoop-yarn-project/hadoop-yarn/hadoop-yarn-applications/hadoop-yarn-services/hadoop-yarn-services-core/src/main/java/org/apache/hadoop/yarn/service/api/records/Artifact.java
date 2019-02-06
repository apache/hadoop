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

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlType;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Artifact of an service component.
 **/
@InterfaceAudience.Public
@InterfaceStability.Unstable
@ApiModel(description = "Artifact of an service component")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Artifact implements Serializable {
  private static final long serialVersionUID = 3608929500111099035L;

  private String id = null;

  /**
   * Artifact Type.  DOCKER, TARBALL or SERVICE
   **/
  @XmlType(name = "artifact_type")
  @XmlEnum
  public enum TypeEnum {
    DOCKER("DOCKER"), TARBALL("TARBALL"), SERVICE("SERVICE");

    private String value;

    TypeEnum(String value) {
      this.value = value;
    }

    @Override
    @JsonValue
    public String toString() {
      return value;
    }
  }

  private TypeEnum type = TypeEnum.DOCKER;
  private String uri = null;

  /**
   * Artifact id. Examples are package location uri for tarball based services,
   * image name for docker, etc.
   **/
  public Artifact id(String id) {
    this.id = id;
    return this;
  }

  @ApiModelProperty(example = "null", required = true, value = "Artifact id. Examples are package location uri for tarball based services, image name for docker, etc.")
  @JsonProperty("id")
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  /**
   * Artifact type, like docker, tarball, etc. (optional).
   **/
  public Artifact type(TypeEnum type) {
    this.type = type;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Artifact type, like docker, tarball, etc. (optional).")
  @JsonProperty("type")
  public TypeEnum getType() {
    return type;
  }

  public void setType(TypeEnum type) {
    this.type = type;
  }

  /**
   * Artifact location to support multiple artifact stores (optional).
   **/
  public Artifact uri(String uri) {
    this.uri = uri;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Artifact location to support multiple artifact stores (optional).")
  @JsonProperty("uri")
  public String getUri() {
    return uri;
  }

  public void setUri(String uri) {
    this.uri = uri;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Artifact artifact = (Artifact) o;
    return Objects.equals(this.id, artifact.id)
        && Objects.equals(this.type, artifact.type)
        && Objects.equals(this.uri, artifact.uri);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, type, uri);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Artifact {\n");

    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    type: ").append(toIndentedString(type)).append("\n");
    sb.append("    uri: ").append(toIndentedString(uri)).append("\n");
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

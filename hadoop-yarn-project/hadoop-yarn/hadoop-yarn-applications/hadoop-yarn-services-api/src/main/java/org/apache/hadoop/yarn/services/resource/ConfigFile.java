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

import java.io.Serializable;
import java.util.Objects;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * A config file that needs to be created and made available as a volume in an
 * application component container.
 **/

@ApiModel(description = "A config file that needs to be created and made available as a volume in an application component container.")
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaClientCodegen", date = "2016-06-02T08:15:05.615-07:00")
@XmlRootElement
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ConfigFile implements Serializable {
  private static final long serialVersionUID = -7009402089417704612L;

  public enum TypeEnum {
    XML("XML"), PROPERTIES("PROPERTIES"), JSON("JSON"), YAML("YAML"), TEMPLATE(
        "TEMPLATE"), ENV("ENV"), HADOOP_XML("HADOOP_XML");

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

  private TypeEnum type = null;
  private String destFile = null;
  private String srcFile = null;
  private Object props = null;

  /**
   * Config file in the standard format like xml, properties, json, yaml,
   * template.
   **/
  public ConfigFile type(TypeEnum type) {
    this.type = type;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Config file in the standard format like xml, properties, json, yaml, template.")
  @JsonProperty("type")
  public TypeEnum getType() {
    return type;
  }

  public void setType(TypeEnum type) {
    this.type = type;
  }

  /**
   * The absolute path that this configuration file should be mounted as, in the
   * application container.
   **/
  public ConfigFile destFile(String destFile) {
    this.destFile = destFile;
    return this;
  }

  @ApiModelProperty(example = "null", value = "The absolute path that this configuration file should be mounted as, in the application container.")
  @JsonProperty("dest_file")
  public String getDestFile() {
    return destFile;
  }

  @XmlElement(name = "dest_file")
  public void setDestFile(String destFile) {
    this.destFile = destFile;
  }

  /**
   * Required for type template. This provides the source location of the
   * template which needs to be mounted as dest_file post property
   * substitutions. Typically the src_file would point to a source controlled
   * network accessible file maintained by tools like puppet, chef, etc.
   **/
  public ConfigFile srcFile(String srcFile) {
    this.srcFile = srcFile;
    return this;
  }

  @ApiModelProperty(example = "null", value = "Required for type template. This provides the source location of the template which needs to be mounted as dest_file post property substitutions. Typically the src_file would point to a source controlled network accessible file maintained by tools like puppet, chef, etc.")
  @JsonProperty("src_file")
  public String getSrcFile() {
    return srcFile;
  }

  @XmlElement(name = "src_file")
  public void setSrcFile(String srcFile) {
    this.srcFile = srcFile;
  }

  /**
   * A blob of key value pairs that will be dumped in the dest_file in the
   * format as specified in type. If the type is template then the attribute
   * src_file is mandatory and the src_file content is dumped to dest_file post
   * property substitutions.
   **/
  public ConfigFile props(Object props) {
    this.props = props;
    return this;
  }

  @ApiModelProperty(example = "null", value = "A blob of key value pairs that will be dumped in the dest_file in the format as specified in type. If the type is template then the attribute src_file is mandatory and the src_file content is dumped to dest_file post property substitutions.")
  @JsonProperty("props")
  public Object getProps() {
    return props;
  }

  public void setProps(Object props) {
    this.props = props;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConfigFile configFile = (ConfigFile) o;
    return Objects.equals(this.type, configFile.type)
        && Objects.equals(this.destFile, configFile.destFile)
        && Objects.equals(this.srcFile, configFile.srcFile)
        && Objects.equals(this.props, configFile.props);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, destFile, srcFile, props);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ConfigFile {\n");

    sb.append("    type: ").append(toIndentedString(type)).append("\n");
    sb.append("    destFile: ").append(toIndentedString(destFile)).append("\n");
    sb.append("    srcFile: ").append(toIndentedString(srcFile)).append("\n");
    sb.append("    props: ").append(toIndentedString(props)).append("\n");
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

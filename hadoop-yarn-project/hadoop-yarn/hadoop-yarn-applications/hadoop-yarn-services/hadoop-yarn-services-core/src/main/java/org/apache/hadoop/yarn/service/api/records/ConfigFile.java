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
import com.fasterxml.jackson.annotation.JsonValue;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A config file that needs to be created and made available as a volume in an
 * service component container.
 **/
@InterfaceAudience.Public
@InterfaceStability.Unstable
@ApiModel(description = "A config file that needs to be created and made available as a volume in an service component container.")
@XmlRootElement
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ConfigFile implements Serializable {
  private static final long serialVersionUID = -7009402089417704612L;

  /**
   * Config Type.  XML, JSON, YAML, TEMPLATE and HADOOP_XML are supported.
   **/
  @XmlType(name = "config_type")
  @XmlEnum
  public enum TypeEnum {
    XML("XML"), PROPERTIES("PROPERTIES"), JSON("JSON"), YAML("YAML"), TEMPLATE(
        "TEMPLATE"), HADOOP_XML("HADOOP_XML"), STATIC("STATIC"), ARCHIVE(
        "ARCHIVE");

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
  private Map<String, String> properties = new HashMap<>();

  public ConfigFile copy() {
    ConfigFile copy = new ConfigFile();
    copy.setType(this.getType());
    copy.setSrcFile(this.getSrcFile());
    copy.setDestFile(this.getDestFile());
    if (this.getProperties() != null && !this.getProperties().isEmpty()) {
      copy.getProperties().putAll(this.getProperties());
    }
    return copy;
  }

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
   * service container.
   **/
  public ConfigFile destFile(String destFile) {
    this.destFile = destFile;
    return this;
  }

  @ApiModelProperty(example = "null", value = "The absolute path that this configuration file should be mounted as, in the service container.")
  @JsonProperty("dest_file")
  public String getDestFile() {
    return destFile;
  }

  @XmlElement(name = "dest_file")
  public void setDestFile(String destFile) {
    this.destFile = destFile;
  }

  /**
   * This provides the source location of the configuration file, the content
   * of which is dumped to dest_file post property substitutions, in the format
   * as specified in type. Typically the src_file would point to a source
   * controlled network accessible file maintained by tools like puppet, chef,
   * or hdfs etc. Currently, only hdfs is supported.
   **/
  public ConfigFile srcFile(String srcFile) {
    this.srcFile = srcFile;
    return this;
  }

  @ApiModelProperty(example = "null", value = "This provides the source location of the configuration file, "
      + "the content of which is dumped to dest_file post property substitutions, in the format as specified in type. "
      + "Typically the src_file would point to a source controlled network accessible file maintained by tools like puppet, chef, or hdfs etc. Currently, only hdfs is supported.")
  @JsonProperty("src_file")
  public String getSrcFile() {
    return srcFile;
  }

  @XmlElement(name = "src_file")
  public void setSrcFile(String srcFile) {
    this.srcFile = srcFile;
  }

  /**
   A blob of key value pairs that will be dumped in the dest_file in the format
   as specified in type. If src_file is specified, src_file content are dumped
   in the dest_file and these properties will overwrite, if any, existing
   properties in src_file or be added as new properties in src_file.
   **/
  public ConfigFile properties(Map<String, String> properties) {
    this.properties = properties;
    return this;
  }

  @ApiModelProperty(example = "null", value = "A blob of key value pairs that will be dumped in the dest_file in the format as specified in type."
      + " If src_file is specified, src_file content are dumped in the dest_file and these properties will overwrite, if any,"
      + " existing properties in src_file or be added as new properties in src_file.")
  @JsonProperty("properties")
  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public long getLong(String name, long defaultValue) {
    if (name == null) {
      return defaultValue;
    }
    String value = properties.get(name.trim());
    return Long.parseLong(value);
  }

  public boolean getBoolean(String name, boolean defaultValue) {
    if (name == null) {
      return defaultValue;
    }
    return Boolean.valueOf(properties.get(name.trim()));
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
        && Objects.equals(this.properties, configFile.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, destFile, srcFile, properties);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ConfigFile {\n");

    sb.append("    type: ").append(toIndentedString(type)).append("\n");
    sb.append("    destFile: ").append(toIndentedString(destFile)).append("\n");
    sb.append("    srcFile: ").append(toIndentedString(srcFile)).append("\n");
    sb.append("    properties: ").append(toIndentedString(properties)).append("\n");
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

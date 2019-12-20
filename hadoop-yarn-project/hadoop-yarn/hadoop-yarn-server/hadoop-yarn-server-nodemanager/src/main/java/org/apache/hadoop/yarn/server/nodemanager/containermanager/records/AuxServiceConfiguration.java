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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.records;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Set of configuration properties that can be injected into the service
 * components via envs, files and custom pluggable helper docker containers.
 * Files of several standard formats like xml, properties, json, yaml and
 * templates will be supported.
 **/
@InterfaceAudience.Public
@InterfaceStability.Unstable
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AuxServiceConfiguration {

  private Map<String, String> properties = new HashMap<>();
  private List<AuxServiceFile> files = new ArrayList<>();

  /**
   * A blob of key-value pairs of common service properties.
   **/
  public AuxServiceConfiguration properties(Map<String, String> props) {
    this.properties = props;
    return this;
  }

  @JsonProperty("properties")
  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  /**
   * Array of list of files that needs to be created and made available as
   * volumes in the service component containers.
   **/
  public AuxServiceConfiguration files(List<AuxServiceFile> fileList) {
    this.files = fileList;
    return this;
  }

  @JsonProperty("files")
  public List<AuxServiceFile> getFiles() {
    return files;
  }

  public void setFiles(List<AuxServiceFile> files) {
    this.files = files;
  }

  public String getProperty(String name, String defaultValue) {
    String value = getProperty(name);
    if (StringUtils.isEmpty(value)) {
      return defaultValue;
    }
    return value;
  }

  public void setProperty(String name, String value) {
    properties.put(name, value);
  }

  public String getProperty(String name) {
    return properties.get(name.trim());
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuxServiceConfiguration configuration = (AuxServiceConfiguration) o;
    return Objects.equals(this.properties, configuration.properties)
        && Objects.equals(this.files, configuration.files);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, files);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Configuration {\n");

    sb.append("    properties: ").append(toIndentedString(properties))
        .append("\n");
    sb.append("    files: ").append(toIndentedString(files)).append("\n");
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

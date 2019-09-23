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
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.Objects;

/**
 * A config file that needs to be created and made available as a volume in an
 * service component container.
 **/
@InterfaceAudience.Public
@InterfaceStability.Unstable
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AuxServiceFile {

  /**
   * Config Type.
   **/
  public enum TypeEnum {
    STATIC("STATIC"), ARCHIVE("ARCHIVE");

    private String value;

    TypeEnum(String type) {
      this.value = type;
    }

    @Override
    @JsonValue
    public String toString() {
      return value;
    }
  }

  private TypeEnum type = null;
  private String srcFile = null;

  /**
   * Config file in the standard format like xml, properties, json, yaml,
   * template.
   **/
  public AuxServiceFile type(TypeEnum t) {
    this.type = t;
    return this;
  }

  @JsonProperty("type")
  public TypeEnum getType() {
    return type;
  }

  public void setType(TypeEnum type) {
    this.type = type;
  }

  /**
   * This provides the source location of the configuration file, the content
   * of which is dumped to dest_file post property substitutions, in the format
   * as specified in type. Typically the src_file would point to a source
   * controlled network accessible file maintained by tools like puppet, chef,
   * or hdfs etc. Currently, only hdfs is supported.
   **/
  public AuxServiceFile srcFile(String file) {
    this.srcFile = file;
    return this;
  }

  @JsonProperty("src_file")
  public String getSrcFile() {
    return srcFile;
  }

  public void setSrcFile(String srcFile) {
    this.srcFile = srcFile;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuxServiceFile auxServiceFile = (AuxServiceFile) o;
    return Objects.equals(this.type, auxServiceFile.type)
        && Objects.equals(this.srcFile, auxServiceFile.srcFile);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, srcFile);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class AuxServiceFile {\n");

    sb.append("    type: ").append(toIndentedString(type)).append("\n");
    sb.append("    srcFile: ").append(toIndentedString(srcFile)).append("\n");
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

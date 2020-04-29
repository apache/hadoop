/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.records;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.Date;
import java.util.Objects;

/**
 * An Service resource has the following attributes.
 **/
@InterfaceAudience.Public
@InterfaceStability.Unstable
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "name", "version", "description", "launch_time",
    "configuration" })
public class AuxServiceRecord {

  private String name = null;
  private String version = null;
  private String description = null;
  private Date launchTime = null;
  private AuxServiceConfiguration configuration = new AuxServiceConfiguration();

  /**
   * A unique service name.
   **/
  public AuxServiceRecord name(String n) {
    this.name = n;
    return this;
  }

  @JsonProperty("name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @JsonProperty("version")
  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  /**
   * Version of the service.
   */
  public AuxServiceRecord version(String v) {
    this.version = v;
    return this;
  }

  @JsonProperty("description")
  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * Description of the service.
   */
  public AuxServiceRecord description(String d) {
    this.description = d;
    return this;
  }

  /**
   * The time when the service was created, e.g. 2016-03-16T01:01:49.000Z.
   **/
  public AuxServiceRecord launchTime(Date time) {
    this.launchTime = time == null ? null : (Date) time.clone();
    return this;
  }

  @JsonProperty("launch_time")
  public Date getLaunchTime() {
    return launchTime == null ? null : (Date) launchTime.clone();
  }

  public void setLaunchTime(Date time) {
    this.launchTime = time == null ? null : (Date) time.clone();
  }
  /**
   * Config properties of an service. Configurations provided at the
   * service/global level are available to all the components. Specific
   * properties can be overridden at the component level.
   **/
  public AuxServiceRecord configuration(AuxServiceConfiguration conf) {
    this.configuration = conf;
    return this;
  }

  @JsonProperty("configuration")
  public AuxServiceConfiguration getConfiguration() {
    return configuration;
  }

  public void setConfiguration(AuxServiceConfiguration conf) {
    this.configuration = conf;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuxServiceRecord service = (AuxServiceRecord) o;
    return Objects.equals(this.name, service.name) && Objects.equals(this
        .version, service.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, version);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Service {\n");

    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    version: ").append(toIndentedString(version)).append("\n");
    sb.append("    description: ").append(toIndentedString(description))
        .append("\n");
    sb.append("    configuration: ").append(toIndentedString(configuration))
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

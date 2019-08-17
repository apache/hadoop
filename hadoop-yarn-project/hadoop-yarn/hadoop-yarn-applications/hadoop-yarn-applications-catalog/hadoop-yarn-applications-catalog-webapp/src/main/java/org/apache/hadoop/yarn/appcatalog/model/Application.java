/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.appcatalog.model;

import java.util.Objects;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.hadoop.yarn.service.api.records.Service;

/**
 * Data model of display recommended applications and descriptions.
 */
@XmlRootElement
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({ "organization", "name", "description", "icon" })
public class Application extends Service {
  private static final long serialVersionUID = -1776203219305414248L;

  private String organization;
  private String description;
  private String icon;

  @JsonProperty("organization")
  public String getOrganization() {
    return organization;
  }
  public void setOrganization(String organization) {
    this.organization = organization;
  }

  @JsonProperty("description")
  public String getDescription() {
    return description;
  }
  public void setDescription(String description) {
    this.description = description;
  }

  @JsonProperty("icon")
  public String getIcon() {
    return icon;
  }
  public void setIcon(String icon) {
    this.icon = icon;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    } else if ((obj instanceof Application)) {
      if (((Application) obj).getName().equals(this.getName())
          && ((Application) obj).getVersion().equals(this.getVersion())
          && ((Application) obj).getOrganization()
              .equals(this.getOrganization())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects
        .hash(this.getName() + this.getVersion() + this.getOrganization());
  }
}

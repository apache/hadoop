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

package org.apache.slider.core.registry.docstore;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * JSON-serializable description of a published key-val configuration.
 *
 * The values themselves are not serialized in the external view; they have to be served up by the far end
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class ExportEntry {

  /**
   * The value of the export
   */
  private String value;
  /**
   * The container id of the container that is responsible for the export
   */
  private String containerId;
  /**
   * Tag associated with the container - its usually an identifier different than container id
   * that allows a soft serial id to all containers of a component - e.g. 1, 2, 3, ...
   */
  private String tag;
  /**
   * An export can be at the level of a component or an application
   */
  private String level;
  /**
   * The time when the export was updated
   */
  private String updatedTime;
  /**
   * The time when the export expires
   */
  private String validUntil;

  public ExportEntry() {
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public String getContainerId() {
    return containerId;
  }

  public void setContainerId(String containerId) {
    this.containerId = containerId;
  }

  public String getTag() {
    return tag;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public String getLevel() {
    return level;
  }

  public void setLevel(String level) {
    this.level = level;
  }
  public String getUpdatedTime() {
    return updatedTime;
  }

  public void setUpdatedTime(String updatedTime) {
    this.updatedTime = updatedTime;
  }

  public String getValidUntil() {
    return validUntil;
  }

  public void setValidUntil(String validUntil) {
    this.validUntil = validUntil;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ExportEntry that = (ExportEntry) o;

    if (value != null ? !value.equals(that.value) : that.value != null)
      return false;
    return containerId != null ? containerId.equals(that.containerId) :
        that.containerId == null;
  }

  @Override
  public int hashCode() {
    int result = value != null ? value.hashCode() : 0;
    result = 31 * result + (containerId != null ? containerId.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return new StringBuilder("ExportEntry{").
        append("value='").append(value).append("',").
        append("containerId='").append(containerId).append("',").
        append("tag='").append(tag).append("',").
        append("level='").append(level).append("'").
        append("updatedTime='").append(updatedTime).append("'").
        append("validUntil='").append(validUntil).append("'").
        append(" }").toString();
  }
}

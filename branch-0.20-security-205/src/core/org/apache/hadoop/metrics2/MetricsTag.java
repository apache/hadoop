/**
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

package org.apache.hadoop.metrics2;

/**
 * Immutable tag for metrics (for grouping on host/queue/username etc.)
 */
public class MetricsTag {

  private final String name;
  private final String description;
  private final String value;

  /**
   * Construct the tag with name, description and value
   * @param name  of the tag
   * @param description of the tag
   * @param value of the tag
   */
  public MetricsTag(String name, String description, String value) {
    this.name = name;
    this.description = description;
    this.value = value;
  }

  /**
   * Get the name of the tag
   * @return  the name
   */
  public String name() {
    return name;
  }

  /**
   * Get the description of the tag
   * @return  the description
   */
  public String description() {
    return description;
  }

  /**
   * Get the value of the tag
   * @return  the value
   */
  public String value() {
    return value;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final MetricsTag other = (MetricsTag) obj;
    if (!this.name.equals(other.name())) {
      return false;
    }
    if (!this.description.equals(other.description())) {
      return false;
    }
    if (this.value == null || other.value() == null) {
      if (this.value == null && other.value() == null) return true;
      return false;
    }
    if (!this.value.equals(other.value())) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return name.hashCode() ^ (value == null ? 0 : value.hashCode());
  }

  @Override
  public String toString() {
    return "MetricsTag{" + "name='" + name + "' description='" + description +
           "' value='" + value + "'}";
  }

}

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

package org.apache.hadoop.registry.client.types;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Output of a <code>RegistryOperations.stat()</code> call
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@JsonIgnoreProperties(ignoreUnknown = true)
public final class RegistryPathStatus {

  /**
   * Short path in the registry to this entry
   */
  public final String path;

  /**
   * Timestamp
   */
  public final long time;

  /**
   * Entry size in bytes, as returned by the storage infrastructure.
   * In zookeeper, even "empty" nodes have a non-zero size.
   */
  public final long size;

  /**
   * Number of child nodes
   */
  public final int children;

  /**
   * Construct an instance
   * @param path full path
   * @param time time
   * @param size entry size
   * @param children number of children
   */
  public RegistryPathStatus(
      @JsonProperty("path") String path,
      @JsonProperty("time") long time,
      @JsonProperty("size") long size,
      @JsonProperty("children") int children) {
    this.path = path;
    this.time = time;
    this.size = size;
    this.children = children;
  }

  /**
   * Equality operator checks size, time and path of the entries.
   * It does <i>not</i> check {@link #children}.
   * @param other the other entry
   * @return true if the entries are considered equal.
   */
  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    RegistryPathStatus status = (RegistryPathStatus) other;

    if (size != status.size) {
      return false;
    }
    if (time != status.time) {
      return false;
    }
    if (path != null ? !path.equals(status.path) : status.path != null) {
      return false;
    }
    return true;
  }

  /**
   * The hash code is derived from the path.
   * @return hash code for storing the path in maps.
   */
  @Override
  public int hashCode() {
    return path != null ? path.hashCode() : 0;
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder("RegistryPathStatus{");
    sb.append("path='").append(path).append('\'');
    sb.append(", time=").append(time);
    sb.append(", size=").append(size);
    sb.append(", children=").append(children);
    sb.append('}');
    return sb.toString();
  }
}

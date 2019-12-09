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
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Base class for objects that are unique to a namespace.
 */
public abstract class RemoteLocationContext
    implements Comparable<RemoteLocationContext> {

  /**
   * Returns an identifier for a unique namespace.
   *
   * @return Namespace identifier.
   */
  public abstract String getNameserviceId();

  /**
   * Destination in this location. For example the path in a remote namespace.
   *
   * @return Destination in this location.
   */
  public abstract String getDest();

  /**
   * Original source location.
   *
   * @return Source path.
   */
  public abstract String getSrc();

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 31)
        .append(getNameserviceId())
        .append(getDest())
        .toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RemoteLocationContext) {
      RemoteLocationContext other = (RemoteLocationContext) obj;
      return this.getNameserviceId().equals(other.getNameserviceId()) &&
          this.getDest().equals(other.getDest());
    }
    return false;
  }

  @Override
  public int compareTo(RemoteLocationContext info) {
    int ret = this.getNameserviceId().compareTo(info.getNameserviceId());
    if (ret == 0) {
      ret = this.getDest().compareTo(info.getDest());
    }
    return ret;
  }
}

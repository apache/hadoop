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

package org.apache.hadoop.yarn.service.component.instance;

import org.apache.hadoop.yarn.api.records.ContainerId;

public class ComponentInstanceId implements Comparable<ComponentInstanceId> {

  private long Id;
  private String name;
  private ContainerId containerId;

  public ComponentInstanceId(long id, String name) {
    Id = id;
    this.name = name;
  }

  public long getId() {
    return Id;
  }

  public String getCompName() {
    return name;
  }

  public String getCompInstanceName() {
    return getCompName() + "-" + getId();
  }

  public void setContainerId(ContainerId containerId) {
    this.containerId = containerId;
  }

  @Override
  public String toString() {
    if (containerId == null) {
      return "[COMPINSTANCE " + getCompInstanceName() + "]";
    } else {
      return "[COMPINSTANCE " + getCompInstanceName() + " : " + containerId + "]";
    }
  }

  @Override public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    ComponentInstanceId that = (ComponentInstanceId) o;

    if (getId() != that.getId())
      return false;
    return getCompName() != null ? getCompName().equals(that.getCompName()) :
        that.getCompName() == null;

  }

  @Override public int hashCode() {
    int result = (int) (getId() ^ (getId() >>> 32));
    result = 31 * result + (getCompName() != null ? getCompName().hashCode() : 0);
    return result;
  }

  @Override
  public int compareTo(ComponentInstanceId to) {
    int delta = this.getCompName().compareTo(to.getCompName());
    if (delta == 0) {
      return Long.compare(this.getId(), to.getId());
    } else if (delta < 0) {
      return -1;
    } else {
      return 1;
    }
  }
}

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

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;


import org.apache.hadoop.yarn.api.records.ContainerId;

public class AllocationExpirationInfo implements
    Comparable<AllocationExpirationInfo> {

  private final ContainerId containerId;
  private final boolean increase;

  public AllocationExpirationInfo(ContainerId containerId) {
    this(containerId, false);
  }

  public AllocationExpirationInfo(
      ContainerId containerId, boolean increase) {
    this.containerId = containerId;
    this.increase = increase;
  }

  public ContainerId getContainerId() {
    return this.containerId;
  }

  public boolean isIncrease() {
    return this.increase;
  }

  @Override
  public int hashCode() {
    return (getContainerId().hashCode() << 16);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof AllocationExpirationInfo)) {
      return false;
    }
    return compareTo((AllocationExpirationInfo)other) == 0;
  }

  @Override
  public int compareTo(AllocationExpirationInfo other) {
    if (other == null) {
      return -1;
    }
    // Only need to compare containerId.
    return getContainerId().compareTo(other.getContainerId());
  }

  @Override
  public String toString() {
    return "<container=" + getContainerId() + ", increase="
        + isIncrease() + ">";
  }
}

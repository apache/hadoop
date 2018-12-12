/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;

import java.io.Serializable;
import java.util.Objects;

/**
 * Device wrapper class used for NM REST API.
 * */
public class AssignedDevice implements Serializable, Comparable {

  private static final long serialVersionUID = -544285507952217366L;

  private Device device;
  private String containerId;

  public AssignedDevice(ContainerId cId, Device dev) {
    this.device = dev;
    this.containerId = cId.toString();
  }

  public Device getDevice() {
    return device;
  }

  public String getContainerId() {
    return containerId;
  }

  @Override
  public int compareTo(Object o) {
    if (o == null || !(o instanceof AssignedDevice)) {
      return -1;
    }
    AssignedDevice other = (AssignedDevice) o;
    int result = getDevice().compareTo(other.getDevice());
    if (0 != result) {
      return result;
    }
    return getContainerId().compareTo(other.getContainerId());
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof AssignedDevice)) {
      return false;
    }
    AssignedDevice other = (AssignedDevice) o;
    return getDevice().equals(other.getDevice())
        && getContainerId().equals(other.getContainerId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getDevice(), getContainerId());
  }

}

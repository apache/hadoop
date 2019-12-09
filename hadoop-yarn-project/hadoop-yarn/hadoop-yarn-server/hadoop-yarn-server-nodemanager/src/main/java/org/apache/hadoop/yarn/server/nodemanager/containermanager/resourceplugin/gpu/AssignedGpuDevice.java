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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu;

import org.apache.hadoop.yarn.api.records.ContainerId;

/**
 * In addition to {@link GpuDevice}, this include container id and more runtime
 * information related to who is using the GPU device if possible
 */
public class AssignedGpuDevice extends GpuDevice {
  private static final long serialVersionUID = -12983712986315L;

  String containerId;

  public AssignedGpuDevice(int index, int minorNumber,
      ContainerId containerId) {
    super(index, minorNumber);
    this.containerId = containerId.toString();
  }

  public String getContainerId() {
    return containerId;
  }

  public void setContainerId(String containerId) {
    this.containerId = containerId;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof AssignedGpuDevice)) {
      return false;
    }
    AssignedGpuDevice other = (AssignedGpuDevice) obj;
    return index == other.index && minorNumber == other.minorNumber
        && containerId.equals(other.containerId);
  }

  @Override
  public int compareTo(Object obj) {
    if (obj == null || (!(obj instanceof AssignedGpuDevice))) {
      return -1;
    }

    AssignedGpuDevice other = (AssignedGpuDevice) obj;

    int result = Integer.compare(index, other.index);
    if (0 != result) {
      return result;
    }
    result = Integer.compare(minorNumber, other.minorNumber);
    if (0 != result) {
      return result;
    }
    return containerId.compareTo(other.containerId);
  }

  @Override
  public int hashCode() {
    final int prime = 47;
    return prime * (prime * index + minorNumber) + containerId.hashCode();
  }
}

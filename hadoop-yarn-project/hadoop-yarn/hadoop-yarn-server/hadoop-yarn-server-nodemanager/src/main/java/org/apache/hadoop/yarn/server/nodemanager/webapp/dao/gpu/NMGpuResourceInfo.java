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

package org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.AssignedGpuDevice;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuDevice;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NMResourceInfo;

import java.util.List;

/**
 * Gpu device information return to client when
 * {@link org.apache.hadoop.yarn.server.nodemanager.webapp.NMWebServices#getNMResourceInfo(String)}
 * is invoked.
 */
public class NMGpuResourceInfo extends NMResourceInfo {
  GpuDeviceInformation gpuDeviceInformation;

  List<GpuDevice> totalGpuDevices;
  List<AssignedGpuDevice> assignedGpuDevices;

  public NMGpuResourceInfo(GpuDeviceInformation gpuDeviceInformation,
      List<GpuDevice> totalGpuDevices,
      List<AssignedGpuDevice> assignedGpuDevices) {
    this.gpuDeviceInformation = gpuDeviceInformation;
    this.totalGpuDevices = totalGpuDevices;
    this.assignedGpuDevices = assignedGpuDevices;
  }

  public GpuDeviceInformation getGpuDeviceInformation() {
    return gpuDeviceInformation;
  }

  public void setGpuDeviceInformation(
      GpuDeviceInformation gpuDeviceInformation) {
    this.gpuDeviceInformation = gpuDeviceInformation;
  }

  public List<GpuDevice> getTotalGpuDevices() {
    return totalGpuDevices;
  }

  public void setTotalGpuDevices(List<GpuDevice> totalGpuDevices) {
    this.totalGpuDevices = totalGpuDevices;
  }

  public List<AssignedGpuDevice> getAssignedGpuDevices() {
    return assignedGpuDevices;
  }

  public void setAssignedGpuDevices(
      List<AssignedGpuDevice> assignedGpuDevices) {
    this.assignedGpuDevices = assignedGpuDevices;
  }
}

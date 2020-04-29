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

package org.apache.hadoop.yarn.server.nodemanager.webapp.dao;

import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework.AssignedDevice;

import java.util.List;

/**
 * Wrapper class of Device allocation for NMWebServices.
 * */
public class NMDeviceResourceInfo extends NMResourceInfo {

  private List<Device> totalDevices;
  private List<AssignedDevice> assignedDevices;

  public NMDeviceResourceInfo(
      List<Device> totalDevices, List<AssignedDevice> assignedDevices) {
    this.assignedDevices = assignedDevices;
    this.totalDevices = totalDevices;
  }

  public List<Device> getTotalDevices() {
    return totalDevices;
  }

  public void setTotalDevices(List<Device> totalDevices) {
    this.totalDevices = totalDevices;
  }

  public List<AssignedDevice> getAssignedDevices() {
    return assignedDevices;
  }

  public void setAssignedDevices(
      List<AssignedDevice> assignedDevices) {
    this.assignedDevices = assignedDevices;
  }
}

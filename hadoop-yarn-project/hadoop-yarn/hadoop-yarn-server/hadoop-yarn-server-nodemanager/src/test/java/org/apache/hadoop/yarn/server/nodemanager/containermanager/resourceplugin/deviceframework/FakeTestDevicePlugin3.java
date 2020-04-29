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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework;

import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRegisterRequest;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRuntimeSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.YarnRuntimeType;

import java.util.Set;
import java.util.TreeSet;

/**
 * Only used for testing.
 * This plugin register a same name with FakeTestDevicePlugin1
 * */
public class FakeTestDevicePlugin3 implements DevicePlugin {
  @Override
  public DeviceRegisterRequest getRegisterRequestInfo() {
    return DeviceRegisterRequest.Builder.newInstance()
        .setResourceName("cmpA.com/hdwA").build();
  }

  @Override
  public Set<Device> getDevices() {
    TreeSet<Device> r = new TreeSet<>();
    r.add(Device.Builder.newInstance()
        .setId(0)
        .setDevPath("/dev/hdwA0")
        .setMajorNumber(243)
        .setMinorNumber(0)
        .setBusID("0000:65:00.0")
        .setHealthy(true)
        .build());
    return r;
  }

  @Override
  public DeviceRuntimeSpec onDevicesAllocated(Set<Device> allocatedDevices,
      YarnRuntimeType yarnRuntime) throws Exception {
    return null;
  }

  @Override
  public void onDevicesReleased(Set<Device> allocatedDevices) {

  }
}

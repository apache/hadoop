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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.nvidia.com;

import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRuntimeSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.YarnRuntimeType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.com.nvidia.NvidiaGPUPluginForRuntimeV2;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;
import java.util.TreeSet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test case for Nvidia GPU device plugin.
 * */
public class TestNvidiaGpuPlugin {

  @Test
  public void testGetNvidiaDevices() throws Exception {
    NvidiaGPUPluginForRuntimeV2.NvidiaCommandExecutor mockShell =
        mock(NvidiaGPUPluginForRuntimeV2.NvidiaCommandExecutor.class);
    String deviceInfoShellOutput =
        "0, 00000000:04:00.0\n" +
        "1, 00000000:82:00.0";
    String majorMinorNumber0 = "c3:0";
    String majorMinorNumber1 = "c3:1";
    when(mockShell.getDeviceInfo()).thenReturn(deviceInfoShellOutput);
    when(mockShell.getMajorMinorInfo("nvidia0"))
        .thenReturn(majorMinorNumber0);
    when(mockShell.getMajorMinorInfo("nvidia1"))
        .thenReturn(majorMinorNumber1);
    NvidiaGPUPluginForRuntimeV2 plugin = new NvidiaGPUPluginForRuntimeV2();
    plugin.setShellExecutor(mockShell);
    plugin.setPathOfGpuBinary("/fake/nvidia-smi");

    Set<Device> expectedDevices = new TreeSet<>();
    expectedDevices.add(Device.Builder.newInstance()
        .setId(0).setHealthy(true)
        .setBusID("00000000:04:00.0")
        .setDevPath("/dev/nvidia0")
        .setMajorNumber(195)
        .setMinorNumber(0).build());
    expectedDevices.add(Device.Builder.newInstance()
        .setId(1).setHealthy(true)
        .setBusID("00000000:82:00.0")
        .setDevPath("/dev/nvidia1")
        .setMajorNumber(195)
        .setMinorNumber(1).build());
    Set<Device> devices = plugin.getDevices();
    Assert.assertEquals(expectedDevices, devices);
  }

  @Test
  public void testOnDeviceAllocated() throws Exception {
    NvidiaGPUPluginForRuntimeV2 plugin = new NvidiaGPUPluginForRuntimeV2();
    Set<Device> allocatedDevices = new TreeSet<>();

    DeviceRuntimeSpec spec = plugin.onDevicesAllocated(allocatedDevices,
        YarnRuntimeType.RUNTIME_DEFAULT);
    Assert.assertNull(spec);

    // allocate one device
    allocatedDevices.add(Device.Builder.newInstance()
        .setId(0).setHealthy(true)
        .setBusID("00000000:04:00.0")
        .setDevPath("/dev/nvidia0")
        .setMajorNumber(195)
        .setMinorNumber(0).build());
    spec = plugin.onDevicesAllocated(allocatedDevices,
        YarnRuntimeType.RUNTIME_DOCKER);
    Assert.assertEquals("nvidia", spec.getContainerRuntime());
    Assert.assertEquals("0", spec.getEnvs().get("NVIDIA_VISIBLE_DEVICES"));

    // two device allowed
    allocatedDevices.add(Device.Builder.newInstance()
        .setId(0).setHealthy(true)
        .setBusID("00000000:82:00.0")
        .setDevPath("/dev/nvidia1")
        .setMajorNumber(195)
        .setMinorNumber(1).build());
    spec = plugin.onDevicesAllocated(allocatedDevices,
        YarnRuntimeType.RUNTIME_DOCKER);
    Assert.assertEquals("nvidia", spec.getContainerRuntime());
    Assert.assertEquals("0,1", spec.getEnvs().get("NVIDIA_VISIBLE_DEVICES"));

  }
}

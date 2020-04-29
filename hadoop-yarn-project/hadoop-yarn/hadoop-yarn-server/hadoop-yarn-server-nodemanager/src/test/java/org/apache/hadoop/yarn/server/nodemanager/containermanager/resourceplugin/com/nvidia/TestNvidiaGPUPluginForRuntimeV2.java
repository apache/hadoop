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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.com.nvidia;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRuntimeSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.YarnRuntimeType;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test case for NvidiaGPUPluginForRuntimeV2 device plugin.
 * */
public class TestNvidiaGPUPluginForRuntimeV2 {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestNvidiaGPUPluginForRuntimeV2.class);

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

  private NvidiaGPUPluginForRuntimeV2 mockEightGPUPlugin() throws IOException {
    String topoInfo =
        "\tGPU0\tGPU1\tGPU2\tGPU3\tGPU4\tGPU5\tGPU6\tGPU7\tCPU Affinity\n"
        + "GPU0\t X \tNV1\tNV1\tNV2\tNV2\tPHB\tPHB\tPHB\t0-63\n"
        + "GPU1\tNV1\t X \tNV2\tNV1\tPHB\tNV2\tPHB\tPHB\t0-63\n"
        + "GPU2\tNV1\tNV2\t X \tNV2\tPHB\tPHB\tNV1\tPHB\t0-63\n"
        + "GPU3\tNV2\tNV1\tNV2\t X \tPHB\tPHB\tPHB\tNV1\t0-63\n"
        + "GPU4\tNV2\tPHB\tPHB\tPHB\t X \tNV1\tNV1\tNV2\t0-63\n"
        + "GPU5\tPHB\tNV2\tPHB\tPHB\tNV1\t X \tNV2\tNV1\t0-63\n"
        + "GPU6\tPHB\tPHB\tNV1\tPHB\tNV1\tNV2\t X \tNV2\t0-63\n"
        + "GPU7\tPHB\tPHB\tPHB\tNV1\tNV2\tNV1\tNV2\t X \t0-63\n"
        + "\n"
        + "Legend:\n"
        + "\n"
        + "  X    = Self\n"
        + "  SYS  = Connection traversing PCIe as well as the SMP interconnect"
        + " between NUMA nodes (e.g., QPI/UPI)\n"
        + "  NODE = Connection traversing PCIe as well as the interconnect"
        + " between PCIe Host Bridges within a NUMA node\n"
        + "  PHB  = Connection traversing PCIe as well as a PCIe Host Bridge"
        + " (typically the CPU)\n"
        + "  PXB  = Connection traversing multiple PCIe switches"
        + " (without traversing the PCIe Host Bridge)\n"
        + "  PIX  = Connection traversing a single PCIe switch\n"
        + "  NV#  = Connection traversing a bonded set of # NVLinks\n";

    String deviceInfoShellOutput = "0, 00000000:04:00.0\n"
        + "1, 00000000:82:00.0\n"
        + "2, 00000000:83:00.0\n"
        + "3, 00000000:84:00.0\n"
        + "4, 00000000:85:00.0\n"
        + "5, 00000000:86:00.0\n"
        + "6, 00000000:87:00.0\n"
        + "7, 00000000:88:00.0";
    String majorMinorNumber0 = "c3:0";
    String majorMinorNumber1 = "c3:1";
    String majorMinorNumber2 = "c3:2";
    String majorMinorNumber3 = "c3:3";
    String majorMinorNumber4 = "c3:4";
    String majorMinorNumber5 = "c3:5";
    String majorMinorNumber6 = "c3:6";
    String majorMinorNumber7 = "c3:7";
    NvidiaGPUPluginForRuntimeV2.NvidiaCommandExecutor mockShell =
        mock(NvidiaGPUPluginForRuntimeV2.NvidiaCommandExecutor.class);
    when(mockShell.getDeviceInfo()).thenReturn(deviceInfoShellOutput);
    when(mockShell.getMajorMinorInfo("nvidia0"))
        .thenReturn(majorMinorNumber0);
    when(mockShell.getMajorMinorInfo("nvidia1"))
        .thenReturn(majorMinorNumber1);
    when(mockShell.getMajorMinorInfo("nvidia2"))
        .thenReturn(majorMinorNumber2);
    when(mockShell.getMajorMinorInfo("nvidia3"))
        .thenReturn(majorMinorNumber3);
    when(mockShell.getMajorMinorInfo("nvidia4"))
        .thenReturn(majorMinorNumber4);
    when(mockShell.getMajorMinorInfo("nvidia5"))
        .thenReturn(majorMinorNumber5);
    when(mockShell.getMajorMinorInfo("nvidia6"))
        .thenReturn(majorMinorNumber6);
    when(mockShell.getMajorMinorInfo("nvidia7"))
        .thenReturn(majorMinorNumber7);
    when(mockShell.getTopologyInfo()).thenReturn(topoInfo);
    when(mockShell.getDeviceInfo()).thenReturn(deviceInfoShellOutput);

    NvidiaGPUPluginForRuntimeV2 plugin = new NvidiaGPUPluginForRuntimeV2();
    plugin.setShellExecutor(mockShell);
    plugin.setPathOfGpuBinary("/fake/nvidia-smi");
    return plugin;
  }

  private NvidiaGPUPluginForRuntimeV2 mockFourGPUPlugin() throws IOException {
    String topoInfo = "\tGPU0\tGPU1\tGPU2\tGPU3\tCPU Affinity\n"
        + "GPU0\t X \tPHB\tSOC\tSOC\t0-31\n"
        + "GPU1\tPHB\t X \tSOC\tSOC\t0-31\n"
        + "GPU2\tSOC\tSOC\t X \tPHB\t0-31\n"
        + "GPU3\tSOC\tSOC\tPHB\t X \t0-31\n"
        + "\n"
        + "\n"
        + " Legend:\n"
        + "\n"
        + " X   = Self\n"
        + " SOC  = Connection traversing PCIe as well as the SMP link between\n"
        + " CPU sockets(e.g. QPI)\n"
        + " PHB  = Connection traversing PCIe as well as a PCIe Host Bridge\n"
        + " (typically the CPU)\n"
        + " PXB  = Connection traversing multiple PCIe switches\n"
        + " (without traversing the PCIe Host Bridge)\n"
        + " PIX  = Connection traversing a single PCIe switch\n"
        + " NV#  = Connection traversing a bonded set of # NVLinks";

    String deviceInfoShellOutput = "0, 00000000:04:00.0\n"
        + "1, 00000000:82:00.0\n"
        + "2, 00000000:83:00.0\n"
        + "3, 00000000:84:00.0";
    String majorMinorNumber0 = "c3:0";
    String majorMinorNumber1 = "c3:1";
    String majorMinorNumber2 = "c3:2";
    String majorMinorNumber3 = "c3:3";
    NvidiaGPUPluginForRuntimeV2.NvidiaCommandExecutor mockShell =
        mock(NvidiaGPUPluginForRuntimeV2.NvidiaCommandExecutor.class);
    when(mockShell.getDeviceInfo()).thenReturn(deviceInfoShellOutput);
    when(mockShell.getMajorMinorInfo("nvidia0"))
        .thenReturn(majorMinorNumber0);
    when(mockShell.getMajorMinorInfo("nvidia1"))
        .thenReturn(majorMinorNumber1);
    when(mockShell.getMajorMinorInfo("nvidia2"))
        .thenReturn(majorMinorNumber2);
    when(mockShell.getMajorMinorInfo("nvidia3"))
        .thenReturn(majorMinorNumber3);
    when(mockShell.getTopologyInfo()).thenReturn(topoInfo);
    when(mockShell.getDeviceInfo()).thenReturn(deviceInfoShellOutput);

    NvidiaGPUPluginForRuntimeV2 plugin = new NvidiaGPUPluginForRuntimeV2();
    plugin.setShellExecutor(mockShell);
    plugin.setPathOfGpuBinary("/fake/nvidia-smi");
    return plugin;
  }

  @Test
  public void testTopologySchedulingWithPackPolicy() throws Exception {
    NvidiaGPUPluginForRuntimeV2 plugin = mockFourGPUPlugin();
    NvidiaGPUPluginForRuntimeV2 spyPlugin = spy(plugin);
    // cache the total devices
    Set<Device> allDevices = spyPlugin.getDevices();
    // environment variable to use PACK policy
    Map<String, String> env = new HashMap<>();
    env.put(NvidiaGPUPluginForRuntimeV2.TOPOLOGY_POLICY_ENV_KEY,
        NvidiaGPUPluginForRuntimeV2.TOPOLOGY_POLICY_PACK);
    // Case 0. if available devices is less than 3, no topo scheduling needed
    Set<Device> copyAvailableDevices = new TreeSet<>(allDevices);
    Iterator<Device> iterator0 = copyAvailableDevices.iterator();
    iterator0.next();
    iterator0.remove();
    iterator0.next();
    iterator0.remove();
    // Case 0. allocate 1 device
    reset(spyPlugin);
    Set<Device> allocation = spyPlugin.allocateDevices(copyAvailableDevices,
        1, env);
    assertThat(allocation).hasSize(1);
    verify(spyPlugin).basicSchedule(anySet(), anyInt(), anySet());
    Assert.assertFalse(spyPlugin.isTopoInitialized());

    // Case 1. allocate 1 device
    reset(spyPlugin);
    allocation = spyPlugin.allocateDevices(allDevices, 1, env);
    // ensure no topology scheduling needed
    assertThat(allocation).hasSize(1);
    verify(spyPlugin).basicSchedule(anySet(), anyInt(), anySet());
    reset(spyPlugin);
    // Case 2. allocate all available
    allocation = spyPlugin.allocateDevices(allDevices, allDevices.size(), env);
    Assert.assertEquals(allocation.size(), allDevices.size());
    verify(spyPlugin).basicSchedule(anySet(), anyInt(), anySet());
    // Case 3. allocate 2 devices
    reset(spyPlugin);
    int count = 2;
    Map<String, Integer> pairToWeight = spyPlugin.getDevicePairToWeight();
    allocation = spyPlugin.allocateDevices(allDevices, count, env);
    assertThat(allocation).hasSize(count);
    // the costTable should be init and used topology scheduling
    verify(spyPlugin).initCostTable();
    Assert.assertTrue(spyPlugin.isTopoInitialized());
    verify(spyPlugin).topologyAwareSchedule(anySet(), anyInt(), anyMap(),
        anySet(), anyMap());
    assertThat(allocation).hasSize(count);
    Device[] allocatedDevices =
        allocation.toArray(new Device[count]);
    // Check weights
    Assert.assertEquals(NvidiaGPUPluginForRuntimeV2.DeviceLinkType
        .P2PLinkSameCPUSocket.getWeight(),
        spyPlugin.computeCostOfDevices(allocatedDevices));
    // Case 4. allocate 3 devices
    reset(spyPlugin);
    count = 3;
    allocation = spyPlugin.allocateDevices(allDevices, count, env);
    assertThat(allocation).hasSize(count);
    // the costTable should be init and used topology scheduling
    verify(spyPlugin, times(0)).initCostTable();
    Assert.assertTrue(spyPlugin.isTopoInitialized());
    verify(spyPlugin).topologyAwareSchedule(anySet(), anyInt(), anyMap(),
        anySet(), anyMap());
    assertThat(allocation).hasSize(count);
    allocatedDevices =
        allocation.toArray(new Device[count]);
    // check weights
    int expectedWeight =
        NvidiaGPUPluginForRuntimeV2.DeviceLinkType
            .P2PLinkSameCPUSocket.getWeight()
            + 2 * NvidiaGPUPluginForRuntimeV2.DeviceLinkType
            .P2PLinkCrossCPUSocket.getWeight();
    Assert.assertEquals(expectedWeight,
        spyPlugin.computeCostOfDevices(allocatedDevices));
    // Case 5. allocate 2 GPUs from three available devices
    reset(spyPlugin);
    Iterator<Device> iterator = allDevices.iterator();
    iterator.next();
    // remove GPU0
    iterator.remove();
    count = 2;
    allocation = spyPlugin.allocateDevices(allDevices, count, env);
    assertThat(allocation).hasSize(count);
    // the costTable should be init and used topology scheduling
    verify(spyPlugin, times(0)).initCostTable();
    Assert.assertTrue(spyPlugin.isTopoInitialized());
    verify(spyPlugin).topologyAwareSchedule(anySet(), anyInt(), anyMap(),
        anySet(), anyMap());
    assertThat(allocation).hasSize(count);
    allocatedDevices =
        allocation.toArray(new Device[count]);
    // check weights
    Assert.assertEquals(NvidiaGPUPluginForRuntimeV2.DeviceLinkType
            .P2PLinkSameCPUSocket.getWeight(),
        spyPlugin.computeCostOfDevices(allocatedDevices));
    // it should allocate GPU 2 and 3
    for (Device device : allocation) {
      if (device.getMinorNumber() == 2) {
        Assert.assertTrue(true);
      } else if (device.getMinorNumber() == 3) {
        Assert.assertTrue(true);
      } else {
        Assert.assertTrue("Should allocate GPU 2 and 3", false);
      }
    }
  }

  @Test
  public void testTopologySchedulingWithSpreadPolicy() throws Exception {
    NvidiaGPUPluginForRuntimeV2 plugin = mockFourGPUPlugin();
    NvidiaGPUPluginForRuntimeV2 spyPlugin = spy(plugin);
    // cache the total devices
    Set<Device> allDevices = spyPlugin.getDevices();
    // environment variable to use PACK policy
    Map<String, String> env = new HashMap<>();
    env.put(NvidiaGPUPluginForRuntimeV2.TOPOLOGY_POLICY_ENV_KEY,
        NvidiaGPUPluginForRuntimeV2.TOPOLOGY_POLICY_SPREAD);
    // Case 1. allocate 1 device
    Set<Device> allocation = spyPlugin.allocateDevices(allDevices, 1, env);
    // ensure no topology scheduling needed
    Assert.assertEquals(allocation.size(), 1);
    verify(spyPlugin).basicSchedule(anySet(), anyInt(), anySet());
    reset(spyPlugin);
    // Case 2. allocate all available
    allocation = spyPlugin.allocateDevices(allDevices, allDevices.size(), env);
    Assert.assertEquals(allocation.size(), allDevices.size());
    verify(spyPlugin).basicSchedule(anySet(), anyInt(), anySet());
    // Case 3. allocate 2 devices
    reset(spyPlugin);
    int count = 2;
    Map<String, Integer> pairToWeight = spyPlugin.getDevicePairToWeight();
    allocation = spyPlugin.allocateDevices(allDevices, count, env);
    assertThat(allocation).hasSize(count);
    // the costTable should be init and used topology scheduling
    verify(spyPlugin).initCostTable();
    Assert.assertTrue(spyPlugin.isTopoInitialized());
    verify(spyPlugin).topologyAwareSchedule(anySet(), anyInt(), anyMap(),
        anySet(), anyMap());
    assertThat(allocation).hasSize(count);
    Device[] allocatedDevices =
        allocation.toArray(new Device[count]);
    // Check weights
    Assert.assertEquals(NvidiaGPUPluginForRuntimeV2.DeviceLinkType
        .P2PLinkCrossCPUSocket.getWeight(),
        spyPlugin.computeCostOfDevices(allocatedDevices));
    // Case 4. allocate 3 devices
    reset(spyPlugin);
    count = 3;
    allocation = spyPlugin.allocateDevices(allDevices, count, env);
    assertThat(allocation).hasSize(count);
    // the costTable should be init and used topology scheduling
    verify(spyPlugin, times(0)).initCostTable();
    Assert.assertTrue(spyPlugin.isTopoInitialized());
    verify(spyPlugin).topologyAwareSchedule(anySet(), anyInt(), anyMap(),
        anySet(), anyMap());
    assertThat(allocation).hasSize(count);
    allocatedDevices =
        allocation.toArray(new Device[count]);
    // check weights
    int expectedWeight =
        NvidiaGPUPluginForRuntimeV2.DeviceLinkType
            .P2PLinkSameCPUSocket.getWeight()
            + 2 * NvidiaGPUPluginForRuntimeV2.DeviceLinkType
            .P2PLinkCrossCPUSocket.getWeight();
    Assert.assertEquals(expectedWeight,
        spyPlugin.computeCostOfDevices(allocatedDevices));
    // Case 5. allocate 2 GPUs from three available devices
    reset(spyPlugin);
    Iterator<Device> iterator = allDevices.iterator();
    iterator.next();
    // remove GPU0
    iterator.remove();
    count = 2;
    allocation = spyPlugin.allocateDevices(allDevices, count, env);
    assertThat(allocation).hasSize(count);
    // the costTable should be init and used topology scheduling
    verify(spyPlugin, times(0)).initCostTable();
    Assert.assertTrue(spyPlugin.isTopoInitialized());
    verify(spyPlugin).topologyAwareSchedule(anySet(), anyInt(), anyMap(),
        anySet(), anyMap());
    assertThat(allocation).hasSize(count);
    allocatedDevices =
        allocation.toArray(new Device[count]);
    // check weights
    Assert.assertEquals(NvidiaGPUPluginForRuntimeV2.DeviceLinkType
            .P2PLinkCrossCPUSocket.getWeight(),
        spyPlugin.computeCostOfDevices(allocatedDevices));
    // it should allocate GPU 1 and 2
    for (Device device : allocation) {
      if (device.getMinorNumber() == 0) {
        Assert.assertTrue("Shouldn't allocate GPU 0", false);
      }
    }
  }

  @Test
  public void testCostTableWithNVlink() throws Exception {
    NvidiaGPUPluginForRuntimeV2 plugin = mockEightGPUPlugin();
    NvidiaGPUPluginForRuntimeV2 spyPlugin = spy(plugin);
    // verify the device pair to weight map
    spyPlugin.initCostTable();
    Map<String, Integer> devicePairToWeight = spyPlugin.getDevicePairToWeight();
    // 12 combinations when choose 2 GPUs from 8 respect the order. 8!/6!
    Assert.assertEquals(56, devicePairToWeight.size());
    int sameCPUWeight =
        NvidiaGPUPluginForRuntimeV2.DeviceLinkType
            .P2PLinkSameCPUSocket.getWeight();
    int nv1Weight =
        NvidiaGPUPluginForRuntimeV2.DeviceLinkType
            .P2PLinkNVLink1.getWeight();
    int nv2Weight =
        NvidiaGPUPluginForRuntimeV2.DeviceLinkType
            .P2PLinkNVLink2.getWeight();

    Assert.assertEquals(nv1Weight, (int)devicePairToWeight.get("0-1"));
    Assert.assertEquals(nv1Weight, (int)devicePairToWeight.get("1-0"));

    Assert.assertEquals(nv2Weight, (int)devicePairToWeight.get("0-4"));
    Assert.assertEquals(nv2Weight, (int)devicePairToWeight.get("4-0"));

    Assert.assertEquals(nv2Weight, (int)devicePairToWeight.get("0-3"));
    Assert.assertEquals(nv2Weight, (int)devicePairToWeight.get("3-0"));

    Assert.assertEquals(sameCPUWeight, (int)devicePairToWeight.get("6-3"));
    Assert.assertEquals(sameCPUWeight, (int)devicePairToWeight.get("3-6"));

    Assert.assertEquals(nv2Weight, (int)devicePairToWeight.get("6-7"));
    Assert.assertEquals(nv2Weight, (int)devicePairToWeight.get("7-6"));

    Assert.assertEquals(nv1Weight, (int)devicePairToWeight.get("1-3"));
    Assert.assertEquals(nv1Weight, (int)devicePairToWeight.get("3-1"));

    // verify cost Table
    Map<Integer, List<Map.Entry<Set<Device>, Integer>>> costTable =
        spyPlugin.getCostTable();
    Assert.assertNull(costTable.get(1));
    // C8:2 = 8!/2!/6! = 28
    Assert.assertEquals(28, costTable.get(2).size());
    // C8:4 = 8!/4!/4! = 70
    Assert.assertEquals(70, costTable.get(4).size());
    Assert.assertNull(costTable.get(8));

    Set<Device> allDevices = spyPlugin.getDevices();
    Map<String, String> env = new HashMap<>();
    env.put(NvidiaGPUPluginForRuntimeV2.TOPOLOGY_POLICY_ENV_KEY,
        NvidiaGPUPluginForRuntimeV2.TOPOLOGY_POLICY_PACK);
    spyPlugin.allocateDevices(allDevices, 3, env);
    spyPlugin.allocateDevices(allDevices, 2, env);
  }

  /**
   * Test the key cost table used for topology scheduling.
   * */
  @Test
  public void testCostTable() throws IOException {
    NvidiaGPUPluginForRuntimeV2 plugin = mockFourGPUPlugin();
    NvidiaGPUPluginForRuntimeV2 spyPlugin = spy(plugin);
    // verify the device pair to weight map
    spyPlugin.initCostTable();
    Map<String, Integer> devicePairToWeight = spyPlugin.getDevicePairToWeight();
    // 12 combinations when choose 2 GPUs from 4 respect the order. 4!/2!
    Assert.assertEquals(12, devicePairToWeight.size());
    int sameCPUWeight =
        NvidiaGPUPluginForRuntimeV2.DeviceLinkType
            .P2PLinkSameCPUSocket.getWeight();
    int crossCPUWeight =
        NvidiaGPUPluginForRuntimeV2.DeviceLinkType
            .P2PLinkCrossCPUSocket.getWeight();
    Assert.assertEquals(sameCPUWeight, (int)devicePairToWeight.get("0-1"));
    Assert.assertEquals(sameCPUWeight, (int)devicePairToWeight.get("1-0"));

    Assert.assertEquals(crossCPUWeight, (int)devicePairToWeight.get("0-2"));
    Assert.assertEquals(crossCPUWeight, (int)devicePairToWeight.get("2-0"));

    Assert.assertEquals(crossCPUWeight, (int)devicePairToWeight.get("0-3"));
    Assert.assertEquals(crossCPUWeight, (int)devicePairToWeight.get("3-0"));

    Assert.assertEquals(crossCPUWeight, (int)devicePairToWeight.get("1-2"));
    Assert.assertEquals(crossCPUWeight, (int)devicePairToWeight.get("2-1"));

    Assert.assertEquals(crossCPUWeight, (int)devicePairToWeight.get("1-3"));
    Assert.assertEquals(crossCPUWeight, (int)devicePairToWeight.get("3-1"));

    Assert.assertEquals(sameCPUWeight, (int)devicePairToWeight.get("2-3"));
    Assert.assertEquals(sameCPUWeight, (int)devicePairToWeight.get("3-2"));

    // verify cost Table
    Map<Integer, List<Map.Entry<Set<Device>, Integer>>> costTable =
        spyPlugin.getCostTable();
    Assert.assertNull(costTable.get(1));
    Assert.assertEquals(6, costTable.get(2).size());
    Assert.assertEquals(4, costTable.get(3).size());
    Assert.assertNull(costTable.get(4));
  }
  /**
   * Test GPU topology allocation.
   * And analysis the GPU allocation's performance against the actual
   * performance data using tensorflow benchmarks.
   * https://github.com/tensorflow/benchmarks
   * */
  @Test
  public void testTopologySchedulingPerformanceWithPackPolicyWithNVLink()
      throws Exception {
    NvidiaGPUPluginForRuntimeV2 plugin = mockEightGPUPlugin();
    NvidiaGPUPluginForRuntimeV2 spyPlugin = spy(plugin);
    Set<Device> allDevices = spyPlugin.getDevices();
    Map<String, String> env = new HashMap<>();
    env.put(NvidiaGPUPluginForRuntimeV2.TOPOLOGY_POLICY_ENV_KEY,
        NvidiaGPUPluginForRuntimeV2.TOPOLOGY_POLICY_PACK);

    /**
     * Analyze performance against the real data.
     * Get the topology scheduling algorithm's allocation's
     * average performance boost against median imagePerSecond and minimum
     * imagePerSecond in certain model and batch size combinations.
     * And then calculate the average performance boost.
     * The average performance boost against
     * median value means topology scheduler's allocation can stably
     * outperforms 50% of possible allocations.
     * The average performance boost against min value means the average boost
     * comparing to the worst allocations in various scenarios. Which is more
     * beautiful number for public promotion.
     * And also the analysis shows the best performance boost against median
     * and min value.
     * */
    ActualPerformanceReport report  = new ActualPerformanceReport();
    report.readFromFile();
    ArrayList<ActualPerformanceReport.DataRecord> dataSet =
        report.getDataSet();
    assertThat(dataSet).hasSize(2952);
    String[] allModels = {"alexnet", "resnet50", "vgg16", "inception3"};
    int[] batchSizes = {32, 64, 128};
    int[] gpuCounts = {2, 3, 4, 5, 6, 7};
    float totalBoostAgainstMedian = 0;
    int count = 0;
    float maxBoostAgainstMedian = 0;
    float totalBoostAgainstMin = 0;
    float maxBoostAgainstMin = 0;
    for (String model : allModels) {
      float totalBoostAgainstMinCertainModel = 0;
      float totalBoostAgainstMedianCertainModel = 0;
      float maxBoostAgainstMinCertainModel = 0;
      float maxBoostAgainstMedianCertainModel = 0;
      int countOfEachModel = 0;
      for (int bs : batchSizes) {
        for (int gpuCount: gpuCounts) {
          float bstAgainstMedian = calculatePerformanceBoostAgainstMedian(
              report, model, bs, gpuCount, plugin, allDevices, env);
          float bstAgainstMinimum = calculatePerformanceBoostAgainstMinimum(
              report, model, bs, gpuCount, plugin, allDevices, env);
          totalBoostAgainstMedian += bstAgainstMedian;
          totalBoostAgainstMin += bstAgainstMinimum;
          count++;
          if (maxBoostAgainstMedian < bstAgainstMedian) {
            maxBoostAgainstMedian = bstAgainstMedian;
          }
          if (maxBoostAgainstMin < bstAgainstMinimum) {
            maxBoostAgainstMin = bstAgainstMinimum;
          }
          totalBoostAgainstMinCertainModel += bstAgainstMinimum;
          totalBoostAgainstMedianCertainModel += bstAgainstMedian;
          if (maxBoostAgainstMinCertainModel < bstAgainstMinimum) {
            maxBoostAgainstMinCertainModel = bstAgainstMinimum;
          }
          if (maxBoostAgainstMedianCertainModel < bstAgainstMedian) {
            maxBoostAgainstMedianCertainModel = bstAgainstMedian;
          }
          countOfEachModel++;
        }
      }
      LOG.info("Model:{}, The best performance boost against median value is "
              + "{}", model, maxBoostAgainstMedianCertainModel);
      LOG.info("Model:{}, The aggregated average performance boost against "
          + "median value is {}",
          model, totalBoostAgainstMedianCertainModel/countOfEachModel);
      LOG.info("Model:{}, The best performance boost against min value is {}",
          model, maxBoostAgainstMinCertainModel);
      LOG.info("Model:{}, The aggregated average performance boost against "
              + "min value is {}",
          model, totalBoostAgainstMinCertainModel/countOfEachModel);
    }
    LOG.info("For all, the best performance boost against median value is "
        + maxBoostAgainstMedian);
    LOG.info("For all, the aggregated average performance boost against median "
        + "value is " + totalBoostAgainstMedian/count);
    LOG.info("For all, the best performance boost against min value is "
        + maxBoostAgainstMin);
    LOG.info("For all, the aggregated average performance boost against min "
        + "value is " + totalBoostAgainstMin/count);
  }

  /**
   * For <code>gpuCount</code> GPUs allocated by the topology algorithm, return
   * its performance boost against the median value.
   *
   * */
  private float calculatePerformanceBoostAgainstMedian(
      ActualPerformanceReport report,
      String model, int bs, int gpuCount,
      NvidiaGPUPluginForRuntimeV2 plugin, Set<Device> allDevice,
      Map<String, String> env) {
    Set<Device> allocation = plugin.allocateDevices(allDevice, gpuCount, env);
    String gpuAllocationString = convertAllocationToGpuString(allocation);
    float[] metrics = report.getVariousImagePerSecond(model, bs,
        gpuCount, gpuAllocationString);
    return metrics[7];
  }

  /**
   * For <code>gpuCount</code> GPUs allocated by the topology algorithm, return
   * its performance boost against the minimum value.
   *
   * */
  private float calculatePerformanceBoostAgainstMinimum(
      ActualPerformanceReport report,
      String model, int bs, int gpuCount,
      NvidiaGPUPluginForRuntimeV2 plugin, Set<Device> allDevice,
      Map<String, String> env) {
    Set<Device> allocation = plugin.allocateDevices(allDevice, gpuCount, env);
    String gpuAllocationString = convertAllocationToGpuString(allocation);
    float[] metrics = report.getVariousImagePerSecond(model, bs,
        gpuCount, gpuAllocationString);
    return metrics[5];
  }

  private String convertAllocationToGpuString(Set<Device> allocation) {
    StringBuilder sb = new StringBuilder();
    for (Device device : allocation) {
      sb.append(device.getMinorNumber() + "_");
    }
    return sb.toString().substring(0, sb.lastIndexOf("_"));
  }

  /**
   * Representation of the performance data report.
   * */
  private class ActualPerformanceReport {

    private ArrayList<DataRecord> dataSet = new ArrayList<>();

    public ArrayList<DataRecord> getDataSet() {
      return dataSet;
    }

    /**
     * One line in the report.
     * */
    private class DataRecord {
      DataRecord(String model, int bs, String combination, float fps,
          int count) {
        this.batchSize = bs;
        this.gpuCombination = combination;
        this.gpuCount = count;
        this.model = model;
        this.imagePerSecond = fps;
      }

      public String getModel() {
        return model;
      }

      public int getBatchSize() {
        return batchSize;
      }

      public String getGpuCombination() {
        return gpuCombination;
      }

      public float getImagePerSecond() {
        return imagePerSecond;
      }

      public int getGpuCount() {
        return gpuCount;
      }

      private String model;
      private int batchSize;
      private String gpuCombination;
      private float imagePerSecond;
      private int gpuCount;
    }

    /**
     * The file is a real performance report got from a 8 GPUs AWS instance.
     * It contains every combination GPUs' training performance of Tensorflow
     * benchmark.
     * The columns are the model name, batch size, gpu ids and imagesPerSecond
     * */
    public void readFromFile() {
      String csvReportFilePath = getClass().getClassLoader()
          .getResource("tensorflow-bench-result-for-GPU.csv").getFile();
      BufferedReader br = null;
      String line = "";
      try {
        br = new BufferedReader(new FileReader(csvReportFilePath));
        String model;
        int batchSize;
        String gpuCombination;
        float imagePerSecond;
        int gpuCount;
        while ((line = br.readLine()) != null) {
          // skip the licence content
          if (line.startsWith("#")) {
            continue;
          }
          String[] tokens = line.replaceAll("\"", "").split(",");
          if (tokens.length != 4) {
            LOG.error("unexpected performance data format!");
            break;
          }
          model = tokens[0];
          batchSize = Integer.parseInt(tokens[1].trim());
          gpuCombination = tokens[2];
          imagePerSecond = Float.parseFloat(tokens[3]);
          gpuCount = getGpuCount(gpuCombination);
          this.dataSet.add(new DataRecord(model, batchSize, gpuCombination,
              imagePerSecond, gpuCount));
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        if (br != null) {
          try {
            br.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      } // end finally
    }

    /**
     * Return the maximum, minimum, mean and median performance for model &
     * bs & gpuCount. And the imagePerSecond for model & bs & gpuCount &
     * gpuCombinations. And imagePerSecond performance boost comparing to
     * minimum, mean and media value.
     * */
    private float[] getVariousImagePerSecond(String model, int bs,
        int gpuCount, String gpuCombinations) {
      float[] result = new float[8];
      float max = 0;
      float min = Float.MAX_VALUE;
      float sum = 0;
      int count = 0;
      float wantedImagePerSecond = 0;
      float currentImagePerSecond;
      ArrayList<Float> allFps = new ArrayList<>();
      for (DataRecord dr : getDataSet()) {
        currentImagePerSecond = dr.getImagePerSecond();
        if (dr.batchSize == bs
            && model.equals(dr.getModel())
            && gpuCount == dr.getGpuCount()) {
          sum += currentImagePerSecond;
          count++;
          if (max < currentImagePerSecond) {
            max = currentImagePerSecond;
          }
          if (min > currentImagePerSecond) {
            min = currentImagePerSecond;
          }
          if (gpuCombinations.equals(dr.getGpuCombination())) {
            wantedImagePerSecond = dr.getImagePerSecond();
          }
          allFps.add(dr.getImagePerSecond());
        }
      }
      float median = getMedian(allFps);
      float mean = sum/count;
      result[0] = max;
      result[1] = min;
      result[2] = mean;
      result[3] = median;
      result[4] = wantedImagePerSecond;
      result[5] = wantedImagePerSecond/min - 1;
      result[6] = wantedImagePerSecond/mean - 1;
      result[7] = wantedImagePerSecond/median - 1;
      return result;
    }

    private float getMedian(ArrayList<Float> allFps) {
      float[] all = ArrayUtils.toPrimitive(allFps.toArray(new Float[0]), 0);
      Arrays.sort(all);
      float median;
      int size = all.length;
      if (allFps.size() % 2 == 0) {
        median = (all[size/2] + all[size/2 - 1])/2;
      } else {
        median = all[size/2];
      }
      return median;
    }

    private int getGpuCount(String gpuCombination) {
      String[] tokens = gpuCombination.split("_");
      return tokens.length;
    }
  }

}

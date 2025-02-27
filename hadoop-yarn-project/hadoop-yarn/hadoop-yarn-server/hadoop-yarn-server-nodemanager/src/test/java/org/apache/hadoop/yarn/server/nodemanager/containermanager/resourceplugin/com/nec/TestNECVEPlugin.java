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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.com.nec;

import static org.apache.hadoop.test.MockitoUtil.verifyZeroInteractions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyString;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.compress.utils.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Shell.CommandExecutor;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for NECVEPlugin class.
 *
 */
@ExtendWith(MockitoExtension.class)
public class TestNECVEPlugin {
  private static final String DEFAULT_SCRIPT_NAME = "nec-ve-get.py";
  private static final String[] EMPTY_SEARCH_DIRS = new String[] {};
  private static final Comparator<Device> DEVICE_COMPARATOR =
      Comparator.comparingInt(Device::getId);
  private Function<String, String> envProvider;
  private Map<String, String> env;
  private String[] defaultSearchDirs;
  private Function<String[], CommandExecutor>
      commandExecutorProvider;
  private String testFolder;

  @Mock
  private CommandExecutor mockCommandExecutor;

  @Mock
  private UdevUtil udevUtil;

  private String defaultScriptOutput;

  private NECVEPlugin plugin;

  @BeforeEach
  public void setup() throws IOException {
    env = new HashMap<>();
    envProvider = (String var) -> env.get(var);

    commandExecutorProvider = (String[] cmd) -> mockCommandExecutor;

    // default output of MockCommandExecutor - single device
    defaultScriptOutput = getOutputForDevice(
        0,
        "/dev/ve0",
        "ONLINE",
        "0000:65:00.0",
        243,
        0);
  }

  @AfterEach
  public void teardown() throws IOException {
    if (testFolder != null) {
      File f = new File(testFolder);
      FileUtils.deleteDirectory(f);
    }
  }

  @Test
  public void testParseScriptOutput()
      throws ResourceHandlerException, IOException {
    setupTestDirectoryWithScript();

    plugin = new NECVEPlugin(envProvider, defaultSearchDirs, udevUtil);
    plugin.setCommandExecutorProvider(commandExecutorProvider);
    when(mockCommandExecutor.getOutput()).thenReturn(defaultScriptOutput);

    Set<Device> devices = plugin.getDevices();

    assertEquals(1, devices.size(), "Number of devices");
    Device device = devices.iterator().next();
    assertEquals(0, device.getId(), "Device id");
    assertEquals("/dev/ve0", device.getDevPath(), "Device path");
    assertEquals("0000:65:00.0", device.getBusID(), "Bus Id");
    assertEquals("ONLINE", device.getStatus(), "Status");
    assertEquals(243, device.getMajorNumber(), "Major number");
    assertEquals(0, device.getMinorNumber(), "Minor number");
  }

  @Test
  public void testParseMultipleDevices()
      throws ResourceHandlerException, IOException {
    setupTestDirectoryWithScript();

    plugin = new NECVEPlugin(envProvider, defaultSearchDirs, udevUtil);
    plugin.setCommandExecutorProvider(commandExecutorProvider);

    defaultScriptOutput += "\n";
    defaultScriptOutput += getOutputForDevice(1,
        "/dev/ve1",
        "ONLINE",
        "0000:66:00.0",
        244,
        1);

    defaultScriptOutput += "\n";
    defaultScriptOutput += getOutputForDevice(2,
        "/dev/ve2",
        "ONLINE",
        "0000:67:00.0",
        245,
        2);

    when(mockCommandExecutor.getOutput()).thenReturn(defaultScriptOutput);

    Set<Device> devices = plugin.getDevices();

    assertEquals(3, devices.size(), "Number of devices");
    List<Device> devicesList = Lists.newArrayList(devices);
    // Sort devices by id
    Collections.sort(devicesList, DEVICE_COMPARATOR);

    Device device0 = devicesList.get(0);
    assertEquals(0, device0.getId(), "Device id");
    assertEquals("/dev/ve0", device0.getDevPath(), "Device path");
    assertEquals("0000:65:00.0", device0.getBusID(), "Bus Id");
    assertEquals("ONLINE", device0.getStatus(), "Status");
    assertEquals(243, device0.getMajorNumber(), "Major number");
    assertEquals(0, device0.getMinorNumber(), "Minor number");

    Device device1 = devicesList.get(1);
    assertEquals(1, device1.getId(), "Device id");
    assertEquals("/dev/ve1", device1.getDevPath(), "Device path");
    assertEquals("0000:66:00.0", device1.getBusID(), "Bus Id");
    assertEquals("ONLINE", device1.getStatus(), "Status");
    assertEquals(244, device1.getMajorNumber(), "Major number");
    assertEquals(1, device1.getMinorNumber(), "Minor number");

    Device device2 = devicesList.get(2);
    assertEquals(2, device2.getId(), "Device id");
    assertEquals("/dev/ve2", device2.getDevPath(), "Device path");
    assertEquals("0000:67:00.0", device2.getBusID(), "Bus Id");
    assertEquals("ONLINE", device2.getStatus(), "Status");
    assertEquals(245, device2.getMajorNumber(), "Major number");
    assertEquals(2, device2.getMinorNumber(), "Minor number");
  }

  @Test
  public void testOfflineDeviceIsSkipped()
      throws ResourceHandlerException, IOException {
    setupTestDirectoryWithScript();

    plugin = new NECVEPlugin(envProvider, defaultSearchDirs, udevUtil);
    plugin.setCommandExecutorProvider(commandExecutorProvider);
    defaultScriptOutput = getOutputForDevice(
        0,
        "/dev/ve0",
        "OFFLINE",
        "0000:65:00.0",
        243,
        0);
    when(mockCommandExecutor.getOutput()).thenReturn(defaultScriptOutput);

    Set<Device> devices = plugin.getDevices();

    assertEquals(0, devices.size(), "Number of devices");
  }

  @Test
  public void testUnparseableLineSkipped()
      throws ResourceHandlerException, IOException {
    setupTestDirectoryWithScript();

    plugin = new NECVEPlugin(envProvider, defaultSearchDirs, udevUtil);
    plugin.setCommandExecutorProvider(commandExecutorProvider);

    defaultScriptOutput += "\n";
    defaultScriptOutput += "cannot,be,parsed\n";

    defaultScriptOutput += getOutputForDevice(1,
        "/dev/ve1",
        "ONLINE",
        "0000:66:00.0",
        244,
        1);

    when(mockCommandExecutor.getOutput()).thenReturn(defaultScriptOutput);

    Set<Device> devices = plugin.getDevices();

    assertEquals(2, devices.size(), "Number of devices");
    List<Device> devicesList = Lists.newArrayList(devices);
    Collections.sort(devicesList, DEVICE_COMPARATOR);

    Device device0 = devicesList.get(0);
    assertEquals(0, device0.getId(), "Device id");
    assertEquals("/dev/ve0", device0.getDevPath(), "Device path");
    assertEquals("0000:65:00.0", device0.getBusID(), "Bus Id");
    assertEquals("ONLINE", device0.getStatus(), "Status");
    assertEquals(243, device0.getMajorNumber(), "Major number");
    assertEquals(0, device0.getMinorNumber(), "Minor number");

    Device device1 = devicesList.get(1);
    assertEquals(1, device1.getId(), "Device id");
    assertEquals("/dev/ve1", device1.getDevPath(), "Device path");
    assertEquals("0000:66:00.0", device1.getBusID(), "Bus Id");
    assertEquals("ONLINE", device1.getStatus(), "Status");
    assertEquals(244, device1.getMajorNumber(), "Major number");
    assertEquals(1, device1.getMinorNumber(), "Minor number");
  }

  @Test
  public void testScriptFoundWithDifferentName()
      throws ResourceHandlerException, IOException {
    setupTestDirectoryWithScript();

    final String dummyScriptName = "dummy-script.py";

    Path scriptPath = Paths.get(testFolder, dummyScriptName);
    Files.createFile(scriptPath);
    Files.delete(Paths.get(testFolder, DEFAULT_SCRIPT_NAME));
    env.put("NEC_VE_GET_SCRIPT_NAME", dummyScriptName);

    plugin = new NECVEPlugin(envProvider, defaultSearchDirs, udevUtil);

    verifyBinaryPathSet(scriptPath);
  }

  @Test
  public void testScriptFoundWithExplicitPath()
      throws ResourceHandlerException, IOException {
    setupTestDirectory("_temp_" + System.currentTimeMillis());

    Path scriptPath = Paths.get(testFolder, DEFAULT_SCRIPT_NAME);
    Files.createFile(scriptPath);
    scriptPath.toFile().setExecutable(true);
    assertTrue(scriptPath.toFile().canExecute(), "Cannot set executable flag");

    env.put("NEC_VE_GET_SCRIPT_PATH",
        testFolder + "/" + DEFAULT_SCRIPT_NAME);

    plugin = new NECVEPlugin(envProvider, EMPTY_SEARCH_DIRS, udevUtil);

    verifyBinaryPathSet(scriptPath);
  }

  @Test
  public void testExplicitPathPointsToDirectory()
      throws ResourceHandlerException, IOException {
    assertThrows(ResourceHandlerException.class, () -> {
      setupTestDirectory("_temp_" + System.currentTimeMillis());

      env.put("NEC_VE_GET_SCRIPT_PATH", testFolder);

      plugin = new NECVEPlugin(envProvider, EMPTY_SEARCH_DIRS, udevUtil);
    });
  }

  @Test
  public void testExplicitPathIsNotExecutable()
      throws ResourceHandlerException, IOException{
    assertThrows(ResourceHandlerException.class, ()->{
      setupTestDirectory("_temp_" + System.currentTimeMillis());

      Path scriptPath = Paths.get(testFolder, DEFAULT_SCRIPT_NAME);
      Files.createFile(scriptPath);
      scriptPath.toFile().setExecutable(false);
      assertFalse(scriptPath.toFile().canExecute(), "File is executable");

      env.put("NEC_VE_GET_SCRIPT_PATH",
              testFolder + "/" + DEFAULT_SCRIPT_NAME);

      plugin = new NECVEPlugin(envProvider, EMPTY_SEARCH_DIRS, udevUtil);
    });
  }

  @Test
  public void testScriptFoundUnderHadoopCommonPath()
      throws ResourceHandlerException, IOException {
    setupTestDirectory("_temp_" + System.currentTimeMillis());

    Path p = Paths.get(testFolder, "/sbin/DevicePluginScript");
    Files.createDirectories(p);

    Path scriptPath = Paths.get(testFolder, "/sbin/DevicePluginScript",
        DEFAULT_SCRIPT_NAME);
    Files.createFile(scriptPath);

    env.put("HADOOP_COMMON_HOME", testFolder);

    plugin = new NECVEPlugin(envProvider, EMPTY_SEARCH_DIRS, udevUtil);
    verifyBinaryPathSet(scriptPath);
  }

  @Test
  public void testScriptFoundUnderBasicSearchDirs()
      throws ResourceHandlerException, IOException {
    setupTestDirectoryWithScript();

    plugin = new NECVEPlugin(envProvider, defaultSearchDirs, udevUtil);

    Path scriptPath = Paths.get(testFolder, DEFAULT_SCRIPT_NAME);
    verifyBinaryPathSet(scriptPath);
  }

  @Test
  public void testAllocateSingleDevice()
      throws ResourceHandlerException, IOException {
    setupTestDirectoryWithScript();
    plugin = new NECVEPlugin(envProvider, defaultSearchDirs, udevUtil);
    Set<Device> available = new HashSet<>();
    Device device = getTestDevice(0);
    available.add(device);

    Set<Device> allocated = plugin.allocateDevices(available, 1, env);

    assertEquals(1, allocated.size(), "No. of devices");
    Device allocatedDevice = allocated.iterator().next();
    assertSame(device, allocatedDevice, "Device");
  }

  @Test
  public void testAllocateMultipleDevices()
      throws ResourceHandlerException, IOException {
    setupTestDirectoryWithScript();
    plugin = new NECVEPlugin(envProvider, defaultSearchDirs, udevUtil);
    Set<Device> available = new HashSet<>();
    Device device0 = getTestDevice(0);
    Device device1 = getTestDevice(1);
    available.add(device0);
    available.add(device1);

    Set<Device> allocated = plugin.allocateDevices(available, 2, env);

    assertEquals(2, allocated.size(), "No. of devices");
    assertTrue(allocated.contains(device0), "Device missing");
    assertTrue(allocated.contains(device1), "Device missing");
  }

  @Test
  public void testFindDevicesWithUdev()
      throws ResourceHandlerException, IOException {
    @SuppressWarnings("unchecked")
    Function<String, String> mockEnvProvider = mock(Function.class);
    VEDeviceDiscoverer veDeviceDiscoverer = mock(VEDeviceDiscoverer.class);
    when(mockEnvProvider.apply(eq("NEC_USE_UDEV"))).thenReturn("true");
    Device testDevice = getTestDevice(0);
    when(veDeviceDiscoverer.getDevicesFromPath(anyString()))
      .thenReturn(Sets.newHashSet(testDevice));
    plugin = new NECVEPlugin(mockEnvProvider, defaultSearchDirs, udevUtil);
    plugin.setVeDeviceDiscoverer(veDeviceDiscoverer);

    Set<Device> devices = plugin.getDevices();

    assertEquals(1, devices.size(), "No. of devices");
    Device device = devices.iterator().next();
    assertSame(device, testDevice, "Device");
    verifyZeroInteractions(mockCommandExecutor);
    verify(mockEnvProvider).apply(eq("NEC_USE_UDEV"));
    verifyNoMoreInteractions(mockEnvProvider);
  }

  private void setupTestDirectoryWithScript() throws IOException {
    setupTestDirectory(null);

    Files.createFile(Paths.get(testFolder, DEFAULT_SCRIPT_NAME));
  }

  private String getOutputForDevice(int id, String devPath, String state,
      String busId, int major, int minor) {
    return String.format(
        "id=%d, dev=%s, state=%s, busId=%s, major=%d, minor=%d",
        id, devPath, state, busId, major, minor);
  }

  private void setupTestDirectory(String postFix) throws IOException {
    String path = "target/temp/" +
        TestNECVEPlugin.class.getName() +
        (postFix == null ? "" : postFix);
    testFolder = new File(path).getAbsolutePath();
    File f = new File(testFolder);
    FileUtils.deleteDirectory(f);

    if (!f.mkdirs()) {
      throw new RuntimeException("Could not create directory: " +
          f.getAbsolutePath());
    }

    defaultSearchDirs = new String[]{testFolder};
  }

  private Device getTestDevice(int id) {
    Device.Builder builder = Device.Builder.newInstance();
    return builder.setId(id)
      .setDevPath("/mock/path")
      .setMajorNumber(200)
      .setMinorNumber(id)
      .setBusID("0000:66:00.0")
      .setHealthy(true)
      .build();
  }

  private void verifyBinaryPathSet(Path expectedPath) {
    assertEquals(expectedPath.toString(), plugin.getBinaryPath(), "Binary path");
    verifyZeroInteractions(udevUtil);
  }
}

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
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
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.util.Shell.CommandExecutor;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Unit tests for NECVEPlugin class.
 *
 */
@RunWith(MockitoJUnitRunner.class)
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

  @Before
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

  @After
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

    assertEquals("Number of devices", 1, devices.size());
    Device device = devices.iterator().next();
    assertEquals("Device id", 0, device.getId());
    assertEquals("Device path", "/dev/ve0", device.getDevPath());
    assertEquals("Bus Id", "0000:65:00.0", device.getBusID());
    assertEquals("Status", "ONLINE", device.getStatus());
    assertEquals("Major number", 243, device.getMajorNumber());
    assertEquals("Minor number", 0, device.getMinorNumber());
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

    assertEquals("Number of devices", 3, devices.size());
    List<Device> devicesList = Lists.newArrayList(devices);
    // Sort devices by id
    Collections.sort(devicesList, DEVICE_COMPARATOR);

    Device device0 = devicesList.get(0);
    assertEquals("Device id", 0, device0.getId());
    assertEquals("Device path", "/dev/ve0", device0.getDevPath());
    assertEquals("Bus Id", "0000:65:00.0", device0.getBusID());
    assertEquals("Status", "ONLINE", device0.getStatus());
    assertEquals("Major number", 243, device0.getMajorNumber());
    assertEquals("Minor number", 0, device0.getMinorNumber());

    Device device1 = devicesList.get(1);
    assertEquals("Device id", 1, device1.getId());
    assertEquals("Device path", "/dev/ve1", device1.getDevPath());
    assertEquals("Bus Id", "0000:66:00.0", device1.getBusID());
    assertEquals("Status", "ONLINE", device1.getStatus());
    assertEquals("Major number", 244, device1.getMajorNumber());
    assertEquals("Minor number", 1, device1.getMinorNumber());

    Device device2 = devicesList.get(2);
    assertEquals("Device id", 2, device2.getId());
    assertEquals("Device path", "/dev/ve2", device2.getDevPath());
    assertEquals("Bus Id", "0000:67:00.0", device2.getBusID());
    assertEquals("Status", "ONLINE", device2.getStatus());
    assertEquals("Major number", 245, device2.getMajorNumber());
    assertEquals("Minor number", 2, device2.getMinorNumber());
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

    assertEquals("Number of devices", 0, devices.size());
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

    assertEquals("Number of devices", 2, devices.size());
    List<Device> devicesList = Lists.newArrayList(devices);
    Collections.sort(devicesList, DEVICE_COMPARATOR);

    Device device0 = devicesList.get(0);
    assertEquals("Device id", 0, device0.getId());
    assertEquals("Device path", "/dev/ve0", device0.getDevPath());
    assertEquals("Bus Id", "0000:65:00.0", device0.getBusID());
    assertEquals("Status", "ONLINE", device0.getStatus());
    assertEquals("Major number", 243, device0.getMajorNumber());
    assertEquals("Minor number", 0, device0.getMinorNumber());

    Device device1 = devicesList.get(1);
    assertEquals("Device id", 1, device1.getId());
    assertEquals("Device path", "/dev/ve1", device1.getDevPath());
    assertEquals("Bus Id", "0000:66:00.0", device1.getBusID());
    assertEquals("Status", "ONLINE", device1.getStatus());
    assertEquals("Major number", 244, device1.getMajorNumber());
    assertEquals("Minor number", 1, device1.getMinorNumber());
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
    assertTrue("Cannot set executable flag", scriptPath.toFile().canExecute());

    env.put("NEC_VE_GET_SCRIPT_PATH",
        testFolder + "/" + DEFAULT_SCRIPT_NAME);

    plugin = new NECVEPlugin(envProvider, EMPTY_SEARCH_DIRS, udevUtil);

    verifyBinaryPathSet(scriptPath);
  }

  @Test(expected = ResourceHandlerException.class)
  public void testExplicitPathPointsToDirectory()
      throws ResourceHandlerException, IOException {
    setupTestDirectory("_temp_" + System.currentTimeMillis());

    env.put("NEC_VE_GET_SCRIPT_PATH", testFolder);

    plugin = new NECVEPlugin(envProvider, EMPTY_SEARCH_DIRS, udevUtil);
  }

  @Test(expected = ResourceHandlerException.class)
  public void testExplicitPathIsNotExecutable()
      throws ResourceHandlerException, IOException{
    setupTestDirectory("_temp_" + System.currentTimeMillis());

    Path scriptPath = Paths.get(testFolder, DEFAULT_SCRIPT_NAME);
    Files.createFile(scriptPath);
    scriptPath.toFile().setExecutable(false);
    assertFalse("File is executable", scriptPath.toFile().canExecute());

    env.put("NEC_VE_GET_SCRIPT_PATH",
        testFolder + "/" + DEFAULT_SCRIPT_NAME);

    plugin = new NECVEPlugin(envProvider, EMPTY_SEARCH_DIRS, udevUtil);
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

    assertEquals("No. of devices", 1, allocated.size());
    Device allocatedDevice = allocated.iterator().next();
    assertSame("Device", device, allocatedDevice);
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

    assertEquals("No. of devices", 2, allocated.size());
    assertTrue("Device missing", allocated.contains(device0));
    assertTrue("Device missing", allocated.contains(device1));
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

    assertEquals("No. of devices", 1, devices.size());
    Device device = devices.iterator().next();
    assertSame("Device", device, testDevice);
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
    assertEquals("Binary path", expectedPath.toString(),
        plugin.getBinaryPath());
    verifyZeroInteractions(udevUtil);
  }
}

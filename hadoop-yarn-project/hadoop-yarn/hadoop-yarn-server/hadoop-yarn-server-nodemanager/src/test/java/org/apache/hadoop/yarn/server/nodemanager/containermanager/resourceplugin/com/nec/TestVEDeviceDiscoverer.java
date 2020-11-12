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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyChar;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.Shell.CommandExecutor;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

/**
 * Unit tests for VEDeviceDiscoverer class.
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class TestVEDeviceDiscoverer {
  private static final Comparator<Device> DEVICE_COMPARATOR =
      Comparator.comparingInt(Device::getId);

  @Rule
  public ExpectedException expected = ExpectedException.none();

  @Mock
  private UdevUtil udevUtil;

  @Mock
  private CommandExecutor mockCommandExecutor;

  private String testFolder;
  private VEDeviceDiscoverer discoverer;

  @Before
  public void setup() throws IOException {
    Function<String[], CommandExecutor> commandExecutorProvider =
        (String[] cmd) -> mockCommandExecutor;
    discoverer = new VEDeviceDiscoverer(udevUtil);
    discoverer.setCommandExecutorProvider(commandExecutorProvider);
    setupTestDirectory();
  }

  @After
  public void teardown() throws IOException {
    if (testFolder != null) {
      File f = new File(testFolder);
      FileUtils.deleteDirectory(f);
    }
  }

  @Test
  public void testDetectSingleOnlineDevice() throws IOException {
    createVeSlotFile(0);
    createOsStateFile(0);
    when(mockCommandExecutor.getOutput())
      .thenReturn("8:1:character special file");
    when(udevUtil.getSysPath(anyInt(), anyChar())).thenReturn(testFolder);

    Set<Device> devices = discoverer.getDevicesFromPath(testFolder);

    assertEquals("Number of devices", 1, devices.size());
    Device device = devices.iterator().next();
    assertEquals("Device ID", 0, device.getId());
    assertEquals("Major number", 8, device.getMajorNumber());
    assertEquals("Minor number", 1, device.getMinorNumber());
    assertEquals("Status", "ONLINE", device.getStatus());
    assertTrue("Device is not healthy", device.isHealthy());
  }

  @Test
  public void testDetectMultipleOnlineDevices() throws IOException {
    createVeSlotFile(0);
    createVeSlotFile(1);
    createVeSlotFile(2);
    createOsStateFile(0);
    when(mockCommandExecutor.getOutput()).thenReturn(
        "8:1:character special file",
        "9:1:character special file",
        "a:1:character special file");
    when(udevUtil.getSysPath(anyInt(), anyChar())).thenReturn(testFolder);

    Set<Device> devices = discoverer.getDevicesFromPath(testFolder);

    assertEquals("Number of devices", 3, devices.size());
    List<Device> devicesList = Lists.newArrayList(devices);
    devicesList.sort(DEVICE_COMPARATOR);

    Device device0 = devicesList.get(0);
    assertEquals("Device ID", 0, device0.getId());
    assertEquals("Major number", 8, device0.getMajorNumber());
    assertEquals("Minor number", 1, device0.getMinorNumber());
    assertEquals("Status", "ONLINE", device0.getStatus());
    assertTrue("Device is not healthy", device0.isHealthy());

    Device device1 = devicesList.get(1);
    assertEquals("Device ID", 1, device1.getId());
    assertEquals("Major number", 9, device1.getMajorNumber());
    assertEquals("Minor number", 1, device1.getMinorNumber());
    assertEquals("Status", "ONLINE", device1.getStatus());
    assertTrue("Device is not healthy", device1.isHealthy());

    Device device2 = devicesList.get(2);
    assertEquals("Device ID", 2, device2.getId());
    assertEquals("Major number", 10, device2.getMajorNumber());
    assertEquals("Minor number", 1, device2.getMinorNumber());
    assertEquals("Status", "ONLINE", device2.getStatus());
    assertTrue("Device is not healthy", device2.isHealthy());
  }

  @Test
  public void testNegativeDeviceStateNumber() throws IOException {
    createVeSlotFile(0);
    createOsStateFile(-1);
    when(mockCommandExecutor.getOutput())
      .thenReturn("8:1:character special file");
    when(udevUtil.getSysPath(anyInt(), anyChar())).thenReturn(testFolder);

    Set<Device> devices = discoverer.getDevicesFromPath(testFolder);

    assertEquals("Number of devices", 1, devices.size());
    Device device = devices.iterator().next();
    assertEquals("Device ID", 0, device.getId());
    assertEquals("Major number", 8, device.getMajorNumber());
    assertEquals("Minor number", 1, device.getMinorNumber());
    assertEquals("Status", "Unknown (-1)", device.getStatus());
    assertFalse("Device should not be healthy", device.isHealthy());
  }

  @Test
  public void testDeviceStateNumberTooHigh() throws IOException {
    createVeSlotFile(0);
    createOsStateFile(5);
    when(mockCommandExecutor.getOutput())
      .thenReturn("8:1:character special file");
    when(udevUtil.getSysPath(anyInt(), anyChar())).thenReturn(testFolder);

    Set<Device> devices = discoverer.getDevicesFromPath(testFolder);

    assertEquals("Number of devices", 1, devices.size());
    Device device = devices.iterator().next();
    assertEquals("Device ID", 0, device.getId());
    assertEquals("Major number", 8, device.getMajorNumber());
    assertEquals("Minor number", 1, device.getMinorNumber());
    assertEquals("Status", "Unknown (5)", device.getStatus());
    assertFalse("Device should not be healthy", device.isHealthy());
  }

  @Test
  public void testDeviceNumberFromMajorAndMinor() throws IOException {
    createVeSlotFile(0);
    createVeSlotFile(1);
    createVeSlotFile(2);
    createOsStateFile(0);
    when(mockCommandExecutor.getOutput()).thenReturn(
        "10:1:character special file",
        "1d:2:character special file",
        "4:3c:character special file");
    when(udevUtil.getSysPath(anyInt(), anyChar())).thenReturn(testFolder);

    Set<Device> devices = discoverer.getDevicesFromPath(testFolder);

    List<Device> devicesList = Lists.newArrayList(devices);
    devicesList.sort(DEVICE_COMPARATOR);

    Device device0 = devicesList.get(0);
    assertEquals("Major number", 16, device0.getMajorNumber());
    assertEquals("Minor number", 1, device0.getMinorNumber());

    Device device1 = devicesList.get(1);
    assertEquals("Major number", 29, device1.getMajorNumber());
    assertEquals("Minor number", 2, device1.getMinorNumber());

    Device device2 = devicesList.get(2);
    assertEquals("Major number", 4, device2.getMajorNumber());
    assertEquals("Minor number", 60, device2.getMinorNumber());
  }

  @Test
  public void testNonVESlotFilesAreSkipped() throws IOException {
    createVeSlotFile(0);
    createOsStateFile(0);
    createFile("abcde");
    createFile("vexlot");
    createFile("xyzveslot");

    when(mockCommandExecutor.getOutput()).thenReturn(
        "8:1:character special file",
        "9:1:character special file",
        "10:1:character special file",
        "11:1:character special file",
        "12:1:character special file");
    when(udevUtil.getSysPath(anyInt(), anyChar())).thenReturn(testFolder);

    Set<Device> devices = discoverer.getDevicesFromPath(testFolder);

    assertEquals("Number of devices", 1, devices.size());
    Device device = devices.iterator().next();
    assertEquals("Device ID", 0, device.getId());
    assertEquals("Major number", 8, device.getMajorNumber());
    assertEquals("Minor number", 1, device.getMinorNumber());
    assertEquals("Status", "ONLINE", device.getStatus());
    assertTrue("Device is not healthy", device.isHealthy());
  }

  @Test
  public void testNonBlockOrCharFilesAreRejected() throws IOException {
    expected.expect(IllegalArgumentException.class);
    expected.expectMessage("File is neither a char nor block device");
    createVeSlotFile(0);
    when(mockCommandExecutor.getOutput()).thenReturn(
        "0:0:regular file");

    discoverer.getDevicesFromPath(testFolder);
  }

  private void setupTestDirectory() throws IOException {
    String path = "target/temp/" +
        TestVEDeviceDiscoverer.class.getName();

    testFolder = new File(path).getAbsolutePath();
    File f = new File(testFolder);
    FileUtils.deleteDirectory(f);

    if (!f.mkdirs()) {
      throw new RuntimeException("Could not create directory: " +
          f.getAbsolutePath());
    }
  }

  private void createVeSlotFile(int slot) throws IOException {
    Files.createFile(Paths.get(testFolder, "veslot" + String.valueOf(slot)));
  }

  private void createFile(String name) throws IOException {
    Files.createFile(Paths.get(testFolder, name));
  }

  private void createOsStateFile(int state) throws IOException {
    Path path = Paths.get(testFolder, "os_state");
    Files.createFile(path);

    Files.write(path, new byte[]{(byte) state});
  }
}
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Shell.CommandExecutor;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for VEDeviceDiscoverer class.
 *
 */
@ExtendWith(MockitoExtension.class)
public class TestVEDeviceDiscoverer {
  private static final Comparator<Device> DEVICE_COMPARATOR =
      Comparator.comparingInt(Device::getId);

  @Mock
  private UdevUtil udevUtil;

  @Mock
  private CommandExecutor mockCommandExecutor;

  private String testFolder;
  private VEDeviceDiscoverer discoverer;

  @BeforeEach
  public void setup() throws IOException {
    Function<String[], CommandExecutor> commandExecutorProvider =
        (String[] cmd) -> mockCommandExecutor;
    discoverer = new VEDeviceDiscoverer(udevUtil);
    discoverer.setCommandExecutorProvider(commandExecutorProvider);
    setupTestDirectory();
  }

  @AfterEach
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

    assertEquals(1, devices.size(), "Number of devices");
    Device device = devices.iterator().next();
    assertEquals(0, device.getId(), "Device ID");
    assertEquals(8, device.getMajorNumber(), "Major number");
    assertEquals(1, device.getMinorNumber(), "Minor number");
    assertEquals("ONLINE", device.getStatus(), "Status");
    assertTrue(device.isHealthy(), "Device is not healthy");
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

    assertEquals(3, devices.size(), "Number of devices");
    List<Device> devicesList = Lists.newArrayList(devices);
    devicesList.sort(DEVICE_COMPARATOR);

    Device device0 = devicesList.get(0);
    assertEquals(0, device0.getId(), "Device ID");
    assertEquals(8, device0.getMajorNumber(), "Major number");
    assertEquals(1, device0.getMinorNumber(), "Minor number");
    assertEquals("ONLINE", device0.getStatus(), "Status");
    assertTrue(device0.isHealthy(), "Device is not healthy");

    Device device1 = devicesList.get(1);
    assertEquals(1, device1.getId(), "Device ID");
    assertEquals(9, device1.getMajorNumber(), "Major number");
    assertEquals(1, device1.getMinorNumber(), "Minor number");
    assertEquals("ONLINE", device1.getStatus(), "Status");
    assertTrue(device1.isHealthy(), "Device is not healthy");

    Device device2 = devicesList.get(2);
    assertEquals(2, device2.getId(), "Device ID");
    assertEquals(10, device2.getMajorNumber(), "Major number");
    assertEquals(1, device2.getMinorNumber(), "Minor number");
    assertEquals("ONLINE", device2.getStatus(), "Status");
    assertTrue(device2.isHealthy(), "Device is not healthy");
  }

  @Test
  public void testNegativeDeviceStateNumber() throws IOException {
    createVeSlotFile(0);
    createOsStateFile(-1);
    when(mockCommandExecutor.getOutput())
      .thenReturn("8:1:character special file");
    when(udevUtil.getSysPath(anyInt(), anyChar())).thenReturn(testFolder);

    Set<Device> devices = discoverer.getDevicesFromPath(testFolder);

    assertEquals(1, devices.size(), "Number of devices");
    Device device = devices.iterator().next();
    assertEquals(0, device.getId(), "Device ID");
    assertEquals(8, device.getMajorNumber(), "Major number");
    assertEquals(1, device.getMinorNumber(), "Minor number");
    assertEquals("Unknown (-1)", device.getStatus(), "Status");
    assertFalse(device.isHealthy(), "Device should not be healthy");
  }

  @Test
  public void testDeviceStateNumberTooHigh() throws IOException {
    createVeSlotFile(0);
    createOsStateFile(5);
    when(mockCommandExecutor.getOutput())
      .thenReturn("8:1:character special file");
    when(udevUtil.getSysPath(anyInt(), anyChar())).thenReturn(testFolder);

    Set<Device> devices = discoverer.getDevicesFromPath(testFolder);

    assertEquals(1, devices.size(), "Number of devices");
    Device device = devices.iterator().next();
    assertEquals(0, device.getId(), "Device ID");
    assertEquals(8, device.getMajorNumber(), "Major number");
    assertEquals(1, device.getMinorNumber(), "Minor number");
    assertEquals("Unknown (5)", device.getStatus(), "Status");
    assertFalse(device.isHealthy(), "Device should not be healthy");
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
    assertEquals(16, device0.getMajorNumber(), "Major number");
    assertEquals(1, device0.getMinorNumber(), "Minor number");

    Device device1 = devicesList.get(1);
    assertEquals(29, device1.getMajorNumber(), "Major number");
    assertEquals(2, device1.getMinorNumber(), "Minor number");

    Device device2 = devicesList.get(2);
    assertEquals(4, device2.getMajorNumber(), "Major number");
    assertEquals(60, device2.getMinorNumber(), "Minor number");
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

    assertEquals(1, devices.size(), "Number of devices");
    Device device = devices.iterator().next();
    assertEquals(0, device.getId(), "Device ID");
    assertEquals(8, device.getMajorNumber(), "Major number");
    assertEquals(1, device.getMinorNumber(), "Minor number");
    assertEquals("ONLINE", device.getStatus(), "Status");
    assertTrue(device.isHealthy(), "Device is not healthy");
  }

  @Test
  public void testNonBlockOrCharFilesAreRejected() throws IOException {
    IllegalArgumentException exception =
          assertThrows(IllegalArgumentException.class, () -> {
            createVeSlotFile(0);
            when(mockCommandExecutor.getOutput()).thenReturn("0:0:regular file");
            discoverer.getDevicesFromPath(testFolder);
          });
    assertThat(exception.getMessage()).contains("File is neither a char nor block device");
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
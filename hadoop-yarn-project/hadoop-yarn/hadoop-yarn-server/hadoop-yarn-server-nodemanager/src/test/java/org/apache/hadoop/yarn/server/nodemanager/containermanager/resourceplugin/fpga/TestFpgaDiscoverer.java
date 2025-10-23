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


package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestFpgaDiscoverer {

  private File fakeBinary;
  private IntelFpgaOpenclPlugin openclPlugin;
  private Configuration conf;
  private FpgaDiscoverer fpgaDiscoverer;

  private String getTestParentFolder() {
    File f = new File("target/temp/" + TestFpgaDiscoverer.class.getName());
    return f.getAbsolutePath();
  }

  private void touchFile(File f) throws IOException {
    new FileOutputStream(f).close();
  }

  @BeforeEach
  public void before() throws IOException {
    String folder = getTestParentFolder();
    File f = new File(folder);
    FileUtils.deleteDirectory(f);
    f.mkdirs();

    conf = new Configuration();

    openclPlugin = new IntelFpgaOpenclPlugin();
    openclPlugin.initPlugin(conf);
    openclPlugin.setInnerShellExecutor(mockPuginShell());

    fpgaDiscoverer = new FpgaDiscoverer();
    fpgaDiscoverer.setResourceHanderPlugin(openclPlugin);
  }

  @AfterEach
  public void afterTest() {
    if (fakeBinary != null) {
      fakeBinary.delete();
    }
  }

  @Test
  public void testExecutablePathWithoutExplicitConfig()
      throws YarnException {
    fpgaDiscoverer.initialize(conf);

    assertEquals("aocl", openclPlugin.getPathToExecutable(),
        "No configuration(no environment ALTERAOCLSDKROOT set)" +
        " should return just a single binary name");
  }

  @Test
  public void testExecutablePathWithCorrectConfig()
      throws IOException, YarnException {
    fakeBinary = new File(getTestParentFolder() + "/aocl");
    conf.set(YarnConfiguration.NM_FPGA_PATH_TO_EXEC,
        getTestParentFolder() + "/aocl");
    touchFile(fakeBinary);

    fpgaDiscoverer.initialize(conf);

    assertEquals(getTestParentFolder() + "/aocl", openclPlugin.getPathToExecutable(),
        "Correct configuration should return user setting");
  }

  @Test
  public void testExecutablePathWhenFileDoesNotExist()
      throws YarnException {
    conf.set(YarnConfiguration.NM_FPGA_PATH_TO_EXEC,
        getTestParentFolder() + "/aocl");

    fpgaDiscoverer.initialize(conf);

    assertEquals("aocl", openclPlugin.getPathToExecutable(),
        "File doesn't exists - expected a single binary name");
  }

  @Test
  public void testExecutablePathWhenFileIsEmpty()
      throws YarnException {
    conf.set(YarnConfiguration.NM_FPGA_PATH_TO_EXEC, "");

    fpgaDiscoverer.initialize(conf);

    assertEquals("aocl", openclPlugin.getPathToExecutable(),
        "configuration with empty string value, should use aocl");
  }

  @Test
  public void testExecutablePathWithSdkRootSet()
      throws IOException, YarnException {
    fakeBinary = new File(getTestParentFolder() + "/bin/aocl");
    fakeBinary.getParentFile().mkdirs();
    touchFile(fakeBinary);
    Map<String, String> newEnv = new HashMap<String, String>();
    newEnv.put("ALTERAOCLSDKROOT", getTestParentFolder());
    openclPlugin.setEnvProvider(s -> {
      return newEnv.get(s); });

    fpgaDiscoverer.initialize(conf);

    assertEquals(getTestParentFolder() + "/bin/aocl", openclPlugin.getPathToExecutable(),
        "No configuration but with environment ALTERAOCLSDKROOT set");
  }

  @Test
  public void testDiscoveryWhenAvailableDevicesDefined()
      throws YarnException {
    conf.set(YarnConfiguration.NM_FPGA_AVAILABLE_DEVICES,
        "acl0/243:0,acl1/244:1");

    fpgaDiscoverer.initialize(conf);
    List<FpgaDevice> devices = fpgaDiscoverer.discover();

    assertEquals(2, devices.size(), "Number of devices");
    FpgaDevice device0 = devices.get(0);
    FpgaDevice device1 = devices.get(1);

    assertEquals("acl0", device0.getAliasDevName(), "Device id");
    assertEquals(0, device0.getMinor(), "Minor number");
    assertEquals(243, device0.getMajor(), "Major");

    assertEquals("acl1", device1.getAliasDevName(), "Device id");
    assertEquals(1, device1.getMinor(), "Minor number");
    assertEquals(244, device1.getMajor(), "Major");
  }

  @Test
  public void testDiscoveryWhenAvailableDevicesEmpty()
      throws YarnException {
    ResourceHandlerException exception = assertThrows(ResourceHandlerException.class, () -> {
      conf.set(YarnConfiguration.NM_FPGA_AVAILABLE_DEVICES, "");

      fpgaDiscoverer.initialize(conf);
      fpgaDiscoverer.discover();
    });
    assertThat(exception.getMessage()).contains("No FPGA devices were specified");
  }

  @Test
  public void testDiscoveryWhenAvailableDevicesAreIllegalString()
      throws YarnException {
    ResourceHandlerException exception = assertThrows(ResourceHandlerException.class, () -> {
      conf.set(YarnConfiguration.NM_FPGA_AVAILABLE_DEVICES,
              "illegal/243:0,acl1/244=1");

      fpgaDiscoverer.initialize(conf);
      fpgaDiscoverer.discover();
    });
    assertThat(exception.getMessage()).contains("Illegal device specification string");
  }

  @Test
  public void testDiscoveryWhenExternalScriptDefined()
      throws YarnException {
    conf.set(YarnConfiguration.NM_FPGA_DEVICE_DISCOVERY_SCRIPT,
        "/dummy/script");

    fpgaDiscoverer.setScriptRunner(s -> {
      return Optional.of("acl0/243:0,acl1/244:1"); });
    fpgaDiscoverer.initialize(conf);
    List<FpgaDevice> devices = fpgaDiscoverer.discover();

    assertEquals(2, devices.size(), "Number of devices");
    FpgaDevice device0 = devices.get(0);
    FpgaDevice device1 = devices.get(1);

    assertEquals("acl0", device0.getAliasDevName(), "Device id");
    assertEquals(0, device0.getMinor(), "Minor number");
    assertEquals(243, device0.getMajor(), "Major");

    assertEquals("acl1", device1.getAliasDevName(), "Device id");
    assertEquals(1, device1.getMinor(), "Minor number");
    assertEquals(244, device1.getMajor(), "Major");
  }

  @Test
  public void testDiscoveryWhenExternalScriptReturnsEmptyString()
      throws YarnException {

    ResourceHandlerException exception = assertThrows(ResourceHandlerException.class, () -> {
      conf.set(YarnConfiguration.NM_FPGA_DEVICE_DISCOVERY_SCRIPT, "/dummy/script");

      fpgaDiscoverer.setScriptRunner(s -> {
        return Optional.of("");
      });

      fpgaDiscoverer.initialize(conf);
      fpgaDiscoverer.discover();
    });
    assertThat(exception.getMessage()).contains("No FPGA devices were specified");
  }

  @Test
  public void testDiscoveryWhenExternalScriptFails()
      throws YarnException {
    ResourceHandlerException exception = assertThrows(ResourceHandlerException.class, () -> {
      conf.set(YarnConfiguration.NM_FPGA_DEVICE_DISCOVERY_SCRIPT, "/dummy/script");

      fpgaDiscoverer.setScriptRunner(s -> {
        return Optional.empty();
      });

      fpgaDiscoverer.initialize(conf);
      fpgaDiscoverer.discover();
    });
    assertThat(exception.getMessage()).contains("Unable to run external script");
  }

  @Test
  public void testDiscoveryWhenExternalScriptUndefined()
      throws YarnException {
    ResourceHandlerException exception = assertThrows(ResourceHandlerException.class, () -> {
      conf.set(YarnConfiguration.NM_FPGA_DEVICE_DISCOVERY_SCRIPT, "");

      fpgaDiscoverer.initialize(conf);
      fpgaDiscoverer.discover();
    });
    assertThat(exception.getMessage()).contains("Unable to run external script");
  }

  @Test
  public void testDiscoveryWhenExternalScriptCannotBeExecuted()
      throws YarnException, IOException {

    ResourceHandlerException exception = assertThrows(ResourceHandlerException.class, () -> {
      File fakeScript = new File(getTestParentFolder() + "/fakeScript");
      try {
        fakeScript = new File(getTestParentFolder() + "/fakeScript");
        touchFile(fakeScript);
        fakeScript.setExecutable(false);
        conf.set(YarnConfiguration.NM_FPGA_DEVICE_DISCOVERY_SCRIPT,
                fakeScript.getAbsolutePath());

        fpgaDiscoverer.initialize(conf);
        fpgaDiscoverer.discover();
      } finally {
        fakeScript.delete();
      }
    });

    assertThat(exception.getMessage()).contains("Unable to run external script");
  }

  @Test
  public void testCurrentFpgaInfoWhenAllDevicesAreAllowed()
      throws YarnException {
    conf.set(YarnConfiguration.NM_FPGA_AVAILABLE_DEVICES,
        "acl0/243:0,acl1/244:1");

    fpgaDiscoverer.initialize(conf);
    List<FpgaDevice> devices = fpgaDiscoverer.discover();
    List<FpgaDevice> currentFpgaInfo = fpgaDiscoverer.getCurrentFpgaInfo();

    assertEquals(devices, currentFpgaInfo, "Devices");
  }

  @Test
  public void testCurrentFpgaInfoWhenAllowedDevicesDefined()
      throws YarnException {
    conf.set(YarnConfiguration.NM_FPGA_AVAILABLE_DEVICES,
        "acl0/243:0,acl1/244:1");
    conf.set(YarnConfiguration.NM_FPGA_ALLOWED_DEVICES, "0");

    fpgaDiscoverer.initialize(conf);
    List<FpgaDevice> devices = fpgaDiscoverer.discover();
    List<FpgaDevice> currentFpgaInfo = fpgaDiscoverer.getCurrentFpgaInfo();

    assertEquals(devices, currentFpgaInfo, "Devices");
    assertEquals(1, currentFpgaInfo.size(), "List of devices");

    FpgaDevice device = currentFpgaInfo.get(0);
    assertEquals("acl0", device.getAliasDevName(), "Device id");
    assertEquals(0, device.getMinor(), "Minor number");
    assertEquals(243, device.getMajor(), "Major");
  }

  private IntelFpgaOpenclPlugin.InnerShellExecutor mockPuginShell() {
    IntelFpgaOpenclPlugin.InnerShellExecutor shell = mock(IntelFpgaOpenclPlugin.InnerShellExecutor.class);
    when(shell.runDiagnose(anyString(),anyInt())).thenReturn("");
    when(shell.getMajorAndMinorNumber("aclnalla_pcie0")).thenReturn("247:0");
    when(shell.getMajorAndMinorNumber("aclnalla_pcie1")).thenReturn("247:1");
    when(shell.getMajorAndMinorNumber("acla10_ref0")).thenReturn("246:0");
    return shell;
  }
}

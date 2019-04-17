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

import static org.junit.Assert.assertEquals;
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
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.fpga.FpgaResourceAllocator.FpgaDevice;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestFpgaDiscoverer {
  @Rule
  public ExpectedException expected = ExpectedException.none();

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

  @Before
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

  @After
  public void afterTest() {
    if (fakeBinary != null) {
      fakeBinary.delete();
    }
  }

  @Test
  public void testExecutablePathWithoutExplicitConfig()
      throws YarnException {
    fpgaDiscoverer.initialize(conf);

    assertEquals("No configuration(no environment ALTERAOCLSDKROOT set)" +
            " should return just a single binary name",
        "aocl", openclPlugin.getPathToExecutable());
  }

  @Test
  public void testExecutablePathWithCorrectConfig()
      throws IOException, YarnException {
    fakeBinary = new File(getTestParentFolder() + "/aocl");
    conf.set(YarnConfiguration.NM_FPGA_PATH_TO_EXEC,
        getTestParentFolder() + "/aocl");
    touchFile(fakeBinary);

    fpgaDiscoverer.initialize(conf);

    assertEquals("Correct configuration should return user setting",
        getTestParentFolder() + "/aocl", openclPlugin.getPathToExecutable());
  }

  @Test
  public void testExecutablePathWhenFileDoesNotExist()
      throws YarnException {
    conf.set(YarnConfiguration.NM_FPGA_PATH_TO_EXEC,
        getTestParentFolder() + "/aocl");

    fpgaDiscoverer.initialize(conf);

    assertEquals("File doesn't exists - expected a single binary name",
        "aocl", openclPlugin.getPathToExecutable());
  }

  @Test
  public void testExecutablePathWhenFileIsEmpty()
      throws YarnException {
    conf.set(YarnConfiguration.NM_FPGA_PATH_TO_EXEC, "");

    fpgaDiscoverer.initialize(conf);

    assertEquals("configuration with empty string value, should use aocl",
        "aocl", openclPlugin.getPathToExecutable());
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

    assertEquals("No configuration but with environment ALTERAOCLSDKROOT set",
        getTestParentFolder() + "/bin/aocl", openclPlugin.getPathToExecutable());
  }

  @Test
  public void testDiscoveryWhenAvailableDevicesDefined()
      throws YarnException {
    conf.set(YarnConfiguration.NM_FPGA_AVAILABLE_DEVICES,
        "acl0/243:0,acl1/244:1");

    fpgaDiscoverer.initialize(conf);
    List<FpgaDevice> devices = fpgaDiscoverer.discover();

    assertEquals("Number of devices", 2, devices.size());
    FpgaDevice device0 = devices.get(0);
    FpgaDevice device1 = devices.get(1);

    assertEquals("Device id", "acl0", device0.getAliasDevName());
    assertEquals("Minor number", 0, device0.getMinor());
    assertEquals("Major", 243, device0.getMajor());

    assertEquals("Device id", "acl1", device1.getAliasDevName());
    assertEquals("Minor number", 1, device1.getMinor());
    assertEquals("Major", 244, device1.getMajor());
  }

  @Test
  public void testDiscoveryWhenAvailableDevicesEmpty()
      throws YarnException {
    expected.expect(ResourceHandlerException.class);
    expected.expectMessage("No FPGA devices were specified");

    conf.set(YarnConfiguration.NM_FPGA_AVAILABLE_DEVICES,
        "");

    fpgaDiscoverer.initialize(conf);
    fpgaDiscoverer.discover();
  }

  @Test
  public void testDiscoveryWhenAvailableDevicesAreIllegalString()
      throws YarnException {
    expected.expect(ResourceHandlerException.class);
    expected.expectMessage("Illegal device specification string");

    conf.set(YarnConfiguration.NM_FPGA_AVAILABLE_DEVICES,
        "illegal/243:0,acl1/244=1");

    fpgaDiscoverer.initialize(conf);
    fpgaDiscoverer.discover();
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

    assertEquals("Number of devices", 2, devices.size());
    FpgaDevice device0 = devices.get(0);
    FpgaDevice device1 = devices.get(1);

    assertEquals("Device id", "acl0", device0.getAliasDevName());
    assertEquals("Minor number", 0, device0.getMinor());
    assertEquals("Major", 243, device0.getMajor());

    assertEquals("Device id", "acl1", device1.getAliasDevName());
    assertEquals("Minor number", 1, device1.getMinor());
    assertEquals("Major", 244, device1.getMajor());
  }

  @Test
  public void testDiscoveryWhenExternalScriptReturnsEmptyString()
      throws YarnException {
    expected.expect(ResourceHandlerException.class);
    expected.expectMessage("No FPGA devices were specified");

    conf.set(YarnConfiguration.NM_FPGA_DEVICE_DISCOVERY_SCRIPT,
        "/dummy/script");

    fpgaDiscoverer.setScriptRunner(s -> {
      return Optional.of(""); });

    fpgaDiscoverer.initialize(conf);
    fpgaDiscoverer.discover();
  }

  @Test
  public void testDiscoveryWhenExternalScriptFails()
      throws YarnException {
    expected.expect(ResourceHandlerException.class);
    expected.expectMessage("Unable to run external script");

    conf.set(YarnConfiguration.NM_FPGA_DEVICE_DISCOVERY_SCRIPT,
        "/dummy/script");

    fpgaDiscoverer.setScriptRunner(s -> {
      return Optional.empty(); });

    fpgaDiscoverer.initialize(conf);
    fpgaDiscoverer.discover();
  }

  @Test
  public void testDiscoveryWhenExternalScriptUndefined()
      throws YarnException {
    expected.expect(ResourceHandlerException.class);
    expected.expectMessage("Unable to run external script");

    conf.set(YarnConfiguration.NM_FPGA_DEVICE_DISCOVERY_SCRIPT, "");

    fpgaDiscoverer.initialize(conf);
    fpgaDiscoverer.discover();
  }

  @Test
  public void testDiscoveryWhenExternalScriptCannotBeExecuted()
      throws YarnException, IOException {
    File fakeScript = new File(getTestParentFolder() + "/fakeScript");
    try {
      expected.expect(ResourceHandlerException.class);
      expected.expectMessage("Unable to run external script");

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

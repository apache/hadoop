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


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.fpga.FpgaResourceAllocator;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.fpga.FpgaResourceAllocator.FpgaDevice;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFpgaDiscoverer {
  @Rule
  public ExpectedException expected = ExpectedException.none();

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
    FpgaDiscoverer.reset();
  }

  // A dirty hack to modify the env of the current JVM itself - Dirty, but
  // should be okay for testing.
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static void setNewEnvironmentHack(Map<String, String> newenv)
      throws Exception {
    try {
      Class<?> cl = Class.forName("java.lang.ProcessEnvironment");
      Field field = cl.getDeclaredField("theEnvironment");
      field.setAccessible(true);
      Map<String, String> env = (Map<String, String>) field.get(null);
      env.clear();
      env.putAll(newenv);
      Field ciField = cl.getDeclaredField("theCaseInsensitiveEnvironment");
      ciField.setAccessible(true);
      Map<String, String> cienv = (Map<String, String>) ciField.get(null);
      cienv.clear();
      cienv.putAll(newenv);
    } catch (NoSuchFieldException e) {
      Class[] classes = Collections.class.getDeclaredClasses();
      Map<String, String> env = System.getenv();
      for (Class cl : classes) {
        if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
          Field field = cl.getDeclaredField("m");
          field.setAccessible(true);
          Object obj = field.get(env);
          Map<String, String> map = (Map<String, String>) obj;
          map.clear();
          map.putAll(newenv);
        }
      }
    }
  }

  @Test
  public void testLinuxFpgaResourceDiscoverPluginConfig() throws Exception {
    Configuration conf = new Configuration(false);
    FpgaDiscoverer discoverer = FpgaDiscoverer.getInstance();

    IntelFpgaOpenclPlugin openclPlugin = new IntelFpgaOpenclPlugin();
    // because FPGA discoverer is a singleton, we use setPlugin to make
    // FpgaDiscoverer.getInstance().diagnose() work in openclPlugin.initPlugin()
    discoverer.setResourceHanderPlugin(openclPlugin);
    openclPlugin.initPlugin(conf);
    openclPlugin.setShell(mockPuginShell());

    discoverer.initialize(conf);
    // Case 1. No configuration set for binary(no environment "ALTERAOCLSDKROOT" set)
    assertEquals("No configuration(no environment ALTERAOCLSDKROOT set)" +
            "should return just a single binary name",
        "aocl", openclPlugin.getPathToExecutable());

    // Case 2. With correct configuration and file exists
    File fakeBinary = new File(getTestParentFolder() + "/aocl");
    conf.set(YarnConfiguration.NM_FPGA_PATH_TO_EXEC, getTestParentFolder() + "/aocl");
    touchFile(fakeBinary);
    discoverer.initialize(conf);
    assertEquals("Correct configuration should return user setting",
        getTestParentFolder() + "/aocl", openclPlugin.getPathToExecutable());

    // Case 3. With correct configuration but file doesn't exists. Use default
    fakeBinary.delete();
    discoverer.initialize(conf);
    assertEquals("Should return just a single binary name",
        "aocl", openclPlugin.getPathToExecutable());

    // Case 4. Set a empty value
    conf.set(YarnConfiguration.NM_FPGA_PATH_TO_EXEC, "");
    discoverer.initialize(conf);
    assertEquals("configuration with empty string value, should use aocl",
        "aocl", openclPlugin.getPathToExecutable());

    // Case 5. No configuration set for binary, but set environment "ALTERAOCLSDKROOT"
    // we load the default configuration to start with
    conf = new Configuration(true);
    fakeBinary = new File(getTestParentFolder() + "/bin/aocl");
    fakeBinary.getParentFile().mkdirs();
    touchFile(fakeBinary);
    Map<String, String> newEnv = new HashMap<String, String>();
    newEnv.put("ALTERAOCLSDKROOT", getTestParentFolder());
    setNewEnvironmentHack(newEnv);
    discoverer.initialize(conf);
    assertEquals("No configuration but with environment ALTERAOCLSDKROOT set",
        getTestParentFolder() + "/bin/aocl", openclPlugin.getPathToExecutable());

  }

  @Test
  public void testDiscoverPluginParser() throws YarnException {
    String output = "------------------------- acl0 -------------------------\n" +
        "Vendor: Nallatech ltd\n" +
        "Phys Dev Name  Status   Information\n" +
        "aclnalla_pcie0Passed   nalla_pcie (aclnalla_pcie0)\n" +
        "                       PCIe dev_id = 2494, bus:slot.func = 02:00.00, Gen3 x8\n" +
        "                       FPGA temperature = 53.1 degrees C.\n" +
        "                       Total Card Power Usage = 31.7 Watts.\n" +
        "                       Device Power Usage = 0.0 Watts.\n" +
        "DIAGNOSTIC_PASSED" +
        "---------------------------------------------------------\n";
    output = output +
        "------------------------- acl1 -------------------------\n" +
        "Vendor: Nallatech ltd\n" +
        "Phys Dev Name  Status   Information\n" +
        "aclnalla_pcie1Passed   nalla_pcie (aclnalla_pcie1)\n" +
        "                       PCIe dev_id = 2495, bus:slot.func = 03:00.00, Gen3 x8\n" +
        "                       FPGA temperature = 43.1 degrees C.\n" +
        "                       Total Card Power Usage = 11.7 Watts.\n" +
        "                       Device Power Usage = 0.0 Watts.\n" +
        "DIAGNOSTIC_PASSED" +
        "---------------------------------------------------------\n";
    output = output +
        "------------------------- acl2 -------------------------\n" +
        "Vendor: Intel(R) Corporation\n" +
        "\n" +
        "Phys Dev Name  Status   Information\n" +
        "\n" +
        "acla10_ref0   Passed   Arria 10 Reference Platform (acla10_ref0)\n" +
        "                       PCIe dev_id = 2494, bus:slot.func = 09:00.00, Gen2 x8\n" +
        "                       FPGA temperature = 50.5781 degrees C.\n" +
        "\n" +
        "DIAGNOSTIC_PASSED\n" +
        "---------------------------------------------------------\n";
    Configuration conf = new Configuration(false);
    IntelFpgaOpenclPlugin openclPlugin = new IntelFpgaOpenclPlugin();
    FpgaDiscoverer.getInstance().setResourceHanderPlugin(openclPlugin);

    openclPlugin.initPlugin(conf);
    openclPlugin.setShell(mockPuginShell());

    FpgaDiscoverer.getInstance().initialize(conf);

    List<FpgaResourceAllocator.FpgaDevice> list = new LinkedList<>();

    // Case 1. core parsing
    openclPlugin.parseDiagnoseInfo(output, list);
    assertEquals(3, list.size());
    assertEquals("IntelOpenCL", list.get(0).getType());
    assertEquals("247", list.get(0).getMajor().toString());
    assertEquals("0", list.get(0).getMinor().toString());
    assertEquals("acl0", list.get(0).getAliasDevName());
    assertEquals("aclnalla_pcie0", list.get(0).getDevName());
    assertEquals("02:00.00", list.get(0).getBusNum());
    assertEquals("53.1 degrees C", list.get(0).getTemperature());
    assertEquals("31.7 Watts", list.get(0).getCardPowerUsage());

    assertEquals("IntelOpenCL", list.get(1).getType());
    assertEquals("247", list.get(1).getMajor().toString());
    assertEquals("1", list.get(1).getMinor().toString());
    assertEquals("acl1", list.get(1).getAliasDevName());
    assertEquals("aclnalla_pcie1", list.get(1).getDevName());
    assertEquals("03:00.00", list.get(1).getBusNum());
    assertEquals("43.1 degrees C", list.get(1).getTemperature());
    assertEquals("11.7 Watts", list.get(1).getCardPowerUsage());

    assertEquals("IntelOpenCL", list.get(2).getType());
    assertEquals("246", list.get(2).getMajor().toString());
    assertEquals("0", list.get(2).getMinor().toString());
    assertEquals("acl2", list.get(2).getAliasDevName());
    assertEquals("acla10_ref0", list.get(2).getDevName());
    assertEquals("09:00.00", list.get(2).getBusNum());
    assertEquals("50.5781 degrees C", list.get(2).getTemperature());
    assertEquals("", list.get(2).getCardPowerUsage());

    // Case 2. check alias map
    Map<String, String> aliasMap = openclPlugin.getAliasMap();
    assertEquals("acl0", aliasMap.get("247:0"));
    assertEquals("acl1", aliasMap.get("247:1"));
    assertEquals("acl2", aliasMap.get("246:0"));
  }

  @Test
  public void testDiscoveryWhenAvailableDevicesDefined()
      throws YarnException {
    Configuration conf = new Configuration(false);
    conf.set(YarnConfiguration.NM_FPGA_AVAILABLE_DEVICES,
        "acl0/243:0,acl1/244:1");
    FpgaDiscoverer discoverer = FpgaDiscoverer.getInstance();

    IntelFpgaOpenclPlugin openclPlugin = new IntelFpgaOpenclPlugin();
    discoverer.setResourceHanderPlugin(openclPlugin);
    openclPlugin.initPlugin(conf);
    openclPlugin.setShell(mockPuginShell());

    discoverer.initialize(conf);
    List<FpgaDevice> devices = discoverer.discover();
    assertEquals("Number of devices", 2, devices.size());
    FpgaDevice device0 = devices.get(0);
    FpgaDevice device1 = devices.get(1);

    assertEquals("Device id", "acl0", device0.getAliasDevName());
    assertEquals("Minor number", new Integer(0), device0.getMinor());
    assertEquals("Major", new Integer(243), device0.getMajor());

    assertEquals("Device id", "acl1", device1.getAliasDevName());
    assertEquals("Minor number", new Integer(1), device1.getMinor());
    assertEquals("Major", new Integer(244), device1.getMajor());
  }

  @Test
  public void testDiscoveryWhenAvailableDevicesEmpty()
      throws YarnException {
    expected.expect(ResourceHandlerException.class);
    expected.expectMessage("No FPGA devices were specified");

    Configuration conf = new Configuration(false);
    conf.set(YarnConfiguration.NM_FPGA_AVAILABLE_DEVICES,
        "");
    FpgaDiscoverer discoverer = FpgaDiscoverer.getInstance();

    IntelFpgaOpenclPlugin openclPlugin = new IntelFpgaOpenclPlugin();
    discoverer.setResourceHanderPlugin(openclPlugin);
    openclPlugin.initPlugin(conf);
    openclPlugin.setShell(mockPuginShell());

    discoverer.initialize(conf);
    discoverer.discover();
  }

  @Test
  public void testDiscoveryWhenAvailableDevicesAreIllegalString()
      throws YarnException {
    expected.expect(ResourceHandlerException.class);
    expected.expectMessage("Illegal device specification string");

    Configuration conf = new Configuration(false);
    conf.set(YarnConfiguration.NM_FPGA_AVAILABLE_DEVICES,
        "illegal/243:0,acl1/244=1");
    FpgaDiscoverer discoverer = FpgaDiscoverer.getInstance();

    IntelFpgaOpenclPlugin openclPlugin = new IntelFpgaOpenclPlugin();
    discoverer.setResourceHanderPlugin(openclPlugin);
    openclPlugin.initPlugin(conf);
    openclPlugin.setShell(mockPuginShell());

    discoverer.initialize(conf);
    discoverer.discover();
  }

  @Test
  public void testDiscoveryWhenExternalScriptDefined()
      throws YarnException {
    Configuration conf = new Configuration(false);
    conf.set(YarnConfiguration.NM_FPGA_DEVICE_DISCOVERY_SCRIPT,
        "/dummy/script");
    FpgaDiscoverer discoverer = FpgaDiscoverer.getInstance();

    IntelFpgaOpenclPlugin openclPlugin = new IntelFpgaOpenclPlugin();
    discoverer.setResourceHanderPlugin(openclPlugin);
    openclPlugin.initPlugin(conf);
    openclPlugin.setShell(mockPuginShell());
    discoverer.setScriptRunner(s -> {
      return Optional.of("acl0/243:0,acl1/244:1"); });

    discoverer.initialize(conf);
    List<FpgaDevice> devices = discoverer.discover();
    assertEquals("Number of devices", 2, devices.size());
    FpgaDevice device0 = devices.get(0);
    FpgaDevice device1 = devices.get(1);

    assertEquals("Device id", "acl0", device0.getAliasDevName());
    assertEquals("Minor number", new Integer(0), device0.getMinor());
    assertEquals("Major", new Integer(243), device0.getMajor());

    assertEquals("Device id", "acl1", device1.getAliasDevName());
    assertEquals("Minor number", new Integer(1), device1.getMinor());
    assertEquals("Major", new Integer(244), device1.getMajor());
  }

  @Test
  public void testDiscoveryWhenExternalScriptReturnsEmptyString()
      throws YarnException {
    expected.expect(ResourceHandlerException.class);
    expected.expectMessage("No FPGA devices were specified");

    Configuration conf = new Configuration(false);
    conf.set(YarnConfiguration.NM_FPGA_DEVICE_DISCOVERY_SCRIPT,
        "/dummy/script");
    FpgaDiscoverer discoverer = FpgaDiscoverer.getInstance();

    IntelFpgaOpenclPlugin openclPlugin = new IntelFpgaOpenclPlugin();
    discoverer.setResourceHanderPlugin(openclPlugin);
    openclPlugin.initPlugin(conf);
    openclPlugin.setShell(mockPuginShell());
    discoverer.setScriptRunner(s -> {
      return Optional.of(""); });

    discoverer.initialize(conf);
    discoverer.discover();
  }

  @Test

  public void testDiscoveryWhenExternalScriptFails()
      throws YarnException {
    expected.expect(ResourceHandlerException.class);
    expected.expectMessage("Unable to run external script");

    Configuration conf = new Configuration(false);
    conf.set(YarnConfiguration.NM_FPGA_DEVICE_DISCOVERY_SCRIPT,
        "/dummy/script");
    FpgaDiscoverer discoverer = FpgaDiscoverer.getInstance();

    IntelFpgaOpenclPlugin openclPlugin = new IntelFpgaOpenclPlugin();
    discoverer.setResourceHanderPlugin(openclPlugin);
    openclPlugin.initPlugin(conf);
    openclPlugin.setShell(mockPuginShell());
    discoverer.setScriptRunner(s -> {
      return Optional.empty(); });

    discoverer.initialize(conf);
    discoverer.discover();
  }

  @Test
  public void testDiscoveryWhenExternalScriptUndefined()
      throws YarnException {
    expected.expect(ResourceHandlerException.class);
    expected.expectMessage("Unable to run external script");

    Configuration conf = new Configuration(false);
    conf.set(YarnConfiguration.NM_FPGA_DEVICE_DISCOVERY_SCRIPT, "");
    FpgaDiscoverer discoverer = FpgaDiscoverer.getInstance();

    IntelFpgaOpenclPlugin openclPlugin = new IntelFpgaOpenclPlugin();
    discoverer.setResourceHanderPlugin(openclPlugin);
    openclPlugin.initPlugin(conf);
    openclPlugin.setShell(mockPuginShell());

    discoverer.initialize(conf);
    discoverer.discover();
  }

  @Test
  public void testDiscoveryWhenExternalScriptCannotBeExecuted()
      throws YarnException, IOException {
    File fakeScript = new File(getTestParentFolder() + "/fakeScript");
    try {
      expected.expect(ResourceHandlerException.class);
      expected.expectMessage("Unable to run external script");

      Configuration conf = new Configuration(false);
      fakeScript = new File(getTestParentFolder() + "/fakeScript");
      touchFile(fakeScript);
      fakeScript.setExecutable(false);
      conf.set(YarnConfiguration.NM_FPGA_DEVICE_DISCOVERY_SCRIPT,
          fakeScript.getAbsolutePath());
      FpgaDiscoverer discoverer = FpgaDiscoverer.getInstance();

      IntelFpgaOpenclPlugin openclPlugin = new IntelFpgaOpenclPlugin();
      discoverer.setResourceHanderPlugin(openclPlugin);
      openclPlugin.initPlugin(conf);
      openclPlugin.setShell(mockPuginShell());

      discoverer.initialize(conf);
      discoverer.discover();
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

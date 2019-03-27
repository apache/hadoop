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
import java.lang.reflect.Field;
import java.util.Collections;
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
    openclPlugin.setInnerShellExecutor(mockPuginShell());

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
  public void testDiscoveryWhenAvailableDevicesDefined()
      throws YarnException {
    Configuration conf = new Configuration(false);
    conf.set(YarnConfiguration.NM_FPGA_AVAILABLE_DEVICES,
        "acl0/243:0,acl1/244:1");
    FpgaDiscoverer discoverer = FpgaDiscoverer.getInstance();

    IntelFpgaOpenclPlugin openclPlugin = new IntelFpgaOpenclPlugin();
    discoverer.setResourceHanderPlugin(openclPlugin);
    openclPlugin.initPlugin(conf);
    openclPlugin.setInnerShellExecutor(mockPuginShell());

    discoverer.initialize(conf);
    List<FpgaDevice> devices = discoverer.discover();
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

    Configuration conf = new Configuration(false);
    conf.set(YarnConfiguration.NM_FPGA_AVAILABLE_DEVICES,
        "");
    FpgaDiscoverer discoverer = FpgaDiscoverer.getInstance();

    IntelFpgaOpenclPlugin openclPlugin = new IntelFpgaOpenclPlugin();
    discoverer.setResourceHanderPlugin(openclPlugin);
    openclPlugin.initPlugin(conf);
    openclPlugin.setInnerShellExecutor(mockPuginShell());

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
    openclPlugin.setInnerShellExecutor(mockPuginShell());

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
    openclPlugin.setInnerShellExecutor(mockPuginShell());
    discoverer.setScriptRunner(s -> {
      return Optional.of("acl0/243:0,acl1/244:1"); });

    discoverer.initialize(conf);
    List<FpgaDevice> devices = discoverer.discover();
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

    Configuration conf = new Configuration(false);
    conf.set(YarnConfiguration.NM_FPGA_DEVICE_DISCOVERY_SCRIPT,
        "/dummy/script");
    FpgaDiscoverer discoverer = FpgaDiscoverer.getInstance();

    IntelFpgaOpenclPlugin openclPlugin = new IntelFpgaOpenclPlugin();
    discoverer.setResourceHanderPlugin(openclPlugin);
    openclPlugin.initPlugin(conf);
    openclPlugin.setInnerShellExecutor(mockPuginShell());
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
    openclPlugin.setInnerShellExecutor(mockPuginShell());
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
    openclPlugin.setInnerShellExecutor(mockPuginShell());

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
      openclPlugin.setInnerShellExecutor(mockPuginShell());

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

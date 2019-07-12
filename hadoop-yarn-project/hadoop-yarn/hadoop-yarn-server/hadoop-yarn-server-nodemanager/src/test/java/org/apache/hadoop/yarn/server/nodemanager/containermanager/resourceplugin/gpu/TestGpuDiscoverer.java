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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.GpuDeviceInformation;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.hadoop.test.PlatformAssumptions.assumeNotWindows;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_GPU_ALLOWED_DEVICES;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuDiscoverer.DEFAULT_BINARY_NAME;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class TestGpuDiscoverer {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestGpuDiscoverer.class);

  private static final String PATH = "PATH";
  private static final String NVIDIA = "nvidia";
  private static final String EXEC_PERMISSION = "u+x";
  private static final String BASH_SHEBANG = "#!/bin/bash\n\n";
  private static final String TEST_PARENT_DIR = new File("target/temp/" +
      TestGpuDiscoverer.class.getName()).getAbsolutePath();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private String getTestParentFolder() {
    File f = new File("target/temp/" + TestGpuDiscoverer.class.getName());
    return f.getAbsolutePath();
  }

  private void touchFile(File f) throws IOException {
    new FileOutputStream(f).close();
  }

  private File setupFakeBinary(Configuration conf) {
    File fakeBinary;
    try {
      fakeBinary = new File(getTestParentFolder(),
          GpuDiscoverer.DEFAULT_BINARY_NAME);
      touchFile(fakeBinary);
      conf.set(YarnConfiguration.NM_GPU_PATH_TO_EXEC, getTestParentFolder());
    } catch (Exception e) {
      throw new RuntimeException("Failed to init fake binary", e);
    }
    return fakeBinary;
  }

  @Before
  public void before() throws IOException {
    assumeNotWindows();
    File f = new File(TEST_PARENT_DIR);
    FileUtils.deleteDirectory(f);
    f.mkdirs();
  }

  private Configuration createConfigWithAllowedDevices(String s) {
    Configuration conf = new Configuration(false);
    conf.set(NM_GPU_ALLOWED_DEVICES, s);
    setupFakeBinary(conf);
    return conf;
  }

  private void createNvidiaSmiScript(File file) {
    writeToFile(file, BASH_SHEBANG +
        "echo '<nvidia_smi_log></nvidia_smi_log>'");
  }

  private void createFaultyNvidiaSmiScript(File file) {
    writeToFile(file, BASH_SHEBANG + "echo <<'");
  }

  private void createNvidiaSmiScriptWithInvalidXml(File file) {
    writeToFile(file, BASH_SHEBANG + "echo '<nvidia_smi_log></bla>'");
  }

  private static void writeToFile(File file, String contents) {
    try {
      PrintWriter fileWriter = new PrintWriter(file);
      fileWriter.write(contents);
      fileWriter.close();
    } catch (Exception e) {
      throw new RuntimeException("Error while writing nvidia-smi script file!",
          e);
    }
  }

  private void assertNvidiaIsOnPath(GpuDiscoverer discoverer) {
    String path = discoverer.getEnvironmentToRunCommand().get(PATH);
    assertNotNull(path);
    assertTrue(path.contains(NVIDIA));
  }

  private File createFakeNvidiaSmiScriptAsRunnableFile(
      Consumer<File> scriptFileCreator) throws IOException {
    File fakeBinary = new File(TEST_PARENT_DIR, DEFAULT_BINARY_NAME);
    touchFile(fakeBinary);
    scriptFileCreator.accept(fakeBinary);
    Shell.execCommand(Shell.getSetPermissionCommand(EXEC_PERMISSION, false,
        fakeBinary.getAbsolutePath()));

    return fakeBinary;
  }

  private GpuDiscoverer creatediscovererWithGpuPathDefined(
      Configuration conf) throws YarnException {
    conf.set(YarnConfiguration.NM_GPU_PATH_TO_EXEC, TEST_PARENT_DIR);
    GpuDiscoverer discoverer = new GpuDiscoverer();
    discoverer.initialize(conf);
    return discoverer;
  }

  @Test
  public void testLinuxGpuResourceDiscoverPluginConfig() throws Exception {
    // Only run this on demand.
    Assume.assumeTrue(Boolean.valueOf(
        System.getProperty("RunLinuxGpuResourceDiscoverPluginConfigTest")));

    // test case 1, check default setting.
    Configuration conf = new Configuration(false);
    GpuDiscoverer discoverer = new GpuDiscoverer();
    discoverer.initialize(conf);
    assertEquals(DEFAULT_BINARY_NAME, discoverer.getPathOfGpuBinary());
    assertNvidiaIsOnPath(discoverer);

    // test case 2, check mandatory set path.
    File fakeBinary = setupFakeBinary(conf);
    discoverer = new GpuDiscoverer();
    discoverer.initialize(conf);
    assertEquals(fakeBinary.getAbsolutePath(),
        discoverer.getPathOfGpuBinary());
    assertNull(discoverer.getEnvironmentToRunCommand().get(PATH));

    // test case 3, check mandatory set path,
    // but binary doesn't exist so default path will be used.
    fakeBinary.delete();
    discoverer = new GpuDiscoverer();
    discoverer.initialize(conf);
    assertEquals(DEFAULT_BINARY_NAME,
        discoverer.getPathOfGpuBinary());
    assertNvidiaIsOnPath(discoverer);
  }

  @Test
  public void testGetGpuDeviceInformationValidNvidiaSmiScript()
      throws YarnException, IOException {
    Configuration conf = new Configuration(false);

    File fakeBinary = createFakeNvidiaSmiScriptAsRunnableFile(
        this::createNvidiaSmiScript);

    GpuDiscoverer discoverer = creatediscovererWithGpuPathDefined(conf);
    assertEquals(fakeBinary.getAbsolutePath(),
        discoverer.getPathOfGpuBinary());
    assertNull(discoverer.getEnvironmentToRunCommand().get(PATH));

    GpuDeviceInformation result =
        discoverer.getGpuDeviceInformation();
    assertNotNull(result);
  }

  @Test
  public void testGetGpuDeviceInformationFakeNvidiaSmiScriptConsecutiveRun()
      throws YarnException, IOException {
    Configuration conf = new Configuration(false);

    File fakeBinary = createFakeNvidiaSmiScriptAsRunnableFile(
        this::createNvidiaSmiScript);

    GpuDiscoverer discoverer = creatediscovererWithGpuPathDefined(conf);
    assertEquals(fakeBinary.getAbsolutePath(),
        discoverer.getPathOfGpuBinary());
    assertNull(discoverer.getEnvironmentToRunCommand().get(PATH));

    for (int i = 0; i < 5; i++) {
      GpuDeviceInformation result = discoverer.getGpuDeviceInformation();
      assertNotNull(result);
    }
  }

  @Test
  public void testGetGpuDeviceInformationFaultyNvidiaSmiScript()
      throws YarnException, IOException {
    Configuration conf = new Configuration(false);

    File fakeBinary = createFakeNvidiaSmiScriptAsRunnableFile(
        this::createFaultyNvidiaSmiScript);

    GpuDiscoverer discoverer = creatediscovererWithGpuPathDefined(conf);
    assertEquals(fakeBinary.getAbsolutePath(),
        discoverer.getPathOfGpuBinary());
    assertNull(discoverer.getEnvironmentToRunCommand().get(PATH));

    exception.expect(YarnException.class);
    exception.expectMessage("Failed to execute GPU device detection script");
    discoverer.getGpuDeviceInformation();
  }

  @Test
  public void testGetGpuDeviceInformationFaultyNvidiaSmiScriptConsecutiveRun()
      throws YarnException, IOException {
    Configuration conf = new Configuration(false);

    File fakeBinary = createFakeNvidiaSmiScriptAsRunnableFile(
        this::createNvidiaSmiScript);

    GpuDiscoverer discoverer = creatediscovererWithGpuPathDefined(conf);
    assertEquals(fakeBinary.getAbsolutePath(),
        discoverer.getPathOfGpuBinary());
    assertNull(discoverer.getEnvironmentToRunCommand().get(PATH));

    LOG.debug("Querying nvidia-smi correctly, once...");
    discoverer.getGpuDeviceInformation();

    LOG.debug("Replacing script with faulty version!");
    createFaultyNvidiaSmiScript(fakeBinary);

    final String terminateMsg = "Failed to execute GPU device " +
        "detection script (" + fakeBinary.getAbsolutePath() + ") for 10 times";
    final String msg = "Failed to execute GPU device detection script";

    for (int i = 0; i < 10; i++) {
      try {
        LOG.debug("Executing faulty nvidia-smi script...");
        discoverer.getGpuDeviceInformation();
        fail("Query of GPU device info via nvidia-smi should fail as " +
            "script should be faulty: " + fakeBinary);
      } catch (YarnException e) {
        assertThat(e.getMessage(), containsString(msg));
        assertThat(e.getMessage(), not(containsString(terminateMsg)));
      }
    }

    try {
      LOG.debug("Executing faulty nvidia-smi script again..." +
          "We should reach the error threshold now!");
      discoverer.getGpuDeviceInformation();
      fail("Query of GPU device info via nvidia-smi should fail as " +
          "script should be faulty: " + fakeBinary);
    } catch (YarnException e) {
      assertThat(e.getMessage(), containsString(terminateMsg));
    }

    LOG.debug("Verifying if GPUs are still hold the value of " +
        "first successful query");
    assertNotNull(discoverer.getGpusUsableByYarn());
  }

  @Test
  public void testGetGpuDeviceInformationNvidiaSmiScriptWithInvalidXml()
      throws YarnException, IOException {
    Configuration conf = new Configuration(false);

    File fakeBinary = createFakeNvidiaSmiScriptAsRunnableFile(
        this::createNvidiaSmiScriptWithInvalidXml);

    GpuDiscoverer discoverer = creatediscovererWithGpuPathDefined(conf);
    assertEquals(fakeBinary.getAbsolutePath(),
        discoverer.getPathOfGpuBinary());
    assertNull(discoverer.getEnvironmentToRunCommand().get(PATH));

    exception.expect(YarnException.class);
    exception.expectMessage("Failed to parse XML output of " +
        "GPU device detection script");
    discoverer.getGpuDeviceInformation();
  }

  @Test
  public void testGpuDiscover() throws YarnException {
    // Since this is more of a performance unit test, only run if
    // RunUserLimitThroughput is set (-DRunUserLimitThroughput=true)
    Assume.assumeTrue(
        Boolean.valueOf(System.getProperty("runGpuDiscoverUnitTest")));
    Configuration conf = new Configuration(false);
    GpuDiscoverer discoverer = new GpuDiscoverer();
    discoverer.initialize(conf);
    GpuDeviceInformation info = discoverer.getGpuDeviceInformation();

    assertTrue(info.getGpus().size() > 0);
    assertEquals(discoverer.getGpusUsableByYarn().size(),
        info.getGpus().size());
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfigSingleDevice()
      throws YarnException {
    Configuration conf = createConfigWithAllowedDevices("1:2");

    GpuDiscoverer discoverer = new GpuDiscoverer();
    discoverer.initialize(conf);
    List<GpuDevice> usableGpuDevices = discoverer.getGpusUsableByYarn();
    assertEquals(1, usableGpuDevices.size());

    assertEquals(1, usableGpuDevices.get(0).getIndex());
    assertEquals(2, usableGpuDevices.get(0).getMinorNumber());
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfigIllegalFormat()
      throws YarnException {
    Configuration conf = createConfigWithAllowedDevices("0:0,1:1,2:2,3");

    exception.expect(GpuDeviceSpecificationException.class);
    GpuDiscoverer discoverer = new GpuDiscoverer();
    discoverer.initialize(conf);
    discoverer.getGpusUsableByYarn();
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfig() throws YarnException {
    Configuration conf = createConfigWithAllowedDevices("0:0,1:1,2:2,3:4");
    GpuDiscoverer discoverer = new GpuDiscoverer();
    discoverer.initialize(conf);

    List<GpuDevice> usableGpuDevices = discoverer.getGpusUsableByYarn();
    assertEquals(4, usableGpuDevices.size());

    assertEquals(0, usableGpuDevices.get(0).getIndex());
    assertEquals(0, usableGpuDevices.get(0).getMinorNumber());

    assertEquals(1, usableGpuDevices.get(1).getIndex());
    assertEquals(1, usableGpuDevices.get(1).getMinorNumber());

    assertEquals(2, usableGpuDevices.get(2).getIndex());
    assertEquals(2, usableGpuDevices.get(2).getMinorNumber());

    assertEquals(3, usableGpuDevices.get(3).getIndex());
    assertEquals(4, usableGpuDevices.get(3).getMinorNumber());
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfigDuplicateValues()
      throws YarnException {
    Configuration conf = createConfigWithAllowedDevices("0:0,1:1,2:2,1:1");

    exception.expect(GpuDeviceSpecificationException.class);
    GpuDiscoverer discoverer = new GpuDiscoverer();
    discoverer.initialize(conf);
    discoverer.getGpusUsableByYarn();
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfigDuplicateValues2()
      throws YarnException {
    Configuration conf = createConfigWithAllowedDevices("0:0,1:1,2:2,1:1,2:2");

    exception.expect(GpuDeviceSpecificationException.class);
    GpuDiscoverer discoverer = new GpuDiscoverer();
    discoverer.initialize(conf);
    discoverer.getGpusUsableByYarn();
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfigIncludingSpaces()
      throws YarnException {
    Configuration conf = createConfigWithAllowedDevices("0 : 0,1 : 1");

    exception.expect(GpuDeviceSpecificationException.class);
    GpuDiscoverer discoverer = new GpuDiscoverer();
    discoverer.initialize(conf);
    discoverer.getGpusUsableByYarn();
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfigIncludingGibberish()
      throws YarnException {
    Configuration conf = createConfigWithAllowedDevices("0:@$1,1:1");

    exception.expect(GpuDeviceSpecificationException.class);
    GpuDiscoverer discoverer = new GpuDiscoverer();
    discoverer.initialize(conf);
    discoverer.getGpusUsableByYarn();
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfigIncludingLetters()
      throws YarnException {
    Configuration conf = createConfigWithAllowedDevices("x:0, 1:y");

    exception.expect(GpuDeviceSpecificationException.class);
    GpuDiscoverer discoverer = new GpuDiscoverer();
    discoverer.initialize(conf);
    discoverer.getGpusUsableByYarn();
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfigWithoutIndexNumber()
      throws YarnException {
    Configuration conf = createConfigWithAllowedDevices(":0, :1");

    exception.expect(GpuDeviceSpecificationException.class);
    GpuDiscoverer discoverer = new GpuDiscoverer();
    discoverer.initialize(conf);
    discoverer.getGpusUsableByYarn();
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfigEmptyString()
      throws YarnException {
    Configuration conf = createConfigWithAllowedDevices("");

    exception.expect(GpuDeviceSpecificationException.class);
    GpuDiscoverer discoverer = new GpuDiscoverer();
    discoverer.initialize(conf);
    discoverer.getGpusUsableByYarn();
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfigValueWithoutComma()
      throws YarnException {
    Configuration conf = createConfigWithAllowedDevices("0:0 0:1");

    exception.expect(GpuDeviceSpecificationException.class);
    GpuDiscoverer discoverer = new GpuDiscoverer();
    discoverer.initialize(conf);
    discoverer.getGpusUsableByYarn();
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfigValueWithoutComma2()
      throws YarnException {
    Configuration conf = createConfigWithAllowedDevices("0.1 0.2");

    exception.expect(GpuDeviceSpecificationException.class);
    GpuDiscoverer discoverer = new GpuDiscoverer();
    discoverer.initialize(conf);
    discoverer.getGpusUsableByYarn();
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfigValueWithoutColonSeparator()
      throws YarnException {
    Configuration conf = createConfigWithAllowedDevices("0.1,0.2");

    exception.expect(GpuDeviceSpecificationException.class);
    GpuDiscoverer discoverer = new GpuDiscoverer();
    discoverer.initialize(conf);
    discoverer.getGpusUsableByYarn();
  }

  @Test
  public void testGpuBinaryIsANotExistingFile() {
    Configuration conf = new Configuration(false);
    conf.set(YarnConfiguration.NM_GPU_PATH_TO_EXEC, "/blabla");
    GpuDiscoverer plugin = new GpuDiscoverer();
    try {
      plugin.initialize(conf);
      plugin.getGpusUsableByYarn();
      fail("Illegal format, should fail.");
    } catch (YarnException e) {
      String message = e.getMessage();
      assertTrue(message.startsWith("Failed to find GPU discovery " +
          "executable, please double check"));
      assertTrue(message.contains("Also tried to find the " +
          "executable in the default directories:"));
    }
  }

  @Test
  public void testScriptNotCalled() throws YarnException {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_GPU_ALLOWED_DEVICES, "0:1,2:3");

    GpuDiscoverer gpuSpy = spy(new GpuDiscoverer());

    gpuSpy.initialize(conf);
    gpuSpy.getGpusUsableByYarn();

    verify(gpuSpy, never()).getGpuDeviceInformation();
  }
}

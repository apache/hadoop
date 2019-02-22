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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.GpuDeviceInformation;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestGpuDiscoverer {
  @Rule
  public ExpectedException exception = ExpectedException.none();

  private String getTestParentFolder() {
    File f = new File("target/temp/" + TestGpuDiscoverer.class.getName());
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
  }

  private Configuration createConfigWithAllowedDevices(String s) {
    Configuration conf = new Configuration(false);
    conf.set(YarnConfiguration.NM_GPU_ALLOWED_DEVICES, s);
    return conf;
  }

  @Test
  public void testLinuxGpuResourceDiscoverPluginConfig() throws Exception {
    // Only run this on demand.
    Assume.assumeTrue(Boolean.valueOf(
        System.getProperty("RunLinuxGpuResourceDiscoverPluginConfigTest")));

    // test case 1, check default setting.
    Configuration conf = new Configuration(false);
    GpuDiscoverer plugin = new GpuDiscoverer();
    plugin.initialize(conf);
    assertEquals(GpuDiscoverer.DEFAULT_BINARY_NAME,
        plugin.getPathOfGpuBinary());
    assertNotNull(plugin.getEnvironmentToRunCommand().get("PATH"));
    assertTrue(
        plugin.getEnvironmentToRunCommand().get("PATH").contains("nvidia"));

    // test case 2, check mandatory set path.
    File fakeBinary = new File(getTestParentFolder(),
        GpuDiscoverer.DEFAULT_BINARY_NAME);
    touchFile(fakeBinary);
    conf.set(YarnConfiguration.NM_GPU_PATH_TO_EXEC, getTestParentFolder());
    plugin = new GpuDiscoverer();
    plugin.initialize(conf);
    assertEquals(fakeBinary.getAbsolutePath(),
        plugin.getPathOfGpuBinary());
    assertNull(plugin.getEnvironmentToRunCommand().get("PATH"));

    // test case 3, check mandatory set path, but binary doesn't exist so default
    // path will be used.
    fakeBinary.delete();
    plugin = new GpuDiscoverer();
    plugin.initialize(conf);
    assertEquals(GpuDiscoverer.DEFAULT_BINARY_NAME,
        plugin.getPathOfGpuBinary());
    assertTrue(
        plugin.getEnvironmentToRunCommand().get("PATH").contains("nvidia"));
  }

  @Test
  public void testGpuDiscover() throws YarnException {
    // Since this is more of a performance unit test, only run if
    // RunUserLimitThroughput is set (-DRunUserLimitThroughput=true)
    Assume.assumeTrue(
        Boolean.valueOf(System.getProperty("runGpuDiscoverUnitTest")));
    Configuration conf = new Configuration(false);
    GpuDiscoverer plugin = new GpuDiscoverer();
    plugin.initialize(conf);
    GpuDeviceInformation info = plugin.getGpuDeviceInformation();

    assertTrue(info.getGpus().size() > 0);
    assertEquals(plugin.getGpusUsableByYarn().size(),
        info.getGpus().size());
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfigSingleDevice()
      throws YarnException {
    Configuration conf = createConfigWithAllowedDevices("1:2");

    GpuDiscoverer plugin = new GpuDiscoverer();
    plugin.initialize(conf);
    List<GpuDevice> usableGpuDevices = plugin.getGpusUsableByYarn();
    assertEquals(1, usableGpuDevices.size());

    assertEquals(1, usableGpuDevices.get(0).getIndex());
    assertEquals(2, usableGpuDevices.get(0).getMinorNumber());
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfigIllegalFormat()
      throws YarnException {
    Configuration conf = createConfigWithAllowedDevices("0:0,1:1,2:2,3");

    exception.expect(GpuDeviceSpecificationException.class);
    GpuDiscoverer plugin = new GpuDiscoverer();
    plugin.initialize(conf);
    plugin.getGpusUsableByYarn();
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfig() throws YarnException {
    Configuration conf = createConfigWithAllowedDevices("0:0,1:1,2:2,3:4");
    GpuDiscoverer plugin = new GpuDiscoverer();
    plugin.initialize(conf);

    List<GpuDevice> usableGpuDevices = plugin.getGpusUsableByYarn();
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
    GpuDiscoverer plugin = new GpuDiscoverer();
    plugin.initialize(conf);
    plugin.getGpusUsableByYarn();
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfigDuplicateValues2()
      throws YarnException {
    Configuration conf = createConfigWithAllowedDevices("0:0,1:1,2:2,1:1,2:2");

    exception.expect(GpuDeviceSpecificationException.class);
    GpuDiscoverer plugin = new GpuDiscoverer();
    plugin.initialize(conf);
    plugin.getGpusUsableByYarn();
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfigIncludingSpaces()
      throws YarnException {
    Configuration conf = createConfigWithAllowedDevices("0 : 0,1 : 1");

    exception.expect(GpuDeviceSpecificationException.class);
    GpuDiscoverer plugin = new GpuDiscoverer();
    plugin.initialize(conf);
    plugin.getGpusUsableByYarn();
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfigIncludingGibberish()
      throws YarnException {
    Configuration conf = createConfigWithAllowedDevices("0:@$1,1:1");

    exception.expect(GpuDeviceSpecificationException.class);
    GpuDiscoverer plugin = new GpuDiscoverer();
    plugin.initialize(conf);
    plugin.getGpusUsableByYarn();
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfigIncludingLetters()
      throws YarnException {
    Configuration conf = createConfigWithAllowedDevices("x:0, 1:y");

    exception.expect(GpuDeviceSpecificationException.class);
    GpuDiscoverer plugin = new GpuDiscoverer();
    plugin.initialize(conf);
    plugin.getGpusUsableByYarn();
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfigWithoutIndexNumber()
      throws YarnException {
    Configuration conf = createConfigWithAllowedDevices(":0, :1");

    exception.expect(GpuDeviceSpecificationException.class);
    GpuDiscoverer plugin = new GpuDiscoverer();
    plugin.initialize(conf);
    plugin.getGpusUsableByYarn();
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfigEmptyString()
      throws YarnException {
    Configuration conf = createConfigWithAllowedDevices("");

    exception.expect(GpuDeviceSpecificationException.class);
    GpuDiscoverer plugin = new GpuDiscoverer();
    plugin.initialize(conf);
    plugin.getGpusUsableByYarn();
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfigValueWithoutComma()
      throws YarnException {
    Configuration conf = createConfigWithAllowedDevices("0:0 0:1");

    exception.expect(GpuDeviceSpecificationException.class);
    GpuDiscoverer plugin = new GpuDiscoverer();
    plugin.initialize(conf);
    plugin.getGpusUsableByYarn();
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfigValueWithoutComma2()
      throws YarnException {
    Configuration conf = createConfigWithAllowedDevices("0.1 0.2");

    exception.expect(GpuDeviceSpecificationException.class);
    GpuDiscoverer plugin = new GpuDiscoverer();
    plugin.initialize(conf);
    plugin.getGpusUsableByYarn();
  }

  @Test
  public void testGetNumberOfUsableGpusFromConfigValueWithoutColonSeparator()
      throws YarnException {
    Configuration conf = createConfigWithAllowedDevices("0.1,0.2");

    exception.expect(GpuDeviceSpecificationException.class);
    GpuDiscoverer plugin = new GpuDiscoverer();
    plugin.initialize(conf);
    plugin.getGpusUsableByYarn();
  }
}

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
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

public class TestGpuDiscoverer {
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

  @Test
  public void testLinuxGpuResourceDiscoverPluginConfig() throws Exception {
    // Only run this on demand.
    Assume.assumeTrue(Boolean.valueOf(
        System.getProperty("RunLinuxGpuResourceDiscoverPluginConfigTest")));

    // test case 1, check default setting.
    Configuration conf = new Configuration(false);
    GpuDiscoverer plugin = new GpuDiscoverer();
    plugin.initialize(conf);
    Assert.assertEquals(GpuDiscoverer.DEFAULT_BINARY_NAME,
        plugin.getPathOfGpuBinary());
    Assert.assertNotNull(plugin.getEnvironmentToRunCommand().get("PATH"));
    Assert.assertTrue(
        plugin.getEnvironmentToRunCommand().get("PATH").contains("nvidia"));

    // test case 2, check mandatory set path.
    File fakeBinary = new File(getTestParentFolder(),
        GpuDiscoverer.DEFAULT_BINARY_NAME);
    touchFile(fakeBinary);
    conf.set(YarnConfiguration.NM_GPU_PATH_TO_EXEC, getTestParentFolder());
    plugin = new GpuDiscoverer();
    plugin.initialize(conf);
    Assert.assertEquals(fakeBinary.getAbsolutePath(),
        plugin.getPathOfGpuBinary());
    Assert.assertNull(plugin.getEnvironmentToRunCommand().get("PATH"));

    // test case 3, check mandatory set path, but binary doesn't exist so default
    // path will be used.
    fakeBinary.delete();
    plugin = new GpuDiscoverer();
    plugin.initialize(conf);
    Assert.assertEquals(GpuDiscoverer.DEFAULT_BINARY_NAME,
        plugin.getPathOfGpuBinary());
    Assert.assertTrue(
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

    Assert.assertTrue(info.getGpus().size() > 0);
    Assert.assertEquals(plugin.getMinorNumbersOfGpusUsableByYarn().size(),
        info.getGpus().size());
  }

  @Test
  public void getNumberOfUsableGpusFromConfig() throws YarnException {
    Configuration conf = new Configuration(false);
    conf.set(YarnConfiguration.NM_GPU_ALLOWED_DEVICES, "0,1,2,4");
    GpuDiscoverer plugin = new GpuDiscoverer();
    plugin.initialize(conf);

    List<Integer> minorNumbers = plugin.getMinorNumbersOfGpusUsableByYarn();
    Assert.assertEquals(4, minorNumbers.size());

    Assert.assertTrue(0 == minorNumbers.get(0));
    Assert.assertTrue(1 == minorNumbers.get(1));
    Assert.assertTrue(2 == minorNumbers.get(2));
    Assert.assertTrue(4 == minorNumbers.get(3));
  }
}

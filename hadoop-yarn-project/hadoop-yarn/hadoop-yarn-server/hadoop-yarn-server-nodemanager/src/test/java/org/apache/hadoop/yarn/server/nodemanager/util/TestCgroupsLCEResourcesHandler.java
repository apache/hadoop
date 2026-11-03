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
package org.apache.hadoop.yarn.server.nodemanager.util;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.TestCGroupsHandlerImpl;

import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.*;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@Deprecated
public class TestCgroupsLCEResourcesHandler {
  private static File cgroupDir = null;

  @BeforeEach
  public void setUp() throws Exception {
    cgroupDir =
        new File(System.getProperty("test.build.data",
            System.getProperty("java.io.tmpdir", "target")), this.getClass()
            .getName());
    FileUtils.deleteQuietly(cgroupDir);
  }

  @AfterEach
  public void tearDown() throws Exception {
    FileUtils.deleteQuietly(cgroupDir);
  }

  @Test
  public void testcheckAndDeleteCgroup() throws Exception {
    CgroupsLCEResourcesHandler handler = new CgroupsLCEResourcesHandler();
    handler.setConf(new YarnConfiguration());
    handler.initConfig();

    FileUtils.deleteQuietly(cgroupDir);
    // Test 0
    // tasks file not present, should return false
    assertFalse(handler.checkAndDeleteCgroup(cgroupDir));

    File tfile = new File(cgroupDir.getAbsolutePath(), "tasks");
    FileOutputStream fos = FileUtils.openOutputStream(tfile);
    File fspy = spy(cgroupDir);

    // Test 1, tasks file is empty
    // tasks file has no data, should return true
    when(fspy.delete()).thenReturn(true);
    assertTrue(handler.checkAndDeleteCgroup(fspy));

    // Test 2, tasks file has data
    fos.write("1234".getBytes());
    fos.close();
    // tasks has data, would not be able to delete, should return false
    assertFalse(handler.checkAndDeleteCgroup(fspy));
    FileUtils.deleteQuietly(cgroupDir);

  }

  // Verify DeleteCgroup times out if "tasks" file contains data
  @Test
  public void testDeleteCgroup() throws Exception {
    final ControlledClock clock = new ControlledClock();
    CgroupsLCEResourcesHandler handler = new CgroupsLCEResourcesHandler();
    handler.setConf(new YarnConfiguration());
    handler.initConfig();
    handler.clock = clock;

    FileUtils.deleteQuietly(cgroupDir);

    // Create a non-empty tasks file
    File tfile = new File(cgroupDir.getAbsolutePath(), "tasks");
    FileOutputStream fos = FileUtils.openOutputStream(tfile);
    fos.write("1234".getBytes());
    fos.close();

    final CountDownLatch latch = new CountDownLatch(1);
    new Thread() {
      @Override
      public void run() {
        latch.countDown();
        try {
          Thread.sleep(200);
        } catch (InterruptedException ex) {
          //NOP
        }
        clock.tickMsec(YarnConfiguration.
            DEFAULT_NM_LINUX_CONTAINER_CGROUPS_DELETE_TIMEOUT);
      }
    }.start();
    latch.await();
    assertFalse(handler.deleteCgroup(cgroupDir.getAbsolutePath()));
    FileUtils.deleteQuietly(cgroupDir);
  }

  @Deprecated
  static class MockLinuxContainerExecutor extends LinuxContainerExecutor {
    @Override
    public void mountCgroups(List<String> x, String y) {
    }
  }

  @Deprecated
  static class CustomCgroupsLCEResourceHandler extends
      CgroupsLCEResourcesHandler {

    private String mtabFile;
    private int[] limits = new int[2];
    private boolean generateLimitsMode = false;

    @Override
    int[] getOverallLimits(float x) {
      if (generateLimitsMode) {
        return super.getOverallLimits(x);
      }
      return limits;
    }

    void setMtabFile(String file) {
      mtabFile = file;
    }

    @Override
    String getMtabFileName() {
      return mtabFile;
    }
  }

  private static File createMockCgroupMount(File parentDir,
                                            String type)
      throws IOException {
    File cgroupMountDir =
        new File(parentDir.getAbsolutePath(), type + "/hadoop-yarn");
    FileUtils.deleteQuietly(cgroupMountDir);
    if (!cgroupMountDir.mkdirs()) {
      String message =
          "Could not create dir " + cgroupMountDir.getAbsolutePath();
      throw new IOException(message);
    }
    return cgroupMountDir;
  }

  @Test
  public void testInit() throws IOException {
    LinuxContainerExecutor mockLCE = new MockLinuxContainerExecutor();
    CustomCgroupsLCEResourceHandler handler =
        new CustomCgroupsLCEResourceHandler();
    YarnConfiguration conf = new YarnConfiguration();
    final int numProcessors = 4;
    ResourceCalculatorPlugin plugin =
        mock(ResourceCalculatorPlugin.class);
    doReturn(numProcessors).when(plugin).getNumProcessors();
    doReturn(numProcessors).when(plugin).getNumCores();
    handler.setConf(conf);
    handler.initConfig();

    // create mock mtab
    File mockMtab =
        TestCGroupsHandlerImpl.createPremountedCgroups(cgroupDir, false);

    // create mock cgroup
    File cpuCgroupMountDir = createMockCgroupMount(
        cgroupDir, "cpu");

    // setup our handler and call init()
    handler.setMtabFile(mockMtab.getAbsolutePath());

    // check values
    // in this case, we're using all cpu so the files
    // shouldn't exist(because init won't create them
    handler.init(mockLCE, plugin);
    File periodFile = new File(cpuCgroupMountDir, "cpu.cfs_period_us");
    File quotaFile = new File(cpuCgroupMountDir, "cpu.cfs_quota_us");
    assertFalse(periodFile.exists());
    assertFalse(quotaFile.exists());

    // subset of cpu being used, files should be created
    conf
      .setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT, 75);
    handler.limits[0] = 100 * 1000;
    handler.limits[1] = 1000 * 1000;
    handler.init(mockLCE, plugin);
    int period = readIntFromFile(periodFile);
    int quota = readIntFromFile(quotaFile);
    assertEquals(100 * 1000, period);
    assertEquals(1000 * 1000, quota);

    // set cpu back to 100, quota should be -1
    conf.setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT,
        100);
    handler.limits[0] = 100 * 1000;
    handler.limits[1] = 1000 * 1000;
    handler.init(mockLCE, plugin);
    quota = readIntFromFile(quotaFile);
    assertEquals(-1, quota);

    FileUtils.deleteQuietly(cgroupDir);
  }

  private int readIntFromFile(File targetFile) throws IOException {
    Scanner scanner = new Scanner(targetFile);
    try {
      return scanner.hasNextInt() ? scanner.nextInt() : -1;
    } finally {
      scanner.close();
    }
  }

  @Test
  public void testGetOverallLimits() {

    int expectedQuota = 1000 * 1000;
    CgroupsLCEResourcesHandler handler = new CgroupsLCEResourcesHandler();

    int[] ret = handler.getOverallLimits(2);
    assertEquals(expectedQuota / 2, ret[0]);
    assertEquals(expectedQuota, ret[1]);

    ret = handler.getOverallLimits(2000);
    assertEquals(expectedQuota, ret[0]);
    assertEquals(-1, ret[1]);

    int[] params = {0, -1};
    for (int cores : params) {
      try {
        handler.getOverallLimits(cores);
        fail("Function call should throw error.");
      } catch (IllegalArgumentException ie) {
        // expected
      }
    }

    // test minimums
    ret = handler.getOverallLimits(1000 * 1000);
    assertEquals(1000 * 1000, ret[0]);
    assertEquals(-1, ret[1]);
  }

  @Test
  public void testContainerLimits() throws IOException {
    LinuxContainerExecutor mockLCE = new MockLinuxContainerExecutor();
    CustomCgroupsLCEResourceHandler handler =
        new CustomCgroupsLCEResourceHandler();
    handler.generateLimitsMode = true;
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_DISK_RESOURCE_ENABLED, true);
    final int numProcessors = 4;
    ResourceCalculatorPlugin plugin =
        mock(ResourceCalculatorPlugin.class);
    doReturn(numProcessors).when(plugin).getNumProcessors();
    doReturn(numProcessors).when(plugin).getNumCores();
    handler.setConf(conf);
    handler.initConfig();

    // create mock mtab
    File mockMtab =
        TestCGroupsHandlerImpl.createPremountedCgroups(cgroupDir, false);

    // create mock cgroup
    File cpuCgroupMountDir = createMockCgroupMount(
        cgroupDir, "cpu");

    // setup our handler and call init()
    handler.setMtabFile(mockMtab.getAbsolutePath());
    handler.init(mockLCE, plugin);

    // check the controller paths map isn't empty
    ContainerId id = ContainerId.fromString("container_1_1_1_1");
    handler.preExecute(id, Resource.newInstance(1024, 1));
    assertNotNull(handler.getControllerPaths());
    // check values
    // default case - files shouldn't exist, strict mode off by default
    File containerCpuDir = new File(cpuCgroupMountDir, id.toString());
    assertTrue(containerCpuDir.exists());
    assertTrue(containerCpuDir.isDirectory());
    File periodFile = new File(containerCpuDir, "cpu.cfs_period_us");
    File quotaFile = new File(containerCpuDir, "cpu.cfs_quota_us");
    assertFalse(periodFile.exists());
    assertFalse(quotaFile.exists());

    // no files created because we're using all cpu
    FileUtils.deleteQuietly(containerCpuDir);
    conf.setBoolean(
        YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE,
        true);
    handler.initConfig();
    handler.preExecute(id,
        Resource.newInstance(1024, YarnConfiguration.DEFAULT_NM_VCORES));
    assertTrue(containerCpuDir.exists());
    assertTrue(containerCpuDir.isDirectory());
    periodFile = new File(containerCpuDir, "cpu.cfs_period_us");
    quotaFile = new File(containerCpuDir, "cpu.cfs_quota_us");
    assertFalse(periodFile.exists());
    assertFalse(quotaFile.exists());

    // 50% of CPU
    FileUtils.deleteQuietly(containerCpuDir);
    conf.setBoolean(
        YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE,
        true);
    handler.initConfig();
    handler.preExecute(id,
        Resource.newInstance(1024, YarnConfiguration.DEFAULT_NM_VCORES / 2));
    assertTrue(containerCpuDir.exists());
    assertTrue(containerCpuDir.isDirectory());
    periodFile = new File(containerCpuDir, "cpu.cfs_period_us");
    quotaFile = new File(containerCpuDir, "cpu.cfs_quota_us");
    assertTrue(periodFile.exists());
    assertTrue(quotaFile.exists());
    assertEquals(500 * 1000, readIntFromFile(periodFile));
    assertEquals(1000 * 1000, readIntFromFile(quotaFile));

    // CGroups set to 50% of CPU, container set to 50% of YARN CPU
    FileUtils.deleteQuietly(containerCpuDir);
    conf.setBoolean(
        YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE,
        true);
    conf
      .setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT, 50);
    handler.initConfig();
    handler.init(mockLCE, plugin);
    handler.preExecute(id,
        Resource.newInstance(1024, YarnConfiguration.DEFAULT_NM_VCORES / 2));
    assertTrue(containerCpuDir.exists());
    assertTrue(containerCpuDir.isDirectory());
    periodFile = new File(containerCpuDir, "cpu.cfs_period_us");
    quotaFile = new File(containerCpuDir, "cpu.cfs_quota_us");
    assertTrue(periodFile.exists());
    assertTrue(quotaFile.exists());
    assertEquals(1000 * 1000, readIntFromFile(periodFile));
    assertEquals(1000 * 1000, readIntFromFile(quotaFile));

    FileUtils.deleteQuietly(cgroupDir);
  }

  @Test
  public void testSelectCgroup() {
    File cpu = new File(cgroupDir, "cpu");
    File cpuNoExist = new File(cgroupDir, "cpuNoExist");
    File memory = new File(cgroupDir, "memory");
    try {
      CgroupsLCEResourcesHandler handler = new CgroupsLCEResourcesHandler();
      Map<String, Set<String>> cgroups = new LinkedHashMap<>();

      assertTrue(cpu.mkdirs(), "temp dir should be created");
      assertTrue(memory.mkdirs(), "temp dir should be created");
      assertFalse(cpuNoExist.exists(), "temp dir should not be created");

      cgroups.put(
          memory.getAbsolutePath(), Collections.singleton("memory"));
      cgroups.put(
          cpuNoExist.getAbsolutePath(), Collections.singleton("cpu"));
      cgroups.put(cpu.getAbsolutePath(), Collections.singleton("cpu"));
      String selectedCPU = handler.findControllerInMtab("cpu", cgroups);
      assertEquals(cpu.getAbsolutePath(), selectedCPU,
          "Wrong CPU mount point selected");
    } finally {
      FileUtils.deleteQuietly(cpu);
      FileUtils.deleteQuietly(memory);
    }
  }

  @Test
  public void testManualCgroupSetting() throws IOException {
    CgroupsLCEResourcesHandler handler = new CgroupsLCEResourcesHandler();
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH,
        cgroupDir.getAbsolutePath());
    handler.setConf(conf);
    File cpu = new File(new File(cgroupDir, "cpuacct,cpu"), "/hadoop-yarn");

    try {
      assertTrue(cpu.mkdirs(), "temp dir should be created");

      final int numProcessors = 4;
      ResourceCalculatorPlugin plugin =
              mock(ResourceCalculatorPlugin.class);
      doReturn(numProcessors).when(plugin).getNumProcessors();
      doReturn(numProcessors).when(plugin).getNumCores();
      when(plugin.getNumProcessors()).thenReturn(8);
      handler.init(null, plugin);

      assertEquals(cpu.getParent(), handler.getControllerPaths().get("cpu"),
          "CPU CGRoup path was not set");

    } finally {
      FileUtils.deleteQuietly(cpu);
    }
  }

}

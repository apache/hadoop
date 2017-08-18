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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;


import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ProcfsBasedProcessTree;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;
import org.junit.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Random;

/**
 * Unit test for CGroupsResourceCalculator.
 */
public class TestCompareResourceCalculators {
  private Process target = null;
  String cgroupCPU = null;
  String cgroupMemory = null;

  @Before
  public void setup() throws IOException, YarnException {
    Assume.assumeTrue(SystemUtils.IS_OS_LINUX);

    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT, false);
    conf.setStrings(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH, "/sys/fs/cgroup");
    conf.setBoolean(YarnConfiguration.NM_CPU_RESOURCE_ENABLED, true);
    ResourceHandlerChain module = null;
    try {
      module = ResourceHandlerModule.getConfiguredResourceHandlerChain(conf);
    } catch (ResourceHandlerException e) {
      throw new YarnException("Cannot access cgroups", e);
    }
    Assume.assumeNotNull(module);

    Random random = new Random();
    String cgroup = Long.toString(random.nextLong());
    cgroupCPU = ResourceHandlerModule.getCGroupsHandler()
        .getPathForCGroup(CGroupsHandler.CGroupController.CPU, cgroup);
    cgroupMemory = ResourceHandlerModule.getCGroupsHandler()
        .getPathForCGroup(CGroupsHandler.CGroupController.MEMORY, cgroup);
  }

  @After
  public void tearDown() {
    stopTestProcess();
  }


  @Test
  public void testCompareResults()
      throws YarnException, InterruptedException, IOException {

    startTestProcess();

    ProcfsBasedProcessTree legacyCalculator =
        new ProcfsBasedProcessTree(Long.toString(getPid()));
    CGroupsResourceCalculator cgroupsCalculator =
        new CGroupsResourceCalculator(Long.toString(getPid()));

    for (int i = 0; i < 10; ++i) {
      Thread.sleep(3000);
      compareMetrics(legacyCalculator, cgroupsCalculator);
    }

    stopTestProcess();
    Thread.sleep(3000);
    compareMetrics(legacyCalculator, cgroupsCalculator);

  }

  private void compareMetrics(
      ResourceCalculatorProcessTree metric1,
      ResourceCalculatorProcessTree metric2) {
    metric1.updateProcessTree();
    metric2.updateProcessTree();
    Assert.assertTrue("pmem Error outside range",
        Math.abs(
            metric1.getRssMemorySize(0) -
                metric2.getRssMemorySize(0)) < 10);
    Assert.assertTrue("pmem Error outside range",
        Math.abs(
            metric1.getRssMemorySize(0) -
                metric2.getRssMemorySize(0)) < 10);
    Assert.assertTrue("vmem Error outside range",
        Math.abs(
            metric1.getVirtualMemorySize(0) -
                metric2.getVirtualMemorySize(0)) < 1);
    Assert.assertTrue("vmem Error outside range",
        Math.abs(
            metric1.getVirtualMemorySize(0) -
                metric2.getVirtualMemorySize(0)) < 1);
    Assert.assertTrue("CPU% Error outside range",
        Math.abs(
            metric1.getCpuUsagePercent() -
                metric2.getCpuUsagePercent()) < 0.1);
    Assert.assertTrue("CPU% Error outside range",
        Math.abs(
            metric1.getCpuUsagePercent() -
                metric2.getCpuUsagePercent()) < 0.1);
  }

  private void startTestProcess() throws IOException {
    ProcessBuilder builder = new ProcessBuilder();
    String script =
        "mkdir -p " + cgroupCPU + ";" +
        "echo $$ >" + cgroupCPU + "/tasks;" +
        "mkdir -p " + cgroupMemory + ";" +
        "echo $$ >" + cgroupMemory + "/tasks;" +
        "dd if=/dev/zero of=/dev/null bs=1k;";
    builder.command("bash", "-c", script);
    target = builder.start();
  }

  private void stopTestProcess() {
    if (target != null) {
      target.destroyForcibly();
      target = null;
    }
    try {
      ProcessBuilder builder = new ProcessBuilder();
      String script =
          "rmdir " + cgroupCPU + ";" +
          "rmdir " + cgroupMemory + ";";
      builder.command("bash", "-c", script);
      target = builder.start();
    } catch (IOException e) {
    }
  }

  private long getPid() throws YarnException {
    Class processClass = target.getClass();
    if (processClass.getName().equals("java.lang.UNIXProcess")) {
      try {
        Field pidField = processClass.getDeclaredField("pid");
        pidField.setAccessible(true);
        long pid = pidField.getLong(target);
        pidField.setAccessible(false);
        return pid;
      } catch (NoSuchFieldException|IllegalAccessException e) {
        throw new YarnException("Reflection error", e);
      }
    } else {
      throw new YarnException("Not Unix " + processClass.getName());
    }
  }


}

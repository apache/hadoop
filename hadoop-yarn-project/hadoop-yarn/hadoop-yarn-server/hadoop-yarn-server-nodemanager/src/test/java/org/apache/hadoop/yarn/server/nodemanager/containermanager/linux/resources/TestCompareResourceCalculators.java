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
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.util.ProcfsBasedProcessTree;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Random;

import static org.mockito.Mockito.mock;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Functional test for CGroupsResourceCalculator to compare two resource
 * calculators. It is OS dependent.
 * Ignored in automated tests due to flakiness by design.
 */
public class TestCompareResourceCalculators {
  private Process target = null;
  private String cgroup = null;
  private String cgroupCPU = null;
  private String cgroupMemory = null;
  public static final long SHMEM_KB = 100 * 1024;

  @BeforeEach
  public void setup() throws IOException, YarnException {
    assumeTrue(SystemUtils.IS_OS_LINUX);

    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_HIERARCHY,
        "TestCompareResourceCalculators");
    conf.setBoolean(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT, false);
    conf.setStrings(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH,
        "/sys/fs/cgroup");
    conf.setBoolean(YarnConfiguration.NM_CPU_RESOURCE_ENABLED, true);
    ResourceHandlerChain module = null;
    try {
      module = ResourceHandlerModule.getConfiguredResourceHandlerChain(conf,
          mock(Context.class));
    } catch (ResourceHandlerException e) {
      throw new YarnException("Cannot access cgroups", e);
    }
    assumeTrue(module != null);
    assumeTrue(ResourceHandlerModule.getCGroupsHandler()
        .getControllerPath(CGroupsHandler.CGroupController.CPU) != null);
    assumeTrue(ResourceHandlerModule.getCGroupsHandler()
        .getControllerPath(CGroupsHandler.CGroupController.MEMORY) != null);

    Random random = new Random(System.currentTimeMillis());
    cgroup = Long.toString(random.nextLong());
    cgroupCPU = ResourceHandlerModule.getCGroupsHandler()
        .getPathForCGroup(CGroupsHandler.CGroupController.CPU, cgroup);
    cgroupMemory = ResourceHandlerModule.getCGroupsHandler()
        .getPathForCGroup(CGroupsHandler.CGroupController.MEMORY, cgroup);
  }

  @AfterEach
  public void tearDown() throws YarnException {
    stopTestProcess();
  }


  // Ignored in automated tests due to flakiness by design
  @Disabled
  @Test
  public void testCompareResults()
      throws YarnException, InterruptedException, IOException {

    startTestProcess();

    ProcfsBasedProcessTree legacyCalculator =
        new ProcfsBasedProcessTree(Long.toString(getPid()));
    CGroupsResourceCalculator cgroupsCalculator =
        new CGroupsResourceCalculator(Long.toString(getPid()));
    cgroupsCalculator.initialize();

    for (int i = 0; i < 5; ++i) {
      Thread.sleep(3000);
      compareMetrics(legacyCalculator, cgroupsCalculator);
    }

    stopTestProcess();

    ensureCleanedUp(legacyCalculator, cgroupsCalculator);
  }

  private void ensureCleanedUp(
          ResourceCalculatorProcessTree metric1,
          ResourceCalculatorProcessTree metric2) {
    metric1.updateProcessTree();
    metric2.updateProcessTree();
    long pmem1 = metric1.getRssMemorySize(0);
    long pmem2 = metric2.getRssMemorySize(0);
    System.out.println(pmem1 + " " + pmem2);
    assertTrue(pmem1 == ResourceCalculatorProcessTree.UNAVAILABLE &&
        pmem2 == ResourceCalculatorProcessTree.UNAVAILABLE,
        "pmem should be invalid " + pmem1 + " " + pmem2);
    long vmem1 = metric1.getRssMemorySize(0);
    long vmem2 = metric2.getRssMemorySize(0);
    System.out.println(vmem1 + " " + vmem2);
    assertTrue(vmem1 == ResourceCalculatorProcessTree.UNAVAILABLE &&
        vmem2 == ResourceCalculatorProcessTree.UNAVAILABLE,
        "vmem Error outside range " + vmem1 + " " + vmem2);
    float cpu1 = metric1.getCpuUsagePercent();
    float cpu2 = metric2.getCpuUsagePercent();
    // TODO ProcfsBasedProcessTree may report negative on process exit
    assertTrue(cpu1 == 0 && cpu2 == 0,
        "CPU% Error outside range " + cpu1 + " " + cpu2);
  }

  private void compareMetrics(
      ResourceCalculatorProcessTree metric1,
      ResourceCalculatorProcessTree metric2) {
    metric1.updateProcessTree();
    metric2.updateProcessTree();
    long pmem1 = metric1.getRssMemorySize(0);
    long pmem2 = metric2.getRssMemorySize(0);
    // TODO The calculation is different and cgroup
    // can report a small amount after process stop
    // This is not an issue since the cgroup is deleted
    System.out.println(pmem1 + " " + (pmem2 - SHMEM_KB * 1024));
    assertTrue(Math.abs(pmem1 - (pmem2 - SHMEM_KB * 1024)) < 5000000,
        "pmem Error outside range " + pmem1 + " " + pmem2);
    long vmem1 = metric1.getRssMemorySize(0);
    long vmem2 = metric2.getRssMemorySize(0);
    System.out.println(vmem1 + " " + (vmem2 - SHMEM_KB * 1024));
    // TODO The calculation is different and cgroup
    // can report a small amount after process stop
    // This is not an issue since the cgroup is deleted
    assertTrue(Math.abs(vmem1 - (vmem2 - SHMEM_KB * 1024)) < 5000000,
        "vmem Error outside range " + vmem1 + " " + vmem2);
    float cpu1 = metric1.getCpuUsagePercent();
    float cpu2 = metric2.getCpuUsagePercent();
    if (cpu1 > 0) {
      // TODO ProcfsBasedProcessTree may report negative on process exit
      assertTrue(Math.abs(cpu2 - cpu1) < 10,
          "CPU% Error outside range " + cpu1 + " " + cpu2);
    }
  }

  private void startTestProcess() throws IOException {
    ProcessBuilder builder = new ProcessBuilder();
    String script =
        "mkdir -p " + cgroupCPU + ";" +
        "echo $$ >" + cgroupCPU + "/tasks;" +
        "mkdir -p " + cgroupMemory + ";" +
        "echo $$ >" + cgroupMemory + "/tasks;" +
        "dd if=/dev/zero of=/dev/shm/" +
            cgroup + " bs=1k count=" + SHMEM_KB + ";" +
        "dd if=/dev/zero of=/dev/null bs=1k &" +
        "echo $! >/tmp/\" + cgroup + \".pid;" +
        //"echo while [ -f /tmp/" + cgroup + ".pid ]; do sleep 1; done;" +
        "sleep 10000;" +
        "echo kill $(jobs -p);";
    builder.command("bash", "-c", script);
    builder.redirectError(new File("/tmp/a.txt"));
    builder.redirectOutput(new File("/tmp/b.txt"));
    target = builder.start();
  }

  private void stopTestProcess() throws YarnException {
    if (target != null) {
      target.destroyForcibly();
      target = null;
    }
    try {
      ProcessBuilder builder = new ProcessBuilder();
      String script =
          "rm -f /dev/shm/" + cgroup + ";" +
          "cat " + cgroupCPU + "/tasks | xargs kill;" +
          "rm -f /tmp/" + cgroup + ".pid;" +
          "sleep 4;" +
          "rmdir " + cgroupCPU + ";" +
          "rmdir " + cgroupMemory + ";";
      builder.command("bash", "-c", script);
      Process cleanup = builder.start();
      cleanup.waitFor();
    } catch (IOException|InterruptedException e) {
      throw new YarnException("Could not clean up", e);
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

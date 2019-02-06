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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;

import static org.mockito.Mockito.*;

/**
 * Unit test for CGroupsResourceCalculator.
 */
public class TestCGroupsResourceCalculator {

  private ControlledClock clock = new ControlledClock();
  private CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
  private String basePath = "/tmp/" + this.getClass().getName();

  public TestCGroupsResourceCalculator() {
    when(cGroupsHandler.getRelativePathForCGroup("container_1"))
        .thenReturn("/yarn/container_1");
    when(cGroupsHandler.getRelativePathForCGroup("")).thenReturn("/yarn/");
  }

  @Test(expected = YarnException.class)
  public void testPidNotFound() throws Exception {
    CGroupsResourceCalculator calculator =
        new CGroupsResourceCalculator(
            "1234", ".", cGroupsHandler, clock, 10);
    calculator.setCGroupFilePaths();
    Assert.assertEquals("Expected exception", null, calculator);
  }

  @Test(expected = YarnException.class)
  public void testNoMemoryCGgroupMount() throws Exception {
    File procfs = new File(basePath + "/1234");
    Assert.assertTrue("Setup error", procfs.mkdirs());
    try {
      FileUtils.writeStringToFile(
          new File(procfs, CGroupsResourceCalculator.CGROUP),
          "7:devices:/yarn/container_1\n" +
              "6:cpuacct,cpu:/yarn/container_1\n" +
              "5:pids:/yarn/container_1\n", StandardCharsets.UTF_8);
      CGroupsResourceCalculator calculator =
          new CGroupsResourceCalculator(
              "1234", basePath,
              cGroupsHandler, clock, 10);
      calculator.setCGroupFilePaths();
      Assert.assertEquals("Expected exception", null, calculator);
    } finally {
      FileUtils.deleteDirectory(new File(basePath));
    }
  }

  @Test
  public void testCGgroupNotFound() throws Exception {
    File procfs = new File(basePath + "/1234");
    Assert.assertTrue("Setup error", procfs.mkdirs());
    try {
      FileUtils.writeStringToFile(
          new File(procfs, CGroupsResourceCalculator.CGROUP),
          "7:devices:/yarn/container_1\n" +
              "6:cpuacct,cpu:/yarn/container_1\n" +
              "5:pids:/yarn/container_1\n" +
              "4:memory:/yarn/container_1\n", StandardCharsets.UTF_8);

      CGroupsResourceCalculator calculator =
          new CGroupsResourceCalculator(
              "1234", basePath,
              cGroupsHandler, clock, 10);
      calculator.setCGroupFilePaths();
      calculator.updateProcessTree();
      Assert.assertEquals("cgroups should be missing",
          (long)ResourceCalculatorProcessTree.UNAVAILABLE,
          calculator.getRssMemorySize(0));
    } finally {
      FileUtils.deleteDirectory(new File(basePath));
    }
  }

  @Test
  public void testCPUParsing() throws Exception {
    File cgcpuacctDir =
        new File(basePath + "/cgcpuacct");
    File cgcpuacctContainerDir =
        new File(cgcpuacctDir, "/yarn/container_1");
    File procfs = new File(basePath + "/1234");
    when(cGroupsHandler.getControllerPath(
        CGroupsHandler.CGroupController.CPUACCT)).
        thenReturn(cgcpuacctDir.getAbsolutePath());
    Assert.assertTrue("Setup error", procfs.mkdirs());
    Assert.assertTrue("Setup error", cgcpuacctContainerDir.mkdirs());
    try {
      FileUtils.writeStringToFile(
          new File(procfs, CGroupsResourceCalculator.CGROUP),
          "7:devices:/yarn/container_1\n" +
              "6:cpuacct,cpu:/yarn/container_1\n" +
              "5:pids:/yarn/container_1\n" +
              "4:memory:/yarn/container_1\n", StandardCharsets.UTF_8);
      FileUtils.writeStringToFile(
          new File(cgcpuacctContainerDir, CGroupsResourceCalculator.CPU_STAT),
          "Can you handle this?\n" +
              "user 5415\n" +
              "system 3632", StandardCharsets.UTF_8);
      CGroupsResourceCalculator calculator =
          new CGroupsResourceCalculator(
              "1234", basePath,
              cGroupsHandler, clock, 10);
      calculator.setCGroupFilePaths();
      calculator.updateProcessTree();
      Assert.assertEquals("Incorrect CPU usage",
          90470,
          calculator.getCumulativeCpuTime());
    } finally {
      FileUtils.deleteDirectory(new File(basePath));
    }
  }

  @Test
  public void testMemoryParsing() throws Exception {
    File cgcpuacctDir =
        new File(basePath + "/cgcpuacct");
    File cgcpuacctContainerDir =
        new File(cgcpuacctDir, "/yarn/container_1");
    File cgmemoryDir =
        new File(basePath + "/memory");
    File cgMemoryContainerDir =
        new File(cgmemoryDir, "/yarn/container_1");
    File procfs = new File(basePath + "/1234");
    when(cGroupsHandler.getControllerPath(
        CGroupsHandler.CGroupController.MEMORY)).
        thenReturn(cgmemoryDir.getAbsolutePath());
    Assert.assertTrue("Setup error", procfs.mkdirs());
    Assert.assertTrue("Setup error", cgcpuacctContainerDir.mkdirs());
    Assert.assertTrue("Setup error", cgMemoryContainerDir.mkdirs());
    try {
      FileUtils.writeStringToFile(
          new File(procfs, CGroupsResourceCalculator.CGROUP),
              "6:cpuacct,cpu:/yarn/container_1\n" +
              "4:memory:/yarn/container_1\n", StandardCharsets.UTF_8);
      FileUtils.writeStringToFile(
          new File(cgMemoryContainerDir, CGroupsResourceCalculator.MEM_STAT),
          "418496512\n", StandardCharsets.UTF_8);

      CGroupsResourceCalculator calculator =
          new CGroupsResourceCalculator(
              "1234", basePath,
              cGroupsHandler, clock, 10);
      calculator.setCGroupFilePaths();

      calculator.updateProcessTree();
      // Test the case where memsw is not available (Ubuntu)
      Assert.assertEquals("Incorrect memory usage",
          418496512,
          calculator.getRssMemorySize());
      Assert.assertEquals("Incorrect swap usage",
          (long)ResourceCalculatorProcessTree.UNAVAILABLE,
          calculator.getVirtualMemorySize());

      // Test the case where memsw is available
      FileUtils.writeStringToFile(
          new File(cgMemoryContainerDir, CGroupsResourceCalculator.MEMSW_STAT),
          "418496513\n", StandardCharsets.UTF_8);
      calculator.updateProcessTree();
      Assert.assertEquals("Incorrect swap usage",
          418496513,
          calculator.getVirtualMemorySize());
    } finally {
      FileUtils.deleteDirectory(new File(basePath));
    }
  }

  @Test
  public void testCPUParsingRoot() throws Exception {
    File cgcpuacctDir =
        new File(basePath + "/cgcpuacct");
    File cgcpuacctRootDir =
        new File(cgcpuacctDir, "/yarn");
    when(cGroupsHandler.getControllerPath(
        CGroupsHandler.CGroupController.CPUACCT)).
        thenReturn(cgcpuacctDir.getAbsolutePath());
    Assert.assertTrue("Setup error", cgcpuacctRootDir.mkdirs());
    try {
      FileUtils.writeStringToFile(
          new File(cgcpuacctRootDir, CGroupsResourceCalculator.CPU_STAT),
              "user 5415\n" +
              "system 3632", StandardCharsets.UTF_8);
      CGroupsResourceCalculator calculator =
          new CGroupsResourceCalculator(
              null, basePath,
              cGroupsHandler, clock, 10);
      calculator.setCGroupFilePaths();
      calculator.updateProcessTree();
      Assert.assertEquals("Incorrect CPU usage",
          90470,
          calculator.getCumulativeCpuTime());
    } finally {
      FileUtils.deleteDirectory(new File(basePath));
    }
  }

  @Test
  public void testMemoryParsingRoot() throws Exception {
    File cgcpuacctDir =
        new File(basePath + "/cgcpuacct");
    File cgcpuacctRootDir =
        new File(cgcpuacctDir, "/yarn");
    File cgmemoryDir =
        new File(basePath + "/memory");
    File cgMemoryRootDir =
        new File(cgmemoryDir, "/yarn");
    File procfs = new File(basePath + "/1234");
    when(cGroupsHandler.getControllerPath(
        CGroupsHandler.CGroupController.MEMORY)).
        thenReturn(cgmemoryDir.getAbsolutePath());
    Assert.assertTrue("Setup error", procfs.mkdirs());
    Assert.assertTrue("Setup error", cgcpuacctRootDir.mkdirs());
    Assert.assertTrue("Setup error", cgMemoryRootDir.mkdirs());
    try {
      FileUtils.writeStringToFile(
          new File(cgMemoryRootDir, CGroupsResourceCalculator.MEM_STAT),
              "418496512\n", StandardCharsets.UTF_8);

      CGroupsResourceCalculator calculator =
          new CGroupsResourceCalculator(
              null, basePath,
              cGroupsHandler, clock, 10);
      calculator.setCGroupFilePaths();

      calculator.updateProcessTree();

      // Test the case where memsw is not available (Ubuntu)
      Assert.assertEquals("Incorrect memory usage",
          418496512,
          calculator.getRssMemorySize());
      Assert.assertEquals("Incorrect swap usage",
          (long)ResourceCalculatorProcessTree.UNAVAILABLE,
          calculator.getVirtualMemorySize());

      // Test the case where memsw is available
      FileUtils.writeStringToFile(
          new File(cgMemoryRootDir, CGroupsResourceCalculator.MEMSW_STAT),
          "418496513\n", StandardCharsets.UTF_8);
      calculator.updateProcessTree();
      Assert.assertEquals("Incorrect swap usage",
          418496513,
          calculator.getVirtualMemorySize());
    } finally {
      FileUtils.deleteDirectory(new File(basePath));
    }
  }
}

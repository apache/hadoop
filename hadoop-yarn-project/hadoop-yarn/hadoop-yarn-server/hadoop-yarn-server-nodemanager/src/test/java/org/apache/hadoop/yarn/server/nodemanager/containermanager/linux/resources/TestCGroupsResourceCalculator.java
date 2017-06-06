package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;

import static org.mockito.Mockito.*;

public class TestCGroupsResourceCalculator {

  ControlledClock clock = new ControlledClock();
  CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);

  public TestCGroupsResourceCalculator() {
    when(cGroupsHandler.getRelativePathForCGroup(any())).thenReturn("/yarn/container_1");
  }

  @Test
  public void testNoPid() throws Exception {
    try {
      CGroupsResourceCalculator calculator =
          new CGroupsResourceCalculator(
              "1234", ".", cGroupsHandler, clock);
    } catch (YarnException e) {
      Assert.assertTrue("Missing file should be caught",
          e.getCause() instanceof FileNotFoundException);
    }
  }

  @Test
  public void testNoMemoryCGgroupMount() throws Exception {
    File procfs = new File("/tmp/" + this.getClass().getName() + "/1234");
    Assert.assertTrue("Setup error", procfs.mkdirs());
    try {
      FileUtils.writeStringToFile(
          new File(procfs, CGroupsResourceCalculator.CGROUP),
          "7:devices:/yarn/container_1\n" +
              "6:cpuacct,cpu:/yarn/container_1\n" +
              "5:pids:/yarn/container_1\n");
      try {
        CGroupsResourceCalculator calculator =
            new CGroupsResourceCalculator(
                "1234", "/tmp/" + this.getClass().getName(),
                cGroupsHandler, clock);
      } catch (YarnException e) {
        Assert.assertTrue("Missing file should be caught",
            e.getMessage().startsWith("memory CGroup"));
      }
    } finally {
      FileUtils.deleteDirectory(new File("/tmp/" + this.getClass().getName()));
    }
  }

  @Test
  public void testNoCGgroup() throws Exception {
    File procfs = new File("/tmp/" + this.getClass().getName() + "/1234");
    Assert.assertTrue("Setup error", procfs.mkdirs());
    try {
      FileUtils.writeStringToFile(
          new File(procfs, CGroupsResourceCalculator.CGROUP),
          "7:devices:/yarn/container_1\n" +
              "6:cpuacct,cpu:/yarn/container_1\n" +
              "5:pids:/yarn/container_1\n" +
              "4:memory:/yarn/container_1\n");

      CGroupsResourceCalculator calculator =
          new CGroupsResourceCalculator(
              "1234", "/tmp/" + this.getClass().getName(),
              cGroupsHandler, clock);
      Assert.assertEquals("cgroups should be missing",
          (long)ResourceCalculatorProcessTree.UNAVAILABLE,
          calculator.getRssMemorySize(0));
    } finally {
      FileUtils.deleteDirectory(new File("/tmp/" + this.getClass().getName()));
    }
  }

  @Test
  public void testCPUParsing() throws Exception {
    File cgcpuacctDir =
        new File("/tmp/" + this.getClass().getName() + "/cgcpuacct");
    File cgcpuacctContainerDir =
        new File(cgcpuacctDir, "/yarn/container_1");
    File procfs = new File("/tmp/" + this.getClass().getName() + "/1234");
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
              "4:memory:/yarn/container_1\n");
      FileUtils.writeStringToFile(
          new File(cgcpuacctContainerDir, CGroupsResourceCalculator.CPU_STAT),
          "Can you handle this?\n" +
              "user 5415\n" +
              "system 3632");
      CGroupsResourceCalculator calculator =
          new CGroupsResourceCalculator(
              "1234", "/tmp/" + this.getClass().getName(),
              cGroupsHandler, clock);
      Assert.assertEquals("Incorrect CPU usage",
          90470,
          calculator.getCumulativeCpuTime());
    } finally {
      FileUtils.deleteDirectory(new File("/tmp/" + this.getClass().getName()));
    }
  }

  @Test
  public void testMemoryParsing() throws Exception {
    File cgcpuacctDir =
        new File("/tmp/" + this.getClass().getName() + "/cgcpuacct");
    File cgcpuacctContainerDir =
        new File(cgcpuacctDir, "/yarn/container_1");
    File cgmemoryDir =
        new File("/tmp/" + this.getClass().getName() + "/memory");
    File cgMemoryContainerDir =
        new File(cgmemoryDir, "/yarn/container_1");
    File procfs = new File("/tmp/" + this.getClass().getName() + "/1234");
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
              "4:memory:/yarn/container_1\n");
      FileUtils.writeStringToFile(
          new File(cgMemoryContainerDir, CGroupsResourceCalculator.MEM_STAT),
          "418496512\n");

      CGroupsResourceCalculator calculator =
          new CGroupsResourceCalculator(
              "1234", "/tmp/" + this.getClass().getName(),
              cGroupsHandler, clock);

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
          "418496513\n");
      Assert.assertEquals("Incorrect swap usage",
          418496513,
          calculator.getVirtualMemorySize());
    } finally {
      FileUtils.deleteDirectory(new File("/tmp/" + this.getClass().getName()));
    }
  }
}

/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Tests for the CGroups handler implementation.
 */
public class TestCGroupsV2HandlerImpl extends TestCGroupsHandlerBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestCGroupsV2HandlerImpl.class);

  // Create a controller file in the unified hierarchy of cgroup v2
  @Override
  protected String getControllerFilePath(String controllerName) {
    return new File(tmpPath, hierarchy).getAbsolutePath();
  }

  public File createPremountedCgroups(File parentDir)
          throws IOException {
    // Format:
    // cgroup2 /sys/fs/cgroup cgroup2 rw,nosuid,nodev,noexec,relatime,nsdelegate,memory_recursiveprot 0 0
    String baseCgroup2Line =
            "cgroup2 " + parentDir.getAbsolutePath()
                    + " cgroup2 rw,nosuid,nodev,noexec,relatime,nsdelegate,memory_recursiveprot 0 0\n";

    File mockMtab = new File(parentDir, UUID.randomUUID().toString());
    if (!mockMtab.exists()) {
      if (!mockMtab.createNewFile()) {
        String message = "Could not create file " + mockMtab.getAbsolutePath();
        throw new IOException(message);
      }
    }
    FileWriter mtabWriter = new FileWriter(mockMtab.getAbsoluteFile());
    mtabWriter.write(baseCgroup2Line);
    mtabWriter.close();
    mockMtab.deleteOnExit();

    String enabledControllers = "cpuset cpu io memory hugetlb pids rdma misc\n";

    File controllersFile = new File(parentDir, CGroupsHandler.CGROUP_CONTROLLERS_FILE);
    FileWriter controllerWriter = new FileWriter(controllersFile.getAbsoluteFile());
    controllerWriter.write(enabledControllers);
    controllerWriter.close();
    controllersFile.deleteOnExit();

    File hierarchyDir = new File(parentDir, hierarchy);
    if (!hierarchyDir.mkdirs()) {
      String message = "Could not create directory " + hierarchyDir.getAbsolutePath();
      throw new IOException(message);
    }
    hierarchyDir.deleteOnExit();
    FileUtils.copyFile(controllersFile, new File(hierarchyDir, CGroupsHandler.CGROUP_CONTROLLERS_FILE));

    return mockMtab;
  }

  @Test
  public void testCGroupPaths() throws IOException {
    verifyZeroInteractions(privilegedOperationExecutorMock);
    CGroupsHandler cGroupsHandler = null;
    File parentDir = new File(tmpPath);
    File mtab = createPremountedCgroups(parentDir);
    assertTrue("Sample subsystem should be created",
        new File(controllerPath).exists());

    try {
      cGroupsHandler = new CGroupsV2HandlerImpl(createNoMountConfiguration(hierarchy),
              privilegedOperationExecutorMock, mtab.getAbsolutePath());
      cGroupsHandler.initializeCGroupController(controller);
    } catch (ResourceHandlerException e) {
      LOG.error("Caught exception: " + e);
      fail("Unexpected ResourceHandlerException when initializing controller!");
    }

    String testCGroup = "container_01";
    String expectedPath =
        controllerPath + Path.SEPARATOR + testCGroup;
    String path = cGroupsHandler.getPathForCGroup(controller, testCGroup);
    Assert.assertEquals(expectedPath, path);

    String expectedPathTasks = expectedPath + Path.SEPARATOR
        + CGroupsHandler.CGROUP_PROCS_FILE;
    path = cGroupsHandler.getPathForCGroupTasks(controller, testCGroup);
    Assert.assertEquals(expectedPathTasks, path);

    String param = CGroupsHandler.CGROUP_PARAM_CLASSID;
    String expectedPathParam = expectedPath + Path.SEPARATOR
        + controller.getName() + "." + param;
    path = cGroupsHandler.getPathForCGroupParam(controller, testCGroup, param);
    Assert.assertEquals(expectedPathParam, path);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testUnsupportedMountConfiguration() throws Exception {
    //As per junit behavior, we expect a new mock object to be available
    //in this test.
    verifyZeroInteractions(privilegedOperationExecutorMock);
    CGroupsHandler cGroupsHandler;
    File mtab = createEmptyCgroups();

    assertTrue("Sample subsystem should be created",
            new File(controllerPath).mkdirs());

    cGroupsHandler = new CGroupsV2HandlerImpl(createMountConfiguration(),
            privilegedOperationExecutorMock, mtab.getAbsolutePath());
    cGroupsHandler.initializeCGroupController(controller);
    }

  @Test
  public void testCGroupOperations() throws IOException {
    verifyZeroInteractions(privilegedOperationExecutorMock);
    CGroupsHandler cGroupsHandler = null;
    File parentDir = new File(tmpPath);
    File mtab = createPremountedCgroups(parentDir);
    assertTrue("Sample subsystem should be created",
            new File(controllerPath).exists());

    try {
      cGroupsHandler = new CGroupsV2HandlerImpl(createNoMountConfiguration(hierarchy),
          privilegedOperationExecutorMock, mtab.getAbsolutePath());
      cGroupsHandler.initializeCGroupController(controller);
    } catch (ResourceHandlerException e) {
      LOG.error("Caught exception: " + e);
        fail("Unexpected ResourceHandlerException when mounting controller!");
    }

    String testCGroup = "container_01";
    String expectedPath = controllerPath
        + Path.SEPARATOR + testCGroup;
    try {
      String path = cGroupsHandler.createCGroup(controller, testCGroup);

      assertTrue(new File(expectedPath).exists());
      Assert.assertEquals(expectedPath, path);

      //update param and read param tests.
      //We don't use net_cls.classid because as a test param here because
      //cgroups provides very specific read/write semantics for classid (only
      //numbers can be written - potentially as hex but can be read out only
      //as decimal)
      String param = "test_param";
      String paramValue = "test_param_value";

      cGroupsHandler
          .updateCGroupParam(controller, testCGroup, param, paramValue);
      String paramPath = expectedPath
          + Path.SEPARATOR + controller.getName()
          + "." + param;
      File paramFile = new File(paramPath);

      assertTrue(paramFile.exists());
      try {
        Assert.assertEquals(paramValue, new String(Files.readAllBytes(
            paramFile.toPath())));
      } catch (IOException e) {
        LOG.error("Caught exception: " + e);
        Assert.fail("Unexpected IOException trying to read cgroup param!");
      }

      Assert.assertEquals(paramValue,
          cGroupsHandler.getCGroupParam(controller, testCGroup, param));
    } catch (ResourceHandlerException e) {
      LOG.error("Caught exception: " + e);
      Assert
        .fail("Unexpected ResourceHandlerException during cgroup operations!");
    }
  }

  /**
   * Tests whether mtab parsing works as expected with a valid hierarchy set.
   * @throws Exception the test will fail
   */
  @Test
  public void testMtabParsing() throws Exception {
    // Initialize mtab and cgroup dir
    File parentDir = new File(tmpPath);
    // create mock cgroup
    File mockMtabFile = createPremountedCgroups(parentDir);

    CGroupsV2HandlerImpl cGroupsHandler = new CGroupsV2HandlerImpl(
        createMountConfiguration(),
        privilegedOperationExecutorMock, mockMtabFile.getAbsolutePath());

    // Run mtabs parsing
    Map<String, Set<String>> newMtab =
            cGroupsHandler.parseMtab(mockMtabFile.getAbsolutePath());
    Map<CGroupsHandler.CGroupController, String> controllerPaths =
            cGroupsHandler.initializeControllerPathsFromMtab(
            newMtab);

    // Verify
    Assert.assertEquals(4, controllerPaths.size());
    assertTrue(controllerPaths
        .containsKey(CGroupsHandler.CGroupController.CPU));
    assertTrue(controllerPaths
        .containsKey(CGroupsHandler.CGroupController.MEMORY));
    String cpuDir = controllerPaths.get(CGroupsHandler.CGroupController.CPU);
    String memoryDir =
        controllerPaths.get(CGroupsHandler.CGroupController.MEMORY);
    Assert.assertEquals(parentDir.getAbsolutePath(), cpuDir);
    Assert.assertEquals(parentDir.getAbsolutePath(), memoryDir);
  }

  @Test
  public void testManualCgroupSetting() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH, tmpPath);
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_HIERARCHY,
        "/hadoop-yarn");
    File subCgroup = new File(tmpPath, "/hadoop-yarn");
    File controllersFile = new File(subCgroup.getAbsolutePath(), CGroupsHandler.CGROUP_CONTROLLERS_FILE);

    try {
      Assert.assertTrue("temp dir should be created", subCgroup.mkdirs());
      String enabledControllers = "cpuset cpu io memory hugetlb pids rdma misc\n";

      FileWriter controllerWriter = new FileWriter(controllersFile.getAbsoluteFile());
      controllerWriter.write(enabledControllers);
      controllerWriter.close();

      CGroupsV2HandlerImpl cGroupsHandler = new CGroupsV2HandlerImpl(conf, null);
      cGroupsHandler.initializeCGroupController(CGroupsHandler.CGroupController.CPU);

      Assert.assertEquals("CPU cgroup path was not set", subCgroup.getAbsolutePath(),
              new File(cGroupsHandler.getPathForCGroup(
                  CGroupsHandler.CGroupController.CPU, "")).getAbsolutePath());

    } finally {
      FileUtils.deleteQuietly(subCgroup);
      FileUtils.deleteQuietly(controllersFile);
    }
  }
}
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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Tests for the CGroups handler implementation.
 */
public class TestCGroupsV2HandlerImpl extends TestCGroupsHandlerBase {
  // Create a controller file in the unified hierarchy of cgroup v2
  @Override
  protected String getControllerFilePath(String controllerName) {
    return new File(tmpPath, hierarchy).getAbsolutePath();
  }

  /*
    * Create a mock mtab file with the following content:
    * cgroup2 /path/to/parentDir cgroup2 rw,nosuid,nodev,noexec,relatime,nsdelegate,memory_recursiveprot 0 0
    *
    * Create the following cgroup v2 file hierarchy:
    *                      parentDir
    *                  ___________________________________________________
    *                 /                \                                  \
    *          cgroup.controllers   cgroup.subtree_control             test-hadoop-yarn (hierarchyDir)
    *                                                                    _________________
    *                                                                   /                 \
    *                                                             cgroup.controllers   cgroup.subtree_control
   */
  public File createPremountedCgroups(File parentDir)
          throws IOException {
    String baseCgroup2Line =
            "cgroup2 " + parentDir.getAbsolutePath()
                    + " cgroup2 rw,nosuid,nodev,noexec,relatime,nsdelegate,memory_recursiveprot 0 0\n";
    File mockMtab = createFileWithContent(parentDir, UUID.randomUUID().toString(), baseCgroup2Line);

    String enabledControllers = "cpuset cpu io memory hugetlb pids rdma misc\n";
    File controllersFile = createFileWithContent(parentDir, CGroupsHandler.CGROUP_CONTROLLERS_FILE,
        enabledControllers);

    File subtreeControlFile = new File(parentDir, CGroupsHandler.CGROUP_SUBTREE_CONTROL_FILE);
    Assert.assertTrue("empty subtree_control file should be created",
        subtreeControlFile.createNewFile());

    File hierarchyDir = new File(parentDir, hierarchy);
    if (!hierarchyDir.mkdirs()) {
      String message = "Could not create directory " + hierarchyDir.getAbsolutePath();
      throw new IOException(message);
    }
    hierarchyDir.deleteOnExit();

    FileUtils.copyFile(controllersFile, new File(hierarchyDir,
        CGroupsHandler.CGROUP_CONTROLLERS_FILE));
    FileUtils.copyFile(subtreeControlFile, new File(hierarchyDir,
        CGroupsHandler.CGROUP_SUBTREE_CONTROL_FILE));

    return mockMtab;
  }

  @Test
  public void testCGroupPaths() throws IOException, ResourceHandlerException {
    verifyZeroInteractions(privilegedOperationExecutorMock);
    File parentDir = new File(tmpPath);
    File mtab = createPremountedCgroups(parentDir);
    assertTrue("Sample subsystem should be created",
        new File(controllerPath).exists());

    CGroupsHandler cGroupsHandler = new CGroupsV2HandlerImpl(createNoMountConfiguration(hierarchy),
        privilegedOperationExecutorMock, mtab.getAbsolutePath());
    cGroupsHandler.initializeCGroupController(controller);

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
    File mtab = createEmptyMtabFile();

    assertTrue("Sample subsystem should be created",
            new File(controllerPath).mkdirs());

    cGroupsHandler = new CGroupsV2HandlerImpl(createMountConfiguration(),
            privilegedOperationExecutorMock, mtab.getAbsolutePath());
    cGroupsHandler.initializeCGroupController(controller);
  }

  @Test
  public void testCGroupOperations() throws IOException, ResourceHandlerException {
    verifyZeroInteractions(privilegedOperationExecutorMock);
    File parentDir = new File(tmpPath);
    File mtab = createPremountedCgroups(parentDir);
    assertTrue("Sample subsystem should be created",
            new File(controllerPath).exists());

    CGroupsHandler cGroupsHandler = new CGroupsV2HandlerImpl(createNoMountConfiguration(hierarchy),
        privilegedOperationExecutorMock, mtab.getAbsolutePath());
    cGroupsHandler.initializeCGroupController(controller);

    String testCGroup = "container_01";
    String expectedPath = controllerPath
        + Path.SEPARATOR + testCGroup;
    String path = cGroupsHandler.createCGroup(controller, testCGroup);

    assertTrue(new File(expectedPath).exists());
    Assert.assertEquals(expectedPath, path);

    String param = "test_param";
    String paramValue = "test_param_value";

    cGroupsHandler
        .updateCGroupParam(controller, testCGroup, param, paramValue);
    String paramPath = expectedPath + Path.SEPARATOR + controller.getName()
        + "." + param;
    File paramFile = new File(paramPath);

    assertTrue(paramFile.exists());
    Assert.assertEquals(paramValue, new String(Files.readAllBytes(
        paramFile.toPath())));
    Assert.assertEquals(paramValue,
        cGroupsHandler.getCGroupParam(controller, testCGroup, param));
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

    File baseCgroup = new File(tmpPath);
    File subCgroup = new File(tmpPath, "/hadoop-yarn");
    Assert.assertTrue("temp dir should be created", subCgroup.mkdirs());
    subCgroup.deleteOnExit();

    String enabledControllers = "cpuset cpu io memory hugetlb pids rdma misc\n";
    createFileWithContent(baseCgroup, CGroupsHandler.CGROUP_CONTROLLERS_FILE, enabledControllers);
    createFileWithContent(subCgroup, CGroupsHandler.CGROUP_CONTROLLERS_FILE, enabledControllers);

    File subtreeControlFile = new File(subCgroup.getAbsolutePath(),
        CGroupsHandler.CGROUP_SUBTREE_CONTROL_FILE);
    Assert.assertTrue("empty subtree_control file should be created",
        subtreeControlFile.createNewFile());

    CGroupsV2HandlerImpl cGroupsHandler = new CGroupsV2HandlerImpl(conf, null);
    cGroupsHandler.initializeCGroupController(CGroupsHandler.CGroupController.CPU);

    Assert.assertEquals("CPU cgroup path was not set", subCgroup.getAbsolutePath(),
            new File(cGroupsHandler.getPathForCGroup(
                CGroupsHandler.CGroupController.CPU, "")).getAbsolutePath());

    // Verify that the subtree control file was updated
    String subtreeControllersEnabledString = FileUtils.readFileToString(subtreeControlFile,
        StandardCharsets.UTF_8);
    Assert.assertEquals("The newly added controller doesn't contain + sign",
        1, StringUtils.countMatches(subtreeControllersEnabledString, "+"));
    Assert.assertEquals("Controller is not enabled in subtree control file",
        controller.getName(), subtreeControllersEnabledString.replace("+", "").trim());

    cGroupsHandler.initializeCGroupController(CGroupsHandler.CGroupController.MEMORY);

    subtreeControllersEnabledString = FileUtils.readFileToString(subtreeControlFile,
        StandardCharsets.UTF_8);
    Assert.assertEquals("The newly added controllers doesn't contain + signs",
        2, StringUtils.countMatches(subtreeControllersEnabledString, "+"));

    Set<String> subtreeControllersEnabled = new HashSet<>(Arrays.asList(
        subtreeControllersEnabledString.replace("+", " ").trim().split(" ")));
    Assert.assertEquals(2, subtreeControllersEnabled.size());
    Assert.assertTrue("Controller is not enabled in subtree control file",
        cGroupsHandler.getValidCGroups().containsAll(subtreeControllersEnabled));

    // Test that the subtree control file is appended correctly
    // even if some controllers are present
    subtreeControlFile.delete();
    createFileWithContent(subCgroup, CGroupsHandler.CGROUP_SUBTREE_CONTROL_FILE, "cpu io");
    cGroupsHandler.initializeCGroupController(CGroupsHandler.CGroupController.MEMORY);

    subtreeControllersEnabledString = FileUtils.readFileToString(subtreeControlFile,
        StandardCharsets.UTF_8);
    Assert.assertEquals("The newly added controller doesn't contain + sign",
        1, StringUtils.countMatches(subtreeControllersEnabledString, "+"));

    subtreeControllersEnabled = new HashSet<>(Arrays.asList(
        subtreeControllersEnabledString.replace("+", " ").split(" ")));
    Assert.assertEquals(3, subtreeControllersEnabled.size());
    Assert.assertTrue("Controllers not enabled in subtree control file",
        cGroupsHandler.getValidCGroups().containsAll(subtreeControllersEnabled));
  }
}
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

import static org.apache.hadoop.test.MockitoUtil.verifyZeroInteractions;
import static org.junit.Assert.assertTrue;

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

  /*
   * Create a mock mtab file with the following content for hybrid v1/v2:
   * cgroup2 /path/to/parentV2Dir cgroup2 rw,nosuid,nodev,noexec,relatime,memory_recursiveprot 0 0
   * cgroup /path/to/parentDir/memory cgroup rw,nosuid,nodev,noexec,relatime,memory 0 0
   *
   * Create the following cgroup hierarchy:
   *
   *                                           parentDir
   *                              __________________________________
   *                             /                                  \
   *                          unified                             memory
   *       _________________________________________________
   *      /                     \                           \
   *  cgroup.controllers     cgroup.subtree_control   test-hadoop-yarn (hierarchyDir)
   *                                                        _________________
   *                                                       /                 \
   *                                              cgroup.controllers   cgroup.subtree_control
   */
  public File createPremountedHybridCgroups(File v1ParentDir)
      throws IOException {
    File v2ParentDir = new File(v1ParentDir, "unified");

    String mtabContent =
        "cgroup " + v1ParentDir.getAbsolutePath() + "/memory"
            + " cgroup rw,nosuid,nodev,noexec,relatime,memory 0 0\n"
        + "cgroup2 " + v2ParentDir.getAbsolutePath()
            + " cgroup2 rw,nosuid,nodev,noexec,relatime,memory_recursiveprot 0 0\n";

    File mockMtab = createFileWithContent(v1ParentDir, UUID.randomUUID().toString(), mtabContent);

    String enabledV2Controllers = "cpuset cpu io hugetlb pids rdma misc\n";
    File controllersFile = createFileWithContent(v2ParentDir,
        CGroupsHandler.CGROUP_CONTROLLERS_FILE, enabledV2Controllers);

    File subtreeControlFile = new File(v2ParentDir, CGroupsHandler.CGROUP_SUBTREE_CONTROL_FILE);
    Assert.assertTrue("empty subtree_control file should be created",
        subtreeControlFile.createNewFile());

    File hierarchyDir = new File(v2ParentDir, hierarchy);
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
  public void testHybridMtabParsing() throws Exception {
    // Initialize mtab and cgroup dir
    File v1ParentDir = new File(tmpPath);

    File v2ParentDir = new File(v1ParentDir, "unified");
    Assert.assertTrue("temp dir should be created", v2ParentDir.mkdirs());
    v2ParentDir.deleteOnExit();

    // create mock cgroup
    File mockMtabFile = createPremountedHybridCgroups(v1ParentDir);

    // create memory cgroup for v1
    File memoryCgroup = new File(v1ParentDir, "memory");
    assertTrue("Directory should be created", memoryCgroup.mkdirs());

    // init v1 and v2 handlers
    CGroupsHandlerImpl cGroupsHandler = new CGroupsHandlerImpl(
        createMountConfiguration(),
        privilegedOperationExecutorMock, mockMtabFile.getAbsolutePath());
    CGroupsV2HandlerImpl cGroupsV2Handler = new CGroupsV2HandlerImpl(
        createMountConfiguration(),
        privilegedOperationExecutorMock, mockMtabFile.getAbsolutePath());

    // Verify resource handlers that are enabled in v1
    Map<String, Set<String>> newMtab =
        cGroupsHandler.parseMtab(mockMtabFile.getAbsolutePath());
    Map<CGroupsHandler.CGroupController, String> controllerv1Paths =
        cGroupsHandler.initializeControllerPathsFromMtab(
            newMtab);

    Assert.assertEquals(1, controllerv1Paths.size());
    assertTrue(controllerv1Paths
        .containsKey(CGroupsHandler.CGroupController.MEMORY));
    String memoryDir =
        controllerv1Paths.get(CGroupsHandler.CGroupController.MEMORY);
    Assert.assertEquals(memoryCgroup.getAbsolutePath(), memoryDir);

    // Verify resource handlers that are enabled in v2
    newMtab =
        cGroupsV2Handler.parseMtab(mockMtabFile.getAbsolutePath());
    Map<CGroupsHandler.CGroupController, String> controllerPaths =
        cGroupsV2Handler.initializeControllerPathsFromMtab(
            newMtab);

    Assert.assertEquals(3, controllerPaths.size());
    assertTrue(controllerPaths
        .containsKey(CGroupsHandler.CGroupController.CPU));
    String cpuDir = controllerPaths.get(CGroupsHandler.CGroupController.CPU);
    Assert.assertEquals(v2ParentDir.getAbsolutePath(), cpuDir);
  }

  @Test
  public void testManualCgroupSetting() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH, tmpPath);
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_HIERARCHY,
        "/hadoop-yarn");

    validateCgroupV2Controllers(conf, tmpPath);
  }

  @Test
  public void testManualHybridCgroupSetting() throws Exception {
    String unifiedPath = tmpPath + "/unified";

    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH, tmpPath);
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_V2_MOUNT_PATH, unifiedPath);
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_HIERARCHY,
        "/hadoop-yarn");

    validateCgroupV1Controllers(conf, tmpPath);
    validateCgroupV2Controllers(conf, unifiedPath);
  }

  private void validateCgroupV2Controllers(YarnConfiguration conf, String mountPath)
      throws Exception {
    File baseCgroup = new File(mountPath);
    File subCgroup = new File(mountPath, "/hadoop-yarn");
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

  private void validateCgroupV1Controllers(YarnConfiguration conf, String mountPath)
      throws ResourceHandlerException {
    File blkio = new File(new File(mountPath, "blkio"), "/hadoop-yarn");

    Assert.assertTrue("temp dir should be created", blkio.mkdirs());

    CGroupsHandlerImpl cGroupsv1Handler = new CGroupsHandlerImpl(conf, null);
    cGroupsv1Handler.initializeCGroupController(
        CGroupsHandler.CGroupController.BLKIO);

    Assert.assertEquals("BLKIO CGRoup path was not set", blkio.getAbsolutePath(),
        new File(cGroupsv1Handler.getPathForCGroup(
            CGroupsHandler.CGroupController.BLKIO, "")).getAbsolutePath());

    FileUtils.deleteQuietly(blkio);
  }
}
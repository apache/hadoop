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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.security.Permission;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Tests for the CGroups handler implementation.
 */
public class TestCGroupsHandlerImpl {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestCGroupsHandlerImpl.class);

  private PrivilegedOperationExecutor privilegedOperationExecutorMock;
  private String tmpPath;
  private String hierarchy;
  private CGroupsHandler.CGroupController controller;
  private String controllerPath;

  @Before
  public void setup() {
    privilegedOperationExecutorMock = mock(PrivilegedOperationExecutor.class);

    // Prepare test directory
    tmpPath = System.getProperty("test.build.data") + "/cgroups";
    File tmpDir = new File(tmpPath);
    FileUtils.deleteQuietly(tmpDir);
    assertTrue(tmpDir.mkdirs());

    //no leading or trailing slashes here
    hierarchy = "test-hadoop-yarn";

    // Sample subsystem. Not used by all the tests
    controller = CGroupsHandler.CGroupController.NET_CLS;
    controllerPath =
        new File(new File(tmpPath, controller.getName()), hierarchy)
            .getAbsolutePath();
  }

  @After
  public void teardown() {
    FileUtil.fullyDelete(new File(tmpPath));
  }

  /**
   * Security manager simulating access denied.
   */
  private class MockSecurityManagerDenyWrite extends SecurityManager {
    @Override
    public void checkPermission(Permission perm) {
      if(perm.getActions().equals("write")) {
        throw new SecurityException("Mock not allowed");
      }
    }
  }

  /**
   * Create configuration to mount cgroups that do not exist.
   * @return configuration object
   */
  private YarnConfiguration createMountConfiguration() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_HIERARCHY, hierarchy);
    conf.setBoolean(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT, true);
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH, tmpPath);
    return conf;
  }

  /**
   * Create configuration where the cgroups are premounted.
   * @param myHierarchy Yarn cgroup
   * @return configuration object
   */
  private Configuration createNoMountConfiguration(String myHierarchy) {
    Configuration confNoMount = new Configuration();
    confNoMount.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_HIERARCHY,
        myHierarchy);
    confNoMount.setBoolean(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT,
        false);
    return confNoMount;
  }

  /**
   * Create an empty mtab file. No cgroups are premounted
   * @return mtab file
   * @throws IOException could not create file
   */
  private File createEmptyCgroups() throws IOException {
    File emptyMtab = new File(tmpPath, "mtab");
    assertTrue("New file should have been created", emptyMtab.createNewFile());
    return emptyMtab;
  }

  /**
   * Create simulated cgroups mount point.
   * @param parentDir cgroups mount point
   * @param cpuAcct simulate newer Linux behavior by mounting cpu with cpuacct
   * @return simulated mtab file location
   * @throws IOException mtab file was not created
   */
  public static File createPremountedCgroups(File parentDir, boolean cpuAcct)
      throws IOException {
    // Mark an empty directory called 'cp' cgroup. It is processed before 'cpu'
    String cpuMtabContentMissing =
        "none " + parentDir.getAbsolutePath()
            + "/cp cgroup rw,relatime,cpu 0 0\n";

    File cpuCgroup = new File(parentDir, "cpu");
    String cpuMtabContent =
        "none " + cpuCgroup.getAbsolutePath()
            + " cgroup rw,relatime,cpu"
            + (cpuAcct ? ",cpuacct" :"")
            + " 0 0\n";
    assertTrue("Directory should be created", cpuCgroup.mkdirs());

    File blkioCgroup = new File(parentDir, "blkio");
    String blkioMtabContent =
        "none " + blkioCgroup.getAbsolutePath()
            + " cgroup rw,relatime,blkio 0 0\n";
    assertTrue("Directory should be created", blkioCgroup.mkdirs());

    File mockMtab = new File(parentDir, UUID.randomUUID().toString());
    if (!mockMtab.exists()) {
      if (!mockMtab.createNewFile()) {
        String message = "Could not create file " + mockMtab.getAbsolutePath();
        throw new IOException(message);
      }
    }
    FileWriter mtabWriter = new FileWriter(mockMtab.getAbsoluteFile());
    mtabWriter.write(cpuMtabContentMissing);
    mtabWriter.write(cpuMtabContent);
    mtabWriter.write(blkioMtabContent);
    mtabWriter.close();
    mockMtab.deleteOnExit();
    return mockMtab;
  }

  @Test
  public void testMountController() throws IOException {
    File parentDir = new File(tmpPath);
    File cgroup = new File(parentDir, controller.getName());
    assertTrue("cgroup dir should be cerated", cgroup.mkdirs());
    //Since we enabled (deferred) cgroup controller mounting, no interactions
    //should have occurred, with this mock
    verifyZeroInteractions(privilegedOperationExecutorMock);
    File emptyMtab = createEmptyCgroups();

    try {
      CGroupsHandler cGroupsHandler = new CGroupsHandlerImpl(
          createMountConfiguration(),
          privilegedOperationExecutorMock,
          emptyMtab.getAbsolutePath());
      PrivilegedOperation expectedOp = new PrivilegedOperation(
          PrivilegedOperation.OperationType.MOUNT_CGROUPS);
      //This is expected to be of the form :
      //net_cls=<mount_path>/net_cls
      String controllerKV = controller.getName() + "=" + tmpPath
          + Path.SEPARATOR + controller.getName();
      expectedOp.appendArgs(hierarchy, controllerKV);

      cGroupsHandler.initializeCGroupController(controller);
      try {
        ArgumentCaptor<PrivilegedOperation> opCaptor = ArgumentCaptor.forClass(
            PrivilegedOperation.class);
        verify(privilegedOperationExecutorMock)
            .executePrivilegedOperation(opCaptor.capture(), eq(false));

        //we'll explicitly capture and assert that the
        //captured op and the expected op are identical.
        Assert.assertEquals(expectedOp, opCaptor.getValue());
        verifyNoMoreInteractions(privilegedOperationExecutorMock);

        //Try mounting the same controller again - this should be a no-op
        cGroupsHandler.initializeCGroupController(controller);
        verifyNoMoreInteractions(privilegedOperationExecutorMock);
      } catch (PrivilegedOperationException e) {
        LOG.error("Caught exception: " + e);
        assertTrue("Unexpected PrivilegedOperationException from mock!",
            false);
      }
    } catch (ResourceHandlerException e) {
      LOG.error("Caught exception: " + e);
      assertTrue("Unexpected ResourceHandler Exception!", false);
    }
  }

  @Test
  public void testCGroupPaths() throws IOException {
    //As per junit behavior, we expect a new mock object to be available
    //in this test.
    verifyZeroInteractions(privilegedOperationExecutorMock);
    CGroupsHandler cGroupsHandler = null;
    File mtab = createEmptyCgroups();

    // Lets manually create a path to (partially) simulate a controller mounted
    // later in the test. This is required because the handler uses a mocked
    // privileged operation executor
    assertTrue("Sample subsystem should be created",
        new File(controllerPath).mkdirs());

    try {
      cGroupsHandler = new CGroupsHandlerImpl(createMountConfiguration(),
          privilegedOperationExecutorMock, mtab.getAbsolutePath());
      cGroupsHandler.initializeCGroupController(controller);
    } catch (ResourceHandlerException e) {
      LOG.error("Caught exception: " + e);
      assertTrue(
          "Unexpected ResourceHandlerException when mounting controller!",
          false);
    }

    String testCGroup = "container_01";
    String expectedPath =
        controllerPath + Path.SEPARATOR + testCGroup;
    String path = cGroupsHandler.getPathForCGroup(controller, testCGroup);
    Assert.assertEquals(expectedPath, path);

    String expectedPathTasks = expectedPath + Path.SEPARATOR
        + CGroupsHandler.CGROUP_FILE_TASKS;
    path = cGroupsHandler.getPathForCGroupTasks(controller, testCGroup);
    Assert.assertEquals(expectedPathTasks, path);

    String param = CGroupsHandler.CGROUP_PARAM_CLASSID;
    String expectedPathParam = expectedPath + Path.SEPARATOR
        + controller.getName() + "." + param;
    path = cGroupsHandler.getPathForCGroupParam(controller, testCGroup, param);
    Assert.assertEquals(expectedPathParam, path);
  }

  @Test
  public void testCGroupOperations() throws IOException {
    //As per junit behavior, we expect a new mock object to be available
    //in this test.
    verifyZeroInteractions(privilegedOperationExecutorMock);
    CGroupsHandler cGroupsHandler = null;
    File mtab = createEmptyCgroups();

    // Lets manually create a path to (partially) simulate a controller mounted
    // later in the test. This is required because the handler uses a mocked
    // privileged operation executor
    assertTrue("Sample subsystem should be created",
        new File(controllerPath).mkdirs());

    try {
      cGroupsHandler = new CGroupsHandlerImpl(createMountConfiguration(),
          privilegedOperationExecutorMock, mtab.getAbsolutePath());
      cGroupsHandler.initializeCGroupController(controller);
    } catch (ResourceHandlerException e) {
      LOG.error("Caught exception: " + e);
      assertTrue(
          "Unexpected ResourceHandlerException when mounting controller!",
          false);
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

      //We can't really do a delete test here. Linux cgroups
      //implementation provides additional semantics - the cgroup cannot be
      //deleted if there are any tasks still running in the cgroup even if
      //the user attempting the delete has the file permissions to do so - we
      //cannot simulate that here. Even if we create a dummy 'tasks' file, we
      //wouldn't be able to simulate the delete behavior we need, since a cgroup
      //can be deleted using using 'rmdir' if the tasks file is empty. Such a
      //delete is not possible with a regular non-empty directory.
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
    File mockMtabFile = createPremountedCgroups(parentDir, false);

    // Run mtabs parsing
    Map<String, Set<String>> newMtab =
        CGroupsHandlerImpl.parseMtab(mockMtabFile.getAbsolutePath());
    Map<CGroupsHandler.CGroupController, String> controllerPaths =
        CGroupsHandlerImpl.initializeControllerPathsFromMtab(
            newMtab);

    // Verify
    Assert.assertEquals(2, controllerPaths.size());
    assertTrue(controllerPaths
        .containsKey(CGroupsHandler.CGroupController.CPU));
    assertTrue(controllerPaths
        .containsKey(CGroupsHandler.CGroupController.BLKIO));
    String cpuDir = controllerPaths.get(CGroupsHandler.CGroupController.CPU);
    String blkioDir =
        controllerPaths.get(CGroupsHandler.CGroupController.BLKIO);
    Assert.assertEquals(parentDir.getAbsolutePath() + "/cpu", cpuDir);
    Assert.assertEquals(parentDir.getAbsolutePath() + "/blkio", blkioDir);
  }

  /**
   * Tests whether mtab parsing works as expected with the specified hierarchy.
   * @param myHierarchy path to local cgroup hierarchy
   * @throws Exception the test will fail
   */
  private void testPreMountedControllerInitialization(String myHierarchy)
      throws Exception {
    // Initialize mount point
    File parentDir = new File(tmpPath);
    File mtab = createPremountedCgroups(parentDir, false);
    File mountPoint = new File(parentDir, "cpu");

    // Initialize Yarn classes
    Configuration confNoMount = createNoMountConfiguration(myHierarchy);
    CGroupsHandlerImpl cGroupsHandler = new CGroupsHandlerImpl(confNoMount,
        privilegedOperationExecutorMock, mtab.getAbsolutePath());

    File cpuCgroupMountDir = new File(
        cGroupsHandler.getPathForCGroup(CGroupsHandler.CGroupController.CPU,
            ""));
    // Test that a missing yarn hierarchy will be created automatically
    if (!cpuCgroupMountDir.equals(mountPoint)) {
      assertTrue("Directory should be deleted",
          !cpuCgroupMountDir.exists());
    }
    cGroupsHandler.initializeCGroupController(
        CGroupsHandler.CGroupController.CPU);
    assertTrue("Cgroups not writable", cpuCgroupMountDir.exists() &&
        cpuCgroupMountDir.canWrite());

    // Test that an inaccessible yarn hierarchy results in an exception
    assertTrue(cpuCgroupMountDir.setWritable(false));
    try {
      cGroupsHandler.initializeCGroupController(
          CGroupsHandler.CGroupController.CPU);
      Assert.fail("An inaccessible path should result in an exception");
    } catch (Exception e) {
      assertTrue("Unexpected exception " + e.getClass().toString(),
          e instanceof ResourceHandlerException);
    } finally {
      assertTrue("Could not revert writable permission",
          cpuCgroupMountDir.setWritable(true));
    }

    // Test that a non-accessible mount directory results in an exception
    if (!cpuCgroupMountDir.equals(mountPoint)) {
      assertTrue("Could not delete cgroups", cpuCgroupMountDir.delete());
      assertTrue("Directory should be deleted",
          !cpuCgroupMountDir.exists());
    }
    assertTrue(mountPoint.setWritable(false));
    try {
      cGroupsHandler.initializeCGroupController(
          CGroupsHandler.CGroupController.CPU);
      Assert.fail("An inaccessible path should result in an exception");
    } catch (Exception e) {
      assertTrue("Unexpected exception " + e.getClass().toString(),
          e instanceof ResourceHandlerException);
    } finally {
      assertTrue("Could not revert writable permission",
          mountPoint.setWritable(true));
    }

    // Test that a SecurityException results in an exception
    if (!cpuCgroupMountDir.equals(mountPoint)) {
      Assert.assertFalse("Could not delete cgroups",
          cpuCgroupMountDir.delete());
      assertTrue("Directory should be deleted",
          !cpuCgroupMountDir.exists());
      SecurityManager manager = System.getSecurityManager();
      System.setSecurityManager(new MockSecurityManagerDenyWrite());
      try {
        cGroupsHandler.initializeCGroupController(
            CGroupsHandler.CGroupController.CPU);
        Assert.fail("An inaccessible path should result in an exception");
      } catch (Exception e) {
        assertTrue("Unexpected exception " + e.getClass().toString(),
            e instanceof ResourceHandlerException);
      } finally {
        System.setSecurityManager(manager);
      }
    }

    // Test that a non-existing mount directory results in an exception
    if (!cpuCgroupMountDir.equals(mountPoint)) {
      Assert.assertFalse("Could not delete cgroups",
          cpuCgroupMountDir.delete());
      assertTrue("Directory should be deleted",
          !cpuCgroupMountDir.exists());
    }
    FileUtils.deleteQuietly(mountPoint);
    assertTrue("cgroups mount point should be deleted",
        !mountPoint.exists());
    try {
      cGroupsHandler.initializeCGroupController(
          CGroupsHandler.CGroupController.CPU);
      Assert.fail("An inaccessible path should result in an exception");
    } catch (Exception e) {
      assertTrue("Unexpected exception " + e.getClass().toString(),
          e instanceof ResourceHandlerException);
    }
  }

  @Test
  public void testSelectCgroup() throws Exception {
    File cpu = new File(tmpPath, "cpu");
    File cpuNoExist = new File(tmpPath, "cpuNoExist");
    File memory = new File(tmpPath, "memory");
    try {
      CGroupsHandlerImpl handler = new CGroupsHandlerImpl(
          createNoMountConfiguration(tmpPath),
          privilegedOperationExecutorMock);
      Map<String, Set<String>> cgroups = new LinkedHashMap<>();

      Assert.assertTrue("temp dir should be created", cpu.mkdirs());
      Assert.assertTrue("temp dir should be created", memory.mkdirs());
      Assert.assertFalse("temp dir should not be created", cpuNoExist.exists());

      cgroups.put(
          memory.getAbsolutePath(), Collections.singleton("memory"));
      cgroups.put(
          cpuNoExist.getAbsolutePath(), Collections.singleton("cpu"));
      cgroups.put(cpu.getAbsolutePath(), Collections.singleton("cpu"));
      String selectedCPU = handler.findControllerInMtab("cpu", cgroups);
      Assert.assertEquals("Wrong CPU mount point selected",
          cpu.getAbsolutePath(), selectedCPU);
    } finally {
      FileUtils.deleteQuietly(cpu);
      FileUtils.deleteQuietly(memory);
    }
  }

  /**
   * Tests whether mtab parsing works as expected with an empty hierarchy set.
   * @throws Exception the test will fail
   */
  @Test
  public void testPreMountedControllerEmpty() throws Exception {
    testPreMountedControllerInitialization("");
  }

  /**
   * Tests whether mtab parsing works as expected with a / hierarchy set.
   * @throws Exception the test will fail
   */
  @Test
  public void testPreMountedControllerRoot() throws Exception {
    testPreMountedControllerInitialization("/");
  }

  /**
   * Tests whether mtab parsing works as expected with the specified hierarchy.
   * @throws Exception the test will fail
   */
  @Test
  public void testRemount()
      throws Exception {
    // Initialize mount point
    File parentDir = new File(tmpPath);

    final String oldMountPointDir = "oldmount";
    final String newMountPointDir = "newmount";

    File oldMountPoint = new File(parentDir, oldMountPointDir);
    File mtab = createPremountedCgroups(
        oldMountPoint, true);

    File newMountPoint = new File(parentDir, newMountPointDir);
    assertTrue("Could not create dirs",
        new File(newMountPoint, "cpu").mkdirs());

    // Initialize Yarn classes
    Configuration confMount = createMountConfiguration();
    confMount.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH,
        parentDir.getAbsolutePath() + Path.SEPARATOR + newMountPointDir);
    CGroupsHandlerImpl cGroupsHandler = new CGroupsHandlerImpl(confMount,
        privilegedOperationExecutorMock, mtab.getAbsolutePath());

    cGroupsHandler.initializeCGroupController(
        CGroupsHandler.CGroupController.CPU);

    ArgumentCaptor<PrivilegedOperation> opCaptor = ArgumentCaptor.forClass(
        PrivilegedOperation.class);
    verify(privilegedOperationExecutorMock)
        .executePrivilegedOperation(opCaptor.capture(), eq(false));
    File hierarchyFile =
        new File(new File(newMountPoint, "cpu"), this.hierarchy);
    assertTrue("Yarn cgroup should exist", hierarchyFile.exists());
  }


  @Test
  public void testManualCgroupSetting() throws ResourceHandlerException {
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH, tmpPath);
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_HIERARCHY,
        "/hadoop-yarn");
    File cpu = new File(new File(tmpPath, "cpuacct,cpu"), "/hadoop-yarn");

    try {
      Assert.assertTrue("temp dir should be created", cpu.mkdirs());

      CGroupsHandlerImpl cGroupsHandler = new CGroupsHandlerImpl(conf, null);
      cGroupsHandler.initializeCGroupController(
              CGroupsHandler.CGroupController.CPU);

      Assert.assertEquals("CPU CGRoup path was not set", cpu.getAbsolutePath(),
              new File(cGroupsHandler.getPathForCGroup(
                  CGroupsHandler.CGroupController.CPU, "")).getAbsolutePath());

    } finally {
      FileUtils.deleteQuietly(cpu);
    }
  }
}
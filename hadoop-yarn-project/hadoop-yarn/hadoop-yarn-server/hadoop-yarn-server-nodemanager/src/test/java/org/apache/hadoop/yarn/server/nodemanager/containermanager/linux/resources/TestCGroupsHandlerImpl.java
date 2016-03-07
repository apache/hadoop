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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
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
import java.util.Map;
import java.util.UUID;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Tests for the CGroups handler implementation.
 */
public class TestCGroupsHandlerImpl {
  private static final Log LOG =
      LogFactory.getLog(TestCGroupsHandlerImpl.class);

  private PrivilegedOperationExecutor privilegedOperationExecutorMock;
  private Configuration conf;
  private String tmpPath;
  private String hierarchy;
  private CGroupsHandler.CGroupController controller;
  private String controllerPath;

  @Before
  public void setup() {
    privilegedOperationExecutorMock = mock(PrivilegedOperationExecutor.class);
    conf = new YarnConfiguration();
    tmpPath = System.getProperty("test.build.data") + "/cgroups";
    //no leading or trailing slashes here
    hierarchy = "test-hadoop-yarn";

    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_HIERARCHY, hierarchy);
    conf.setBoolean(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT, true);
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH, tmpPath);
    controller = CGroupsHandler.CGroupController.NET_CLS;
    controllerPath = new StringBuffer(tmpPath).append('/')
        .append(controller.getName()).append('/').append(hierarchy).toString();
  }

  @Test
  public void testMountController() {
    CGroupsHandler cGroupsHandler = null;
    //Since we enabled (deferred) cgroup controller mounting, no interactions
    //should have occurred, with this mock
    verifyZeroInteractions(privilegedOperationExecutorMock);

    try {
      cGroupsHandler = new CGroupsHandlerImpl(conf,
          privilegedOperationExecutorMock);
      PrivilegedOperation expectedOp = new PrivilegedOperation(
          PrivilegedOperation.OperationType.MOUNT_CGROUPS);
      //This is expected to be of the form :
      //net_cls=<mount_path>/net_cls
      StringBuffer controllerKV = new StringBuffer(controller.getName())
          .append('=').append(tmpPath).append('/').append(controller.getName());
      expectedOp.appendArgs(hierarchy, controllerKV.toString());

      cGroupsHandler.mountCGroupController(controller);
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
        cGroupsHandler.mountCGroupController(controller);
        verifyNoMoreInteractions(privilegedOperationExecutorMock);
      } catch (PrivilegedOperationException e) {
        LOG.error("Caught exception: " + e);
        Assert.assertTrue("Unexpected PrivilegedOperationException from mock!",
            false);
      }
    } catch (ResourceHandlerException e) {
      LOG.error("Caught exception: " + e);
      Assert.assertTrue("Unexpected ResourceHandler Exception!", false);
    }
  }

  @Test
  public void testCGroupPaths() {
    //As per junit behavior, we expect a new mock object to be available
    //in this test.
    verifyZeroInteractions(privilegedOperationExecutorMock);
    CGroupsHandler cGroupsHandler = null;
    try {
      cGroupsHandler = new CGroupsHandlerImpl(conf,
          privilegedOperationExecutorMock);
      cGroupsHandler.mountCGroupController(controller);
    } catch (ResourceHandlerException e) {
      LOG.error("Caught exception: " + e);
      Assert.assertTrue(
          "Unexpected ResourceHandlerException when mounting controller!",
          false);
    }

    String testCGroup = "container_01";
    String expectedPath = new StringBuffer(controllerPath).append('/')
        .append(testCGroup).toString();
    String path = cGroupsHandler.getPathForCGroup(controller, testCGroup);
    Assert.assertEquals(expectedPath, path);

    String expectedPathTasks = new StringBuffer(expectedPath).append('/')
        .append(CGroupsHandler.CGROUP_FILE_TASKS).toString();
    path = cGroupsHandler.getPathForCGroupTasks(controller, testCGroup);
    Assert.assertEquals(expectedPathTasks, path);

    String param = CGroupsHandler.CGROUP_PARAM_CLASSID;
    String expectedPathParam = new StringBuffer(expectedPath).append('/')
        .append(controller.getName()).append('.').append(param).toString();
    path = cGroupsHandler.getPathForCGroupParam(controller, testCGroup, param);
    Assert.assertEquals(expectedPathParam, path);
  }

  @Test
  public void testCGroupOperations() {
    //As per junit behavior, we expect a new mock object to be available
    //in this test.
    verifyZeroInteractions(privilegedOperationExecutorMock);
    CGroupsHandler cGroupsHandler = null;

    try {
      cGroupsHandler = new CGroupsHandlerImpl(conf,
          privilegedOperationExecutorMock);
      cGroupsHandler.mountCGroupController(controller);
    } catch (ResourceHandlerException e) {
      LOG.error("Caught exception: " + e);
      Assert.assertTrue(
          "Unexpected ResourceHandlerException when mounting controller!",
          false);
    }
    //Lets manually create a path to (partially) simulate a mounted controller
    //this is required because the handler uses a mocked privileged operation
    //executor
    new File(controllerPath).mkdirs();

    String testCGroup = "container_01";
    String expectedPath = new StringBuffer(controllerPath).append('/')
        .append(testCGroup).toString();
    try {
      String path = cGroupsHandler.createCGroup(controller, testCGroup);

      Assert.assertTrue(new File(expectedPath).exists());
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
      String paramPath = new StringBuffer(expectedPath).append('/')
          .append(controller.getName()).append('.').append(param).toString();
      File paramFile = new File(paramPath);

      Assert.assertTrue(paramFile.exists());
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

  public static File createMockCgroupMount(File parentDir, String type)
      throws IOException {
    return createMockCgroupMount(parentDir, type, "hadoop-yarn");
  }

  public static File createMockCgroupMount(File parentDir, String type,
      String hierarchy) throws IOException {
    File cgroupMountDir =
        new File(parentDir.getAbsolutePath(), type + "/" + hierarchy);
    FileUtils.deleteQuietly(cgroupMountDir);
    if (!cgroupMountDir.mkdirs()) {
      String message =
          "Could not create dir " + cgroupMountDir.getAbsolutePath();
      throw new IOException(message);
    }
    return cgroupMountDir;
  }

  public static File createMockMTab(File parentDir) throws IOException {
    String cpuMtabContent =
        "none " + parentDir.getAbsolutePath()
            + "/cpu cgroup rw,relatime,cpu 0 0\n";
    String blkioMtabContent =
        "none " + parentDir.getAbsolutePath()
            + "/blkio cgroup rw,relatime,blkio 0 0\n";

    File mockMtab = new File(parentDir, UUID.randomUUID().toString());
    if (!mockMtab.exists()) {
      if (!mockMtab.createNewFile()) {
        String message = "Could not create file " + mockMtab.getAbsolutePath();
        throw new IOException(message);
      }
    }
    FileWriter mtabWriter = new FileWriter(mockMtab.getAbsoluteFile());
    mtabWriter.write(cpuMtabContent);
    mtabWriter.write(blkioMtabContent);
    mtabWriter.close();
    mockMtab.deleteOnExit();
    return mockMtab;
  }


  @Test
  public void testMtabParsing() throws Exception {
    File parentDir = new File(tmpPath);
    // create mock cgroup
    File cpuCgroupMountDir = createMockCgroupMount(parentDir, "cpu",
        hierarchy);
    Assert.assertTrue(cpuCgroupMountDir.exists());
    File blkioCgroupMountDir = createMockCgroupMount(parentDir,
        "blkio", hierarchy);
    Assert.assertTrue(blkioCgroupMountDir.exists());
    File mockMtabFile = createMockMTab(parentDir);
    Map<CGroupsHandler.CGroupController, String> controllerPaths =
        CGroupsHandlerImpl.initializeControllerPathsFromMtab(
          mockMtabFile.getAbsolutePath(), hierarchy);
    Assert.assertEquals(2, controllerPaths.size());
    Assert.assertTrue(controllerPaths
        .containsKey(CGroupsHandler.CGroupController.CPU));
    Assert.assertTrue(controllerPaths
        .containsKey(CGroupsHandler.CGroupController.BLKIO));
    String cpuDir = controllerPaths.get(CGroupsHandler.CGroupController.CPU);
    String blkioDir =
        controllerPaths.get(CGroupsHandler.CGroupController.BLKIO);
    Assert.assertEquals(parentDir.getAbsolutePath() + "/cpu", cpuDir);
    Assert.assertEquals(parentDir.getAbsolutePath() + "/blkio", blkioDir);
  }

  @After
  public void teardown() {
    FileUtil.fullyDelete(new File(tmpPath));
  }
}
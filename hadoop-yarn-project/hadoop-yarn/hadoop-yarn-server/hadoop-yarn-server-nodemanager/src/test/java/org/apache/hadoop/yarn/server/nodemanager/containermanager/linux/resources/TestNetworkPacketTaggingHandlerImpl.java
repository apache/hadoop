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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.List;

import static org.mockito.Mockito.doNothing;
import static org.mockito.ArgumentMatchers.any;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test NetworkPacketTagging Handler.
 *
 */
public class TestNetworkPacketTaggingHandlerImpl {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestNetworkPacketTaggingHandlerImpl.class);
  private static final String TEST_CLASSID = "0x100001";
  private static final String TEST_CONTAINER_ID_STR = "container_01";
  private static final String TEST_TASKS_FILE = "testTasksFile";

  private NetworkTagMappingManager mockManager;
  private PrivilegedOperationExecutor privilegedOperationExecutorMock;
  private CGroupsHandler cGroupsHandlerMock;
  private Configuration conf;
  private String tmpPath;
  private ContainerId containerIdMock;
  private Container containerMock;

  @Before
  public void setup() {
    privilegedOperationExecutorMock = mock(PrivilegedOperationExecutor.class);
    cGroupsHandlerMock = mock(CGroupsHandler.class);
    conf = new YarnConfiguration();
    tmpPath = new StringBuffer(System.getProperty("test.build.data"))
        .append('/').append("hadoop.tmp.dir").toString();
    containerIdMock = mock(ContainerId.class);
    containerMock = mock(Container.class);
    when(containerIdMock.toString()).thenReturn(TEST_CONTAINER_ID_STR);
    //mock returning a mock - an angel died somewhere.
    when(containerMock.getContainerId()).thenReturn(containerIdMock);

    conf.set("hadoop.tmp.dir", tmpPath);
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, false);

    mockManager = mock(NetworkTagMappingManager.class);
    doNothing().when(mockManager).initialize(any(Configuration.class));
    when(mockManager.getNetworkTagHexID(any(Container.class)))
        .thenReturn(TEST_CLASSID);
  }

  @Test
  public void testBootstrap() {
    NetworkPacketTaggingHandlerImpl handlerImpl =
        createNetworkPacketTaggingHandlerImpl();

    try {
      handlerImpl.bootstrap(conf);
      verify(cGroupsHandlerMock).initializeCGroupController(
          eq(CGroupsHandler.CGroupController.NET_CLS));
      verifyNoMoreInteractions(cGroupsHandlerMock);
    } catch (ResourceHandlerException e) {
      LOG.error("Unexpected exception: " + e);
      Assert.fail("Caught unexpected ResourceHandlerException!");
    }
  }

  @Test
  public void testLifeCycle() {
    NetworkPacketTaggingHandlerImpl handlerImpl =
        createNetworkPacketTaggingHandlerImpl();
    try {
      handlerImpl.bootstrap(conf);
      testPreStart(handlerImpl);
      testPostComplete(handlerImpl);
    } catch (ResourceHandlerException e) {
      LOG.error("Unexpected exception: " + e);
      Assert.fail("Caught unexpected ResourceHandlerException!");
    }
  }

  private void testPreStart(NetworkPacketTaggingHandlerImpl handlerImpl) throws
      ResourceHandlerException {
    reset(privilegedOperationExecutorMock);

    when(cGroupsHandlerMock.getPathForCGroupTasks(CGroupsHandler
        .CGroupController.NET_CLS, TEST_CONTAINER_ID_STR)).thenReturn(
        TEST_TASKS_FILE);

    List<PrivilegedOperation> ops = handlerImpl.preStart(containerMock);

    //Ensure that cgroups is created and updated as expected
    verify(cGroupsHandlerMock).createCGroup(
        eq(CGroupsHandler.CGroupController.NET_CLS), eq(TEST_CONTAINER_ID_STR));
    verify(cGroupsHandlerMock).updateCGroupParam(
        eq(CGroupsHandler.CGroupController.NET_CLS), eq(TEST_CONTAINER_ID_STR),
        eq(CGroupsHandler.CGROUP_PARAM_CLASSID), eq(TEST_CLASSID));

    //Now check the privileged operations being returned
    //We expect one operations - for adding pid to tasks file
    Assert.assertEquals(1, ops.size());

    //Verify that the add pid op is correct
    PrivilegedOperation addPidOp = ops.get(0);
    String expectedAddPidOpArg = PrivilegedOperation.CGROUP_ARG_PREFIX +
        TEST_TASKS_FILE;
    List<String> addPidOpArgs = addPidOp.getArguments();

    Assert.assertEquals(PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
        addPidOp.getOperationType());
    Assert.assertEquals(1, addPidOpArgs.size());
    Assert.assertEquals(expectedAddPidOpArg, addPidOpArgs.get(0));
  }

  private void testPostComplete(NetworkPacketTaggingHandlerImpl handlerImpl)
      throws ResourceHandlerException {
    reset(privilegedOperationExecutorMock);

    List<PrivilegedOperation> ops = handlerImpl.postComplete(containerIdMock);

    verify(cGroupsHandlerMock).deleteCGroup(
        eq(CGroupsHandler.CGroupController.NET_CLS), eq(TEST_CONTAINER_ID_STR));

    //We don't expect any operations to be returned here
    Assert.assertNull(ops);
  }

  private NetworkPacketTaggingHandlerImpl
      createNetworkPacketTaggingHandlerImpl() {
    return new NetworkPacketTaggingHandlerImpl(
        privilegedOperationExecutorMock, cGroupsHandlerMock) {
        @Override
        public NetworkTagMappingManager createNetworkTagMappingManager(
            Configuration conf) {
          return mockManager;
        }
    };
  }

  @After
  public void teardown() {
    FileUtil.fullyDelete(new File(tmpPath));
  }
}

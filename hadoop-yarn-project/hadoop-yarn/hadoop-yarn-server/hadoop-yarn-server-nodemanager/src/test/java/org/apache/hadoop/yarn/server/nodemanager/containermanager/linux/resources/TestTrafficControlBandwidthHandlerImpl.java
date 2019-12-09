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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TestTrafficControlBandwidthHandlerImpl {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestTrafficControlBandwidthHandlerImpl.class);
  private static final int ROOT_BANDWIDTH_MBIT = 100;
  private static final int YARN_BANDWIDTH_MBIT = 70;
  private static final int TEST_CLASSID = 100;
  private static final String TEST_CLASSID_STR = "42:100";
  private static final String TEST_CONTAINER_ID_STR = "container_01";
  private static final String TEST_TASKS_FILE = "testTasksFile";

  private PrivilegedOperationExecutor privilegedOperationExecutorMock;
  private CGroupsHandler cGroupsHandlerMock;
  private TrafficController trafficControllerMock;
  private Configuration conf;
  private String tmpPath;
  private String device;
  ContainerId containerIdMock;
  Container containerMock;

  @Before
  public void setup() {
    privilegedOperationExecutorMock = mock(PrivilegedOperationExecutor.class);
    cGroupsHandlerMock = mock(CGroupsHandler.class);
    trafficControllerMock = mock(TrafficController.class);
    conf = new YarnConfiguration();
    tmpPath = new StringBuffer(System.getProperty("test.build.data")).append
        ('/').append("hadoop.tmp.dir").toString();
    device = YarnConfiguration.DEFAULT_NM_NETWORK_RESOURCE_INTERFACE;
    containerIdMock = mock(ContainerId.class);
    containerMock = mock(Container.class);
    when(containerIdMock.toString()).thenReturn(TEST_CONTAINER_ID_STR);
    //mock returning a mock - an angel died somewhere.
    when(containerMock.getContainerId()).thenReturn(containerIdMock);

    conf.setInt(YarnConfiguration
        .NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_MBIT, ROOT_BANDWIDTH_MBIT);
    conf.setInt(YarnConfiguration
        .NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_YARN_MBIT, YARN_BANDWIDTH_MBIT);
    conf.set("hadoop.tmp.dir", tmpPath);
    //In these tests, we'll only use TrafficController with recovery disabled
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, false);
  }

  @Test
  public void testBootstrap() {
    TrafficControlBandwidthHandlerImpl handlerImpl = new
        TrafficControlBandwidthHandlerImpl(privilegedOperationExecutorMock,
        cGroupsHandlerMock, trafficControllerMock);

    try {
      handlerImpl.bootstrap(conf);
      verify(cGroupsHandlerMock).initializeCGroupController(
          eq(CGroupsHandler.CGroupController.NET_CLS));
      verifyNoMoreInteractions(cGroupsHandlerMock);
      verify(trafficControllerMock).bootstrap(eq(device),
          eq(ROOT_BANDWIDTH_MBIT),
          eq(YARN_BANDWIDTH_MBIT));
      verifyNoMoreInteractions(trafficControllerMock);
    } catch (ResourceHandlerException e) {
      LOG.error("Unexpected exception: " + e);
      Assert.fail("Caught unexpected ResourceHandlerException!");
    }
  }

  @Test
  public void testLifeCycle() {
    TrafficController trafficControllerSpy = spy(new TrafficController(conf,
        privilegedOperationExecutorMock));
    TrafficControlBandwidthHandlerImpl handlerImpl = new
        TrafficControlBandwidthHandlerImpl(privilegedOperationExecutorMock,
        cGroupsHandlerMock, trafficControllerSpy);

    try {
      handlerImpl.bootstrap(conf);
      testPreStart(trafficControllerSpy, handlerImpl);
      testPostComplete(trafficControllerSpy, handlerImpl);
    } catch (ResourceHandlerException e) {
      LOG.error("Unexpected exception: " + e);
      Assert.fail("Caught unexpected ResourceHandlerException!");
    }
  }

  private void testPreStart(TrafficController trafficControllerSpy,
      TrafficControlBandwidthHandlerImpl handlerImpl) throws
      ResourceHandlerException {
    //This is not the cleanest of solutions - but since we are testing the
    //preStart/postComplete lifecycle, we don't have a different way of
    //handling this - we don't keep track of the number of invocations by
    //a class we are not testing here (TrafficController)
    //So, we'll reset this mock. This is not a problem with other mocks.
    reset(privilegedOperationExecutorMock);

    doReturn(TEST_CLASSID).when(trafficControllerSpy).getNextClassId();
    doReturn(TEST_CLASSID_STR).when(trafficControllerSpy)
        .getStringForNetClsClassId(TEST_CLASSID);
    when(cGroupsHandlerMock.getPathForCGroupTasks(CGroupsHandler
        .CGroupController.NET_CLS, TEST_CONTAINER_ID_STR)).thenReturn(
        TEST_TASKS_FILE);

    List<PrivilegedOperation> ops = handlerImpl.preStart(containerMock);

    //Ensure that cgroups is created and updated as expected
    verify(cGroupsHandlerMock).createCGroup(
        eq(CGroupsHandler.CGroupController.NET_CLS), eq(TEST_CONTAINER_ID_STR));
    verify(cGroupsHandlerMock).updateCGroupParam(
        eq(CGroupsHandler.CGroupController.NET_CLS), eq(TEST_CONTAINER_ID_STR),
        eq(CGroupsHandler.CGROUP_PARAM_CLASSID), eq(TEST_CLASSID_STR));

    //Now check the privileged operations being returned
    //We expect two operations - one for adding pid to tasks file and another
    //for a tc modify operation
    Assert.assertEquals(2, ops.size());

    //Verify that the add pid op is correct
    PrivilegedOperation addPidOp = ops.get(0);
    String expectedAddPidOpArg = PrivilegedOperation.CGROUP_ARG_PREFIX +
        TEST_TASKS_FILE;
    List<String> addPidOpArgs = addPidOp.getArguments();

    Assert.assertEquals(PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
        addPidOp.getOperationType());
    Assert.assertEquals(1, addPidOpArgs.size());
    Assert.assertEquals(expectedAddPidOpArg, addPidOpArgs.get(0));

    //Verify that that tc modify op is correct
    PrivilegedOperation tcModifyOp = ops.get(1);
    List<String> tcModifyOpArgs = tcModifyOp.getArguments();

    Assert.assertEquals(PrivilegedOperation.OperationType.TC_MODIFY_STATE,
        tcModifyOp.getOperationType());
    Assert.assertEquals(1, tcModifyOpArgs.size());
    //verify that the tc command file exists
    Assert.assertTrue(new File(tcModifyOpArgs.get(0)).exists());
  }

  private void testPostComplete(TrafficController trafficControllerSpy,
      TrafficControlBandwidthHandlerImpl handlerImpl) throws
      ResourceHandlerException {
    //This is not the cleanest of solutions - but since we are testing the
    //preStart/postComplete lifecycle, we don't have a different way of
    //handling this - we don't keep track of the number of invocations by
    //a class we are not testing here (TrafficController)
    //So, we'll reset this mock. This is not a problem with other mocks.
    reset(privilegedOperationExecutorMock);

    List<PrivilegedOperation> ops = handlerImpl.postComplete(containerIdMock);

    verify(cGroupsHandlerMock).deleteCGroup(
        eq(CGroupsHandler.CGroupController.NET_CLS), eq(TEST_CONTAINER_ID_STR));

    try {
      //capture privileged op argument and ensure it is correct
      ArgumentCaptor<PrivilegedOperation> opCaptor = ArgumentCaptor.forClass
          (PrivilegedOperation.class);

      verify(privilegedOperationExecutorMock)
          .executePrivilegedOperation(opCaptor.capture(), eq(false));

      List<String> args = opCaptor.getValue().getArguments();

      Assert.assertEquals(PrivilegedOperation.OperationType.TC_MODIFY_STATE,
          opCaptor.getValue().getOperationType());
      Assert.assertEquals(1, args.size());
      //ensure that tc command file exists
      Assert.assertTrue(new File(args.get(0)).exists());

      verify(trafficControllerSpy).releaseClassId(TEST_CLASSID);
    } catch (PrivilegedOperationException e) {
      LOG.error("Caught exception: " + e);
      Assert.fail("Unexpected PrivilegedOperationException from mock!");
    }

    //We don't expect any operations to be returned here
    Assert.assertNull(ops);
  }

  @After
  public void teardown() {
    FileUtil.fullyDelete(new File(tmpPath));
  }
}

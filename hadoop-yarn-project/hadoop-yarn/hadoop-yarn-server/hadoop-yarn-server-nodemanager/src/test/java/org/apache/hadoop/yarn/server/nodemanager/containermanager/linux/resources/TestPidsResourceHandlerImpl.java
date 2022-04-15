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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

/**
 * Test class for CGroupsPidsResourceHandlerImpl.
 *
 */
public class TestPidsResourceHandlerImpl {

  private CGroupsPidsResourceHandlerImpl pidsResourceHandler;
  private CGroupsHandler mockCGroupsHandler;

  @Before
  public void setUp() throws IOException, ResourceHandlerException {
    mockCGroupsHandler = mock(CGroupsHandler.class);
    when(mockCGroupsHandler.getPathForCGroup(any(), any())).thenReturn(".");
    pidsResourceHandler =
        new CGroupsPidsResourceHandlerImpl(mockCGroupsHandler);
  }

  @Test
  public void testBootstrap() throws Exception {
    Configuration conf = new YarnConfiguration();
    List<PrivilegedOperation> ret =
        pidsResourceHandler.bootstrap(conf);
    verify(mockCGroupsHandler, times(1))
        .initializeCGroupController(CGroupsHandler.CGroupController.PIDS);
    Assert.assertNull(ret);
    Assert.assertEquals("Default process number incorrect", 10000,
        pidsResourceHandler.getProcessMaxCount());
  }

  @Test
  public void testProcessNumbers() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.NM_CONTAINER_PROCESS_MAX_LIMIT_NUM, -1);
    try {
      pidsResourceHandler.bootstrap(conf);
      Assert.fail("Negative values for process number should not be allowed.");
    } catch (ResourceHandlerException re) {
      // do nothing
    }

    conf.setInt(YarnConfiguration.NM_CONTAINER_PROCESS_MAX_LIMIT_NUM, 1000);
    pidsResourceHandler.bootstrap(conf);
    Assert.assertEquals("process number value incorrect", 1000,
        pidsResourceHandler.getProcessMaxCount());
  }

  @Test
  public void testPreStart() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(YarnConfiguration.NM_CONTAINER_PROCESS_MAX_LIMIT_NUM, 1024);
    pidsResourceHandler.bootstrap(conf);
    String id = "container_01_01";
    String path = "test-path/" + id;
    ContainerId mockContainerId = mock(ContainerId.class);
    when(mockContainerId.toString()).thenReturn(id);
    Container mockContainer = mock(Container.class);
    when(mockContainer.getContainerId()).thenReturn(mockContainerId);
    when(mockCGroupsHandler
        .getPathForCGroupTasks(CGroupsHandler.CGroupController.PIDS, id))
        .thenReturn(path);
    int maxProcess = 1024;
    List<PrivilegedOperation> ret =
        pidsResourceHandler.preStart(mockContainer);
    verify(mockCGroupsHandler, times(1))
        .createCGroup(CGroupsHandler.CGroupController.PIDS, id);
    verify(mockCGroupsHandler, times(1))
        .updateCGroupParam(CGroupsHandler.CGroupController.PIDS, id,
          CGroupsHandler.CGROUP_PIDS_MAX, String.valueOf(maxProcess));
    Assert.assertNotNull(ret);
    Assert.assertEquals(1, ret.size());
    PrivilegedOperation op = ret.get(0);
    Assert.assertEquals(PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
        op.getOperationType());
    List<String> args = op.getArguments();
    Assert.assertEquals(1, args.size());
    Assert.assertEquals(PrivilegedOperation.CGROUP_ARG_PREFIX + path,
        args.get(0));
  }

  @Test
  public void testReacquireContainer() throws Exception {
    ContainerId containerIdMock = mock(ContainerId.class);
    Assert.assertNull(
        pidsResourceHandler.reacquireContainer(containerIdMock));
  }

  @Test
  public void testPostComplete() throws Exception {
    String id = "container_01_01";
    ContainerId mockContainerId = mock(ContainerId.class);
    when(mockContainerId.toString()).thenReturn(id);
    Assert
        .assertNull(pidsResourceHandler.postComplete(mockContainerId));
    verify(mockCGroupsHandler, times(1))
        .deleteCGroup(CGroupsHandler.CGroupController.PIDS, id);
  }

  @Test
  public void testTeardown() throws Exception {
    Assert.assertNull(pidsResourceHandler.teardown());
  }
}

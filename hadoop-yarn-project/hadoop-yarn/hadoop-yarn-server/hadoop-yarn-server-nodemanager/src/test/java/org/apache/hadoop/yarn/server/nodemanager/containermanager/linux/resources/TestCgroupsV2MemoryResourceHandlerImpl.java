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
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestCgroupsV2MemoryResourceHandlerImpl {

  private CGroupsHandler mockCGroupsHandler;
  private CGroupsV2MemoryResourceHandlerImpl cGroupsMemoryResourceHandler;

  @BeforeEach
  public void setup() {
    mockCGroupsHandler = mock(CGroupsHandler.class);
    when(mockCGroupsHandler.getPathForCGroup(any(), any())).thenReturn(".");
    cGroupsMemoryResourceHandler =
        new CGroupsV2MemoryResourceHandlerImpl(mockCGroupsHandler);
  }

  @Test
  public void testBootstrap() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
    conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);
    List<PrivilegedOperation> ret =
        cGroupsMemoryResourceHandler.bootstrap(conf);
    verify(mockCGroupsHandler, times(1))
        .initializeCGroupController(CGroupsHandler.CGroupController.MEMORY);
    assertNull(ret);
    conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, true);
    try {
      cGroupsMemoryResourceHandler.bootstrap(conf);
    } catch (ResourceHandlerException re) {
      fail("Pmem check should be allowed to run with cgroups");
    }
    conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
    conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, true);
    try {
      cGroupsMemoryResourceHandler.bootstrap(conf);
    } catch (ResourceHandlerException re) {
      fail("Vmem check should be allowed to run with cgroups");
    }
  }

  @Test
  public void testPreStart() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
    conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);
    cGroupsMemoryResourceHandler.bootstrap(conf);
    String id = "container_01_01";
    String path = "test-path/" + id;
    ContainerId mockContainerId = mock(ContainerId.class);
    when(mockContainerId.toString()).thenReturn(id);
    Container mockContainer = mock(Container.class);
    when(mockContainer.getContainerId()).thenReturn(mockContainerId);
    when(mockCGroupsHandler
        .getPathForCGroupTasks(CGroupsHandler.CGroupController.MEMORY, id))
        .thenReturn(path);
    int memory = 1024;
    when(mockContainer.getResource())
        .thenReturn(Resource.newInstance(memory, 1));
    List<PrivilegedOperation> ret =
        cGroupsMemoryResourceHandler.preStart(mockContainer);
    verify(mockCGroupsHandler, times(1))
        .createCGroup(CGroupsHandler.CGroupController.MEMORY, id);
    verify(mockCGroupsHandler, times(1))
        .updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, id,
            CGroupsHandler.CGROUP_MEMORY_MAX,
            String.valueOf(memory) + "M");
    verify(mockCGroupsHandler, times(1))
        .updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, id,
            CGroupsHandler.CGROUP_MEMORY_LOW,
            String.valueOf((int) (memory * 0.9)) + "M");
    assertNotNull(ret);
    assertEquals(1, ret.size());
    PrivilegedOperation op = ret.get(0);
    assertEquals(PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
        op.getOperationType());
    List<String> args = op.getArguments();
    assertEquals(1, args.size());
    assertEquals(PrivilegedOperation.CGROUP_ARG_PREFIX + path,
        args.get(0));
  }

  @Test
  public void testPreStartNonEnforced() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
    conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);
    conf.setBoolean(YarnConfiguration.NM_MEMORY_RESOURCE_ENFORCED, false);
    cGroupsMemoryResourceHandler.bootstrap(conf);
    String id = "container_01_01";
    String path = "test-path/" + id;
    ContainerId mockContainerId = mock(ContainerId.class);
    when(mockContainerId.toString()).thenReturn(id);
    Container mockContainer = mock(Container.class);
    when(mockContainer.getContainerId()).thenReturn(mockContainerId);
    when(mockCGroupsHandler
        .getPathForCGroupTasks(CGroupsHandler.CGroupController.MEMORY, id))
        .thenReturn(path);
    int memory = 1024;
    when(mockContainer.getResource())
        .thenReturn(Resource.newInstance(memory, 1));
    List<PrivilegedOperation> ret =
        cGroupsMemoryResourceHandler.preStart(mockContainer);
    verify(mockCGroupsHandler, times(1))
        .createCGroup(CGroupsHandler.CGroupController.MEMORY, id);
    verify(mockCGroupsHandler, times(0))
        .updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, id,
            CGroupsHandler.CGROUP_MEMORY_MAX,
            String.valueOf(memory) + "M");
    verify(mockCGroupsHandler, times(0))
        .updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, id,
            CGroupsHandler.CGROUP_MEMORY_LOW,
            String.valueOf((int) (memory * 0.9)) + "M");
    assertNotNull(ret);
    assertEquals(1, ret.size());
    PrivilegedOperation op = ret.get(0);
    assertEquals(PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
        op.getOperationType());
    List<String> args = op.getArguments();
    assertEquals(1, args.size());
    assertEquals(PrivilegedOperation.CGROUP_ARG_PREFIX + path,
        args.get(0));
  }

  @Test
  public void testReacquireContainer() throws Exception {
    ContainerId containerIdMock = mock(ContainerId.class);
    assertNull(
        cGroupsMemoryResourceHandler.reacquireContainer(containerIdMock));
  }

  @Test
  public void testPostComplete() throws Exception {
    String id = "container_01_01";
    ContainerId mockContainerId = mock(ContainerId.class);
    when(mockContainerId.toString()).thenReturn(id);
    assertNull(cGroupsMemoryResourceHandler.postComplete(mockContainerId));
    verify(mockCGroupsHandler, times(1))
        .deleteCGroup(CGroupsHandler.CGroupController.MEMORY, id);
  }

  @Test
  public void testTeardown() throws Exception {
    assertNull(cGroupsMemoryResourceHandler.teardown());
  }

  @Test
  public void testOpportunistic() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
    conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);

    cGroupsMemoryResourceHandler.bootstrap(conf);
    ContainerTokenIdentifier tokenId = mock(ContainerTokenIdentifier.class);
    when(tokenId.getExecutionType()).thenReturn(ExecutionType.OPPORTUNISTIC);
    Container container = mock(Container.class);
    String id = "container_01_01";
    ContainerId mockContainerId = mock(ContainerId.class);
    when(mockContainerId.toString()).thenReturn(id);
    when(container.getContainerId()).thenReturn(mockContainerId);
    when(container.getContainerTokenIdentifier()).thenReturn(tokenId);
    when(container.getResource()).thenReturn(Resource.newInstance(1024, 2));
    cGroupsMemoryResourceHandler.preStart(container);
    verify(mockCGroupsHandler, times(1))
        .updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, id,
            CGroupsHandler.CGROUP_MEMORY_LOW, "0M");
    verify(mockCGroupsHandler, times(1))
        .updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, id,
            CGroupsHandler.CGROUP_MEMORY_MAX, "1024M");
  }
}

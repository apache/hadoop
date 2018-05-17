/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

public class TestCGroupsCpuResourceHandlerImpl {

  private CGroupsHandler mockCGroupsHandler;
  private CGroupsCpuResourceHandlerImpl cGroupsCpuResourceHandler;
  private ResourceCalculatorPlugin plugin;
  final int numProcessors = 4;

  @Before
  public void setup() {
    mockCGroupsHandler = mock(CGroupsHandler.class);
    when(mockCGroupsHandler.getPathForCGroup(any(), any())).thenReturn(".");
    cGroupsCpuResourceHandler =
        new CGroupsCpuResourceHandlerImpl(mockCGroupsHandler);

    plugin = mock(ResourceCalculatorPlugin.class);
    Mockito.doReturn(numProcessors).when(plugin).getNumProcessors();
    Mockito.doReturn(numProcessors).when(plugin).getNumCores();
  }

  @Test
  public void testBootstrap() throws Exception {
    Configuration conf = new YarnConfiguration();

    List<PrivilegedOperation> ret =
        cGroupsCpuResourceHandler.bootstrap(plugin, conf);
    verify(mockCGroupsHandler, times(1))
        .initializeCGroupController(CGroupsHandler.CGroupController.CPU);
    verify(mockCGroupsHandler, times(0))
        .updateCGroupParam(CGroupsHandler.CGroupController.CPU, "",
            CGroupsHandler.CGROUP_CPU_PERIOD_US, "");
    verify(mockCGroupsHandler, times(0))
        .updateCGroupParam(CGroupsHandler.CGroupController.CPU, "",
            CGroupsHandler.CGROUP_CPU_QUOTA_US, "");
    Assert.assertNull(ret);
  }

  @Test
  public void testBootstrapLimits() throws Exception {
    Configuration conf = new YarnConfiguration();

    int cpuPerc = 80;
    conf.setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT,
        cpuPerc);
    int period = (CGroupsCpuResourceHandlerImpl.MAX_QUOTA_US * 100) / (cpuPerc
        * numProcessors);
    List<PrivilegedOperation> ret =
        cGroupsCpuResourceHandler.bootstrap(plugin, conf);
    verify(mockCGroupsHandler, times(1))
        .initializeCGroupController(CGroupsHandler.CGroupController.CPU);
    verify(mockCGroupsHandler, times(1))
        .updateCGroupParam(CGroupsHandler.CGroupController.CPU, "",
            CGroupsHandler.CGROUP_CPU_PERIOD_US, String.valueOf(period));
    verify(mockCGroupsHandler, times(1))
        .updateCGroupParam(CGroupsHandler.CGroupController.CPU, "",
            CGroupsHandler.CGROUP_CPU_QUOTA_US,
            String.valueOf(CGroupsCpuResourceHandlerImpl.MAX_QUOTA_US));
    Assert.assertNull(ret);
  }

  @Test
  public void testBootstrapExistingLimits() throws Exception {
    File existingLimit = new File(CGroupsHandler.CGroupController.CPU.getName()
        + "." + CGroupsHandler.CGROUP_CPU_QUOTA_US);
    try {
      FileUtils.write(existingLimit, "10000"); // value doesn't matter
      when(mockCGroupsHandler
          .getPathForCGroup(CGroupsHandler.CGroupController.CPU, ""))
          .thenReturn(".");
      Configuration conf = new YarnConfiguration();

      List<PrivilegedOperation> ret =
          cGroupsCpuResourceHandler.bootstrap(plugin, conf);
      verify(mockCGroupsHandler, times(1))
          .initializeCGroupController(CGroupsHandler.CGroupController.CPU);
      verify(mockCGroupsHandler, times(1))
          .updateCGroupParam(CGroupsHandler.CGroupController.CPU, "",
              CGroupsHandler.CGROUP_CPU_QUOTA_US, "-1");
      Assert.assertNull(ret);
    } finally {
      FileUtils.deleteQuietly(existingLimit);
    }
  }

  @Test
  public void testPreStart() throws Exception {
    String id = "container_01_01";
    String path = "test-path/" + id;
    ContainerId mockContainerId = mock(ContainerId.class);
    when(mockContainerId.toString()).thenReturn(id);
    Container mockContainer = mock(Container.class);
    when(mockContainer.getContainerId()).thenReturn(mockContainerId);
    when(mockCGroupsHandler
        .getPathForCGroupTasks(CGroupsHandler.CGroupController.CPU, id))
        .thenReturn(path);
    when(mockContainer.getResource()).thenReturn(Resource.newInstance(1024, 2));

    List<PrivilegedOperation> ret =
        cGroupsCpuResourceHandler.preStart(mockContainer);
    verify(mockCGroupsHandler, times(1))
        .createCGroup(CGroupsHandler.CGroupController.CPU, id);
    verify(mockCGroupsHandler, times(1))
        .updateCGroupParam(CGroupsHandler.CGroupController.CPU, id,
            CGroupsHandler.CGROUP_CPU_SHARES, String
                .valueOf(CGroupsCpuResourceHandlerImpl.CPU_DEFAULT_WEIGHT * 2));

    // don't set quota or period
    verify(mockCGroupsHandler, never())
        .updateCGroupParam(eq(CGroupsHandler.CGroupController.CPU), eq(id),
            eq(CGroupsHandler.CGROUP_CPU_PERIOD_US), anyString());
    verify(mockCGroupsHandler, never())
        .updateCGroupParam(eq(CGroupsHandler.CGroupController.CPU), eq(id),
            eq(CGroupsHandler.CGROUP_CPU_QUOTA_US), anyString());
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
  public void testPreStartStrictUsage() throws Exception {
    String id = "container_01_01";
    String path = "test-path/" + id;
    ContainerId mockContainerId = mock(ContainerId.class);
    when(mockContainerId.toString()).thenReturn(id);
    Container mockContainer = mock(Container.class);
    when(mockContainer.getContainerId()).thenReturn(mockContainerId);
    when(mockCGroupsHandler
        .getPathForCGroupTasks(CGroupsHandler.CGroupController.CPU, id))
        .thenReturn(path);
    when(mockContainer.getResource()).thenReturn(Resource.newInstance(1024, 1));
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(
        YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE,
        true);
    cGroupsCpuResourceHandler.bootstrap(plugin, conf);
    int defaultVCores = 8;
    float share = (float) numProcessors / (float) defaultVCores;
    List<PrivilegedOperation> ret =
        cGroupsCpuResourceHandler.preStart(mockContainer);
    verify(mockCGroupsHandler, times(1))
        .createCGroup(CGroupsHandler.CGroupController.CPU, id);
    verify(mockCGroupsHandler, times(1))
        .updateCGroupParam(CGroupsHandler.CGroupController.CPU, id,
            CGroupsHandler.CGROUP_CPU_SHARES,
            String.valueOf(CGroupsCpuResourceHandlerImpl.CPU_DEFAULT_WEIGHT));
    // set quota and period
    verify(mockCGroupsHandler, times(1))
        .updateCGroupParam(CGroupsHandler.CGroupController.CPU, id,
            CGroupsHandler.CGROUP_CPU_PERIOD_US,
            String.valueOf(CGroupsCpuResourceHandlerImpl.MAX_QUOTA_US));
    verify(mockCGroupsHandler, times(1))
        .updateCGroupParam(CGroupsHandler.CGroupController.CPU, id,
            CGroupsHandler.CGROUP_CPU_QUOTA_US, String.valueOf(
                (int) (CGroupsCpuResourceHandlerImpl.MAX_QUOTA_US * share)));
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
  public void testPreStartRestrictedContainers() throws Exception {
    String id = "container_01_01";
    String path = "test-path/" + id;
    int defaultVCores = 8;
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(
        YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE,
        true);
    int cpuPerc = 75;
    conf.setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT,
        cpuPerc);
    cGroupsCpuResourceHandler.bootstrap(plugin, conf);
    verify(mockCGroupsHandler, times(1))
        .updateCGroupParam(CGroupsHandler.CGroupController.CPU, "",
            CGroupsHandler.CGROUP_CPU_PERIOD_US, String.valueOf("333333"));
    verify(mockCGroupsHandler, times(1))
        .updateCGroupParam(CGroupsHandler.CGroupController.CPU, "",
            CGroupsHandler.CGROUP_CPU_QUOTA_US,
            String.valueOf(CGroupsCpuResourceHandlerImpl.MAX_QUOTA_US));
    float yarnCores = (cpuPerc * numProcessors) / 100;
    int[] containerVCores = { 2, 4 };
    for (int cVcores : containerVCores) {
      ContainerId mockContainerId = mock(ContainerId.class);
      when(mockContainerId.toString()).thenReturn(id);
      Container mockContainer = mock(Container.class);
      when(mockContainer.getContainerId()).thenReturn(mockContainerId);
      when(mockCGroupsHandler
          .getPathForCGroupTasks(CGroupsHandler.CGroupController.CPU, id))
          .thenReturn(path);
      when(mockContainer.getResource())
          .thenReturn(Resource.newInstance(1024, cVcores));
      when(mockCGroupsHandler
          .getPathForCGroupTasks(CGroupsHandler.CGroupController.CPU, id))
          .thenReturn(path);

      float share = (cVcores * yarnCores) / defaultVCores;
      int quotaUS;
      int periodUS;
      if (share > 1.0f) {
        quotaUS = CGroupsCpuResourceHandlerImpl.MAX_QUOTA_US;
        periodUS =
            (int) ((float) CGroupsCpuResourceHandlerImpl.MAX_QUOTA_US / share);
      } else {
        quotaUS = (int) (CGroupsCpuResourceHandlerImpl.MAX_QUOTA_US * share);
        periodUS = CGroupsCpuResourceHandlerImpl.MAX_QUOTA_US;
      }
      cGroupsCpuResourceHandler.preStart(mockContainer);
      verify(mockCGroupsHandler, times(1))
          .updateCGroupParam(CGroupsHandler.CGroupController.CPU, id,
              CGroupsHandler.CGROUP_CPU_SHARES, String.valueOf(
              CGroupsCpuResourceHandlerImpl.CPU_DEFAULT_WEIGHT * cVcores));
      // set quota and period
      verify(mockCGroupsHandler, times(1))
          .updateCGroupParam(CGroupsHandler.CGroupController.CPU, id,
              CGroupsHandler.CGROUP_CPU_PERIOD_US, String.valueOf(periodUS));
      verify(mockCGroupsHandler, times(1))
          .updateCGroupParam(CGroupsHandler.CGroupController.CPU, id,
              CGroupsHandler.CGROUP_CPU_QUOTA_US, String.valueOf(quotaUS));
    }
  }

  @Test
  public void testReacquireContainer() throws Exception {
    ContainerId containerIdMock = mock(ContainerId.class);
    Assert.assertNull(
        cGroupsCpuResourceHandler.reacquireContainer(containerIdMock));
  }

  @Test
  public void testPostComplete() throws Exception {
    String id = "container_01_01";
    ContainerId mockContainerId = mock(ContainerId.class);
    when(mockContainerId.toString()).thenReturn(id);
    Assert.assertNull(cGroupsCpuResourceHandler.postComplete(mockContainerId));
    verify(mockCGroupsHandler, times(1))
        .deleteCGroup(CGroupsHandler.CGroupController.CPU, id);
  }

  @Test
  public void testTeardown() throws Exception {
    Assert.assertNull(cGroupsCpuResourceHandler.teardown());
  }

  @Test
  public void testStrictResourceUsage() throws Exception {
    Assert.assertNull(cGroupsCpuResourceHandler.teardown());
  }

  @Test
  public void testOpportunistic() throws Exception {
    Configuration conf = new YarnConfiguration();

    cGroupsCpuResourceHandler.bootstrap(plugin, conf);
    ContainerTokenIdentifier tokenId = mock(ContainerTokenIdentifier.class);
    when(tokenId.getExecutionType()).thenReturn(ExecutionType.OPPORTUNISTIC);
    Container container = mock(Container.class);
    String id = "container_01_01";
    ContainerId mockContainerId = mock(ContainerId.class);
    when(mockContainerId.toString()).thenReturn(id);
    when(container.getContainerId()).thenReturn(mockContainerId);
    when(container.getContainerTokenIdentifier()).thenReturn(tokenId);
    when(container.getResource()).thenReturn(Resource.newInstance(1024, 2));
    cGroupsCpuResourceHandler.preStart(container);
    verify(mockCGroupsHandler, times(1))
        .updateCGroupParam(CGroupsHandler.CGroupController.CPU, id,
            CGroupsHandler.CGROUP_CPU_SHARES, "2");
  }

}

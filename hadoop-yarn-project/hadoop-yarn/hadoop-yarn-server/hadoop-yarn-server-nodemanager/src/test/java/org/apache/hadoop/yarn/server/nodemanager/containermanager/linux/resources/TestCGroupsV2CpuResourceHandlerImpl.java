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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestCGroupsV2CpuResourceHandlerImpl {

  private CGroupsHandler mockCGroupsHandler;
  private CGroupsV2CpuResourceHandlerImpl cGroupsCpuResourceHandler;
  private ResourceCalculatorPlugin plugin;
  final int numProcessors = 4;

  @BeforeEach
  public void setup() {
    mockCGroupsHandler = mock(CGroupsHandler.class);
    when(mockCGroupsHandler.getPathForCGroup(any(), any())).thenReturn(".");
    cGroupsCpuResourceHandler =
        new CGroupsV2CpuResourceHandlerImpl(mockCGroupsHandler);

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
            CGroupsHandler.CGROUP_CPU_MAX, "");
    assertNull(ret);
  }

  @Test
  public void testBootstrapLimits() throws Exception {
    Configuration conf = new YarnConfiguration();

    int cpuPerc = 80;
    conf.setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT,
        cpuPerc);
    int period = (CGroupsV2CpuResourceHandlerImpl.MAX_QUOTA_US * 100) / (cpuPerc
        * numProcessors);
    String cpuMaxValue = CGroupsV2CpuResourceHandlerImpl.MAX_QUOTA_US + " " + period;
    List<PrivilegedOperation> ret =
        cGroupsCpuResourceHandler.bootstrap(plugin, conf);
    verify(mockCGroupsHandler, times(1))
        .initializeCGroupController(CGroupsHandler.CGroupController.CPU);
    verify(mockCGroupsHandler, times(1))
        .updateCGroupParam(CGroupsHandler.CGroupController.CPU, "",
            CGroupsHandler.CGROUP_CPU_MAX, cpuMaxValue);
    assertNull(ret);
  }

  @Test
  public void testBootstrapExistingLimits() throws Exception {
    Configuration conf = new YarnConfiguration();

    when(mockCGroupsHandler
        .getCGroupParam(CGroupsHandler.CGroupController.CPU, "",
            CGroupsHandler.CGROUP_CPU_MAX))
        .thenReturn("100 100000");

    List<PrivilegedOperation> ret =
        cGroupsCpuResourceHandler.bootstrap(plugin, conf);
    verify(mockCGroupsHandler, times(1))
        .initializeCGroupController(CGroupsHandler.CGroupController.CPU);
    verify(mockCGroupsHandler, times(2))
        .getCGroupParam(CGroupsHandler.CGroupController.CPU, "",
            CGroupsHandler.CGROUP_CPU_MAX);
    verify(mockCGroupsHandler, times(1))
        .updateCGroupParam(CGroupsHandler.CGroupController.CPU, "",
            CGroupsHandler.CGROUP_CPU_MAX, "max 100000");
    assertNull(ret);
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
            CGroupsHandler.CGROUP_PARAM_WEIGHT, String
                .valueOf(CGroupsV2CpuResourceHandlerImpl.CPU_DEFAULT_WEIGHT * 2)); // 2 vcores

    // don't set cpu.max
    verify(mockCGroupsHandler, never())
        .updateCGroupParam(eq(CGroupsHandler.CGroupController.CPU), eq(id),
            eq(CGroupsHandler.CGROUP_CPU_MAX), anyString());

    validatePrivilegedOperationList(ret, path);
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
            CGroupsHandler.CGROUP_PARAM_WEIGHT,
            String.valueOf(CGroupsV2CpuResourceHandlerImpl.CPU_DEFAULT_WEIGHT));

    // set quota and period
    String cpuMaxValue = (int) (CGroupsV2CpuResourceHandlerImpl.MAX_QUOTA_US * share) +
        " " + CGroupsV2CpuResourceHandlerImpl.MAX_QUOTA_US;
    verify(mockCGroupsHandler, times(1))
        .updateCGroupParam(CGroupsHandler.CGroupController.CPU, id,
            CGroupsHandler.CGROUP_CPU_MAX, cpuMaxValue);

    validatePrivilegedOperationList(ret, path);
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

    String maxCpuLimit = CGroupsV2CpuResourceHandlerImpl.MAX_QUOTA_US + " " +
        CGroupsV2CpuResourceHandlerImpl.MAX_QUOTA_US * 100 / (cpuPerc * numProcessors);
    verify(mockCGroupsHandler, times(1))
        .updateCGroupParam(CGroupsHandler.CGroupController.CPU, "",
            CGroupsHandler.CGROUP_CPU_MAX, maxCpuLimit);

    float yarnCores = (float) (cpuPerc * numProcessors) / 100;
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
        quotaUS = CGroupsV2CpuResourceHandlerImpl.MAX_QUOTA_US;
        periodUS =
            (int) ((float) CGroupsV2CpuResourceHandlerImpl.MAX_QUOTA_US / share);
      } else {
        quotaUS = (int) (CGroupsV2CpuResourceHandlerImpl.MAX_QUOTA_US * share);
        periodUS = CGroupsV2CpuResourceHandlerImpl.MAX_QUOTA_US;
      }

      cGroupsCpuResourceHandler.preStart(mockContainer);

      verify(mockCGroupsHandler, times(1))
          .updateCGroupParam(CGroupsHandler.CGroupController.CPU, id,
              CGroupsHandler.CGROUP_PARAM_WEIGHT, String.valueOf(
                  CGroupsV2CpuResourceHandlerImpl.CPU_DEFAULT_WEIGHT * cVcores));

      // set cpu.max
      verify(mockCGroupsHandler, times(1))
          .updateCGroupParam(CGroupsHandler.CGroupController.CPU, id,
              CGroupsHandler.CGROUP_CPU_MAX, quotaUS + " " + periodUS);
    }
  }

  @Test
  public void testReacquireContainer() throws Exception {
    ContainerId containerIdMock = mock(ContainerId.class);
    assertNull(
        cGroupsCpuResourceHandler.reacquireContainer(containerIdMock));
  }

  @Test
  public void testPostComplete() throws Exception {
    String id = "container_01_01";
    ContainerId mockContainerId = mock(ContainerId.class);
    when(mockContainerId.toString()).thenReturn(id);
    assertNull(cGroupsCpuResourceHandler.postComplete(mockContainerId));
    verify(mockCGroupsHandler, times(1))
        .deleteCGroup(CGroupsHandler.CGroupController.CPU, id);
  }

  @Test
  public void testTeardown() throws Exception {
    assertNull(cGroupsCpuResourceHandler.teardown());
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
            CGroupsHandler.CGROUP_PARAM_WEIGHT, String.valueOf(
                CGroupsV2CpuResourceHandlerImpl.CPU_DEFAULT_WEIGHT_OPPORTUNISTIC));
  }

  private void validatePrivilegedOperationList(List<PrivilegedOperation> ops, String path) {
    assertNotNull(ops);
    assertEquals(1, ops.size());
    PrivilegedOperation op = ops.get(0);
    assertEquals(PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
        op.getOperationType());
    List<String> args = op.getArguments();
    assertEquals(1, args.size());
    assertEquals(PrivilegedOperation.CGROUP_ARG_PREFIX + path,
        args.get(0));
  }
}

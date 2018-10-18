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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.gpu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuDevice;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuDiscoverer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.util.resource.TestResourceUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestGpuResourceHandler {
  private CGroupsHandler mockCGroupsHandler;
  private PrivilegedOperationExecutor mockPrivilegedExecutor;
  private GpuResourceHandlerImpl gpuResourceHandler;
  private NMStateStoreService mockNMStateStore;
  private ConcurrentHashMap<ContainerId, Container> runningContainersMap;

  @Before
  public void setup() {
    TestResourceUtils.addNewTypesToResources(ResourceInformation.GPU_URI);

    mockCGroupsHandler = mock(CGroupsHandler.class);
    mockPrivilegedExecutor = mock(PrivilegedOperationExecutor.class);
    mockNMStateStore = mock(NMStateStoreService.class);

    Configuration conf = new Configuration();

    Context nmctx = mock(Context.class);
    when(nmctx.getNMStateStore()).thenReturn(mockNMStateStore);
    when(nmctx.getConf()).thenReturn(conf);
    runningContainersMap = new ConcurrentHashMap<>();
    when(nmctx.getContainers()).thenReturn(runningContainersMap);

    gpuResourceHandler = new GpuResourceHandlerImpl(nmctx, mockCGroupsHandler,
        mockPrivilegedExecutor);
  }

  @Test
  public void testBootStrap() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_GPU_ALLOWED_DEVICES, "0:0");

    GpuDiscoverer.getInstance().initialize(conf);

    gpuResourceHandler.bootstrap(conf);
    verify(mockCGroupsHandler, times(1)).initializeCGroupController(
        CGroupsHandler.CGroupController.DEVICES);
  }

  private static ContainerId getContainerId(int id) {
    return ContainerId.newContainerId(ApplicationAttemptId
        .newInstance(ApplicationId.newInstance(1234L, 1), 1), id);
  }

  private static Container mockContainerWithGpuRequest(int id, int numGpuRequest,
      boolean dockerContainerEnabled) {
    Container c = mock(Container.class);
    when(c.getContainerId()).thenReturn(getContainerId(id));

    Resource res = Resource.newInstance(1024, 1);
    ResourceMappings resMapping = new ResourceMappings();

    res.setResourceValue(ResourceInformation.GPU_URI, numGpuRequest);
    when(c.getResource()).thenReturn(res);
    when(c.getResourceMappings()).thenReturn(resMapping);

    ContainerLaunchContext clc = mock(ContainerLaunchContext.class);
    Map<String, String> env = new HashMap<>();
    if (dockerContainerEnabled) {
      env.put(ContainerRuntimeConstants.ENV_CONTAINER_TYPE, "docker");
    }
    when(clc.getEnvironment()).thenReturn(env);
    when(c.getLaunchContext()).thenReturn(clc);
    return c;
  }

  private static Container mockContainerWithGpuRequest(int id,
      int numGpuRequest) {
    return mockContainerWithGpuRequest(id, numGpuRequest, false);
  }

  private void verifyDeniedDevices(ContainerId containerId,
      List<GpuDevice> deniedDevices)
      throws ResourceHandlerException, PrivilegedOperationException {
    verify(mockCGroupsHandler, times(1)).createCGroup(
        CGroupsHandler.CGroupController.DEVICES, containerId.toString());

    if (null != deniedDevices && !deniedDevices.isEmpty()) {
      List<Integer> deniedDevicesMinorNumber = new ArrayList<>();
      for (GpuDevice deniedDevice : deniedDevices) {
        deniedDevicesMinorNumber.add(deniedDevice.getMinorNumber());
      }
      verify(mockPrivilegedExecutor, times(1)).executePrivilegedOperation(
          new PrivilegedOperation(PrivilegedOperation.OperationType.GPU, Arrays
              .asList(GpuResourceHandlerImpl.CONTAINER_ID_CLI_OPTION,
                  containerId.toString(),
                  GpuResourceHandlerImpl.EXCLUDED_GPUS_CLI_OPTION,
                  StringUtils.join(",", deniedDevicesMinorNumber))), true);
    }
  }

  private void commonTestAllocation(boolean dockerContainerEnabled)
      throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_GPU_ALLOWED_DEVICES, "0:0,1:1,2:3,3:4");
    GpuDiscoverer.getInstance().initialize(conf);

    gpuResourceHandler.bootstrap(conf);
    Assert.assertEquals(4,
        gpuResourceHandler.getGpuAllocator().getAvailableGpus());

    /* Start container 1, asks 3 containers */
    gpuResourceHandler.preStart(
        mockContainerWithGpuRequest(1, 3, dockerContainerEnabled));

    // Only device=4 will be blocked.
    if (dockerContainerEnabled) {
      verifyDeniedDevices(getContainerId(1), Collections.emptyList());
    } else{
      verifyDeniedDevices(getContainerId(1), Arrays.asList(new GpuDevice(3,4)));
    }

    /* Start container 2, asks 2 containers. Excepted to fail */
    boolean failedToAllocate = false;
    try {
      gpuResourceHandler.preStart(
          mockContainerWithGpuRequest(2, 2, dockerContainerEnabled));
    } catch (ResourceHandlerException e) {
      failedToAllocate = true;
    }
    Assert.assertTrue(failedToAllocate);

    /* Start container 3, ask 1 container, succeeded */
    gpuResourceHandler.preStart(
        mockContainerWithGpuRequest(3, 1, dockerContainerEnabled));

    // devices = 0/1/3 will be blocked
    if (dockerContainerEnabled) {
      verifyDeniedDevices(getContainerId(3), Collections.emptyList());
    } else {
      verifyDeniedDevices(getContainerId(3), Arrays
          .asList(new GpuDevice(0, 0), new GpuDevice(1, 1),
              new GpuDevice(2, 3)));
    }


    /* Start container 4, ask 0 container, succeeded */
    gpuResourceHandler.preStart(
        mockContainerWithGpuRequest(4, 0, dockerContainerEnabled));

    if (dockerContainerEnabled) {
      verifyDeniedDevices(getContainerId(4), Collections.emptyList());
    } else{
      // All devices will be blocked
      verifyDeniedDevices(getContainerId(4), Arrays
          .asList(new GpuDevice(0, 0), new GpuDevice(1, 1), new GpuDevice(2, 3),
              new GpuDevice(3, 4)));
    }

    /* Release container-1, expect cgroups deleted */
    gpuResourceHandler.postComplete(getContainerId(1));

    verify(mockCGroupsHandler, times(1)).createCGroup(
        CGroupsHandler.CGroupController.DEVICES, getContainerId(1).toString());
    Assert.assertEquals(3,
        gpuResourceHandler.getGpuAllocator().getAvailableGpus());

    /* Release container-3, expect cgroups deleted */
    gpuResourceHandler.postComplete(getContainerId(3));

    verify(mockCGroupsHandler, times(1)).createCGroup(
        CGroupsHandler.CGroupController.DEVICES, getContainerId(3).toString());
    Assert.assertEquals(4,
        gpuResourceHandler.getGpuAllocator().getAvailableGpus());
  }

  @Test
  public void testAllocationWhenDockerContainerEnabled() throws Exception {
    // When docker container is enabled, no devices should be written to
    // devices.deny.
    commonTestAllocation(true);
  }

  @Test
  public void testAllocation() throws Exception {
    commonTestAllocation(false);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testAssignedGpuWillBeCleanedupWhenStoreOpFails()
      throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_GPU_ALLOWED_DEVICES, "0:0,1:1,2:3,3:4");
    GpuDiscoverer.getInstance().initialize(conf);

    gpuResourceHandler.bootstrap(conf);
    Assert.assertEquals(4,
        gpuResourceHandler.getGpuAllocator().getAvailableGpus());

    doThrow(new IOException("Exception ...")).when(mockNMStateStore)
        .storeAssignedResources(
        any(Container.class), anyString(), anyList());

    boolean exception = false;
    /* Start container 1, asks 3 containers */
    try {
      gpuResourceHandler.preStart(mockContainerWithGpuRequest(1, 3));
    } catch (ResourceHandlerException e) {
      exception = true;
    }

    Assert.assertTrue("preStart should throw exception", exception);

    // After preStart, we still have 4 available GPU since the store op fails.
    Assert.assertEquals(4,
        gpuResourceHandler.getGpuAllocator().getAvailableGpus());
  }

  @Test
  public void testAllocationWithoutAllowedGpus() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_GPU_ALLOWED_DEVICES, " ");
    GpuDiscoverer.getInstance().initialize(conf);

    try {
      gpuResourceHandler.bootstrap(conf);
      Assert.fail("Should fail because no GPU available");
    } catch (ResourceHandlerException e) {
      // Expected because of no resource available
    }

    /* Start container 1, asks 0 containers */
    gpuResourceHandler.preStart(mockContainerWithGpuRequest(1, 0));
    verifyDeniedDevices(getContainerId(1), Collections.emptyList());

    /* Start container 2, asks 1 containers. Excepted to fail */
    boolean failedToAllocate = false;
    try {
      gpuResourceHandler.preStart(mockContainerWithGpuRequest(2, 1));
    } catch (ResourceHandlerException e) {
      failedToAllocate = true;
    }
    Assert.assertTrue(failedToAllocate);

    /* Release container 1, expect cgroups deleted */
    gpuResourceHandler.postComplete(getContainerId(1));

    verify(mockCGroupsHandler, times(1)).createCGroup(
        CGroupsHandler.CGroupController.DEVICES, getContainerId(1).toString());
    Assert.assertEquals(0,
        gpuResourceHandler.getGpuAllocator().getAvailableGpus());
  }

  @Test
  public void testAllocationStored() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_GPU_ALLOWED_DEVICES, "0:0,1:1,2:3,3:4");
    GpuDiscoverer.getInstance().initialize(conf);

    gpuResourceHandler.bootstrap(conf);
    Assert.assertEquals(4,
        gpuResourceHandler.getGpuAllocator().getAvailableGpus());

    /* Start container 1, asks 3 containers */
    Container container = mockContainerWithGpuRequest(1, 3);
    gpuResourceHandler.preStart(container);

    verify(mockNMStateStore).storeAssignedResources(container,
        ResourceInformation.GPU_URI, Arrays
            .asList(new GpuDevice(0, 0), new GpuDevice(1, 1),
                new GpuDevice(2, 3)));

    // Only device=4 will be blocked.
    verifyDeniedDevices(getContainerId(1), Arrays.asList(new GpuDevice(3, 4)));

    /* Start container 2, ask 0 container, succeeded */
    container = mockContainerWithGpuRequest(2, 0);
    gpuResourceHandler.preStart(container);

    verifyDeniedDevices(getContainerId(2), Arrays
        .asList(new GpuDevice(0, 0), new GpuDevice(1, 1), new GpuDevice(2, 3),
            new GpuDevice(3, 4)));
    Assert.assertEquals(0, container.getResourceMappings()
        .getAssignedResources(ResourceInformation.GPU_URI).size());

    // Store assigned resource will not be invoked.
    verify(mockNMStateStore, never()).storeAssignedResources(
        eq(container), eq(ResourceInformation.GPU_URI), anyList());
  }

  @Test
  public void testAllocationStoredWithNULLStateStore() throws Exception {
    NMNullStateStoreService mockNMNULLStateStore = mock(NMNullStateStoreService.class);

    Configuration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_GPU_ALLOWED_DEVICES, "0:0,1:1,2:3,3:4");

    Context nmnctx = mock(Context.class);
    when(nmnctx.getNMStateStore()).thenReturn(mockNMNULLStateStore);
    when(nmnctx.getConf()).thenReturn(conf);

    GpuResourceHandlerImpl gpuNULLStateResourceHandler =
        new GpuResourceHandlerImpl(nmnctx, mockCGroupsHandler,
        mockPrivilegedExecutor);

    GpuDiscoverer.getInstance().initialize(conf);

    gpuNULLStateResourceHandler.bootstrap(conf);
    Assert.assertEquals(4,
        gpuNULLStateResourceHandler.getGpuAllocator().getAvailableGpus());

    /* Start container 1, asks 3 containers */
    Container container = mockContainerWithGpuRequest(1, 3);
    gpuNULLStateResourceHandler.preStart(container);

    verify(nmnctx.getNMStateStore()).storeAssignedResources(container,
        ResourceInformation.GPU_URI, Arrays
            .asList(new GpuDevice(0, 0), new GpuDevice(1, 1),
                new GpuDevice(2, 3)));
  }

  @Test
  public void testRecoverResourceAllocation() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_GPU_ALLOWED_DEVICES, "0:0,1:1,2:3,3:4");
    GpuDiscoverer.getInstance().initialize(conf);

    gpuResourceHandler.bootstrap(conf);
    Assert.assertEquals(4,
        gpuResourceHandler.getGpuAllocator().getAvailableGpus());

    Container nmContainer = mock(Container.class);
    ResourceMappings rmap = new ResourceMappings();
    ResourceMappings.AssignedResources ar =
        new ResourceMappings.AssignedResources();
    ar.updateAssignedResources(
        Arrays.asList(new GpuDevice(1, 1), new GpuDevice(2, 3)));
    rmap.addAssignedResources(ResourceInformation.GPU_URI, ar);
    when(nmContainer.getResourceMappings()).thenReturn(rmap);

    runningContainersMap.put(getContainerId(1), nmContainer);

    // TEST CASE
    // Reacquire container restore state of GPU Resource Allocator.
    gpuResourceHandler.reacquireContainer(getContainerId(1));

    Map<GpuDevice, ContainerId> deviceAllocationMapping =
        gpuResourceHandler.getGpuAllocator().getDeviceAllocationMappingCopy();
    Assert.assertEquals(2, deviceAllocationMapping.size());
    Assert.assertTrue(
        deviceAllocationMapping.keySet().contains(new GpuDevice(1, 1)));
    Assert.assertTrue(
        deviceAllocationMapping.keySet().contains(new GpuDevice(2, 3)));
    Assert.assertEquals(deviceAllocationMapping.get(new GpuDevice(1, 1)),
        getContainerId(1));

    // TEST CASE
    // Try to reacquire a container but requested device is not in allowed list.
    nmContainer = mock(Container.class);
    rmap = new ResourceMappings();
    ar = new ResourceMappings.AssignedResources();
    // id=5 is not in allowed list.
    ar.updateAssignedResources(
        Arrays.asList(new GpuDevice(3, 4), new GpuDevice(4, 5)));
    rmap.addAssignedResources(ResourceInformation.GPU_URI, ar);
    when(nmContainer.getResourceMappings()).thenReturn(rmap);

    runningContainersMap.put(getContainerId(2), nmContainer);

    boolean caughtException = false;
    try {
      gpuResourceHandler.reacquireContainer(getContainerId(1));
    } catch (ResourceHandlerException e) {
      caughtException = true;
    }
    Assert.assertTrue(
        "Should fail since requested device Id is not in allowed list",
        caughtException);

    // Make sure internal state not changed.
    deviceAllocationMapping =
        gpuResourceHandler.getGpuAllocator().getDeviceAllocationMappingCopy();
    Assert.assertEquals(2, deviceAllocationMapping.size());
    Assert.assertTrue(deviceAllocationMapping.keySet()
        .containsAll(Arrays.asList(new GpuDevice(1, 1), new GpuDevice(2, 3))));
    Assert.assertEquals(deviceAllocationMapping.get(new GpuDevice(1, 1)),
        getContainerId(1));

    // TEST CASE
    // Try to reacquire a container but requested device is already assigned.
    nmContainer = mock(Container.class);
    rmap = new ResourceMappings();
    ar = new ResourceMappings.AssignedResources();
    // id=3 is already assigned
    ar.updateAssignedResources(
        Arrays.asList(new GpuDevice(3, 4), new GpuDevice(2, 3)));
    rmap.addAssignedResources("gpu", ar);
    when(nmContainer.getResourceMappings()).thenReturn(rmap);

    runningContainersMap.put(getContainerId(2), nmContainer);

    caughtException = false;
    try {
      gpuResourceHandler.reacquireContainer(getContainerId(1));
    } catch (ResourceHandlerException e) {
      caughtException = true;
    }
    Assert.assertTrue(
        "Should fail since requested device Id is not in allowed list",
        caughtException);

    // Make sure internal state not changed.
    deviceAllocationMapping =
        gpuResourceHandler.getGpuAllocator().getDeviceAllocationMappingCopy();
    Assert.assertEquals(2, deviceAllocationMapping.size());
    Assert.assertTrue(deviceAllocationMapping.keySet()
        .containsAll(Arrays.asList(new GpuDevice(1, 1), new GpuDevice(2, 3))));
    Assert.assertEquals(deviceAllocationMapping.get(new GpuDevice(1, 1)),
        getContainerId(1));
  }
}

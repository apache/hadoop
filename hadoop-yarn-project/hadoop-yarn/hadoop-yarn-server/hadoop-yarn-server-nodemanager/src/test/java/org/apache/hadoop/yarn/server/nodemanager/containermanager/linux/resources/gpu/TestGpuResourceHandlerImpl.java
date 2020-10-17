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

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
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
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.NvidiaBinaryHelper;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.util.resource.CustomResourceTypesConfigurationProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestGpuResourceHandlerImpl {
  private CGroupsHandler mockCGroupsHandler;
  private PrivilegedOperationExecutor mockPrivilegedExecutor;
  private GpuResourceHandlerImpl gpuResourceHandler;
  private NMStateStoreService mockNMStateStore;
  private ConcurrentHashMap<ContainerId, Container> runningContainersMap;
  private GpuDiscoverer gpuDiscoverer;
  private File testDataDirectory;

  public void createTestDataDirectory() throws IOException {
    String testDirectoryPath = getTestParentDirectory();
    testDataDirectory = new File(testDirectoryPath);
    FileUtils.deleteDirectory(testDataDirectory);
    testDataDirectory.mkdirs();
  }

  private String getTestParentDirectory() {
    File f = new File("target/temp/" + TestGpuResourceHandlerImpl.class.getName());
    return f.getAbsolutePath();
  }

  private void touchFile(File f) throws IOException {
    new FileOutputStream(f).close();
  }

  private Configuration createDefaultConfig() throws IOException {
    Configuration conf = new YarnConfiguration();
    File fakeBinary = setupFakeGpuDiscoveryBinary();
    conf.set(YarnConfiguration.NM_GPU_PATH_TO_EXEC,
        fakeBinary.getAbsolutePath());
    return conf;
  }

  private File setupFakeGpuDiscoveryBinary() throws IOException {
    File fakeBinary = new File(getTestParentDirectory() + "/nvidia-smi");
    touchFile(fakeBinary);
    return fakeBinary;
  }

  @Rule
  public ExpectedException expected = ExpectedException.none();

  private NvidiaBinaryHelper nvidiaBinaryHelper;

  @Before
  public void setup() throws IOException {
    createTestDataDirectory();
    nvidiaBinaryHelper = new NvidiaBinaryHelper();
    CustomResourceTypesConfigurationProvider.
        initResourceTypes(ResourceInformation.GPU_URI);

    mockCGroupsHandler = mock(CGroupsHandler.class);
    mockPrivilegedExecutor = mock(PrivilegedOperationExecutor.class);
    mockNMStateStore = mock(NMStateStoreService.class);

    Configuration conf = new Configuration();
    Context nmContext = createMockNmContext(conf);

    gpuDiscoverer = new GpuDiscoverer();
    gpuResourceHandler = new GpuResourceHandlerImpl(nmContext,
        mockCGroupsHandler, mockPrivilegedExecutor, gpuDiscoverer);
  }

  private Context createMockNmContext(Configuration conf) {
    Context nmctx = mock(Context.class);
    when(nmctx.getNMStateStore()).thenReturn(mockNMStateStore);
    when(nmctx.getConf()).thenReturn(conf);
    runningContainersMap = new ConcurrentHashMap<>();
    when(nmctx.getContainers()).thenReturn(runningContainersMap);
    return nmctx;
  }

  @After
  public void cleanupTestFiles() throws IOException {
    FileUtils.deleteDirectory(testDataDirectory);
    nvidiaBinaryHelper = new NvidiaBinaryHelper();
  }

  @Test
  public void testBootstrapWithRealGpuDiscoverer() throws Exception {
    Configuration conf = createDefaultConfig();
    conf.set(YarnConfiguration.NM_GPU_ALLOWED_DEVICES, "0:0");
    gpuDiscoverer.initialize(conf, nvidiaBinaryHelper);

    gpuResourceHandler.bootstrap(conf);

    List<GpuDevice> allowedGpus =
        gpuResourceHandler.getGpuAllocator().getAllowedGpus();
    assertEquals("Unexpected number of allowed GPU devices!", 1,
        allowedGpus.size());
    assertEquals("Expected GPU device does not equal to found device!",
        new GpuDevice(0, 0), allowedGpus.get(0));
    verify(mockCGroupsHandler).initializeCGroupController(
        CGroupsHandler.CGroupController.DEVICES);
  }

  @Test
  public void testBootstrapWithMockGpuDiscoverer() throws Exception {
    GpuDiscoverer mockDiscoverer = mock(GpuDiscoverer.class);
    Configuration conf = new YarnConfiguration();
    mockDiscoverer.initialize(conf, nvidiaBinaryHelper);

    expected.expect(ResourceHandlerException.class);
    gpuResourceHandler.bootstrap(conf);
  }

  private static ContainerId getContainerId(int id) {
    return ContainerId.newContainerId(ApplicationAttemptId
        .newInstance(ApplicationId.newInstance(1234L, 1), 1), id);
  }

  private static Container mockContainerWithGpuRequest(int id, Resource res,
      ContainerLaunchContext launchContext) {
    Container c = mock(Container.class);
    when(c.getContainerId()).thenReturn(getContainerId(id));
    when(c.getResource()).thenReturn(res);
    when(c.getResourceMappings()).thenReturn(new ResourceMappings());
    when(c.getLaunchContext()).thenReturn(launchContext);
    return c;
  }

  private static Resource createResourceRequest(int numGpuRequest) {
    Resource res = Resource.newInstance(1024, 1);
    res.setResourceValue(ResourceInformation.GPU_URI, numGpuRequest);
    return res;
  }

  private static Container mockContainerWithGpuRequest(int id,
      Resource res) {
    return mockContainerWithGpuRequest(id, res, createLaunchContext());
  }

  private void verifyDeniedDevices(ContainerId containerId,
      List<GpuDevice> deniedDevices)
      throws ResourceHandlerException, PrivilegedOperationException {
    verify(mockCGroupsHandler).createCGroup(
        CGroupsHandler.CGroupController.DEVICES, containerId.toString());

    if (null != deniedDevices && !deniedDevices.isEmpty()) {
      List<Integer> deniedDevicesMinorNumber = new ArrayList<>();
      for (GpuDevice deniedDevice : deniedDevices) {
        deniedDevicesMinorNumber.add(deniedDevice.getMinorNumber());
      }
      verify(mockPrivilegedExecutor).executePrivilegedOperation(
          new PrivilegedOperation(PrivilegedOperation.OperationType.GPU, Arrays
              .asList(GpuResourceHandlerImpl.CONTAINER_ID_CLI_OPTION,
                  containerId.toString(),
                  GpuResourceHandlerImpl.EXCLUDED_GPUS_CLI_OPTION,
                  StringUtils.join(",", deniedDevicesMinorNumber))), true);
    }
  }

  private static ContainerLaunchContext createLaunchContextDocker() {
    ContainerLaunchContext launchContext = mock(ContainerLaunchContext.class);
    ImmutableMap<String, String> env = ImmutableMap.<String, String>builder()
        .put(ContainerRuntimeConstants.ENV_CONTAINER_TYPE,
            ContainerRuntimeConstants.CONTAINER_RUNTIME_DOCKER)
        .build();
    when(launchContext.getEnvironment()).thenReturn(env);
    return launchContext;
  }

  private static ContainerLaunchContext createLaunchContext() {
    ContainerLaunchContext launchContext = mock(ContainerLaunchContext.class);
    when(launchContext.getEnvironment()).thenReturn(Maps.newHashMap());
    return launchContext;
  }

  private void startContainerWithGpuRequestsDocker(int id, int gpus)
      throws ResourceHandlerException {
    gpuResourceHandler.preStart(
        mockContainerWithGpuRequest(id, createResourceRequest(gpus),
            createLaunchContextDocker()));
  }

  private void startContainerWithGpuRequests(int id, int gpus)
      throws ResourceHandlerException {
    gpuResourceHandler.preStart(
        mockContainerWithGpuRequest(id, createResourceRequest(gpus),
            createLaunchContext()));
  }

  private void verifyNumberOfAvailableGpus(int expectedAvailable,
      GpuResourceHandlerImpl resourceHandler) {
    assertEquals("Unexpected number of available GPU devices!",
        expectedAvailable,
        resourceHandler.getGpuAllocator().getAvailableGpus());
  }

  private void verifyCgroupsDeletedForContainer(int i)
      throws ResourceHandlerException {
    verify(mockCGroupsHandler).createCGroup(
        CGroupsHandler.CGroupController.DEVICES, getContainerId(i).toString());
  }

  private void initializeGpus() throws YarnException, IOException {
    Configuration conf = createDefaultConfig();
    conf.set(YarnConfiguration.NM_GPU_ALLOWED_DEVICES, "0:0,1:1,2:3,3:4");

    gpuDiscoverer = new GpuDiscoverer();
    gpuDiscoverer.initialize(conf, nvidiaBinaryHelper);
    Context nmContext = createMockNmContext(conf);
    gpuResourceHandler = new GpuResourceHandlerImpl(nmContext,
        mockCGroupsHandler, mockPrivilegedExecutor, gpuDiscoverer);

    gpuResourceHandler.bootstrap(conf);
    verifyNumberOfAvailableGpus(4, gpuResourceHandler);
  }

  @Test
  public void testAllocationWhenDockerContainerEnabled() throws Exception {
    // When docker container is enabled, no devices should be written to
    // devices.deny.
    initializeGpus();

    startContainerWithGpuRequestsDocker(1, 3);
    verifyDeniedDevices(getContainerId(1), Collections.emptyList());

    /* Start container 2, asks 2 containers. Excepted to fail */
    boolean failedToAllocate = false;
    try {
      startContainerWithGpuRequestsDocker(2, 2);
    } catch (ResourceHandlerException e) {
      failedToAllocate = true;
    }
    assertTrue("Container allocation is expected to fail!", failedToAllocate);

    startContainerWithGpuRequestsDocker(3, 1);
    verifyDeniedDevices(getContainerId(3), Collections.emptyList());

    startContainerWithGpuRequestsDocker(4, 0);
    verifyDeniedDevices(getContainerId(4), Collections.emptyList());

    gpuResourceHandler.postComplete(getContainerId(1));
    verifyCgroupsDeletedForContainer(1);
    verifyNumberOfAvailableGpus(3, gpuResourceHandler);

    gpuResourceHandler.postComplete(getContainerId(3));
    verifyCgroupsDeletedForContainer(3);
    verifyNumberOfAvailableGpus(4, gpuResourceHandler);
  }

  @Test
  public void testAllocation() throws Exception {
    initializeGpus();

    //Start container 1, asks 3 containers --> Only device=4 will be blocked.
    startContainerWithGpuRequests(1, 3);
    verifyDeniedDevices(getContainerId(1),
        Collections.singletonList(new GpuDevice(3, 4)));

    /* Start container 2, asks 2 containers. Excepted to fail */
    boolean failedToAllocate = false;
    try {
      startContainerWithGpuRequests(2, 2);
    } catch (ResourceHandlerException e) {
      failedToAllocate = true;
    }
    assertTrue("Container allocation is expected to fail!", failedToAllocate);

    // Start container 3, ask 1 container, succeeded
    // devices = 0/1/3 will be blocked
    startContainerWithGpuRequests(3, 1);
    verifyDeniedDevices(getContainerId(3), Arrays.asList(new GpuDevice(0, 0),
        new GpuDevice(1, 1), new GpuDevice(2, 3)));

    // Start container 4, ask 0 container, succeeded
    // --> All devices will be blocked
    startContainerWithGpuRequests(4, 0);
    verifyDeniedDevices(getContainerId(4), Arrays.asList(new GpuDevice(0, 0),
        new GpuDevice(1, 1), new GpuDevice(2, 3), new GpuDevice(3, 4)));

    gpuResourceHandler.postComplete(getContainerId(1));
    verifyCgroupsDeletedForContainer(1);
    verifyNumberOfAvailableGpus(3, gpuResourceHandler);

    gpuResourceHandler.postComplete(getContainerId(3));
    verifyCgroupsDeletedForContainer(3);
    verifyNumberOfAvailableGpus(4, gpuResourceHandler);
  }

  @Test
  public void testAssignedGpuWillBeCleanedUpWhenStoreOpFails()
      throws Exception {
    initializeGpus();

    doThrow(new IOException("Exception ...")).when(mockNMStateStore)
        .storeAssignedResources(
        any(Container.class), anyString(), anyList());

    boolean exception = false;
    /* Start container 1, asks 3 containers */
    try {
      gpuResourceHandler.preStart(mockContainerWithGpuRequest(1,
          createResourceRequest(3)));
    } catch (ResourceHandlerException e) {
      exception = true;
    }

    assertTrue("preStart should throw exception", exception);

    // After preStart, we still have 4 available GPU since the store op failed.
    verifyNumberOfAvailableGpus(4, gpuResourceHandler);
  }

  @Test
  public void testAllocationWithoutAllowedGpus() throws Exception {
    Configuration conf = createDefaultConfig();
    conf.set(YarnConfiguration.NM_GPU_ALLOWED_DEVICES, " ");
    gpuDiscoverer.initialize(conf, nvidiaBinaryHelper);

    try {
      gpuResourceHandler.bootstrap(conf);
      fail("Should fail because no GPU available");
    } catch (ResourceHandlerException e) {
      // Expected because of no resource available
    }

    /* Start container 1, asks 0 containers */
    gpuResourceHandler.preStart(mockContainerWithGpuRequest(1,
        createResourceRequest(0)));
    verifyDeniedDevices(getContainerId(1), Collections.emptyList());

    /* Start container 2, asks 1 containers. Excepted to fail */
    boolean failedToAllocate = false;
    try {
      gpuResourceHandler.preStart(mockContainerWithGpuRequest(2,
          createResourceRequest(1)));
    } catch (ResourceHandlerException e) {
      failedToAllocate = true;
    }
    assertTrue("Container allocation is expected to fail!", failedToAllocate);

    /* Release container 1, expect cgroups deleted */
    gpuResourceHandler.postComplete(getContainerId(1));

    verifyCgroupsDeletedForContainer(1);
    verifyNumberOfAvailableGpus(0, gpuResourceHandler);
  }

  @Test
  public void testAllocationStored() throws Exception {
    initializeGpus();

    /* Start container 1, asks 3 containers */
    Container container = mockContainerWithGpuRequest(1,
        createResourceRequest(3));
    gpuResourceHandler.preStart(container);

    verify(mockNMStateStore).storeAssignedResources(container,
        ResourceInformation.GPU_URI, Arrays
            .asList(new GpuDevice(0, 0), new GpuDevice(1, 1),
                new GpuDevice(2, 3)));

    // Only device=4 will be blocked.
    verifyDeniedDevices(getContainerId(1),
        Collections.singletonList(new GpuDevice(3, 4)));

    /* Start container 2, ask 0 container, succeeded */
    container = mockContainerWithGpuRequest(2, createResourceRequest(0));
    gpuResourceHandler.preStart(container);

    verifyDeniedDevices(getContainerId(2), Arrays
        .asList(new GpuDevice(0, 0), new GpuDevice(1, 1), new GpuDevice(2, 3),
            new GpuDevice(3, 4)));
    assertEquals("Number of GPU device allocations is not the expected!", 0,
        container.getResourceMappings()
        .getAssignedResources(ResourceInformation.GPU_URI).size());

    // Store assigned resource will not be invoked.
    verify(mockNMStateStore, never()).storeAssignedResources(
        eq(container), eq(ResourceInformation.GPU_URI), anyList());
  }

  @Test
  public void testAllocationStoredWithNULLStateStore() throws Exception {
    NMNullStateStoreService mockNMNULLStateStore =
        mock(NMNullStateStoreService.class);

    Configuration conf = createDefaultConfig();
    conf.set(YarnConfiguration.NM_GPU_ALLOWED_DEVICES, "0:0,1:1,2:3,3:4");

    Context nmnctx = mock(Context.class);
    when(nmnctx.getNMStateStore()).thenReturn(mockNMNULLStateStore);
    when(nmnctx.getConf()).thenReturn(conf);

    GpuResourceHandlerImpl gpuNULLStateResourceHandler =
        new GpuResourceHandlerImpl(nmnctx, mockCGroupsHandler,
        mockPrivilegedExecutor, gpuDiscoverer);

    gpuDiscoverer.initialize(conf, nvidiaBinaryHelper);

    gpuNULLStateResourceHandler.bootstrap(conf);
    verifyNumberOfAvailableGpus(4, gpuNULLStateResourceHandler);

    /* Start container 1, asks 3 containers */
    Container container = mockContainerWithGpuRequest(1,
        createResourceRequest(3));
    gpuNULLStateResourceHandler.preStart(container);

    verify(nmnctx.getNMStateStore()).storeAssignedResources(container,
        ResourceInformation.GPU_URI, Arrays
            .asList(new GpuDevice(0, 0), new GpuDevice(1, 1),
                new GpuDevice(2, 3)));
  }

  @Test
  public void testRecoverResourceAllocation() throws Exception {
    initializeGpus();

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
        gpuResourceHandler.getGpuAllocator().getDeviceAllocationMapping();
    assertEquals("Unexpected number of allocated GPU devices!", 2,
        deviceAllocationMapping.size());
    assertTrue("Expected GPU device is not found in allocations!",
        deviceAllocationMapping.keySet().contains(new GpuDevice(1, 1)));
    assertTrue("Expected GPU device is not found in allocations!",
        deviceAllocationMapping.keySet().contains(new GpuDevice(2, 3)));
    assertEquals("GPU device is not assigned to the expected container!",
        deviceAllocationMapping.get(new GpuDevice(1, 1)),
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
    assertTrue(
        "Should fail since requested device Id is not in allowed list",
        caughtException);

    // Make sure internal state not changed.
    deviceAllocationMapping =
        gpuResourceHandler.getGpuAllocator().getDeviceAllocationMapping();
    assertEquals("Unexpected number of allocated GPU devices!",
        2, deviceAllocationMapping.size());
    assertTrue("Expected GPU devices are not found in allocations!",
        deviceAllocationMapping.keySet()
        .containsAll(Arrays.asList(new GpuDevice(1, 1), new GpuDevice(2, 3))));
    assertEquals("GPU device is not assigned to the expected container!",
        deviceAllocationMapping.get(new GpuDevice(1, 1)),
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
    assertTrue(
        "Should fail since requested device Id is already assigned",
        caughtException);

    // Make sure internal state not changed.
    deviceAllocationMapping =
        gpuResourceHandler.getGpuAllocator().getDeviceAllocationMapping();
    assertEquals("Unexpected number of allocated GPU devices!",
        2, deviceAllocationMapping.size());
    assertTrue("Expected GPU devices are not found in allocations!",
        deviceAllocationMapping.keySet()
        .containsAll(Arrays.asList(new GpuDevice(1, 1), new GpuDevice(2, 3))));
    assertEquals("GPU device is not assigned to the expected container!",
        deviceAllocationMapping.get(new GpuDevice(1, 1)),
        getContainerId(1));
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework;


import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePluginScheduler;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRegisterRequest;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRuntimeSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.MountDeviceSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.MountVolumeSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.VolumeSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.YarnRuntimeType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRunCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerVolumeCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.DockerCommandPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePluginManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMMemoryStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NMDeviceResourceInfo;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.TestResourceUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;

/**
 * Unit tests for DevicePluginAdapter.
 * About interaction with vendor plugin
 * */
public class TestDevicePluginAdapter {

  protected static final Logger LOG =
      LoggerFactory.getLogger(TestDevicePluginAdapter.class);

  private YarnConfiguration conf;
  private String tempResourceTypesFile;
  private CGroupsHandler mockCGroupsHandler;
  private PrivilegedOperationExecutor mockPrivilegedExecutor;

  @Before
  public void setup() throws Exception {
    this.conf = new YarnConfiguration();
    // setup resource-types.xml
    ResourceUtils.resetResourceTypes();
    String resourceTypesFile = "resource-types-pluggable-devices.xml";
    this.tempResourceTypesFile =
        TestResourceUtils.setupResourceTypes(this.conf, resourceTypesFile);
    mockCGroupsHandler = mock(CGroupsHandler.class);
    mockPrivilegedExecutor = mock(PrivilegedOperationExecutor.class);
  }

  @After
  public void tearDown() throws IOException {
    // cleanup resource-types.xml
    File dest = new File(this.tempResourceTypesFile);
    if (dest.exists()) {
      dest.delete();
    }
  }


  /**
   * Use the MyPlugin which implement {@code DevicePlugin}.
   * Plugin's initialization is tested in TestResourcePluginManager
   * */
  @Test
  public void testBasicWorkflow()
      throws YarnException, IOException {
    NodeManager.NMContext context = mock(NodeManager.NMContext.class);
    NMStateStoreService storeService = mock(NMStateStoreService.class);
    when(context.getNMStateStore()).thenReturn(storeService);
    when(context.getConf()).thenReturn(this.conf);
    doNothing().when(storeService).storeAssignedResources(isA(Container.class),
        isA(String.class),
        isA(ArrayList.class));
    // Init scheduler manager
    DeviceMappingManager dmm = new DeviceMappingManager(context);
    ResourcePluginManager rpm = mock(ResourcePluginManager.class);
    when(rpm.getDeviceMappingManager()).thenReturn(dmm);
    // Init an plugin
    MyPlugin plugin = new MyPlugin();
    MyPlugin spyPlugin = spy(plugin);
    String resourceName = MyPlugin.RESOURCE_NAME;
    // Init an adapter for the plugin
    DevicePluginAdapter adapter = new DevicePluginAdapter(
        resourceName,
        spyPlugin, dmm);
    // Bootstrap, adding device
    adapter.initialize(context);
    // Use mock shell when create resourceHandler
    ShellWrapper mockShellWrapper = mock(ShellWrapper.class);
    when(mockShellWrapper.existFile(anyString())).thenReturn(true);
    when(mockShellWrapper.getDeviceFileType(anyString())).thenReturn("c");
    DeviceResourceHandlerImpl drhl = new DeviceResourceHandlerImpl(resourceName,
        adapter, dmm, mockCGroupsHandler, mockPrivilegedExecutor, context,
        mockShellWrapper);
    adapter.setDeviceResourceHandler(drhl);
    adapter.getDeviceResourceHandler().bootstrap(conf);
    verify(mockCGroupsHandler).initializeCGroupController(
        CGroupsHandler.CGroupController.DEVICES);
    int size = dmm.getAvailableDevices(resourceName);
    Assert.assertEquals(3, size);
    // Case 1. A container c1 requests 1 device
    Container c1 = mockContainerWithDeviceRequest(1,
        resourceName,
        1, false);
    // preStart
    adapter.getDeviceResourceHandler().preStart(c1);
    // check book keeping
    Assert.assertEquals(2,
        dmm.getAvailableDevices(resourceName));
    Assert.assertEquals(1,
        dmm.getAllUsedDevices().get(resourceName).size());
    Assert.assertEquals(3,
        dmm.getAllAllowedDevices().get(resourceName).size());
    Assert.assertEquals(1,
        dmm.getAllocatedDevices(resourceName, c1.getContainerId()).size());
    verify(mockShellWrapper, times(2)).getDeviceFileType(anyString());
    // check device cgroup create operation
    checkCgroupOperation(c1.getContainerId().toString(), 1,
        "c-256:1-rwm,c-256:2-rwm", "256:0");
    // postComplete
    adapter.getDeviceResourceHandler().postComplete(getContainerId(1));
    Assert.assertEquals(3,
        dmm.getAvailableDevices(resourceName));
    Assert.assertEquals(0,
        dmm.getAllUsedDevices().get(resourceName).size());
    Assert.assertEquals(3,
        dmm.getAllAllowedDevices().get(resourceName).size());
    // check cgroup delete operation
    verify(mockCGroupsHandler).deleteCGroup(
        CGroupsHandler.CGroupController.DEVICES,
        c1.getContainerId().toString());
    // Case 2. A container c2 requests 3 device
    Container c2 = mockContainerWithDeviceRequest(2,
        resourceName,
        3, false);
    reset(mockShellWrapper);
    reset(mockCGroupsHandler);
    reset(mockPrivilegedExecutor);
    when(mockShellWrapper.existFile(anyString())).thenReturn(true);
    when(mockShellWrapper.getDeviceFileType(anyString())).thenReturn("c");
    // preStart
    adapter.getDeviceResourceHandler().preStart(c2);
    // check book keeping
    Assert.assertEquals(0,
        dmm.getAvailableDevices(resourceName));
    Assert.assertEquals(3,
        dmm.getAllUsedDevices().get(resourceName).size());
    Assert.assertEquals(3,
        dmm.getAllAllowedDevices().get(resourceName).size());
    Assert.assertEquals(3,
        dmm.getAllocatedDevices(resourceName, c2.getContainerId()).size());
    verify(mockShellWrapper, times(0)).getDeviceFileType(anyString());
    // check device cgroup create operation
    verify(mockCGroupsHandler).createCGroup(
        CGroupsHandler.CGroupController.DEVICES,
        c2.getContainerId().toString());
    // check device cgroup update operation
    checkCgroupOperation(c2.getContainerId().toString(), 1,
        null, "256:0,256:1,256:2");
    // postComplete
    adapter.getDeviceResourceHandler().postComplete(getContainerId(2));
    Assert.assertEquals(3,
        dmm.getAvailableDevices(resourceName));
    Assert.assertEquals(0,
        dmm.getAllUsedDevices().get(resourceName).size());
    Assert.assertEquals(3,
        dmm.getAllAllowedDevices().get(resourceName).size());
    // check cgroup delete operation
    verify(mockCGroupsHandler).deleteCGroup(
        CGroupsHandler.CGroupController.DEVICES,
        c2.getContainerId().toString());
    // Case 3. A container c3 request 0 device
    Container c3 = mockContainerWithDeviceRequest(3,
        resourceName,
        0, false);
    reset(mockShellWrapper);
    reset(mockCGroupsHandler);
    reset(mockPrivilegedExecutor);
    when(mockShellWrapper.existFile(anyString())).thenReturn(true);
    when(mockShellWrapper.getDeviceFileType(anyString())).thenReturn("c");
    // preStart
    adapter.getDeviceResourceHandler().preStart(c3);
    // check book keeping
    Assert.assertEquals(3,
        dmm.getAvailableDevices(resourceName));
    Assert.assertEquals(0,
        dmm.getAllUsedDevices().get(resourceName).size());
    Assert.assertEquals(3,
        dmm.getAllAllowedDevices().get(resourceName).size());
    verify(mockShellWrapper, times(3)).getDeviceFileType(anyString());
    // check device cgroup create operation
    verify(mockCGroupsHandler).createCGroup(
        CGroupsHandler.CGroupController.DEVICES,
        c3.getContainerId().toString());
    // check device cgroup update operation
    checkCgroupOperation(c3.getContainerId().toString(), 1,
        "c-256:0-rwm,c-256:1-rwm,c-256:2-rwm", null);
    // postComplete
    adapter.getDeviceResourceHandler().postComplete(getContainerId(3));
    Assert.assertEquals(3,
        dmm.getAvailableDevices(resourceName));
    Assert.assertEquals(0,
        dmm.getAllUsedDevices().get(resourceName).size());
    Assert.assertEquals(3,
        dmm.getAllAllowedDevices().get(resourceName).size());
    Assert.assertEquals(0,
        dmm.getAllocatedDevices(resourceName, c3.getContainerId()).size());
    // check cgroup delete operation
    verify(mockCGroupsHandler).deleteCGroup(
        CGroupsHandler.CGroupController.DEVICES,
        c3.getContainerId().toString());
  }

  private void checkCgroupOperation(String cId,
      int invokeTimesOfPrivilegedExecutor,
      String excludedParam, String allowedParam)
      throws PrivilegedOperationException, ResourceHandlerException {
    verify(mockCGroupsHandler).createCGroup(
        CGroupsHandler.CGroupController.DEVICES,
        cId);
    // check device cgroup update operation
    ArgumentCaptor<PrivilegedOperation> args =
        ArgumentCaptor.forClass(PrivilegedOperation.class);
    verify(mockPrivilegedExecutor, times(invokeTimesOfPrivilegedExecutor))
        .executePrivilegedOperation(args.capture(), eq(true));
    Assert.assertEquals(PrivilegedOperation.OperationType.DEVICE,
        args.getValue().getOperationType());
    List<String> expectedArgs = new ArrayList<>();
    expectedArgs.add(DeviceResourceHandlerImpl.CONTAINER_ID_CLI_OPTION);
    expectedArgs.add(cId);
    if (excludedParam != null && !excludedParam.isEmpty()) {
      expectedArgs.add(DeviceResourceHandlerImpl.EXCLUDED_DEVICES_CLI_OPTION);
      expectedArgs.add(excludedParam);
    }
    if (allowedParam != null && !allowedParam.isEmpty()) {
      expectedArgs.add(DeviceResourceHandlerImpl.ALLOWED_DEVICES_CLI_OPTION);
      expectedArgs.add(allowedParam);
    }
    Assert.assertArrayEquals(expectedArgs.toArray(),
        args.getValue().getArguments().toArray());
  }

  @Test
  public void testDeviceResourceUpdaterImpl() throws YarnException {
    Resource nodeResource = mock(Resource.class);
    NodeManager.NMContext context = mock(NodeManager.NMContext.class);
    // Init an plugin
    MyPlugin plugin = new MyPlugin();
    MyPlugin spyPlugin = spy(plugin);
    String resourceName = MyPlugin.RESOURCE_NAME;
    // Init scheduler manager
    DeviceMappingManager dmm = new DeviceMappingManager(context);
    // Init an adapter for the plugin
    DevicePluginAdapter adapter = new DevicePluginAdapter(
        resourceName, spyPlugin, dmm);
    adapter.initialize(mock(Context.class));
    adapter.getNodeResourceHandlerInstance()
        .updateConfiguredResource(nodeResource);
    verify(spyPlugin, times(1)).getDevices();
    verify(nodeResource, times(1)).setResourceValue(
        resourceName, 3);
  }

  @Test
  public void testStoreDeviceSchedulerManagerState()
      throws IOException, YarnException {
    NodeManager.NMContext context = mock(NodeManager.NMContext.class);
    NMStateStoreService realStoreService = new NMMemoryStateStoreService();
    NMStateStoreService storeService = spy(realStoreService);
    when(context.getNMStateStore()).thenReturn(storeService);
    when(context.getConf()).thenReturn(this.conf);
    doNothing().when(storeService).storeAssignedResources(isA(Container.class),
        isA(String.class),
        isA(ArrayList.class));

    // Init scheduler manager
    DeviceMappingManager dmm = new DeviceMappingManager(context);

    ResourcePluginManager rpm = mock(ResourcePluginManager.class);
    when(rpm.getDeviceMappingManager()).thenReturn(dmm);

    // Init an plugin
    MyPlugin plugin = new MyPlugin();
    MyPlugin spyPlugin = spy(plugin);
    String resourceName = MyPlugin.RESOURCE_NAME;
    // Init an adapter for the plugin
    DevicePluginAdapter adapter = new DevicePluginAdapter(
        resourceName,
        spyPlugin, dmm);
    // Bootstrap, adding device
    adapter.initialize(context);
    adapter.createResourceHandler(context,
        mockCGroupsHandler, mockPrivilegedExecutor);
    adapter.getDeviceResourceHandler().bootstrap(conf);

    // A container c0 requests 1 device
    Container c0 = mockContainerWithDeviceRequest(0,
        resourceName,
        1, false);
    // preStart
    adapter.getDeviceResourceHandler().preStart(c0);
    // ensure container1's resource is persistent
    verify(storeService).storeAssignedResources(c0, resourceName,
        Arrays.asList(Device.Builder.newInstance()
            .setId(0)
            .setDevPath("/dev/hdwA0")
            .setMajorNumber(256)
            .setMinorNumber(0)
            .setBusID("0000:80:00.0")
            .setHealthy(true)
            .build()));
  }

  @Test
  public void testRecoverDeviceSchedulerManagerState()
      throws IOException, YarnException {
    NodeManager.NMContext context = mock(NodeManager.NMContext.class);
    NMStateStoreService realStoreService = new NMMemoryStateStoreService();
    NMStateStoreService storeService = spy(realStoreService);
    when(context.getNMStateStore()).thenReturn(storeService);
    doNothing().when(storeService).storeAssignedResources(isA(Container.class),
        isA(String.class),
        isA(ArrayList.class));

    // Init scheduler manager
    DeviceMappingManager dmm = new DeviceMappingManager(context);

    ResourcePluginManager rpm = mock(ResourcePluginManager.class);
    when(rpm.getDeviceMappingManager()).thenReturn(dmm);

    // Init an plugin
    MyPlugin plugin = new MyPlugin();
    MyPlugin spyPlugin = spy(plugin);
    String resourceName = MyPlugin.RESOURCE_NAME;
    // Init an adapter for the plugin
    DevicePluginAdapter adapter = new DevicePluginAdapter(
        resourceName,
        spyPlugin, dmm);
    // Bootstrap, adding device
    adapter.initialize(context);
    adapter.createResourceHandler(context,
        mockCGroupsHandler, mockPrivilegedExecutor);
    adapter.getDeviceResourceHandler().bootstrap(conf);
    Assert.assertEquals(3,
        dmm.getAllAllowedDevices().get(resourceName).size());
    // mock NMStateStore
    Device storedDevice = Device.Builder.newInstance()
        .setId(0)
        .setDevPath("/dev/hdwA0")
        .setMajorNumber(256)
        .setMinorNumber(0)
        .setBusID("0000:80:00.0")
        .setHealthy(true)
        .build();
    ConcurrentHashMap<ContainerId, Container> runningContainersMap
        = new ConcurrentHashMap<>();
    Container nmContainer = mock(Container.class);
    ResourceMappings rmap = new ResourceMappings();
    ResourceMappings.AssignedResources ar =
        new ResourceMappings.AssignedResources();
    ar.updateAssignedResources(
        Arrays.asList(storedDevice));
    rmap.addAssignedResources(resourceName, ar);
    when(nmContainer.getResourceMappings()).thenReturn(rmap);
    when(context.getContainers()).thenReturn(runningContainersMap);

    // Test case 1. c0 get recovered. scheduler state restored
    runningContainersMap.put(getContainerId(0), nmContainer);
    adapter.getDeviceResourceHandler().reacquireContainer(
        getContainerId(0));
    Assert.assertEquals(3,
        dmm.getAllAllowedDevices().get(resourceName).size());
    Assert.assertEquals(1,
        dmm.getAllUsedDevices().get(resourceName).size());
    Assert.assertEquals(2,
        dmm.getAvailableDevices(resourceName));
    Map<Device, ContainerId> used = dmm.getAllUsedDevices().get(resourceName);
    Assert.assertTrue(used.keySet().contains(storedDevice));

    // Test case 2. c1 wants get recovered.
    // But stored device is already allocated to c2
    nmContainer = mock(Container.class);
    rmap = new ResourceMappings();
    ar = new ResourceMappings.AssignedResources();
    ar.updateAssignedResources(
        Arrays.asList(storedDevice));
    rmap.addAssignedResources(resourceName, ar);
    // already assigned to c1
    runningContainersMap.put(getContainerId(2), nmContainer);
    boolean caughtException = false;
    try {
      adapter.getDeviceResourceHandler().reacquireContainer(getContainerId(1));
    } catch (ResourceHandlerException e) {
      caughtException = true;
    }
    Assert.assertTrue(
        "Should fail since requested device is assigned already",
        caughtException);
    // don't affect c0 allocation state
    Assert.assertEquals(3,
        dmm.getAllAllowedDevices().get(resourceName).size());
    Assert.assertEquals(1,
        dmm.getAllUsedDevices().get(resourceName).size());
    Assert.assertEquals(2,
        dmm.getAvailableDevices(resourceName));
    used = dmm.getAllUsedDevices().get(resourceName);
    Assert.assertTrue(used.keySet().contains(storedDevice));
  }

  @Test
  public void testAssignedDeviceCleanupWhenStoreOpFails()
      throws IOException, YarnException {
    NodeManager.NMContext context = mock(NodeManager.NMContext.class);
    NMStateStoreService realStoreService = new NMMemoryStateStoreService();
    NMStateStoreService storeService = spy(realStoreService);
    when(context.getConf()).thenReturn(this.conf);
    when(context.getNMStateStore()).thenReturn(storeService);
    doThrow(new IOException("Exception ...")).when(storeService)
        .storeAssignedResources(isA(Container.class),
        isA(String.class),
        isA(ArrayList.class));

    // Init scheduler manager
    DeviceMappingManager dmm = new DeviceMappingManager(context);

    ResourcePluginManager rpm = mock(ResourcePluginManager.class);
    when(rpm.getDeviceMappingManager()).thenReturn(dmm);

    // Init an plugin
    MyPlugin plugin = new MyPlugin();
    MyPlugin spyPlugin = spy(plugin);
    String resourceName = MyPlugin.RESOURCE_NAME;
    // Init an adapter for the plugin
    DevicePluginAdapter adapter = new DevicePluginAdapter(
        resourceName,
        spyPlugin, dmm);
    // Bootstrap, adding device
    adapter.initialize(context);
    adapter.createResourceHandler(context,
        mockCGroupsHandler, mockPrivilegedExecutor);
    adapter.getDeviceResourceHandler().bootstrap(conf);

    // A container c0 requests 1 device
    Container c0 = mockContainerWithDeviceRequest(0,
        resourceName,
        1, false);
    // preStart
    boolean exception = false;
    try {
      adapter.getDeviceResourceHandler().preStart(c0);
    } catch (ResourceHandlerException e) {
      exception = true;
    }
    Assert.assertTrue("Should throw exception in preStart", exception);
    // no device assigned
    Assert.assertEquals(3,
        dmm.getAllAllowedDevices().get(resourceName).size());
    Assert.assertEquals(0,
        dmm.getAllUsedDevices().get(resourceName).size());
    Assert.assertEquals(3,
        dmm.getAvailableDevices(resourceName));

  }

  @Test
  public void testPreferPluginScheduler() throws IOException, YarnException {
    NodeManager.NMContext context = mock(NodeManager.NMContext.class);
    NMStateStoreService storeService = mock(NMStateStoreService.class);
    when(context.getNMStateStore()).thenReturn(storeService);
    when(context.getConf()).thenReturn(this.conf);
    doNothing().when(storeService).storeAssignedResources(isA(Container.class),
        isA(String.class),
        isA(ArrayList.class));

    // Init scheduler manager
    DeviceMappingManager dmm = new DeviceMappingManager(context);

    ResourcePluginManager rpm = mock(ResourcePluginManager.class);
    when(rpm.getDeviceMappingManager()).thenReturn(dmm);

    // Init an plugin
    MyPlugin plugin = new MyPlugin();
    MyPlugin spyPlugin = spy(plugin);
    String resourceName = MyPlugin.RESOURCE_NAME;
    // Add plugin to DeviceMappingManager
    dmm.getDevicePluginSchedulers().put(MyPlugin.RESOURCE_NAME, spyPlugin);
    // Init an adapter for the plugin
    DevicePluginAdapter adapter = new DevicePluginAdapter(
        resourceName,
        spyPlugin, dmm);
    // Bootstrap, adding device
    adapter.initialize(context);
    adapter.createResourceHandler(context,
        mockCGroupsHandler, mockPrivilegedExecutor);
    adapter.getDeviceResourceHandler().bootstrap(conf);
    int size = dmm.getAvailableDevices(resourceName);
    Assert.assertEquals(3, size);

    // A container c1 requests 1 device
    Container c1 = mockContainerWithDeviceRequest(0,
        resourceName,
        1, false);
    // preStart
    adapter.getDeviceResourceHandler().preStart(c1);
    // Use customized scheduler
    verify(spyPlugin, times(1)).allocateDevices(
        anySet(), anyInt(), anyMap());
    Assert.assertEquals(2,
        dmm.getAvailableDevices(resourceName));
    Assert.assertEquals(1,
        dmm.getAllUsedDevices().get(resourceName).size());
    Assert.assertEquals(3,
        dmm.getAllAllowedDevices().get(resourceName).size());
  }

  private static Container mockContainerWithDeviceRequest(int id,
      String resourceName,
      int numDeviceRequest,
      boolean dockerContainerEnabled) {
    Container c = mock(Container.class);
    when(c.getContainerId()).thenReturn(getContainerId(id));

    Resource res = Resource.newInstance(1024, 1);
    ResourceMappings resMapping = new ResourceMappings();

    res.setResourceValue(resourceName, numDeviceRequest);
    when(c.getResource()).thenReturn(res);
    when(c.getResourceMappings()).thenReturn(resMapping);

    ContainerLaunchContext clc = mock(ContainerLaunchContext.class);
    Map<String, String> env = new HashMap<>();
    if (dockerContainerEnabled) {
      env.put(ContainerRuntimeConstants.ENV_CONTAINER_TYPE,
          ContainerRuntimeConstants.CONTAINER_RUNTIME_DOCKER);
    }
    when(clc.getEnvironment()).thenReturn(env);
    when(c.getLaunchContext()).thenReturn(clc);
    return c;
  }

  /**
   * Ensure correct return value generated.
   * */
  @Test
  public void testNMResourceInfoRESTAPI() throws IOException, YarnException {
    NodeManager.NMContext context = mock(NodeManager.NMContext.class);
    NMStateStoreService storeService = mock(NMStateStoreService.class);
    when(context.getNMStateStore()).thenReturn(storeService);
    when(context.getConf()).thenReturn(this.conf);
    doNothing().when(storeService).storeAssignedResources(isA(Container.class),
        isA(String.class),
        isA(ArrayList.class));

    // Init scheduler manager
    DeviceMappingManager dmm = new DeviceMappingManager(context);

    ResourcePluginManager rpm = mock(ResourcePluginManager.class);
    when(rpm.getDeviceMappingManager()).thenReturn(dmm);

    // Init an plugin
    MyPlugin plugin = new MyPlugin();
    MyPlugin spyPlugin = spy(plugin);
    String resourceName = MyPlugin.RESOURCE_NAME;
    // Init an adapter for the plugin
    DevicePluginAdapter adapter = new DevicePluginAdapter(
        resourceName,
        spyPlugin, dmm);
    // Bootstrap, adding device
    adapter.initialize(context);
    adapter.createResourceHandler(context,
        mockCGroupsHandler, mockPrivilegedExecutor);
    adapter.getDeviceResourceHandler().bootstrap(conf);
    int size = dmm.getAvailableDevices(resourceName);
    Assert.assertEquals(3, size);

    // A container c1 requests 1 device
    Container c1 = mockContainerWithDeviceRequest(0,
        resourceName,
        1, false);
    // preStart
    adapter.getDeviceResourceHandler().preStart(c1);
    // check book keeping
    Assert.assertEquals(2,
        dmm.getAvailableDevices(resourceName));
    Assert.assertEquals(1,
        dmm.getAllUsedDevices().get(resourceName).size());
    Assert.assertEquals(3,
        dmm.getAllAllowedDevices().get(resourceName).size());
    // get REST return value
    NMDeviceResourceInfo response =
        (NMDeviceResourceInfo) adapter.getNMResourceInfo();
    Assert.assertEquals(1, response.getAssignedDevices().size());
    Assert.assertEquals(3, response.getTotalDevices().size());
    Device device = response.getAssignedDevices().get(0).getDevice();
    String cId = response.getAssignedDevices().get(0).getContainerId();
    Assert.assertTrue(dmm.getAllAllowedDevices().get(resourceName)
        .contains(device));
    Assert.assertTrue(dmm.getAllUsedDevices().get(resourceName)
        .containsValue(ContainerId.fromString(cId)));
    //finish container
    adapter.getDeviceResourceHandler().postComplete(getContainerId(0));
    response =
        (NMDeviceResourceInfo) adapter.getNMResourceInfo();
    Assert.assertEquals(0, response.getAssignedDevices().size());
    Assert.assertEquals(3, response.getTotalDevices().size());
  }

  /**
   * Test a container run command update when using Docker runtime.
   * And the device plugin it uses is like Nvidia Docker v1.
   * */
  @Test
  public void testDeviceResourceDockerRuntimePlugin1() throws Exception {
    NodeManager.NMContext context = mock(NodeManager.NMContext.class);
    NMStateStoreService storeService = mock(NMStateStoreService.class);
    when(context.getNMStateStore()).thenReturn(storeService);
    when(context.getConf()).thenReturn(this.conf);
    doNothing().when(storeService).storeAssignedResources(isA(Container.class),
        isA(String.class),
        isA(ArrayList.class));
    // Init scheduler manager
    DeviceMappingManager dmm = new DeviceMappingManager(context);
    DeviceMappingManager spyDmm = spy(dmm);
    ResourcePluginManager rpm = mock(ResourcePluginManager.class);
    when(rpm.getDeviceMappingManager()).thenReturn(spyDmm);
    // Init a plugin
    MyPlugin plugin = new MyPlugin();
    MyPlugin spyPlugin = spy(plugin);
    String resourceName = MyPlugin.RESOURCE_NAME;
    // Init an adapter for the plugin
    DevicePluginAdapter adapter = new DevicePluginAdapter(
        resourceName,
        spyPlugin, spyDmm);
    adapter.initialize(context);
    // Bootstrap, adding device
    adapter.initialize(context);
    adapter.createResourceHandler(context,
        mockCGroupsHandler, mockPrivilegedExecutor);
    adapter.getDeviceResourceHandler().bootstrap(conf);
    // Case 1. A container request Docker runtime and 1 device
    Container c1 = mockContainerWithDeviceRequest(1, resourceName, 1, true);
    // generate spec based on v1
    spyPlugin.setDevicePluginVersion("v1");
    // preStart will do allocation
    adapter.getDeviceResourceHandler().preStart(c1);
    Set<Device> allocatedDevice = spyDmm.getAllocatedDevices(resourceName,
        c1.getContainerId());
    reset(spyDmm);
    // c1 is requesting docker runtime.
    // it will create parent cgroup but no cgroups update operation needed.
    // check device cgroup create operation
    verify(mockCGroupsHandler).createCGroup(
        CGroupsHandler.CGroupController.DEVICES,
        c1.getContainerId().toString());
    // ensure no cgroups update operation
    verify(mockPrivilegedExecutor, times(0))
        .executePrivilegedOperation(
            any(PrivilegedOperation.class), anyBoolean());
    DockerCommandPlugin dcp = adapter.getDockerCommandPluginInstance();
    // When DockerLinuxContainerRuntime invoke the DockerCommandPluginInstance
    // First to create volume
    DockerVolumeCommand dvc = dcp.getCreateDockerVolumeCommand(c1);
    // ensure that allocation is get once from device mapping manager
    verify(spyDmm).getAllocatedDevices(resourceName, c1.getContainerId());
    // ensure that plugin's onDeviceAllocated is invoked
    verify(spyPlugin).onDevicesAllocated(
        allocatedDevice,
        YarnRuntimeType.RUNTIME_DEFAULT);
    verify(spyPlugin).onDevicesAllocated(
        allocatedDevice,
        YarnRuntimeType.RUNTIME_DOCKER);
    Assert.assertEquals("nvidia-docker", dvc.getDriverName());
    Assert.assertEquals("create", dvc.getSubCommand());
    Assert.assertEquals("nvidia_driver_352.68", dvc.getVolumeName());

    // then the DockerLinuxContainerRuntime will update docker run command
    DockerRunCommand drc =
        new DockerRunCommand(c1.getContainerId().toString(), "user",
            "image/tensorflow");
    // reset to avoid count times in above invocation
    reset(spyPlugin);
    reset(spyDmm);
    // Second, update the run command.
    dcp.updateDockerRunCommand(drc, c1);
    // The spec is already generated in getCreateDockerVolumeCommand
    // and there should be a cache hit for DeviceRuntime spec.
    verify(spyPlugin, times(0)).onDevicesAllocated(
        allocatedDevice,
        YarnRuntimeType.RUNTIME_DOCKER);
    // ensure that allocation is get from cache instead of device mapping
    // manager
    verify(spyDmm, times(0)).getAllocatedDevices(resourceName,
        c1.getContainerId());
    String runStr = drc.toString();
    Assert.assertTrue(
        runStr.contains("nvidia_driver_352.68:/usr/local/nvidia:ro"));
    Assert.assertTrue(runStr.contains("/dev/hdwA0:/dev/hdwA0"));
    // Third, cleanup in getCleanupDockerVolumesCommand
    dcp.getCleanupDockerVolumesCommand(c1);
    // Ensure device plugin's onDeviceReleased is invoked
    verify(spyPlugin).onDevicesReleased(allocatedDevice);
    // If we run the c1 again. No cache will be used for allocation and spec
    dcp.getCreateDockerVolumeCommand(c1);
    verify(spyDmm).getAllocatedDevices(resourceName, c1.getContainerId());
    verify(spyPlugin).onDevicesAllocated(
        allocatedDevice,
        YarnRuntimeType.RUNTIME_DOCKER);
  }

  /**
   * Test a container run command update when using Docker runtime.
   * And the device plugin it uses is like Nvidia Docker v2.
   * */
  @Test
  public void testDeviceResourceDockerRuntimePlugin2() throws Exception {
    NodeManager.NMContext context = mock(NodeManager.NMContext.class);
    NMStateStoreService storeService = mock(NMStateStoreService.class);
    when(context.getNMStateStore()).thenReturn(storeService);
    when(context.getConf()).thenReturn(this.conf);
    doNothing().when(storeService).storeAssignedResources(isA(Container.class),
        isA(String.class),
        isA(ArrayList.class));
    // Init scheduler manager
    DeviceMappingManager dmm = new DeviceMappingManager(context);
    DeviceMappingManager spyDmm = spy(dmm);
    ResourcePluginManager rpm = mock(ResourcePluginManager.class);
    when(rpm.getDeviceMappingManager()).thenReturn(spyDmm);
    // Init a plugin
    MyPlugin plugin = new MyPlugin();
    MyPlugin spyPlugin = spy(plugin);
    String resourceName = MyPlugin.RESOURCE_NAME;
    // Init an adapter for the plugin
    DevicePluginAdapter adapter = new DevicePluginAdapter(
        resourceName,
        spyPlugin, spyDmm);
    adapter.initialize(context);
    // Bootstrap, adding device
    adapter.initialize(context);
    adapter.createResourceHandler(context,
        mockCGroupsHandler, mockPrivilegedExecutor);
    adapter.getDeviceResourceHandler().bootstrap(conf);
    // Case 1. A container request Docker runtime and 1 device
    Container c1 = mockContainerWithDeviceRequest(1, resourceName, 2, true);
    // generate spec based on v2
    spyPlugin.setDevicePluginVersion("v2");
    // preStart will do allocation
    adapter.getDeviceResourceHandler().preStart(c1);
    Set<Device> allocatedDevice = spyDmm.getAllocatedDevices(resourceName,
        c1.getContainerId());
    reset(spyDmm);
    // c1 is requesting docker runtime.
    // it will create parent cgroup but no cgroups update operation needed.
    // check device cgroup create operation
    verify(mockCGroupsHandler).createCGroup(
        CGroupsHandler.CGroupController.DEVICES,
        c1.getContainerId().toString());
    // ensure no cgroups update operation
    verify(mockPrivilegedExecutor, times(0))
        .executePrivilegedOperation(
            any(PrivilegedOperation.class), anyBoolean());
    DockerCommandPlugin dcp = adapter.getDockerCommandPluginInstance();
    // When DockerLinuxContainerRuntime invoke the DockerCommandPluginInstance
    // First to create volume
    DockerVolumeCommand dvc = dcp.getCreateDockerVolumeCommand(c1);
    // ensure that allocation is get once from device mapping manager
    verify(spyDmm).getAllocatedDevices(resourceName, c1.getContainerId());
    // ensure that plugin's onDeviceAllocated is invoked
    verify(spyPlugin).onDevicesAllocated(
        allocatedDevice,
        YarnRuntimeType.RUNTIME_DEFAULT);
    verify(spyPlugin).onDevicesAllocated(
        allocatedDevice,
        YarnRuntimeType.RUNTIME_DOCKER);
    // No volume creation request
    Assert.assertNull(dvc);

    // then the DockerLinuxContainerRuntime will update docker run command
    DockerRunCommand drc =
        new DockerRunCommand(c1.getContainerId().toString(), "user",
            "image/tensorflow");
    // reset to avoid count times in above invocation
    reset(spyPlugin);
    reset(spyDmm);
    // Second, update the run command.
    dcp.updateDockerRunCommand(drc, c1);
    // The spec is already generated in getCreateDockerVolumeCommand
    // and there should be a cache hit for DeviceRuntime spec.
    verify(spyPlugin, times(0)).onDevicesAllocated(
        allocatedDevice,
        YarnRuntimeType.RUNTIME_DOCKER);
    // ensure that allocation is get once from device mapping manager
    verify(spyDmm, times(0)).getAllocatedDevices(resourceName,
        c1.getContainerId());
    Assert.assertEquals("0,1", drc.getEnv().get("NVIDIA_VISIBLE_DEVICES"));
    Assert.assertTrue(drc.toString().contains("runtime=nvidia"));
    // Third, cleanup in getCleanupDockerVolumesCommand
    dcp.getCleanupDockerVolumesCommand(c1);
    // Ensure device plugin's onDeviceReleased is invoked
    verify(spyPlugin).onDevicesReleased(allocatedDevice);
    // If we run the c1 again. No cache will be used for allocation and spec
    dcp.getCreateDockerVolumeCommand(c1);
    verify(spyDmm).getAllocatedDevices(resourceName, c1.getContainerId());
    verify(spyPlugin).onDevicesAllocated(
        allocatedDevice,
        YarnRuntimeType.RUNTIME_DOCKER);
  }

  private static ContainerId getContainerId(int id) {
    return ContainerId.newContainerId(ApplicationAttemptId
        .newInstance(ApplicationId.newInstance(1234L, 1), 1), id);
  }

  private class MyPlugin implements DevicePlugin, DevicePluginScheduler {
    private final static String RESOURCE_NAME = "cmpA.com/hdwA";

    // v1 means the vendor uses the similar way of Nvidia Docker v1
    // v2 means the vendor user the similar way of Nvidia Docker v2
    private String devicePluginVersion = "v2";

    public void setDevicePluginVersion(String version) {
      devicePluginVersion = version;
    }

    @Override
    public DeviceRegisterRequest getRegisterRequestInfo() {
      return DeviceRegisterRequest.Builder.newInstance()
          .setResourceName(RESOURCE_NAME)
          .setPluginVersion("v1.0").build();
    }

    @Override
    public Set<Device> getDevices() {
      TreeSet<Device> r = new TreeSet<>();
      r.add(Device.Builder.newInstance()
          .setId(0)
          .setDevPath("/dev/hdwA0")
          .setMajorNumber(256)
          .setMinorNumber(0)
          .setBusID("0000:80:00.0")
          .setHealthy(true)
          .build());
      r.add(Device.Builder.newInstance()
          .setId(1)
          .setDevPath("/dev/hdwA1")
          .setMajorNumber(256)
          .setMinorNumber(1)
          .setBusID("0000:80:01.0")
          .setHealthy(true)
          .build());
      r.add(Device.Builder.newInstance()
          .setId(2)
          .setDevPath("/dev/hdwA2")
          .setMajorNumber(256)
          .setMinorNumber(2)
          .setBusID("0000:80:02.0")
          .setHealthy(true)
          .build());
      return r;
    }

    @Override
    public DeviceRuntimeSpec onDevicesAllocated(Set<Device> allocatedDevices,
        YarnRuntimeType yarnRuntime) throws Exception {
      if (yarnRuntime == YarnRuntimeType.RUNTIME_DEFAULT) {
        return null;
      }
      if (yarnRuntime == YarnRuntimeType.RUNTIME_DOCKER) {
        return generateSpec(devicePluginVersion, allocatedDevices);
      }
      return null;
    }

    private DeviceRuntimeSpec generateSpec(String version,
        Set<Device> allocatedDevices) {
      DeviceRuntimeSpec.Builder builder =
          DeviceRuntimeSpec.Builder.newInstance();
      if (version.equals("v1")) {
        // Nvidia v1 examples like below. These info is get from Nvidia v1
        // RESTful.
        // --device=/dev/nvidiactl --device=/dev/nvidia-uvm
        // --device=/dev/nvidia0
        // --volume-driver=nvidia-docker
        // --volume=nvidia_driver_352.68:/usr/local/nvidia:ro
        String volumeDriverName = "nvidia-docker";
        String volumeToBeCreated = "nvidia_driver_352.68";
        String volumePathInContainer = "/usr/local/nvidia";
        // describe volumes to be created and mounted
        builder.addVolumeSpec(
                VolumeSpec.Builder.newInstance()
                    .setVolumeDriver(volumeDriverName)
                    .setVolumeName(volumeToBeCreated)
                    .setVolumeOperation(VolumeSpec.CREATE).build())
            .addMountVolumeSpec(
                MountVolumeSpec.Builder.newInstance()
                    .setHostPath(volumeToBeCreated)
                    .setMountPath(volumePathInContainer)
                    .setReadOnly(true).build());
        // describe devices to be mounted
        for (Device device : allocatedDevices) {
          builder.addMountDeviceSpec(
              MountDeviceSpec.Builder.newInstance()
                  .setDevicePathInHost(device.getDevPath())
                  .setDevicePathInContainer(device.getDevPath())
                  .setDevicePermission(MountDeviceSpec.RW).build());
        }
      }
      if (version.equals("v2")) {
        String nvidiaRuntime = "nvidia";
        String nvidiaVisibleDevices = "NVIDIA_VISIBLE_DEVICES";
        StringBuffer gpuMinorNumbersSB = new StringBuffer();
        for (Device device : allocatedDevices) {
          gpuMinorNumbersSB.append(device.getMinorNumber() + ",");
        }
        String minorNumbers = gpuMinorNumbersSB.toString();
        // set runtime and environment variable is enough for
        // plugin like Nvidia Docker v2
        builder.addEnv(nvidiaVisibleDevices,
            minorNumbers.substring(0, minorNumbers.length() - 1))
            .setContainerRuntime(nvidiaRuntime);
      }
      return builder.build();
    }

    @Override
    public void onDevicesReleased(Set<Device> releasedDevices) {
      // nothing to do
    }

    @Override
    public Set<Device> allocateDevices(Set<Device> availableDevices,
        int count, Map<String, String> env) {
      Set<Device> allocated = new TreeSet<>();
      int number = 0;
      for (Device d : availableDevices) {
        allocated.add(d);
        number++;
        if (number == count) {
          break;
        }
      }
      return allocated;
    }
  } // MyPlugin

}

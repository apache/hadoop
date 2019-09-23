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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRegisterRequest;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRuntimeSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.YarnRuntimeType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.TestResourceUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

/**
 * Tests for DeviceMappingManager.
 * Note that we test it under multi-threaded situation
 * */
public class TestDeviceMappingManager {
  protected static final Logger LOG =
      LoggerFactory.getLogger(TestDeviceMappingManager.class);

  private String tempResourceTypesFile;
  private DeviceMappingManager dmm;
  private ExecutorService containerLauncher;
  private Configuration conf;

  private CGroupsHandler mockCGroupsHandler;
  private PrivilegedOperationExecutor mockPrivilegedExecutor;
  private Context mockCtx;

  @Before
  public void setup() throws Exception {
    // setup resource-types.xml
    conf = new YarnConfiguration();
    ResourceUtils.resetResourceTypes();
    String resourceTypesFile = "resource-types-pluggable-devices.xml";
    this.tempResourceTypesFile =
        TestResourceUtils.setupResourceTypes(this.conf, resourceTypesFile);
    NodeManager.NMContext context = mock(NodeManager.NMContext.class);
    NMStateStoreService storeService = mock(NMStateStoreService.class);
    when(context.getNMStateStore()).thenReturn(storeService);
    doNothing().when(storeService).storeAssignedResources(isA(Container.class),
        isA(String.class),
        isA(ArrayList.class));
    dmm = new DeviceMappingManager(context);
    int deviceCount = 100;
    TreeSet<Device> r = new TreeSet<>();
    for (int i = 0; i < deviceCount; i++) {
      r.add(Device.Builder.newInstance()
          .setId(i)
          .setDevPath("/dev/hdwA" + i)
          .setMajorNumber(243)
          .setMinorNumber(i)
          .setBusID("0000:65:00." + i)
          .setHealthy(true)
          .build());
    }
    TreeSet<Device> r1 = new TreeSet<>();
    for (int i = 0; i < deviceCount; i++) {
      r1.add(Device.Builder.newInstance()
          .setId(i)
          .setDevPath("/dev/cmp" + i)
          .setMajorNumber(100)
          .setMinorNumber(i)
          .setBusID("0000:11:00." + i)
          .setHealthy(true)
          .build());
    }
    dmm.addDeviceSet("cmpA.com/hdwA", r);
    dmm.addDeviceSet("cmp.com/cmp", r1);

    containerLauncher =
        Executors.newFixedThreadPool(10);
    mockCGroupsHandler = mock(CGroupsHandler.class);
    mockPrivilegedExecutor = mock(PrivilegedOperationExecutor.class);
    mockCtx = mock(NodeManager.NMContext.class);
    when(mockCtx.getConf()).thenReturn(conf);
  }

  @After
  public void tearDown() throws IOException {
    // cleanup resource-types.xml
    File dest = new File(this.tempResourceTypesFile);
    if (dest.exists()) {
      boolean flag = dest.delete();
    }
  }

  /**
   * Simulate launch different containers requesting different resource.
   * */
  @Test
  public void testAllocation()
      throws InterruptedException, ResourceHandlerException {
    int totalContainerCount = 10;
    String resourceName1 = "cmpA.com/hdwA";
    String resourceName2 = "cmp.com/cmp";
    DeviceMappingManager dmmSpy = spy(dmm);
    // generate a list of container
    Map<String, Map<Container, Integer>> containerSet = new HashMap<>();
    containerSet.put(resourceName1, new HashMap<>());
    containerSet.put(resourceName2, new HashMap<>());
    Long startTime = System.currentTimeMillis();
    for (int i = 0; i < totalContainerCount; i++) {
      // Random requeted device
      int num = new Random().nextInt(5) + 1;
      // Random requested resource type
      String resourceName;
      int seed = new Random().nextInt(5);
      if (seed % 2 == 0) {
        resourceName = resourceName1;
      } else {
        resourceName = resourceName2;
      }
      Container c = mockContainerWithDeviceRequest(i,
          resourceName,
          num, false);
      containerSet.get(resourceName).put(c, num);
      DevicePlugin myPlugin = new MyTestPlugin();
      DevicePluginAdapter dpa = new DevicePluginAdapter(resourceName,
          myPlugin, dmm);
      DeviceResourceHandlerImpl dri = new DeviceResourceHandlerImpl(
          resourceName, dpa,
          dmmSpy, mockCGroupsHandler, mockPrivilegedExecutor, mockCtx);
      Future<Integer> f = containerLauncher.submit(new MyContainerLaunch(
          dri, c, i, false));
    }

    containerLauncher.shutdown();
    while (!containerLauncher.awaitTermination(10, TimeUnit.SECONDS)) {
      LOG.info("Wait for the threads to finish");
    }

    Long endTime = System.currentTimeMillis();
    LOG.info("Each container preStart spends roughly: {} ms",
        (endTime - startTime)/totalContainerCount);
    // Ensure invocation times
    verify(dmmSpy, times(totalContainerCount)).assignDevices(
        anyString(), any(Container.class));
    // Ensure used devices' count for each type is correct
    int totalAllocatedCount = 0;
    Map<Device, ContainerId> used1 =
        dmm.getAllUsedDevices().get(resourceName1);
    Map<Device, ContainerId> used2 =
        dmm.getAllUsedDevices().get(resourceName2);
    for (Map.Entry<Container, Integer> entry :
        containerSet.get(resourceName1).entrySet()) {
      totalAllocatedCount += entry.getValue();
    }
    for (Map.Entry<Container, Integer> entry :
        containerSet.get(resourceName2).entrySet()) {
      totalAllocatedCount += entry.getValue();
    }
    Assert.assertEquals(totalAllocatedCount, used1.size() + used2.size());
    // Ensure each container has correct devices
    for (Map.Entry<Container, Integer> entry :
        containerSet.get(resourceName1).entrySet()) {
      int containerWanted = entry.getValue();
      int actualAllocated = dmm.getAllocatedDevices(resourceName1,
          entry.getKey().getContainerId()).size();
      Assert.assertEquals(containerWanted, actualAllocated);
    }
    for (Map.Entry<Container, Integer> entry :
        containerSet.get(resourceName2).entrySet()) {
      int containerWanted = entry.getValue();
      int actualAllocated = dmm.getAllocatedDevices(resourceName2,
          entry.getKey().getContainerId()).size();
      Assert.assertEquals(containerWanted, actualAllocated);
    }
  }

  /**
   * Simulate launch containers and cleanup.
   * */
  @Test
  public void testAllocationAndCleanup()
      throws InterruptedException, ResourceHandlerException, IOException {
    int totalContainerCount = 10;
    String resourceName1 = "cmpA.com/hdwA";
    String resourceName2 = "cmp.com/cmp";
    DeviceMappingManager dmmSpy = spy(dmm);
    // generate a list of container
    Map<String, Map<Container, Integer>> containerSet = new HashMap<>();
    containerSet.put(resourceName1, new HashMap<>());
    containerSet.put(resourceName2, new HashMap<>());
    for (int i = 0; i < totalContainerCount; i++) {
      // Random requeted device
      int num = new Random().nextInt(5) + 1;
      // Random requested resource type
      String resourceName;
      int seed = new Random().nextInt(5);
      if (seed % 2 == 0) {
        resourceName = resourceName1;
      } else {
        resourceName = resourceName2;
      }
      Container c = mockContainerWithDeviceRequest(i,
          resourceName,
          num, false);
      containerSet.get(resourceName).put(c, num);
      DevicePlugin myPlugin = new MyTestPlugin();
      DevicePluginAdapter dpa = new DevicePluginAdapter(resourceName,
          myPlugin, dmm);
      DeviceResourceHandlerImpl dri = new DeviceResourceHandlerImpl(
          resourceName, dpa,
          dmmSpy, mockCGroupsHandler, mockPrivilegedExecutor, mockCtx);
      Future<Integer> f = containerLauncher.submit(new MyContainerLaunch(
          dri, c, i, true));
    }

    containerLauncher.shutdown();
    while (!containerLauncher.awaitTermination(10, TimeUnit.SECONDS)) {
      LOG.info("Wait for the threads to finish");
    }

    // Ensure invocation times
    verify(dmmSpy, times(totalContainerCount)).assignDevices(
        anyString(), any(Container.class));
    verify(dmmSpy, times(totalContainerCount)).cleanupAssignedDevices(
        anyString(), any(ContainerId.class));

    // Ensure all devices are back
    Assert.assertEquals(0,
        dmm.getAllUsedDevices().get(resourceName1).size());
    Assert.assertEquals(0,
        dmm.getAllUsedDevices().get(resourceName2).size());
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

  private static ContainerId getContainerId(int id) {
    return ContainerId.newContainerId(ApplicationAttemptId
        .newInstance(ApplicationId.newInstance(1234L, 1), 1), id);
  }

  private static class MyContainerLaunch implements Callable<Integer> {
    private DeviceResourceHandlerImpl deviceResourceHandler;
    private Container container;
    private boolean doCleanup;
    private int cId;

    MyContainerLaunch(DeviceResourceHandlerImpl dri,
        Container c, int id, boolean cleanup) {
      deviceResourceHandler = dri;
      container = c;
      doCleanup = cleanup;
      cId = id;
    }
    @Override
    public Integer call() throws Exception {
      try {

        deviceResourceHandler.preStart(container);
        if (doCleanup) {
          int seconds = new Random().nextInt(5);
          LOG.info("sleep " + seconds);
          Thread.sleep(seconds * 1000);
          deviceResourceHandler.postComplete(getContainerId(cId));
        }
      } catch (ResourceHandlerException e) {
        e.printStackTrace();
      }
      return 0;
    }
  }

  private static class MyTestPlugin implements DevicePlugin {
    private final static String RESOURCE_NAME = "abc";
    @Override
    public DeviceRegisterRequest getRegisterRequestInfo() {
      return DeviceRegisterRequest.Builder.newInstance()
          .setResourceName(RESOURCE_NAME).build();
    }

    @Override
    public Set<Device> getDevices() {
      TreeSet<Device> r = new TreeSet<>();
      return r;
    }

    @Override
    public DeviceRuntimeSpec onDevicesAllocated(Set<Device> allocatedDevices,
        YarnRuntimeType yarnRuntime) throws Exception {
      return null;
    }

    @Override
    public void onDevicesReleased(Set<Device> releasedDevices) {

    }
  } // MyPlugin

}

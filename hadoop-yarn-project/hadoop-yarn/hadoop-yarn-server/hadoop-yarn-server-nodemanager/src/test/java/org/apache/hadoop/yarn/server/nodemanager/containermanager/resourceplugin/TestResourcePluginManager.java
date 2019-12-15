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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.NodeManagerTestBase;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePluginScheduler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerChain;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework.*;
import org.apache.hadoop.yarn.server.nodemanager.health.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.TestResourceUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;

public class TestResourcePluginManager extends NodeManagerTestBase {
  private NodeManager nm;
  private String tempResourceTypesFile;

  @Before
  public void setup() throws Exception {
    // setup resource-types.xml
    ResourceUtils.resetResourceTypes();
    String resourceTypesFile = "resource-types-pluggable-devices.xml";
    this.tempResourceTypesFile =
        TestResourceUtils.setupResourceTypes(this.conf, resourceTypesFile);
  }

  private ResourcePluginManager stubResourcePluginmanager() {
    // Stub ResourcePluginManager
    final ResourcePluginManager rpm = mock(ResourcePluginManager.class);
    Map<String, ResourcePlugin> plugins = new HashMap<>();

    // First resource plugin
    ResourcePlugin resourcePlugin = mock(ResourcePlugin.class);
    NodeResourceUpdaterPlugin nodeResourceUpdaterPlugin = mock(
        NodeResourceUpdaterPlugin.class);
    when(resourcePlugin.getNodeResourceHandlerInstance()).thenReturn(
        nodeResourceUpdaterPlugin);
    plugins.put("resource1", resourcePlugin);

    // Second resource plugin
    resourcePlugin = mock(ResourcePlugin.class);
    when(resourcePlugin.createResourceHandler(any(Context.class), any(
        CGroupsHandler.class), any(PrivilegedOperationExecutor.class)))
        .thenReturn(new CustomizedResourceHandler());
    plugins.put("resource2", resourcePlugin);
    when(rpm.getNameToPlugins()).thenReturn(plugins);
    return rpm;
  }

  @After
  public void tearDown() {
    if (nm != null) {
      try {
        ServiceOperations.stop(nm);
      } catch (Throwable t) {
        // ignore
      }
    }
    // cleanup resource-types.xml
    File dest = new File(this.tempResourceTypesFile);
    if (dest.exists()) {
      assertThat(dest.delete()).isTrue();
    }
  }

  private class CustomizedResourceHandler implements ResourceHandler {

    @Override
    public List<PrivilegedOperation> bootstrap(Configuration configuration) {
      return null;
    }

    @Override
    public List<PrivilegedOperation> preStart(Container container) {
      return null;
    }

    @Override
    public List<PrivilegedOperation> reacquireContainer(
        ContainerId containerId) {
      return null;
    }

    @Override
    public List<PrivilegedOperation> updateContainer(Container container) {
      return null;
    }

    @Override
    public List<PrivilegedOperation> postComplete(ContainerId containerId) {
      return null;
    }

    @Override
    public List<PrivilegedOperation> teardown() {
      return null;
    }
  }

  private class ResourcePluginMockNM extends NodeManager {
    private final ResourcePluginManager rpm;

    ResourcePluginMockNM(ResourcePluginManager rpm) {
      this.rpm = rpm;
    }

    @Override
    protected NodeStatusUpdater createNodeStatusUpdater(Context context,
        Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
      ((NodeManager.NMContext)context).setResourcePluginManager(rpm);
      return new BaseNodeStatusUpdaterForTest(context, dispatcher,
      healthChecker, metrics, new BaseResourceTrackerForTest());
    }

    @Override
    protected ContainerManagerImpl createContainerManager(Context context,
        ContainerExecutor exec, DeletionService del,
        NodeStatusUpdater nodeStatusUpdater,
        ApplicationACLsManager aclsManager,
        LocalDirsHandlerService dirsHandler) {
      return new MyContainerManager(context, exec, del, nodeStatusUpdater,
      metrics, dirsHandler);
    }

    @Override
    protected ResourcePluginManager createResourcePluginManager() {
      return rpm;
    }
  }

  public class MyLCE extends LinuxContainerExecutor {
    private PrivilegedOperationExecutor poe =
        mock(PrivilegedOperationExecutor.class);

    @Override
    protected PrivilegedOperationExecutor getPrivilegedOperationExecutor() {
      return poe;
    }
  }

  /**
   * Make sure {@link ResourcePluginManager} is initialized during NM start up.
   */
  @Test(timeout = 30000)
  public void testResourcePluginManagerInitialization() throws Exception {
    final ResourcePluginManager rpm = stubResourcePluginmanager();
    nm = new ResourcePluginMockNM(rpm);

    nm.init(conf);
    verify(rpm).initialize(
        any(Context.class));
  }

  /**
   * Make sure {@link ResourcePluginManager} is invoked during NM update.
   */
  @Test(timeout = 30000)
  public void testNodeStatusUpdaterWithResourcePluginsEnabled()
      throws Exception {
    final ResourcePluginManager rpm = stubResourcePluginmanager();

    nm = new ResourcePluginMockNM(rpm);

    nm.init(conf);
    nm.start();

    NodeResourceUpdaterPlugin nodeResourceUpdaterPlugin =
        rpm.getNameToPlugins().get("resource1")
            .getNodeResourceHandlerInstance();

    verify(nodeResourceUpdaterPlugin)
        .updateConfiguredResource(any(Resource.class));
  }

  /**
   * Make sure ResourcePluginManager is used to initialize ResourceHandlerChain.
   */
  @Test(timeout = 30000)
  public void testLinuxContainerExecutorWithResourcePluginsEnabled() {
    final ResourcePluginManager rpm = stubResourcePluginmanager();
    final LinuxContainerExecutor lce = new MyLCE();

    nm = new NodeManager() {
      @Override
      protected NodeStatusUpdater createNodeStatusUpdater(Context context,
          Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
        ((NMContext)context).setResourcePluginManager(rpm);
        return new BaseNodeStatusUpdaterForTest(context, dispatcher,
            healthChecker, metrics, new BaseResourceTrackerForTest());
      }

      @Override
      protected ContainerManagerImpl createContainerManager(Context context,
          ContainerExecutor exec, DeletionService del,
          NodeStatusUpdater nodeStatusUpdater,
          ApplicationACLsManager aclsManager,
          LocalDirsHandlerService dirsHandler) {
        return new MyContainerManager(context, exec, del, nodeStatusUpdater,
            metrics, dirsHandler);
      }

      @Override
      protected ContainerExecutor createContainerExecutor(
          Configuration configuration) {
        ((NMContext)this.getNMContext()).setResourcePluginManager(rpm);
        lce.setConf(configuration);
        return lce;
      }
    };

    nm.init(conf);
    nm.start();

    ResourceHandler handler = lce.getResourceHandler();
    assertThat(handler).isNotNull();
    assertThat(handler instanceof ResourceHandlerChain).isTrue();

    boolean newHandlerAdded = false;
    for (ResourceHandler h : ((ResourceHandlerChain) handler)
        .getResourceHandlerList()) {
      if (h instanceof DevicePluginAdapter) {
        fail("ResourceHandler is a DevicePluginAdapter.");
      }
      if (h instanceof CustomizedResourceHandler) {
        newHandlerAdded = true;
        break;
      }
    }
    assertThat(newHandlerAdded).withFailMessage(
        "New ResourceHandler should be added").isTrue();
  }

  /**
   * Disabled pluggable framework in configuration.
   * We use spy object of real rpm to verify "initializePluggableDevicePlugins"
   * because use mock rpm will not working
   */
  @Test(timeout = 30000)
  public void testInitializationWithPluggableDeviceFrameworkDisabled()
      throws Exception {
    ResourcePluginManager rpm = new ResourcePluginManager();

    ResourcePluginManager rpmSpy = spy(rpm);
    nm = new ResourcePluginMockNM(rpmSpy);

    conf.setBoolean(YarnConfiguration.NM_PLUGGABLE_DEVICE_FRAMEWORK_ENABLED,
        false);
    nm.init(conf);
    nm.start();
    verify(rpmSpy).initialize(
        any(Context.class));
    verify(rpmSpy, times(0)).initializePluggableDevicePlugins(
        any(Context.class), any(Configuration.class), anyMap());
  }

  // No related configuration set.
  @Test(timeout = 30000)
  public void testInitializationWithPluggableDeviceFrameworkDisabled2()
      throws Exception {
    ResourcePluginManager rpm = new ResourcePluginManager();

    ResourcePluginManager rpmSpy = spy(rpm);
    nm = new ResourcePluginMockNM(rpmSpy);

    nm.init(conf);
    nm.start();
    verify(rpmSpy).initialize(
        any(Context.class));
    verify(rpmSpy, times(0)).initializePluggableDevicePlugins(
        any(Context.class), any(Configuration.class), anyMap());
  }

  // Enable framework and configure pluggable device classes
  @Test(timeout = 30000)
  public void testInitializationWithPluggableDeviceFrameworkEnabled()
      throws Exception {
    ResourcePluginManager rpm = new ResourcePluginManager();

    ResourcePluginManager rpmSpy = spy(rpm);
    nm = new ResourcePluginMockNM(rpmSpy);

    conf.setBoolean(YarnConfiguration.NM_PLUGGABLE_DEVICE_FRAMEWORK_ENABLED,
        true);
    conf.setStrings(
        YarnConfiguration.NM_PLUGGABLE_DEVICE_FRAMEWORK_DEVICE_CLASSES,
        FakeTestDevicePlugin1.class.getCanonicalName());
    nm.init(conf);
    nm.start();
    verify(rpmSpy).initialize(
        any(Context.class));
    verify(rpmSpy).initializePluggableDevicePlugins(
        any(Context.class), any(Configuration.class), anyMap());
  }

  // Enable pluggable framework, but leave device classes un-configured
  // initializePluggableDevicePlugins invoked but it should throw an exception
  @Test(timeout = 30000)
  public void testInitializationWithPluggableDeviceFrameworkEnabled2()
      throws ClassNotFoundException {
    ResourcePluginManager rpm = new ResourcePluginManager();

    ResourcePluginManager rpmSpy = spy(rpm);
    nm = new ResourcePluginMockNM(rpmSpy);
    boolean fail = false;
    try {
      conf.setBoolean(YarnConfiguration.NM_PLUGGABLE_DEVICE_FRAMEWORK_ENABLED,
          true);

      nm.init(conf);
      nm.start();
    } catch (YarnRuntimeException e) {
      fail = true;
    } catch (Exception ignored) {
      // ignore
    }
    verify(rpmSpy).initializePluggableDevicePlugins(
        any(Context.class), any(Configuration.class), anyMap());
    assertThat(fail).isTrue();
  }

  @Test(timeout = 30000)
  public void testNormalInitializationOfPluggableDeviceClasses() {
    ResourcePluginManager rpm = new ResourcePluginManager();

    ResourcePluginManager rpmSpy = spy(rpm);
    nm = new ResourcePluginMockNM(rpmSpy);

    conf.setBoolean(YarnConfiguration.NM_PLUGGABLE_DEVICE_FRAMEWORK_ENABLED,
        true);
    conf.setStrings(
        YarnConfiguration.NM_PLUGGABLE_DEVICE_FRAMEWORK_DEVICE_CLASSES,
        FakeTestDevicePlugin1.class.getCanonicalName());
    nm.init(conf);
    nm.start();
    Map<String, ResourcePlugin> pluginMap = rpmSpy.getNameToPlugins();
    assertThat(pluginMap.size()).isOne();
    ResourcePlugin rp = pluginMap.get("cmpA.com/hdwA");
    if (!(rp instanceof DevicePluginAdapter)) {
      fail("ResourcePlugin is not DevicePluginAdapter.");
    }
    verify(rpmSpy).checkInterfaceCompatibility(
        DevicePlugin.class, FakeTestDevicePlugin1.class);
  }

  // Fail to load a class which doesn't implement interface DevicePlugin
  @Test(timeout = 30000)
  public void testLoadInvalidPluggableDeviceClasses() {
    ResourcePluginManager rpm = new ResourcePluginManager();

    ResourcePluginManager rpmSpy = spy(rpm);
    nm = new ResourcePluginMockNM(rpmSpy);

    conf.setBoolean(YarnConfiguration.NM_PLUGGABLE_DEVICE_FRAMEWORK_ENABLED,
        true);
    conf.setStrings(
        YarnConfiguration.NM_PLUGGABLE_DEVICE_FRAMEWORK_DEVICE_CLASSES,
        FakeTestDevicePlugin2.class.getCanonicalName());

    String expectedMessage = "Class: "
        + FakeTestDevicePlugin2.class.getCanonicalName()
        + " not instance of " + DevicePlugin.class.getCanonicalName();
    String actualMessage = "";
    try {
      nm.init(conf);
      nm.start();
    } catch (YarnRuntimeException e) {
      actualMessage = e.getMessage();
    }
    assertThat(actualMessage).isEqualTo(expectedMessage);
  }

  // Fail to register duplicated resource name.
  @Test(timeout = 30000)
  public void testLoadDuplicateResourceNameDevicePlugin() {
    ResourcePluginManager rpm = new ResourcePluginManager();

    ResourcePluginManager rpmSpy = spy(rpm);
    nm = new ResourcePluginMockNM(rpmSpy);

    conf.setBoolean(YarnConfiguration.NM_PLUGGABLE_DEVICE_FRAMEWORK_ENABLED,
        true);
    conf.setStrings(
        YarnConfiguration.NM_PLUGGABLE_DEVICE_FRAMEWORK_DEVICE_CLASSES,
        FakeTestDevicePlugin1.class.getCanonicalName() + "," +
            FakeTestDevicePlugin3.class.getCanonicalName());

    String expectedMessage = "cmpA.com/hdwA" +
        " already registered! Please change resource type name"
        + " or configure correct resource type name"
        + " in resource-types.xml for "
        + FakeTestDevicePlugin3.class.getCanonicalName();
    String actualMessage = "";
    try {
      nm.init(conf);
      nm.start();
    } catch (YarnRuntimeException e) {
      actualMessage = e.getMessage();
    }
    assertThat(actualMessage).isEqualTo(expectedMessage);
  }

  /**
   * Fail a plugin due to incompatible interface implemented.
   * It doesn't implement the "getRegisterRequestInfo"
   */
  @Test(timeout = 30000)
  public void testIncompatibleDevicePlugin() {
    ResourcePluginManager rpm = new ResourcePluginManager();

    ResourcePluginManager rpmSpy = spy(rpm);
    nm = new ResourcePluginMockNM(rpmSpy);

    conf.setBoolean(YarnConfiguration.NM_PLUGGABLE_DEVICE_FRAMEWORK_ENABLED,
        true);
    conf.setStrings(
        YarnConfiguration.NM_PLUGGABLE_DEVICE_FRAMEWORK_DEVICE_CLASSES,
        FakeTestDevicePlugin4.class.getCanonicalName());

    String expectedMessage = "Method getRegisterRequestInfo" +
        " is expected but not implemented in "
        + FakeTestDevicePlugin4.class.getCanonicalName();
    String actualMessage = "";
    try {
      nm.init(conf);
      nm.start();
    } catch (YarnRuntimeException e) {
      actualMessage = e.getMessage();
    }
    assertThat(actualMessage).isEqualTo(expectedMessage);
  }

  @Test
  public void testLoadPluginWithCustomizedScheduler() {
    ResourcePluginManager rpm = new ResourcePluginManager();
    DeviceMappingManager dmm = new DeviceMappingManager(mock(Context.class));
    DeviceMappingManager dmmSpy = spy(dmm);

    ResourcePluginManager rpmSpy = spy(rpm);
    rpmSpy.setDeviceMappingManager(dmmSpy);

    nm = new ResourcePluginMockNM(rpmSpy);

    conf.setBoolean(YarnConfiguration.NM_PLUGGABLE_DEVICE_FRAMEWORK_ENABLED,
        true);
    conf.setStrings(
        YarnConfiguration.NM_PLUGGABLE_DEVICE_FRAMEWORK_DEVICE_CLASSES,
        FakeTestDevicePlugin1.class.getCanonicalName()
            + "," + FakeTestDevicePlugin5.class.getCanonicalName());
    nm.init(conf);
    nm.start();
    // only 1 plugin has the customized scheduler
    verify(rpmSpy).checkInterfaceCompatibility(
        DevicePlugin.class, FakeTestDevicePlugin1.class);
    verify(dmmSpy).addDevicePluginScheduler(
        any(String.class), any(DevicePluginScheduler.class));
    assertThat(dmm.getDevicePluginSchedulers().size()).isOne();
  }

  @Test(timeout = 30000)
  public void testRequestedResourceNameIsConfigured() {
    ResourcePluginManager rpm = new ResourcePluginManager();
    String resourceName = "a.com/a";
    assertThat(rpm.isConfiguredResourceName(resourceName)).isFalse();
    resourceName = "cmp.com/cmp";
    assertThat(rpm.isConfiguredResourceName(resourceName)).isTrue();
  }

}

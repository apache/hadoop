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
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.NodeManagerTestBase;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerChain;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.NodeResourceUpdaterPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePluginManager;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestResourcePluginManager extends NodeManagerTestBase {
  private NodeManager nm;

  ResourcePluginManager stubResourcePluginmanager() {
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
  }

  private class CustomizedResourceHandler implements ResourceHandler {

    @Override
    public List<PrivilegedOperation> bootstrap(Configuration configuration)
        throws ResourceHandlerException {
      return null;
    }

    @Override
    public List<PrivilegedOperation> preStart(Container container)
        throws ResourceHandlerException {
      return null;
    }

    @Override
    public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
        throws ResourceHandlerException {
      return null;
    }

    @Override
    public List<PrivilegedOperation> updateContainer(Container container)
        throws ResourceHandlerException {
      return null;
    }

    @Override
    public List<PrivilegedOperation> postComplete(ContainerId containerId)
        throws ResourceHandlerException {
      return null;
    }

    @Override
    public List<PrivilegedOperation> teardown()
        throws ResourceHandlerException {
      return null;
    }
  }

  private class MyMockNM extends NodeManager {
    private final ResourcePluginManager rpm;

    public MyMockNM(ResourcePluginManager rpm) {
      this.rpm = rpm;
    }

    @Override
    protected NodeStatusUpdater createNodeStatusUpdater(Context context,
        Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
      ((NodeManager.NMContext)context).setResourcePluginManager(rpm);
      return new BaseNodeStatusUpdaterForTest(context, dispatcher, healthChecker,
          metrics, new BaseResourceTrackerForTest());
    }

    @Override
    protected ContainerManagerImpl createContainerManager(Context context,
        ContainerExecutor exec, DeletionService del,
        NodeStatusUpdater nodeStatusUpdater,
        ApplicationACLsManager aclsManager,
        LocalDirsHandlerService diskhandler) {
      return new MyContainerManager(context, exec, del, nodeStatusUpdater,
      metrics, diskhandler);
    }

    @Override
    protected ResourcePluginManager createResourcePluginManager() {
      return rpm;
    }
  }

  public class MyLCE extends LinuxContainerExecutor {
    private PrivilegedOperationExecutor poe = mock(PrivilegedOperationExecutor.class);

    @Override
    protected PrivilegedOperationExecutor getPrivilegedOperationExecutor() {
      return poe;
    }
  }

  /*
   * Make sure ResourcePluginManager is initialized during NM start up.
   */
  @Test(timeout = 30000)
  public void testResourcePluginManagerInitialization() throws Exception {
    final ResourcePluginManager rpm = stubResourcePluginmanager();
    nm = new MyMockNM(rpm);

    YarnConfiguration conf = createNMConfig();
    nm.init(conf);
    verify(rpm, times(1)).initialize(
        any(Context.class));
  }

  /*
   * Make sure ResourcePluginManager is invoked during NM update.
   */
  @Test(timeout = 30000)
  public void testNodeStatusUpdaterWithResourcePluginsEnabled() throws Exception {
    final ResourcePluginManager rpm = stubResourcePluginmanager();

    nm = new MyMockNM(rpm);

    YarnConfiguration conf = createNMConfig();
    nm.init(conf);
    nm.start();

    NodeResourceUpdaterPlugin nodeResourceUpdaterPlugin =
        rpm.getNameToPlugins().get("resource1")
            .getNodeResourceHandlerInstance();

    verify(nodeResourceUpdaterPlugin, times(1)).updateConfiguredResource(
        any(Resource.class));
  }

  /*
   * Make sure ResourcePluginManager is used to initialize ResourceHandlerChain
   */
  @Test(timeout = 30000)
  public void testLinuxContainerExecutorWithResourcePluginsEnabled() throws Exception {
    final ResourcePluginManager rpm = stubResourcePluginmanager();
    final LinuxContainerExecutor lce = new MyLCE();

    nm = new NodeManager() {
      @Override
      protected NodeStatusUpdater createNodeStatusUpdater(Context context,
          Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
        ((NMContext)context).setResourcePluginManager(rpm);
        return new BaseNodeStatusUpdaterForTest(context, dispatcher, healthChecker,
            metrics, new BaseResourceTrackerForTest());
      }

      @Override
      protected ContainerManagerImpl createContainerManager(Context context,
          ContainerExecutor exec, DeletionService del,
          NodeStatusUpdater nodeStatusUpdater,
          ApplicationACLsManager aclsManager,
          LocalDirsHandlerService diskhandler) {
        return new MyContainerManager(context, exec, del, nodeStatusUpdater,
            metrics, diskhandler);
      }

      @Override
      protected ContainerExecutor createContainerExecutor(Configuration conf) {
        ((NMContext)this.getNMContext()).setResourcePluginManager(rpm);
        lce.setConf(conf);
        return lce;
      }
    };

    YarnConfiguration conf = createNMConfig();

    nm.init(conf);
    nm.start();

    ResourceHandler handler = lce.getResourceHandler();
    Assert.assertNotNull(handler);
    Assert.assertTrue(handler instanceof ResourceHandlerChain);

    boolean newHandlerAdded = false;
    for (ResourceHandler h : ((ResourceHandlerChain) handler)
        .getResourceHandlerList()) {
      if (h instanceof CustomizedResourceHandler) {
        newHandlerAdded = true;
        break;
      }
    }
    Assert.assertTrue("New ResourceHandler should be added", newHandlerAdded);
  }
}

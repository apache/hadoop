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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.YarnRuntimeType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;

import java.util.List;
import java.util.Set;

/**
 * The Hooks into container lifecycle.
 * Get device list from device plugin in {@code bootstrap}
 * Assign devices for a container in {@code preStart}
 * Restore statue in {@code reacquireContainer}
 * Recycle devices from container in {@code postComplete}
 * */
public class DeviceResourceHandlerImpl implements ResourceHandler {

  static final Log LOG = LogFactory.getLog(DeviceResourceHandlerImpl.class);

  private final String resourceName;
  private final DevicePlugin devicePlugin;
  private final DeviceMappingManager deviceMappingManager;
  private final CGroupsHandler cGroupsHandler;
  private final PrivilegedOperationExecutor privilegedOperationExecutor;
  private final DevicePluginAdapter devicePluginAdapter;

  public DeviceResourceHandlerImpl(String reseName,
      DevicePlugin devPlugin,
      DevicePluginAdapter devPluginAdapter,
      DeviceMappingManager devMappingManager,
      CGroupsHandler cgHandler,
      PrivilegedOperationExecutor operation) {
    this.devicePluginAdapter = devPluginAdapter;
    this.resourceName = reseName;
    this.devicePlugin = devPlugin;
    this.cGroupsHandler = cgHandler;
    this.privilegedOperationExecutor = operation;
    this.deviceMappingManager = devMappingManager;
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration configuration)
      throws ResourceHandlerException {
    Set<Device> availableDevices = null;
    try {
      availableDevices = devicePlugin.getDevices();
    } catch (Exception e) {
      throw new ResourceHandlerException("Exception thrown from"
          + " plugin's \"getDevices\"" + e.getMessage());
    }
    /**
     * We won't fail the NM if plugin returns invalid value here.
     * */
    if (availableDevices == null) {
      LOG.error("Bootstrap " + resourceName
          + " failed. Null value got from plugin's getDevices method");
      return null;
    }
    // Add device set. Here we trust the plugin's return value
    deviceMappingManager.addDeviceSet(resourceName, availableDevices);
    // TODO: Init cgroups

    return null;
  }

  @Override
  public synchronized List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    String containerIdStr = container.getContainerId().toString();
    DeviceMappingManager.DeviceAllocation allocation =
        deviceMappingManager.assignDevices(resourceName, container);
    LOG.debug("Allocated to "
        + containerIdStr + ": " + allocation);

    try {
      devicePlugin.onDevicesAllocated(
          allocation.getAllowed(), YarnRuntimeType.RUNTIME_DEFAULT);
    } catch (Exception e) {
      throw new ResourceHandlerException("Exception thrown from"
          + " plugin's \"onDeviceAllocated\"" + e.getMessage());
    }

    // cgroups operation based on allocation
    /**
     * TODO: implement a general container-executor device module
     * */

    return null;
  }

  @Override
  public synchronized List<PrivilegedOperation> reacquireContainer(
      ContainerId containerId) throws ResourceHandlerException {
    deviceMappingManager.recoverAssignedDevices(resourceName, containerId);
    return null;
  }

  @Override
  public List<PrivilegedOperation> updateContainer(Container container)
      throws ResourceHandlerException {
    return null;
  }

  @Override
  public synchronized List<PrivilegedOperation> postComplete(
      ContainerId containerId) throws ResourceHandlerException {
    deviceMappingManager.cleanupAssignedDevices(resourceName, containerId);
    return null;
  }

  @Override
  public List<PrivilegedOperation> teardown()
      throws ResourceHandlerException {
    return null;
  }

  @Override
  public String toString() {
    return DeviceResourceHandlerImpl.class.getName() + "{" +
        "resourceName='" + resourceName + '\'' +
        ", devicePlugin=" + devicePlugin +
        ", devicePluginAdapter=" + devicePluginAdapter +
        '}';
  }
}

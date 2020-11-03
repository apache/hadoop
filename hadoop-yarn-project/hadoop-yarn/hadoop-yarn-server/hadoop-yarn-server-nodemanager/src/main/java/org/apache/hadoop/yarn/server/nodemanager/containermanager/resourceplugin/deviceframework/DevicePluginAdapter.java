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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.DockerCommandPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.NodeResourceUpdaterPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NMDeviceResourceInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NMResourceInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * The {@link DevicePluginAdapter} will adapt existing hooks.
 * into vendor plugin's logic.
 * It decouples the vendor plugin from YARN's device framework
 *
 * */
public class DevicePluginAdapter implements ResourcePlugin {
  private final static Logger LOG = LoggerFactory.
      getLogger(DevicePluginAdapter.class);

  private final String resourceName;

  private final DevicePlugin devicePlugin;
  private DeviceMappingManager deviceMappingManager;

  private DeviceResourceHandlerImpl deviceResourceHandler;
  private DeviceResourceUpdaterImpl deviceResourceUpdater;
  private DeviceResourceDockerRuntimePluginImpl deviceDockerCommandPlugin;


  @VisibleForTesting
  public void setDeviceResourceHandler(
      DeviceResourceHandlerImpl deviceResourceHandler) {
    this.deviceResourceHandler = deviceResourceHandler;
  }

  public DevicePluginAdapter(String name, DevicePlugin dp,
      DeviceMappingManager dmm) {
    deviceMappingManager = dmm;
    resourceName = name;
    devicePlugin = dp;
  }

  public DeviceMappingManager getDeviceMappingManager() {
    return deviceMappingManager;
  }


  public DevicePlugin getDevicePlugin() {
    return devicePlugin;
  }

  @Override
  public void initialize(Context context) throws YarnException {
    deviceDockerCommandPlugin = new DeviceResourceDockerRuntimePluginImpl(
        resourceName,
        devicePlugin, this);
    deviceResourceUpdater = new DeviceResourceUpdaterImpl(
        resourceName, devicePlugin);
    LOG.info(resourceName + " plugin adapter initialized");
    return;
  }

  @Override
  public ResourceHandler createResourceHandler(Context nmContext,
      CGroupsHandler cGroupsHandler,
      PrivilegedOperationExecutor privilegedOperationExecutor) {
    this.deviceResourceHandler = new DeviceResourceHandlerImpl(resourceName,
        this, deviceMappingManager,
        cGroupsHandler, privilegedOperationExecutor, nmContext);
    return deviceResourceHandler;
  }

  @Override
  public NodeResourceUpdaterPlugin getNodeResourceHandlerInstance() {
    return deviceResourceUpdater;
  }

  @Override
  public void cleanup() {

  }

  @Override
  public DockerCommandPlugin getDockerCommandPluginInstance() {
    return deviceDockerCommandPlugin;
  }

  @Override
  public NMResourceInfo getNMResourceInfo() throws YarnException {
    List<Device> allowed = new ArrayList<>(
        deviceMappingManager.getAllAllowedDevices().get(resourceName));
    List<AssignedDevice> assigned = new ArrayList<>();
    Map<Device, ContainerId> assignedMap =
        deviceMappingManager.getAllUsedDevices().get(resourceName);
    for (Map.Entry<Device, ContainerId> entry : assignedMap.entrySet()) {
      assigned.add(new AssignedDevice(entry.getValue(),
          entry.getKey()));
    }
    return new NMDeviceResourceInfo(allowed, assigned);
  }

  public DeviceResourceHandlerImpl getDeviceResourceHandler() {
    return deviceResourceHandler;
  }

  @Override
  public String toString() {
    return DevicePluginAdapter.class.getName();
  }
}

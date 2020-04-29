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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRuntimeSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.MountDeviceSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.MountVolumeSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.VolumeSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.YarnRuntimeType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRunCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerVolumeCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.DockerCommandPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.util.LRUCacheHashMap;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Bridge DevicePlugin and the hooks related to lunch Docker container.
 * When launching Docker container, DockerLinuxContainerRuntime will invoke
 * this class's methods which get needed info back from DevicePlugin.
 * */
public class DeviceResourceDockerRuntimePluginImpl
    implements DockerCommandPlugin {

  final static Logger LOG = LoggerFactory.getLogger(
      DeviceResourceDockerRuntimePluginImpl.class);

  private String resourceName;
  private DevicePlugin devicePlugin;
  private DevicePluginAdapter devicePluginAdapter;

  private int maxCacheSize = 100;
  // LRU to avoid memory leak if getCleanupDockerVolumesCommand not invoked.
  private Map<ContainerId, Set<Device>> cachedAllocation =
      Collections.synchronizedMap(new LRUCacheHashMap(maxCacheSize, true));

  private Map<ContainerId, DeviceRuntimeSpec> cachedSpec =
      Collections.synchronizedMap(new LRUCacheHashMap<>(maxCacheSize, true));

  public DeviceResourceDockerRuntimePluginImpl(String resourceName,
      DevicePlugin devicePlugin, DevicePluginAdapter devicePluginAdapter) {
    this.resourceName = resourceName;
    this.devicePlugin = devicePlugin;
    this.devicePluginAdapter = devicePluginAdapter;
  }

  @Override
  public void updateDockerRunCommand(DockerRunCommand dockerRunCommand,
      Container container) throws ContainerExecutionException {
    String containerId = container.getContainerId().toString();
    LOG.debug("Try to update docker run command for: {}", containerId);
    if(!requestedDevice(resourceName, container)) {
      return;
    }
    DeviceRuntimeSpec deviceRuntimeSpec = getRuntimeSpec(container);
    if (deviceRuntimeSpec == null) {
      LOG.warn("The device plugin: "
          + devicePlugin.getClass().getCanonicalName()
          + " returns null device runtime spec value for container: "
          + containerId);
      return;
    }
    // handle runtime
    dockerRunCommand.addRuntime(deviceRuntimeSpec.getContainerRuntime());
    LOG.debug("Handle docker container runtime type: {} for container: {}",
        deviceRuntimeSpec.getContainerRuntime(), containerId);
    // handle device mounts
    Set<MountDeviceSpec> deviceMounts = deviceRuntimeSpec.getDeviceMounts();
    LOG.debug("Handle device mounts: {} for container: {}", deviceMounts,
        containerId);
    for (MountDeviceSpec mountDeviceSpec : deviceMounts) {
      dockerRunCommand.addDevice(
          mountDeviceSpec.getDevicePathInHost(),
          mountDeviceSpec.getDevicePathInContainer());
    }
    // handle volume mounts
    Set<MountVolumeSpec> mountVolumeSpecs = deviceRuntimeSpec.getVolumeMounts();
    LOG.debug("Handle volume mounts: {} for container: {}", mountVolumeSpecs,
        containerId);
    for (MountVolumeSpec mountVolumeSpec : mountVolumeSpecs) {
      if (mountVolumeSpec.getReadOnly()) {
        dockerRunCommand.addReadOnlyMountLocation(
            mountVolumeSpec.getHostPath(),
            mountVolumeSpec.getMountPath());
      } else {
        dockerRunCommand.addReadWriteMountLocation(
            mountVolumeSpec.getHostPath(),
            mountVolumeSpec.getMountPath());
      }
    }
    // handle envs
    dockerRunCommand.addEnv(deviceRuntimeSpec.getEnvs());
    LOG.debug("Handle envs: {} for container: {}",
        deviceRuntimeSpec.getEnvs(), containerId);
  }

  @Override
  public DockerVolumeCommand getCreateDockerVolumeCommand(Container container)
      throws ContainerExecutionException {
    if(!requestedDevice(resourceName, container)) {
      return null;
    }
    DeviceRuntimeSpec deviceRuntimeSpec = getRuntimeSpec(container);
    if (deviceRuntimeSpec == null) {
      return null;
    }
    Set<VolumeSpec> volumeClaims = deviceRuntimeSpec.getVolumeSpecs();
    for (VolumeSpec volumeSec: volumeClaims) {
      if (volumeSec.getVolumeOperation().equals(VolumeSpec.CREATE)) {
        DockerVolumeCommand command = new DockerVolumeCommand(
            DockerVolumeCommand.VOLUME_CREATE_SUB_COMMAND);
        command.setDriverName(volumeSec.getVolumeDriver());
        command.setVolumeName(volumeSec.getVolumeName());
        LOG.debug("Get volume create request from plugin:{} for container: {}",
            volumeClaims, container.getContainerId());
        return command;
      }
    }
    return null;
  }

  @Override
  public DockerVolumeCommand getCleanupDockerVolumesCommand(Container container)
      throws ContainerExecutionException {

    if(!requestedDevice(resourceName, container)) {
      return null;
    }
    Set<Device> allocated = getAllocatedDevices(container);
    try {
      devicePlugin.onDevicesReleased(allocated);
    } catch (Exception e) {
      LOG.warn("Exception thrown in onDeviceReleased of "
          + devicePlugin.getClass() + "for container: "
          + container.getContainerId().toString(), e);
    }
    // remove cache
    ContainerId containerId = container.getContainerId();
    cachedAllocation.remove(containerId);
    cachedSpec.remove(containerId);
    return null;
  }

  protected boolean requestedDevice(String resName, Container container) {
    return DeviceMappingManager.
        getRequestedDeviceCount(resName, container.getResource()) > 0;
  }

  private Set<Device> getAllocatedDevices(Container container) {
    // get allocated devices
    Set<Device> allocated;
    ContainerId containerId = container.getContainerId();
    allocated = cachedAllocation.get(containerId);
    if (allocated != null) {
      return allocated;
    }
    allocated = devicePluginAdapter
        .getDeviceMappingManager()
        .getAllocatedDevices(resourceName, containerId);
    LOG.debug("Get allocation from deviceMappingManager: {}, {} for"
        + " container: {}", allocated, resourceName, containerId);
    cachedAllocation.put(containerId, allocated);
    return allocated;
  }

  public synchronized DeviceRuntimeSpec getRuntimeSpec(Container container) {
    ContainerId containerId = container.getContainerId();
    DeviceRuntimeSpec deviceRuntimeSpec = cachedSpec.get(containerId);
    if (deviceRuntimeSpec == null) {
      Set<Device> allocated = getAllocatedDevices(container);
      if (allocated == null || allocated.size() == 0) {
        LOG.error("Cannot get allocation for container:" + containerId);
        return null;
      }
      try {
        deviceRuntimeSpec = devicePlugin.onDevicesAllocated(allocated,
            YarnRuntimeType.RUNTIME_DOCKER);
      } catch (Exception e) {
        LOG.error("Exception thrown in onDeviceAllocated of "
            + devicePlugin.getClass() + " for container: " + containerId, e);
      }
      if (deviceRuntimeSpec == null) {
        LOG.error("Null DeviceRuntimeSpec value got from "
            + devicePlugin.getClass() + " for container: "
            + containerId + ", please check plugin logic");
        return null;
      }
      cachedSpec.put(containerId, deviceRuntimeSpec);
    }
    return deviceRuntimeSpec;
  }

}

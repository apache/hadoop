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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRuntimeSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.YarnRuntimeType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.OCIContainerRuntime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

  static final Logger LOG = LoggerFactory.
      getLogger(DeviceResourceHandlerImpl.class);

  private final String resourceName;
  private final DevicePlugin devicePlugin;
  private final DeviceMappingManager deviceMappingManager;
  private final CGroupsHandler cGroupsHandler;
  private final PrivilegedOperationExecutor privilegedOperationExecutor;
  private final DevicePluginAdapter devicePluginAdapter;
  private final Context nmContext;
  private ShellWrapper shellWrapper;

  // This will be used by container-executor to add necessary clis
  public static final String EXCLUDED_DEVICES_CLI_OPTION = "--excluded_devices";
  public static final String ALLOWED_DEVICES_CLI_OPTION = "--allowed_devices";
  public static final String CONTAINER_ID_CLI_OPTION = "--container_id";

  public DeviceResourceHandlerImpl(String resName,
      DevicePluginAdapter devPluginAdapter,
      DeviceMappingManager devMappingManager,
      CGroupsHandler cgHandler,
      PrivilegedOperationExecutor operation,
      Context ctx) {
    this.devicePluginAdapter = devPluginAdapter;
    this.resourceName = resName;
    this.devicePlugin = devPluginAdapter.getDevicePlugin();
    this.cGroupsHandler = cgHandler;
    this.privilegedOperationExecutor = operation;
    this.deviceMappingManager = devMappingManager;
    this.nmContext = ctx;
    this.shellWrapper = new ShellWrapper();
  }

  @VisibleForTesting
  public DeviceResourceHandlerImpl(String resName,
      DevicePluginAdapter devPluginAdapter,
      DeviceMappingManager devMappingManager,
      CGroupsHandler cgHandler,
      PrivilegedOperationExecutor operation,
      Context ctx, ShellWrapper shell) {
    this.devicePluginAdapter = devPluginAdapter;
    this.resourceName = resName;
    this.devicePlugin = devPluginAdapter.getDevicePlugin();
    this.cGroupsHandler = cgHandler;
    this.privilegedOperationExecutor = operation;
    this.deviceMappingManager = devMappingManager;
    this.nmContext = ctx;
    this.shellWrapper = shell;
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
    // Init cgroups
    this.cGroupsHandler.initializeCGroupController(
        CGroupsHandler.CGroupController.DEVICES);
    return null;
  }

  @Override
  public synchronized List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    String containerIdStr = container.getContainerId().toString();
    DeviceMappingManager.DeviceAllocation allocation =
        deviceMappingManager.assignDevices(resourceName, container);
    LOG.debug("Allocated to {}: {}", containerIdStr, allocation);
    DeviceRuntimeSpec spec;
    try {
      spec = devicePlugin.onDevicesAllocated(
          allocation.getAllowed(), YarnRuntimeType.RUNTIME_DEFAULT);
    } catch (Exception e) {
      throw new ResourceHandlerException("Exception thrown from"
          + " plugin's \"onDeviceAllocated\"" + e.getMessage());
    }

    // cgroups operation based on allocation
    if (spec != null) {
      LOG.warn("Runtime spec in non-Docker container is not supported yet!");
    }
    // Create device cgroups for the container
    cGroupsHandler.createCGroup(CGroupsHandler.CGroupController.DEVICES,
        containerIdStr);
    // non-Docker, use cgroups to do isolation
    if (!OCIContainerRuntime.isOCICompliantContainerRequested(
        nmContext.getConf(),
        container.getLaunchContext().getEnvironment())) {
      tryIsolateDevices(allocation, containerIdStr);
      List<PrivilegedOperation> ret = new ArrayList<>();
      ret.add(new PrivilegedOperation(
          PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
          PrivilegedOperation.CGROUP_ARG_PREFIX + cGroupsHandler
              .getPathForCGroupTasks(CGroupsHandler.CGroupController.DEVICES,
                  containerIdStr)));

      return ret;
    }
    return null;
  }

  /**
   * Try set cgroup devices params for the container using container-executor.
   * If it has real device major number, minor number or dev path,
   * we'll do the enforcement. Otherwise, won't do it.
   *
   * */
  private void tryIsolateDevices(
      DeviceMappingManager.DeviceAllocation allocation,
      String containerIdStr) throws ResourceHandlerException {
    try {
      // Execute c-e to setup device isolation before launch the container
      PrivilegedOperation privilegedOperation = new PrivilegedOperation(
          PrivilegedOperation.OperationType.DEVICE,
          Arrays.asList(CONTAINER_ID_CLI_OPTION, containerIdStr));
      boolean needNativeDeviceOperation = false;
      int majorNumber;
      int minorNumber;
      List<String> devNumbers = new ArrayList<>();
      if (!allocation.getDenied().isEmpty()) {
        DeviceType devType;
        for (Device deniedDevice : allocation.getDenied()) {
          majorNumber = deniedDevice.getMajorNumber();
          minorNumber = deniedDevice.getMinorNumber();
          // Add device type
          devType = getDeviceType(deniedDevice);
          if (devType != null) {
            devNumbers.add(devType.getName() + "-" + majorNumber + ":"
                + minorNumber + "-rwm");
          }
        }
        if (devNumbers.size() != 0) {
          privilegedOperation.appendArgs(
              Arrays.asList(EXCLUDED_DEVICES_CLI_OPTION,
                  StringUtils.join(",", devNumbers)));
          needNativeDeviceOperation = true;
        }
      }

      if (!allocation.getAllowed().isEmpty()) {
        devNumbers.clear();
        for (Device allowedDevice : allocation.getAllowed()) {
          majorNumber = allowedDevice.getMajorNumber();
          minorNumber = allowedDevice.getMinorNumber();
          if (majorNumber != -1 && minorNumber != -1) {
            devNumbers.add(majorNumber + ":" + minorNumber);
          }
        }
        if (devNumbers.size() > 0) {
          privilegedOperation.appendArgs(
              Arrays.asList(ALLOWED_DEVICES_CLI_OPTION,
                  StringUtils.join(",", devNumbers)));
          needNativeDeviceOperation = true;
        }
      }
      if (needNativeDeviceOperation) {
        privilegedOperationExecutor.executePrivilegedOperation(
            privilegedOperation, true);
      }
    } catch (PrivilegedOperationException e) {
      cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.DEVICES,
          containerIdStr);
      LOG.warn("Could not update cgroup for container", e);
      throw new ResourceHandlerException(e);
    }
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
    cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.DEVICES,
        containerId.toString());
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

  public DeviceType getDeviceType(Device device) {
    String devName = device.getDevPath();
    if (devName.isEmpty()) {
      LOG.warn("Empty device path provided, try to get device type from " +
          "major:minor device number");
      int major = device.getMajorNumber();
      int minor = device.getMinorNumber();
      if (major == -1 && minor == -1) {
        LOG.warn("Non device number provided, cannot decide the device type");
        return null;
      }
      // Get type from the device numbers
      return getDeviceTypeFromDeviceNumber(device.getMajorNumber(),
          device.getMinorNumber());
    }
    DeviceType deviceType;
    try {
      LOG.debug("Try to get device type from device path: {}", devName);
      String output = shellWrapper.getDeviceFileType(devName);
      LOG.debug("stat output:{}", output);
      deviceType = output.startsWith("c") ? DeviceType.CHAR : DeviceType.BLOCK;
    } catch (IOException e) {
      String msg =
          "Failed to get device type from stat " + devName;
      LOG.warn(msg);
      return null;
    }
    return deviceType;
  }

  /**
   * Get the device type used for cgroups value set.
   * If sys file "/sys/dev/block/major:minor" exists, it's block device.
   * Otherwise, it's char device. An exception is that Nvidia GPU doesn't
   * create this sys file. so assume character device by default.
   */
  public DeviceType getDeviceTypeFromDeviceNumber(int major, int minor) {
    if (shellWrapper.existFile("/sys/dev/block/"
        + major + ":" + minor)) {
      return DeviceType.BLOCK;
    }
    return DeviceType.CHAR;
  }

  /**
   * Enum for Linux device type. Used when updating device cgroups params.
   * "b" represents block device
   * "c" represents character device
   * */
  private enum DeviceType {
    BLOCK("b"),
    CHAR("c");

    private final String name;

    DeviceType(String n) {
      this.name = n;
    }

    public String getName() {
      return name;
    }
  }

}

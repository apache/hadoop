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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.gpu.GpuResourceAllocator;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.gpu.GpuResourceHandlerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.DockerCommandPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.NodeResourceUpdaterPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NMResourceInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.GpuDeviceInformation;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.NMGpuResourceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GpuResourcePlugin implements ResourcePlugin {

  private static final Logger LOG =
      LoggerFactory.getLogger(GpuResourcePlugin.class);

  private final GpuNodeResourceUpdateHandler resourceDiscoverHandler;
  private final GpuDiscoverer gpuDiscoverer;
  public static final int MAX_REPEATED_ERROR_ALLOWED = 10;

  private int numOfErrorExecutionSinceLastSucceed = 0;

  private GpuResourceHandlerImpl gpuResourceHandler = null;
  private DockerCommandPlugin dockerCommandPlugin = null;

  public GpuResourcePlugin(GpuNodeResourceUpdateHandler resourceDiscoverHandler,
      GpuDiscoverer gpuDiscoverer) {
    this.resourceDiscoverHandler = resourceDiscoverHandler;
    this.gpuDiscoverer = gpuDiscoverer;
  }

  @Override
  public void initialize(Context context) throws YarnException {
    validateExecutorConfig(context.getConf());
    this.gpuDiscoverer.initialize(context.getConf(),
        new NvidiaBinaryHelper());
    this.dockerCommandPlugin =
        GpuDockerCommandPluginFactory.createGpuDockerCommandPlugin(
            context.getConf());
  }

  private void validateExecutorConfig(Configuration conf) {
    Class<? extends ContainerExecutor> executorClass = conf.getClass(
        YarnConfiguration.NM_CONTAINER_EXECUTOR, DefaultContainerExecutor.class,
        ContainerExecutor.class);

    if (executorClass.equals(DefaultContainerExecutor.class)) {
      LOG.warn("Using GPU plugin with disabled LinuxContainerExecutor" +
          " is considered to be unsafe.");
    }
  }

  @Override
  public ResourceHandler createResourceHandler(
      Context context, CGroupsHandler cGroupsHandler,
      PrivilegedOperationExecutor privilegedOperationExecutor) {
    if (gpuResourceHandler == null) {
      gpuResourceHandler = new GpuResourceHandlerImpl(context, cGroupsHandler,
          privilegedOperationExecutor, gpuDiscoverer);
    }

    return gpuResourceHandler;
  }

  @Override
  public NodeResourceUpdaterPlugin getNodeResourceHandlerInstance() {
    return resourceDiscoverHandler;
  }

  @Override
  public void cleanup() throws YarnException {
    // Do nothing.
  }

  public DockerCommandPlugin getDockerCommandPluginInstance() {
    return dockerCommandPlugin;
  }

  @Override
  public synchronized NMResourceInfo getNMResourceInfo() throws YarnException {
    final GpuDeviceInformation gpuDeviceInformation;

    if (gpuDiscoverer.isAutoDiscoveryEnabled()) {
      //At this point the gpu plugin is already enabled
      checkGpuResourceHandler();

      checkErrorCount();
      try{
        gpuDeviceInformation = gpuDiscoverer.getGpuDeviceInformation();
        numOfErrorExecutionSinceLastSucceed = 0;
      } catch (YarnException e) {
        LOG.error(e.getMessage(), e);
        numOfErrorExecutionSinceLastSucceed++;
        throw e;
      }
    } else {
      gpuDeviceInformation = null;
    }
    GpuResourceAllocator gpuResourceAllocator =
        gpuResourceHandler.getGpuAllocator();
    List<GpuDevice> totalGpus = gpuResourceAllocator.getAllowedGpus();
    List<AssignedGpuDevice> assignedGpuDevices =
        gpuResourceAllocator.getAssignedGpus();
    return new NMGpuResourceInfo(gpuDeviceInformation, totalGpus,
        assignedGpuDevices);
  }

  private void checkGpuResourceHandler() throws YarnException {
    if(gpuResourceHandler == null) {
      String errorMsg =
          "Linux Container Executor is not configured for the NodeManager. "
              + "To fully enable GPU feature on the node also set "
              + YarnConfiguration.NM_CONTAINER_EXECUTOR + " properly.";
      LOG.warn(errorMsg);
      throw new YarnException(errorMsg);
    }
  }

  private void checkErrorCount() throws YarnException {
    if (numOfErrorExecutionSinceLastSucceed == MAX_REPEATED_ERROR_ALLOWED) {
      String msg =
          "Failed to execute GPU device information detection script for "
              + MAX_REPEATED_ERROR_ALLOWED
              + " times, skip following executions.";
      LOG.error(msg);
      throw new YarnException(msg);
    }
  }

  @Override
  public String toString() {
    return GpuResourcePlugin.class.getName();
  }
}

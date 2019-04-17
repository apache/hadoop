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

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
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

import java.util.List;

public class GpuResourcePlugin implements ResourcePlugin {
  private final GpuNodeResourceUpdateHandler resourceDiscoverHandler;
  private final GpuDiscoverer gpuDiscoverer;
  private GpuResourceHandlerImpl gpuResourceHandler = null;
  private DockerCommandPlugin dockerCommandPlugin = null;

  public GpuResourcePlugin(GpuNodeResourceUpdateHandler resourceDiscoverHandler,
      GpuDiscoverer gpuDiscoverer) {
    this.resourceDiscoverHandler = resourceDiscoverHandler;
    this.gpuDiscoverer = gpuDiscoverer;
  }

  @Override
  public synchronized void initialize(Context context) throws YarnException {
    this.gpuDiscoverer.initialize(context.getConf());
    this.dockerCommandPlugin =
        GpuDockerCommandPluginFactory.createGpuDockerCommandPlugin(
            context.getConf());
  }

  @Override
  public synchronized ResourceHandler createResourceHandler(
      Context context, CGroupsHandler cGroupsHandler,
      PrivilegedOperationExecutor privilegedOperationExecutor) {
    if (gpuResourceHandler == null) {
      gpuResourceHandler = new GpuResourceHandlerImpl(context, cGroupsHandler,
          privilegedOperationExecutor, gpuDiscoverer);
    }

    return gpuResourceHandler;
  }

  @Override
  public synchronized NodeResourceUpdaterPlugin getNodeResourceHandlerInstance() {
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
    GpuDeviceInformation gpuDeviceInformation =
        gpuDiscoverer.getGpuDeviceInformation();
    GpuResourceAllocator gpuResourceAllocator =
        gpuResourceHandler.getGpuAllocator();
    List<GpuDevice> totalGpus = gpuResourceAllocator.getAllowedGpusCopy();
    List<AssignedGpuDevice> assignedGpuDevices =
        gpuResourceAllocator.getAssignedGpusCopy();

    return new NMGpuResourceInfo(gpuDeviceInformation, totalGpus,
        assignedGpuDevices);
  }

  @Override
  public String toString() {
    return GpuResourcePlugin.class.getName();
  }
}

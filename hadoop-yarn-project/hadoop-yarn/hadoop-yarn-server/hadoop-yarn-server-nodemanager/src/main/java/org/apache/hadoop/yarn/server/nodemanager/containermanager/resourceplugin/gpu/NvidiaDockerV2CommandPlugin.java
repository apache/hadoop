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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.gpu.GpuResourceAllocator;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRunCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerVolumeCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.DockerCommandPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Implementation to use nvidia-docker v2 as GPU docker command plugin.
 */
public class NvidiaDockerV2CommandPlugin implements DockerCommandPlugin {
  final static Logger LOG = LoggerFactory.
      getLogger(NvidiaDockerV2CommandPlugin.class);

  private String nvidiaRuntime = "nvidia";
  private String nvidiaVisibleDevices = "NVIDIA_VISIBLE_DEVICES";

  public NvidiaDockerV2CommandPlugin() {}

  private Set<GpuDevice> getAssignedGpus(Container container) {
    ResourceMappings resourceMappings = container.getResourceMappings();

    // Copy of assigned Resources
    Set<GpuDevice> assignedResources = null;
    if (resourceMappings != null) {
      assignedResources = new HashSet<>();
      for (Serializable s : resourceMappings.getAssignedResources(
          ResourceInformation.GPU_URI)) {
        assignedResources.add((GpuDevice) s);
      }
    }
    if (assignedResources == null || assignedResources.isEmpty()) {
      // When no GPU resource assigned, don't need to update docker command.
      return Collections.emptySet();
    }
    return assignedResources;
  }

  @VisibleForTesting
  protected boolean requestsGpu(Container container) {
    return GpuResourceAllocator.getRequestedGpus(container.getResource()) > 0;
  }

  @Override
  public synchronized void updateDockerRunCommand(
      DockerRunCommand dockerRunCommand, Container container)
      throws ContainerExecutionException {
    if (!requestsGpu(container)) {
      return;
    }
    Set<GpuDevice> assignedResources = getAssignedGpus(container);
    if (assignedResources == null || assignedResources.isEmpty()) {
      return;
    }
    Map<String, String> environment = new HashMap<>();
    String gpuIndexList = "";
    for (GpuDevice gpuDevice : assignedResources) {
      gpuIndexList = gpuIndexList + gpuDevice.getIndex() + ",";
      LOG.info("nvidia docker2 assigned gpu index: " + gpuDevice.getIndex());
    }
    dockerRunCommand.addRuntime(nvidiaRuntime);
    environment.put(nvidiaVisibleDevices,
            gpuIndexList.substring(0, gpuIndexList.length() - 1));
    dockerRunCommand.addEnv(environment);
  }

  @Override
  public DockerVolumeCommand getCreateDockerVolumeCommand(Container container)
      throws ContainerExecutionException {
    // No Volume needed for nvidia-docker2.
    return null;
  }

  @Override
  public DockerVolumeCommand getCleanupDockerVolumesCommand(Container container)
      throws ContainerExecutionException {
    // No cleanup needed.
    return null;
  }
}

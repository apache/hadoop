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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.gpu.GpuResourceAllocator;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRunCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerVolumeCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.DockerCommandPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerVolumeCommand.VOLUME_NAME_PATTERN;

/**
 * Implementation to use nvidia-docker v1 as GPU docker command plugin.
 */
public class NvidiaDockerV1CommandPlugin implements DockerCommandPlugin {
  final static Log LOG = LogFactory.getLog(NvidiaDockerV1CommandPlugin.class);

  private Configuration conf;
  private Map<String, Set<String>> additionalCommands = null;
  private String volumeDriver = "local";

  // Known option
  private String DEVICE_OPTION = "--device";
  private String VOLUME_DRIVER_OPTION = "--volume-driver";
  private String MOUNT_RO_OPTION = "--volume";

  public NvidiaDockerV1CommandPlugin(Configuration conf) {
    this.conf = conf;
  }

  // Get value from key=value
  // Throw exception if '=' not found
  private String getValue(String input) throws IllegalArgumentException {
    int index = input.indexOf('=');
    if (index < 0) {
      throw new IllegalArgumentException(
          "Failed to locate '=' from input=" + input);
    }
    return input.substring(index + 1);
  }

  private void addToCommand(String key, String value) {
    if (additionalCommands == null) {
      additionalCommands = new HashMap<>();
    }
    if (!additionalCommands.containsKey(key)) {
      additionalCommands.put(key, new HashSet<>());
    }
    additionalCommands.get(key).add(value);
  }

  private void init() throws ContainerExecutionException {
    String endpoint = conf.get(
        YarnConfiguration.NVIDIA_DOCKER_PLUGIN_V1_ENDPOINT,
        YarnConfiguration.DEFAULT_NVIDIA_DOCKER_PLUGIN_V1_ENDPOINT);
    if (null == endpoint || endpoint.isEmpty()) {
      LOG.info(YarnConfiguration.NVIDIA_DOCKER_PLUGIN_V1_ENDPOINT
          + " set to empty, skip init ..");
      return;
    }
    String cliOptions;
    try {
      // Talk to plugin server and get options
      URL url = new URL(endpoint);
      URLConnection uc = url.openConnection();
      uc.setRequestProperty("X-Requested-With", "Curl");

      StringWriter writer = new StringWriter();
      IOUtils.copy(uc.getInputStream(), writer, "utf-8");
      cliOptions = writer.toString();

      LOG.info("Additional docker CLI options from plugin to run GPU "
          + "containers:" + cliOptions);

      // Parse cli options
      // Examples like:
      // --device=/dev/nvidiactl --device=/dev/nvidia-uvm --device=/dev/nvidia0
      // --volume-driver=nvidia-docker
      // --volume=nvidia_driver_352.68:/usr/local/nvidia:ro

      for (String str : cliOptions.split(" ")) {
        str = str.trim();
        if (str.startsWith(DEVICE_OPTION)) {
          addToCommand(DEVICE_OPTION, getValue(str));
        } else if (str.startsWith(VOLUME_DRIVER_OPTION)) {
          volumeDriver = getValue(str);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found volume-driver:" + volumeDriver);
          }
        } else if (str.startsWith(MOUNT_RO_OPTION)) {
          String mount = getValue(str);
          if (!mount.endsWith(":ro")) {
            throw new IllegalArgumentException(
                "Should not have mount other than ro, command=" + str);
          }
          addToCommand(MOUNT_RO_OPTION,
              mount.substring(0, mount.lastIndexOf(':')));
        } else{
          throw new IllegalArgumentException("Unsupported option:" + str);
        }
      }
    } catch (RuntimeException e) {
      LOG.warn(
          "RuntimeException of " + this.getClass().getSimpleName() + " init:",
          e);
      throw new ContainerExecutionException(e);
    } catch (IOException e) {
      LOG.warn("IOException of " + this.getClass().getSimpleName() + " init:",
          e);
      throw new ContainerExecutionException(e);
    }
  }

  private int getGpuIndexFromDeviceName(String device) {
    final String NVIDIA = "nvidia";
    int idx = device.lastIndexOf(NVIDIA);
    if (idx < 0) {
      return -1;
    }
    // Get last part
    String str = device.substring(idx + NVIDIA.length());
    for (int i = 0; i < str.length(); i++) {
      if (!Character.isDigit(str.charAt(i))) {
        return -1;
      }
    }
    return Integer.parseInt(str);
  }

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

  /**
   * Do initialize when GPU requested
   * @param container nmContainer
   * @return if #GPU-requested > 0
   * @throws ContainerExecutionException when any issue happens
   */
  private boolean initializeWhenGpuRequested(Container container)
      throws ContainerExecutionException {
    if (!requestsGpu(container)) {
      return false;
    }

    // Do lazy initialization of gpu-docker plugin
    if (additionalCommands == null) {
      init();
    }

    return true;
  }

  @Override
  public synchronized void updateDockerRunCommand(
      DockerRunCommand dockerRunCommand, Container container)
      throws ContainerExecutionException {
    if (!initializeWhenGpuRequested(container)) {
      return;
    }

    Set<GpuDevice> assignedResources = getAssignedGpus(container);
    if (assignedResources == null || assignedResources.isEmpty()) {
      return;
    }

    // Write to dockerRunCommand
    for (Map.Entry<String, Set<String>> option : additionalCommands
        .entrySet()) {
      String key = option.getKey();
      Set<String> values = option.getValue();
      if (key.equals(DEVICE_OPTION)) {
        int foundGpuDevices = 0;
        for (String deviceName : values) {
          // When specified is a GPU card (device name like /dev/nvidia[n]
          // Get index of the GPU (which is [n]).
          Integer gpuIdx = getGpuIndexFromDeviceName(deviceName);
          if (gpuIdx >= 0) {
            // Use assignedResources to filter --device given by
            // nvidia-docker-plugin.
            for (GpuDevice gpuDevice : assignedResources) {
              if (gpuDevice.getIndex() == gpuIdx) {
                foundGpuDevices++;
                dockerRunCommand.addDevice(deviceName, deviceName);
              }
            }
          } else{
            // When gpuIdx < 0, it is a controller device (such as
            // /dev/nvidiactl). In this case, add device directly.
            dockerRunCommand.addDevice(deviceName, deviceName);
          }
        }

        // Cannot get all assigned Gpu devices from docker plugin output
        if (foundGpuDevices < assignedResources.size()) {
          throw new ContainerExecutionException(
              "Cannot get all assigned Gpu devices from docker plugin output");
        }
      } else if (key.equals(MOUNT_RO_OPTION)) {
        for (String value : values) {
          int idx = value.indexOf(':');
          String source = value.substring(0, idx);
          String target = value.substring(idx + 1);
          dockerRunCommand.addReadOnlyMountLocation(source, target, true);
        }
      } else{
        throw new ContainerExecutionException("Unsupported option:" + key);
      }
    }
  }

  @Override
  public DockerVolumeCommand getCreateDockerVolumeCommand(Container container)
      throws ContainerExecutionException {
    if (!initializeWhenGpuRequested(container)) {
      return null;
    }

    String newVolumeName = null;

    // Get volume name
    Set<String> mounts = additionalCommands.get(MOUNT_RO_OPTION);
    for (String mount : mounts) {
      int idx = mount.indexOf(':');
      if (idx >= 0) {
        String mountSource = mount.substring(0, idx);
        if (VOLUME_NAME_PATTERN.matcher(mountSource).matches()) {
          // This is a valid named volume
          newVolumeName = mountSource;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found volume name for GPU:" + newVolumeName);
          }
          break;
        } else{
          if (LOG.isDebugEnabled()) {
            LOG.debug("Failed to match " + mountSource
                + " to named-volume regex pattern");
          }
        }
      }
    }

    if (newVolumeName != null) {
      DockerVolumeCommand command = new DockerVolumeCommand(
          DockerVolumeCommand.VOLUME_CREATE_COMMAND);
      command.setDriverName(volumeDriver);
      command.setVolumeName(newVolumeName);
      return command;
    }

    return null;
  }

  @Override
  public DockerVolumeCommand getCleanupDockerVolumesCommand(Container container)
      throws ContainerExecutionException {
    // No cleanup needed.
    return null;
  }
}

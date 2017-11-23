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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.gpu;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.ResourceNotFoundException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.AssignedGpuDevice;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuDevice;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.GPU_URI;

/**
 * Allocate GPU resources according to requirements
 */
public class GpuResourceAllocator {
  final static Log LOG = LogFactory.getLog(GpuResourceAllocator.class);

  private Set<GpuDevice> allowedGpuDevices = new TreeSet<>();
  private Map<GpuDevice, ContainerId> usedDevices = new TreeMap<>();
  private Context nmContext;

  public GpuResourceAllocator(Context ctx) {
    this.nmContext = ctx;
  }

  /**
   * Contains allowed and denied devices
   * Denied devices will be useful for cgroups devices module to do blacklisting
   */
  static class GpuAllocation {
    private Set<GpuDevice> allowed = Collections.emptySet();
    private Set<GpuDevice> denied = Collections.emptySet();

    GpuAllocation(Set<GpuDevice> allowed, Set<GpuDevice> denied) {
      if (allowed != null) {
        this.allowed = ImmutableSet.copyOf(allowed);
      }
      if (denied != null) {
        this.denied = ImmutableSet.copyOf(denied);
      }
    }

    public Set<GpuDevice> getAllowedGPUs() {
      return allowed;
    }

    public Set<GpuDevice> getDeniedGPUs() {
      return denied;
    }
  }

  /**
   * Add GPU to allowed list
   * @param gpuDevice gpu device
   */
  public synchronized void addGpu(GpuDevice gpuDevice) {
    allowedGpuDevices.add(gpuDevice);
  }

  private String getResourceHandlerExceptionMessage(int numRequestedGpuDevices,
      ContainerId containerId) {
    return "Failed to find enough GPUs, requestor=" + containerId
        + ", #RequestedGPUs=" + numRequestedGpuDevices + ", #availableGpus="
        + getAvailableGpus();
  }

  @VisibleForTesting
  public synchronized int getAvailableGpus() {
    return allowedGpuDevices.size() - usedDevices.size();
  }

  public synchronized void recoverAssignedGpus(ContainerId containerId)
      throws ResourceHandlerException {
    Container c = nmContext.getContainers().get(containerId);
    if (null == c) {
      throw new ResourceHandlerException(
          "This shouldn't happen, cannot find container with id="
              + containerId);
    }

    for (Serializable gpuDeviceSerializable : c.getResourceMappings()
        .getAssignedResources(GPU_URI)) {
      if (!(gpuDeviceSerializable instanceof GpuDevice)) {
        throw new ResourceHandlerException(
            "Trying to recover device id, however it"
                + " is not GpuDevice, this shouldn't happen");
      }

      GpuDevice gpuDevice = (GpuDevice) gpuDeviceSerializable;

      // Make sure it is in allowed GPU device.
      if (!allowedGpuDevices.contains(gpuDevice)) {
        throw new ResourceHandlerException(
            "Try to recover device = " + gpuDevice
                + " however it is not in allowed device list:" + StringUtils
                .join(",", allowedGpuDevices));
      }

      // Make sure it is not occupied by anybody else
      if (usedDevices.containsKey(gpuDevice)) {
        throw new ResourceHandlerException(
            "Try to recover device id = " + gpuDevice
                + " however it is already assigned to container=" + usedDevices
                .get(gpuDevice) + ", please double check what happened.");
      }

      usedDevices.put(gpuDevice, containerId);
    }
  }

  /**
   * Get number of requested GPUs from resource.
   * @param requestedResource requested resource
   * @return #gpus.
   */
  public static int getRequestedGpus(Resource requestedResource) {
    try {
      return Long.valueOf(requestedResource.getResourceValue(
          GPU_URI)).intValue();
    } catch (ResourceNotFoundException e) {
      return 0;
    }
  }

  /**
   * Assign GPU to requestor
   * @param container container to allocate
   * @return allocation results.
   * @throws ResourceHandlerException When failed to assign GPUs.
   */
  public synchronized GpuAllocation assignGpus(Container container)
      throws ResourceHandlerException {
    Resource requestedResource = container.getResource();
    ContainerId containerId = container.getContainerId();
    int numRequestedGpuDevices = getRequestedGpus(requestedResource);
    // Assign Gpus to container if requested some.
    if (numRequestedGpuDevices > 0) {
      if (numRequestedGpuDevices > getAvailableGpus()) {
        throw new ResourceHandlerException(
            getResourceHandlerExceptionMessage(numRequestedGpuDevices,
                containerId));
      }

      Set<GpuDevice> assignedGpus = new TreeSet<>();

      for (GpuDevice gpu : allowedGpuDevices) {
        if (!usedDevices.containsKey(gpu)) {
          usedDevices.put(gpu, containerId);
          assignedGpus.add(gpu);
          if (assignedGpus.size() == numRequestedGpuDevices) {
            break;
          }
        }
      }

      // Record in state store if we allocated anything
      if (!assignedGpus.isEmpty()) {
        try {
          // Update state store.
          nmContext.getNMStateStore().storeAssignedResources(container, GPU_URI,
              new ArrayList<>(assignedGpus));
        } catch (IOException e) {
          cleanupAssignGpus(containerId);
          throw new ResourceHandlerException(e);
        }
      }

      return new GpuAllocation(assignedGpus,
          Sets.difference(allowedGpuDevices, assignedGpus));
    }
    return new GpuAllocation(null, allowedGpuDevices);
  }

  /**
   * Clean up all Gpus assigned to containerId
   * @param containerId containerId
   */
  public synchronized void cleanupAssignGpus(ContainerId containerId) {
    Iterator<Map.Entry<GpuDevice, ContainerId>> iter =
        usedDevices.entrySet().iterator();
    while (iter.hasNext()) {
      if (iter.next().getValue().equals(containerId)) {
        iter.remove();
      }
    }
  }

  @VisibleForTesting
  public synchronized Map<GpuDevice, ContainerId> getDeviceAllocationMappingCopy() {
    return new HashMap<>(usedDevices);
  }

  public synchronized List<GpuDevice> getAllowedGpusCopy() {
    return new ArrayList<>(allowedGpuDevices);
  }

  public synchronized List<AssignedGpuDevice> getAssignedGpusCopy() {
    List<AssignedGpuDevice> assigns = new ArrayList<>();
    for (Map.Entry<GpuDevice, ContainerId> entry : usedDevices.entrySet()) {
      assigns.add(new AssignedGpuDevice(entry.getKey().getIndex(),
          entry.getKey().getMinorNumber(), entry.getValue()));
    }
    return assigns;
  }
}

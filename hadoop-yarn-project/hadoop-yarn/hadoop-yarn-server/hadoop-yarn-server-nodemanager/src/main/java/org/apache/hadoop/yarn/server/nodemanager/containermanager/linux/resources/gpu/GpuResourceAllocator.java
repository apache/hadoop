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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.exceptions.ResourceNotFoundException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.AssignedGpuDevice;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuDevice;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.GPU_URI;

/**
 * Allocate GPU resources according to requirements.
 */
public class GpuResourceAllocator {
  final static Logger LOG = LoggerFactory.
      getLogger(GpuResourceAllocator.class);

  private static final int WAIT_MS_PER_LOOP = 1000;

  private Set<GpuDevice> allowedGpuDevices = new TreeSet<>();
  private Map<GpuDevice, ContainerId> usedDevices = new TreeMap<>();
  private Context nmContext;
  private final int waitPeriodForResource;

  public GpuResourceAllocator(Context ctx) {
    this.nmContext = ctx;
    // Wait for a maximum of 120 seconds if no available GPU are there which
    // are yet to be released.
    this.waitPeriodForResource = 120 * WAIT_MS_PER_LOOP;
  }

  @VisibleForTesting
  GpuResourceAllocator(Context ctx, int waitPeriodForResource) {
    this.nmContext = ctx;
    this.waitPeriodForResource = waitPeriodForResource;
  }

  /**
   * Contains allowed and denied devices.
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
   * Add GPU to the allowed list of GPUs.
   * @param gpuDevice gpu device
   */
  public synchronized void addGpu(GpuDevice gpuDevice) {
    allowedGpuDevices.add(gpuDevice);
  }

  @VisibleForTesting
  public synchronized int getAvailableGpus() {
    return allowedGpuDevices.size() - usedDevices.size();
  }

  public synchronized void recoverAssignedGpus(ContainerId containerId)
      throws ResourceHandlerException {
    Container c = nmContext.getContainers().get(containerId);
    if (c == null) {
      throw new ResourceHandlerException(
          "Cannot find container with id=" + containerId +
              ", this should not occur under normal circumstances!");
    }

    LOG.info("Starting recovery of GpuDevice for {}.", containerId);
    for (Serializable gpuDeviceSerializable : c.getResourceMappings()
        .getAssignedResources(GPU_URI)) {
      if (!(gpuDeviceSerializable instanceof GpuDevice)) {
        throw new ResourceHandlerException(
            "Trying to recover device id, however it"
                + " is not an instance of " + GpuDevice.class.getName()
                + ", this should not occur under normal circumstances!");
      }

      GpuDevice gpuDevice = (GpuDevice) gpuDeviceSerializable;

      // Make sure it is in allowed GPU device.
      if (!allowedGpuDevices.contains(gpuDevice)) {
        throw new ResourceHandlerException(
            "Try to recover device = " + gpuDevice
                + " however it is not in the allowed device list:" +
                StringUtils.join(",", allowedGpuDevices));
      }

      // Make sure it is not occupied by anybody else
      if (usedDevices.containsKey(gpuDevice)) {
        throw new ResourceHandlerException(
            "Try to recover device id = " + gpuDevice
                + " however it is already assigned to container=" + usedDevices
                .get(gpuDevice) + ", please double check what happened.");
      }

      usedDevices.put(gpuDevice, containerId);
      LOG.info("ContainerId {} is assigned to GpuDevice {} on recovery.",
          containerId, gpuDevice);
    }
    LOG.info("Finished recovery of GpuDevice for {}.", containerId);
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
   * Assign GPU to the specified container.
   * @param container container to allocate
   * @return allocation results.
   * @throws ResourceHandlerException When failed to assign GPUs.
   */
  public GpuAllocation assignGpus(Container container)
      throws ResourceHandlerException {
    GpuAllocation allocation = internalAssignGpus(container);

    // Wait for a maximum of waitPeriodForResource seconds if no
    // available GPU are there which are yet to be released.
    int timeWaiting = 0;
    while (allocation == null) {
      if (timeWaiting >= waitPeriodForResource) {
        break;
      }

      // Sleep for 1 sec to ensure there are some free GPU devices which are
      // getting released.
      try {
        LOG.info("Container : " + container.getContainerId()
            + " is waiting for free GPU devices.");
        Thread.sleep(WAIT_MS_PER_LOOP);
        timeWaiting += WAIT_MS_PER_LOOP;
        allocation = internalAssignGpus(container);
      } catch (InterruptedException e) {
        // On any interrupt, break the loop and continue execution.
        Thread.currentThread().interrupt();
        LOG.warn("Interrupted while waiting for available GPU");
        break;
      }
    }

    if(allocation == null) {
      String message = "Could not get valid GPU device for container '" +
          container.getContainerId()
          + "' as some other containers might not releasing GPUs.";
      LOG.warn(message);
      throw new ResourceHandlerException(message);
    }
    return allocation;
  }

  private synchronized GpuAllocation internalAssignGpus(Container container)
      throws ResourceHandlerException {
    Resource requestedResource = container.getResource();
    ContainerId containerId = container.getContainerId();
    int numRequestedGpuDevices = getRequestedGpus(requestedResource);

    // Assign GPUs to container if requested some.
    if (numRequestedGpuDevices > 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Trying to assign %d GPUs to container: %s" +
            ", #AvailableGPUs=%d, #ReleasingGPUs=%d",
            numRequestedGpuDevices, containerId,
            getAvailableGpus(), getReleasingGpus()));
      }
      if (numRequestedGpuDevices > getAvailableGpus()) {
        // If there are some devices which are getting released, wait for few
        // seconds to get it.
        if (numRequestedGpuDevices <= getReleasingGpus() + getAvailableGpus()) {
          return null;
        }
      }

      if (numRequestedGpuDevices > getAvailableGpus()) {
        throw new ResourceHandlerException(
            "Failed to find enough GPUs, requestor=" + containerId +
                ", #RequestedGPUs=" + numRequestedGpuDevices +
                ", #AvailableGPUs=" + getAvailableGpus());
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
          unassignGpus(containerId);
          throw new ResourceHandlerException(e);
        }
      }

      return new GpuAllocation(assignedGpus,
          Sets.difference(allowedGpuDevices, assignedGpus));
    }
    return new GpuAllocation(null, allowedGpuDevices);
  }

  private synchronized long getReleasingGpus() {
    long releasingGpus = 0;
    for (ContainerId containerId : ImmutableSet.copyOf(usedDevices.values())) {
      Container container;
      if ((container = nmContext.getContainers().get(containerId)) != null) {
        if (container.isContainerInFinalStates()) {
          releasingGpus = releasingGpus + container.getResource()
              .getResourceInformation(ResourceInformation.GPU_URI).getValue();
        }
      }
    }
    return releasingGpus;
  }

  /**
   * Clean up all GPUs assigned to containerId.
   * @param containerId containerId
   */
  public synchronized void unassignGpus(ContainerId containerId) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Trying to unassign GPU device from container " + containerId);
    }
    usedDevices.entrySet().removeIf(entry ->
        entry.getValue().equals(containerId));
  }

  @VisibleForTesting
  public synchronized Map<GpuDevice, ContainerId> getDeviceAllocationMapping() {
    return ImmutableMap.copyOf(usedDevices);
  }

  public synchronized List<GpuDevice> getAllowedGpus() {
    return ImmutableList.copyOf(allowedGpuDevices);
  }

  public synchronized List<AssignedGpuDevice> getAssignedGpus() {
    return usedDevices.entrySet().stream()
        .map(e -> {
          final GpuDevice gpu = e.getKey();
          ContainerId containerId = e.getValue();
          return new AssignedGpuDevice(gpu.getIndex(), gpu.getMinorNumber(),
              containerId);
        }).collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return GpuResourceAllocator.class.getName();
  }
}

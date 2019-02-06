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
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.exceptions.ResourceNotFoundException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
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

  private Set<Integer> allowedGpuDevices = new TreeSet<>();
  private Map<Integer, ContainerId> usedDevices = new TreeMap<>();
  private Context nmContext;

  public GpuResourceAllocator(Context ctx) {
    this.nmContext = ctx;
  }

  /**
   * Contains allowed and denied devices with minor number.
   * Denied devices will be useful for cgroups devices module to do blacklisting
   */
  static class GpuAllocation {
    private Set<Integer> allowed = Collections.emptySet();
    private Set<Integer> denied = Collections.emptySet();

    GpuAllocation(Set<Integer> allowed, Set<Integer> denied) {
      if (allowed != null) {
        this.allowed = ImmutableSet.copyOf(allowed);
      }
      if (denied != null) {
        this.denied = ImmutableSet.copyOf(denied);
      }
    }

    public Set<Integer> getAllowedGPUs() {
      return allowed;
    }

    public Set<Integer> getDeniedGPUs() {
      return denied;
    }
  }

  /**
   * Add GPU to allowed list
   * @param minorNumber minor number of the GPU device.
   */
  public synchronized void addGpu(int minorNumber) {
    allowedGpuDevices.add(minorNumber);
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

    for (Serializable deviceId : c.getResourceMappings().getAssignedResources(
        GPU_URI)){
      if (!(deviceId instanceof String)) {
        throw new ResourceHandlerException(
            "Trying to recover device id, however it"
                + " is not String, this shouldn't happen");
      }


      int devId;
      try {
        devId = Integer.parseInt((String)deviceId);
      } catch (NumberFormatException e) {
        throw new ResourceHandlerException("Failed to recover device id because"
            + "it is not a valid integer, devId:" + deviceId);
      }

      // Make sure it is in allowed GPU device.
      if (!allowedGpuDevices.contains(devId)) {
        throw new ResourceHandlerException("Try to recover device id = " + devId
            + " however it is not in allowed device list:" + StringUtils
            .join(",", allowedGpuDevices));
      }

      // Make sure it is not occupied by anybody else
      if (usedDevices.containsKey(devId)) {
        throw new ResourceHandlerException("Try to recover device id = " + devId
            + " however it is already assigned to container=" + usedDevices
            .get(devId) + ", please double check what happened.");
      }

      usedDevices.put(devId, containerId);
    }
  }

  private int getRequestedGpus(Resource requestedResource) {
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
   * @return List of denied Gpus with minor numbers
   * @throws ResourceHandlerException When failed to
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

      Set<Integer> assignedGpus = new HashSet<>();

      for (int deviceNum : allowedGpuDevices) {
        if (!usedDevices.containsKey(deviceNum)) {
          usedDevices.put(deviceNum, containerId);
          assignedGpus.add(deviceNum);
          if (assignedGpus.size() == numRequestedGpuDevices) {
            break;
          }
        }
      }

      // Record in state store if we allocated anything
      if (!assignedGpus.isEmpty()) {
        List<Serializable> allocatedDevices = new ArrayList<>();
        for (int gpu : assignedGpus) {
          allocatedDevices.add(String.valueOf(gpu));
        }
        try {
          // Update Container#getResourceMapping.
          ResourceMappings.AssignedResources assignedResources =
              new ResourceMappings.AssignedResources();
          assignedResources.updateAssignedResources(allocatedDevices);
          container.getResourceMappings().addAssignedResources(GPU_URI,
              assignedResources);

          // Update state store.
          nmContext.getNMStateStore().storeAssignedResources(containerId,
              GPU_URI, allocatedDevices);
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
    Iterator<Map.Entry<Integer, ContainerId>> iter =
        usedDevices.entrySet().iterator();
    while (iter.hasNext()) {
      if (iter.next().getValue().equals(containerId)) {
        iter.remove();
      }
    }
  }

  @VisibleForTesting
  public synchronized Map<Integer, ContainerId> getDeviceAllocationMapping() {
     return new HashMap<>(usedDevices);
  }
}

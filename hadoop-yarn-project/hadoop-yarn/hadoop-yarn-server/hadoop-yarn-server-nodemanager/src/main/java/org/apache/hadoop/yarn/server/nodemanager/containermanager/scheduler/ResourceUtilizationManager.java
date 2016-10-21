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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceUtilizationManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ResourceUtilizationManager.class);

  private ResourceUtilization containersAllocation;
  private ContainerScheduler scheduler;

  public ResourceUtilizationManager(ContainerScheduler scheduler) {
    this.containersAllocation = ResourceUtilization.newInstance(0, 0, 0.0f);
    this.scheduler = scheduler;
  }

  public ResourceUtilization getCurrentUtilization() {
    return this.containersAllocation;
  }

  public void addContainerResources(Container container) {
    increaseResourceUtilization(
        getContainersMonitor(), this.containersAllocation,
        container.getResource());
  }

  public void subtractContainerResource(Container container) {
    decreaseResourceUtilization(
        getContainersMonitor(), this.containersAllocation,
        container.getResource());
  }

  public boolean hasResourcesAvailable(Container container) {
    long pMemBytes = container.getResource().getMemorySize() * 1024 * 1024L;
    return hasResourcesAvailable(pMemBytes,
        (long) (getContainersMonitor().getVmemRatio()* pMemBytes),
        container.getResource().getVirtualCores());
  }

  private boolean hasResourcesAvailable(long pMemBytes, long vMemBytes,
      int cpuVcores) {
    // Check physical memory.
    if (LOG.isDebugEnabled()) {
      LOG.debug("pMemCheck [current={} + asked={} > allowed={}]",
          this.containersAllocation.getPhysicalMemory(),
          (pMemBytes >> 20),
          (getContainersMonitor().getPmemAllocatedForContainers() >> 20));
    }
    if (this.containersAllocation.getPhysicalMemory() +
        (int) (pMemBytes >> 20) >
        (int) (getContainersMonitor()
            .getPmemAllocatedForContainers() >> 20)) {
      return false;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("before vMemCheck" +
              "[isEnabled={}, current={} + asked={} > allowed={}]",
          getContainersMonitor().isVmemCheckEnabled(),
          this.containersAllocation.getVirtualMemory(), (vMemBytes >> 20),
          (getContainersMonitor().getVmemAllocatedForContainers() >> 20));
    }
    // Check virtual memory.
    if (getContainersMonitor().isVmemCheckEnabled() &&
        this.containersAllocation.getVirtualMemory() +
            (int) (vMemBytes >> 20) >
            (int) (getContainersMonitor()
                .getVmemAllocatedForContainers() >> 20)) {
      return false;
    }

    float vCores = (float) cpuVcores /
        getContainersMonitor().getVCoresAllocatedForContainers();
    if (LOG.isDebugEnabled()) {
      LOG.debug("before cpuCheck [asked={} > allowed={}]",
          this.containersAllocation.getCPU(), vCores);
    }
    // Check CPU.
    if (this.containersAllocation.getCPU() + vCores > 1.0f) {
      return false;
    }
    return true;
  }

  public ContainersMonitor getContainersMonitor() {
    return this.scheduler.getContainersMonitor();
  }

  public static void increaseResourceUtilization(
      ContainersMonitor containersMonitor, ResourceUtilization resourceAlloc,
      Resource resource) {
    float vCores = (float) resource.getVirtualCores() /
        containersMonitor.getVCoresAllocatedForContainers();
    int vmem = (int) (resource.getMemorySize()
        * containersMonitor.getVmemRatio());
    resourceAlloc.addTo((int)resource.getMemorySize(), vmem, vCores);
  }

  public static void decreaseResourceUtilization(
      ContainersMonitor containersMonitor, ResourceUtilization resourceAlloc,
      Resource resource) {
    float vCores = (float) resource.getVirtualCores() /
        containersMonitor.getVCoresAllocatedForContainers();
    int vmem = (int) (resource.getMemorySize()
        * containersMonitor.getVmemRatio());
    resourceAlloc.subtractFrom((int)resource.getMemorySize(), vmem, vCores);
  }
}

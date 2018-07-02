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

/**
 * An implementation of the resource utilization tracker that equates
 * resource utilization with the total resource allocated to the container.
 */
public class AllocationBasedResourceTracker
    implements ResourceUtilizationTracker {

  private static final Logger LOG =
      LoggerFactory.getLogger(AllocationBasedResourceTracker.class);

  private static final Resource UNAVAILABLE =
      Resource.newInstance(0, 0);

  private ResourceUtilization containersAllocation;
  private ContainerScheduler scheduler;


  AllocationBasedResourceTracker(ContainerScheduler scheduler) {
    this.containersAllocation = ResourceUtilization.newInstance(0, 0, 0.0f);
    this.scheduler = scheduler;
  }

  /**
   * Get the accumulation of totally allocated resources to containers.
   * @return ResourceUtilization Resource Utilization.
   */
  @Override
  public ResourceUtilization getCurrentUtilization() {
    return getTotalAllocation();
  }

  @Override
  public ResourceUtilization getTotalAllocation() {
    return this.containersAllocation;
  }

  @Override
  public Resource getUnallocatedResources() {
    // unallocated resources = node capacity - containers allocation
    // = -(container allocation - node capacity)
    ResourceUtilization allocationClone =
        ResourceUtilization.newInstance(containersAllocation);
    getContainersMonitor()
        .subtractNodeResourcesFromResourceUtilization(allocationClone);

    Resource unallocated = UNAVAILABLE;
    if (allocationClone.getCPU() <= 0 &&
        allocationClone.getPhysicalMemory() <= 0 &&
        allocationClone.getVirtualMemory() <= 0) {
      int cpu = Math.round(allocationClone.getCPU() *
          getContainersMonitor().getVCoresAllocatedForContainers());
      long memory = allocationClone.getPhysicalMemory();
      unallocated = Resource.newInstance(-memory, -cpu);
    }
    return unallocated;
  }

  @Override
  public Resource getAvailableResources() {
    return getUnallocatedResources();
  }

  /**
   * Add Container's resources to the accumulated allocation.
   * @param container Container.
   */
  @Override
  public void containerLaunched(Container container) {
    ContainersMonitor.increaseResourceUtilization(
        getContainersMonitor(), this.containersAllocation,
        container.getResource());
  }

  /**
   * Subtract Container's resources to the accumulated allocation.
   * @param container Container.
   */
  @Override
  public void containerReleased(Container container) {
    ContainersMonitor.decreaseResourceUtilization(
        getContainersMonitor(), this.containersAllocation,
        container.getResource());
  }

  public ContainersMonitor getContainersMonitor() {
    return this.scheduler.getContainersMonitor();
  }
}

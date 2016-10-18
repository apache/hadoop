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
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;

public class ResourceUtilizationManager {

  private ResourceUtilization containersAllocation;
  private ContainerScheduler scheduler;

  public ResourceUtilizationManager(ContainerScheduler scheduler) {
    this.containersAllocation = ResourceUtilization.newInstance(0, 0, 0.0f);
    this.scheduler = scheduler;
  }

  public ResourceUtilization getCurrentUtilization() {
    return this.containersAllocation;
  }

  public void addResource(Resource resource) {
    increaseResourceUtilization(
        getContainersMonitor(), this.containersAllocation, resource);
  }

  public void subtractResource(Resource resource) {
    decreaseResourceUtilization(
        getContainersMonitor(), this.containersAllocation, resource);
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

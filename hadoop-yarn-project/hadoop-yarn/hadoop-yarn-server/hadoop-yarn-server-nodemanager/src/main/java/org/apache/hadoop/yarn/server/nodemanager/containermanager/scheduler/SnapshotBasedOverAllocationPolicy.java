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
import org.apache.hadoop.yarn.server.api.records.ResourceThresholds;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;

/**
 * An implementation of NMAllocationPolicy based on the
 * snapshot of the latest containers utilization to determine how much
 * resources are available * to launch containers when over-allocation
 * is turned on.
 */
public class SnapshotBasedOverAllocationPolicy
    extends NMAllocationPolicy {

  public SnapshotBasedOverAllocationPolicy(
      ResourceThresholds overAllocationThresholds,
      ContainersMonitor containersMonitor) {
    super(overAllocationThresholds, containersMonitor);
  }

  @Override
  public Resource getAvailableResources() {
    ResourceUtilization utilization =
        containersMonitor.getContainersUtilization(true).getUtilization();
    long memoryAvailable = Math.round(
        overAllocationThresholds.getMemoryThreshold() *
            containersMonitor.getPmemAllocatedForContainers()) -
        (utilization.getPhysicalMemory() << 20);
    int vcoreAvailable = Math.round(
        (overAllocationThresholds.getCpuThreshold() - utilization.getCPU()) *
            containersMonitor.getVCoresAllocatedForContainers());
    return Resource.newInstance(memoryAvailable >> 20, vcoreAvailable);
  }
}

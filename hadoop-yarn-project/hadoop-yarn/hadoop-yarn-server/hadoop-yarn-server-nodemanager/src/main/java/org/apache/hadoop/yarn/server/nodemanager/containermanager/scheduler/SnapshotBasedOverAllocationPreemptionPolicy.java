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

import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.server.api.records.ResourceThresholds;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;

/**
 * An implementation of {@link NMAllocationPreemptionPolicy} based on the
 * snapshot of the latest containers utilization to determine how many
 * resources need to be reclaimed by preempting opportunistic containers
 * when over-allocation is turned on.
 */
public class SnapshotBasedOverAllocationPreemptionPolicy
    extends NMAllocationPreemptionPolicy {
  private final int absoluteMemoryPreemptionThresholdMb;
  private final float cpuPreemptionThreshold;
  private final int maxTimesCpuOverPreemption;
  private int timesCpuOverPreemption;

  public SnapshotBasedOverAllocationPreemptionPolicy(
      ResourceThresholds preemptionThresholds,
      int timesCpuOverPreemptionThreshold,
      ContainersMonitor containersMonitor) {
    super(preemptionThresholds, containersMonitor);
    int memoryCapacityMb = (int)
        (containersMonitor.getPmemAllocatedForContainers() / (1024 * 1024));
    this.absoluteMemoryPreemptionThresholdMb = (int)
        (preemptionThresholds.getMemoryThreshold() * memoryCapacityMb);
    this.cpuPreemptionThreshold = preemptionThresholds.getCpuThreshold();
    this.maxTimesCpuOverPreemption = timesCpuOverPreemptionThreshold;
  }

  @Override
  public ResourceUtilization getResourcesToReclaim() {
    ResourceUtilization utilization =
        getContainersMonitor().getContainersUtilization(true).getUtilization();

    int memoryOverLimit = utilization.getPhysicalMemory() -
        absoluteMemoryPreemptionThresholdMb;
    float vcoreOverLimit = utilization.getCPU() - cpuPreemptionThreshold;

    if (vcoreOverLimit > 0) {
      timesCpuOverPreemption++;
      if (timesCpuOverPreemption > maxTimesCpuOverPreemption) {
        timesCpuOverPreemption = 0;
      } else {
        // report no over limit for cpu if # of times CPU is over the preemption
        // threshold is not greater the max number of times allowed
        vcoreOverLimit = 0;
      }
    } else {
      // reset the counter when cpu utilization goes under the preemption
      // threshold before the max times allowed is reached
      timesCpuOverPreemption = 0;
    }

    // sanitize so that zero is returned if the utilization is below
    // the preemption threshold
    vcoreOverLimit = Math.max(0, vcoreOverLimit);
    memoryOverLimit = Math.max(0, memoryOverLimit);

    return ResourceUtilization.newInstance(memoryOverLimit, 0, vcoreOverLimit);
  }
}

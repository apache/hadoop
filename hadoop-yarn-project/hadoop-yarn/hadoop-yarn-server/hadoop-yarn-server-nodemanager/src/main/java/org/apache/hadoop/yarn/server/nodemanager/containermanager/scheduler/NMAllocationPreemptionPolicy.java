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
 * Keeps track of containers utilization over time and determines how many
 * resources need to be reclaimed by preempting opportunistic containers
 * when over-allocation is turned on.
 */
public abstract class NMAllocationPreemptionPolicy {
  private final ResourceThresholds overAllocationPreemptionThresholds;
  private final ContainersMonitor containersMonitor;

  public NMAllocationPreemptionPolicy(
      ResourceThresholds preemptionThresholds,
      ContainersMonitor containersMonitor) {
    this.containersMonitor = containersMonitor;
    this.overAllocationPreemptionThresholds = preemptionThresholds;
  }

  /**
   * Get the amount of resources to reclaim by preempting opportunistic
   * containers when over-allocation is turned on.
   * @return the amount of resources to be reclaimed
   */
  public abstract ResourceUtilization getResourcesToReclaim();

  public ContainersMonitor getContainersMonitor() {
    return containersMonitor;
  }

  public ResourceThresholds getOverAllocationPreemptionThresholds() {
    return overAllocationPreemptionThresholds;
  }
}

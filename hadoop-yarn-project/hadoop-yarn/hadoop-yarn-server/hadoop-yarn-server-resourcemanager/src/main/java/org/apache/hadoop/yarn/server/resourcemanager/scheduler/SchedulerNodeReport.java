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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * Node usage report.
 */
@Private
@Stable
public class SchedulerNodeReport {
  private final Resource guaranteedResourceUsage;
  private final Resource opportunisticResourceUsage;
  private final Resource guaranteedResourceAvail;
  private final int numOfGuaranteedContainers;
  private final int numOfOpportunisticContainers;

  public SchedulerNodeReport(SchedulerNode node) {
    this.guaranteedResourceUsage = node.getAllocatedResource();
    this.opportunisticResourceUsage = node.getOpportunisticResourceAllocated();
    this.guaranteedResourceAvail = node.getUnallocatedResource();
    this.numOfGuaranteedContainers = node.getNumGuaranteedContainers();
    this.numOfOpportunisticContainers = node.getNumOpportunisticContainers();
  }
  
  /**
   * @return the amount of guaranteed resources currently used by the node.
   */
  public Resource getGuaranteedResourceUsed() {
    return guaranteedResourceUsage;
  }

  /**
   * @return the amount of opportunistic resources currently used by the node.
   */
  public Resource getOpportunisticResourceUsed() {
    return opportunisticResourceUsage;
  }

  /**
   * @return the amount of guaranteed resources currently available on the node
   */
  public Resource getAvailableGuaranteedResource() {
    return guaranteedResourceAvail;
  }

  /**
   * @return the number of guaranteed containers currently running on
   *         this node.
   */
  public int getNumGuaranteedContainers() {
    return numOfGuaranteedContainers;
  }

  /**
   * @return the number of opportunistic containers currently running on
   *         this node.
   */
  public int getNumOpportunisticContainers() {
    return numOfOpportunisticContainers;
  }
}

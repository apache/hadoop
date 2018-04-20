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
import org.apache.hadoop.yarn.server.api.records.ResourceThresholds;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;

/**
 * Keeps track of containers utilization over time and determines how much
 * resources are available to launch containers when over-allocation is on.
 */
public abstract class NMAllocationPolicy {
  protected final ResourceThresholds overAllocationThresholds;
  protected final ContainersMonitor containersMonitor;

  public NMAllocationPolicy(
      ResourceThresholds overAllocationThresholds,
      ContainersMonitor containersMonitor) {
    this.containersMonitor = containersMonitor;
    this.overAllocationThresholds = overAllocationThresholds;
  }

  /**
   * Handle container launch events.
   * @param container the container that has been launched
   */
  public void containerLaunched(Container container) {

  }

  /**
   * Handle container release events.
   * @param container the container that has been released
   */
  public void containerReleased(Container container) {

  }

  /**
   * Get the amount of resources to launch containers when
   * over-allocation is turned on.
   * @return the amount of resources available to launch containers
   */
  public abstract Resource getAvailableResources();
}

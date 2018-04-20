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
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* An resource availability tracker that determines if there are resources
* available based on if there are unallocated resources or if there are
* un-utilized resources.
*/
public class UtilizationBasedResourceTracker
    extends AllocationBasedResourceTracker {
  private static final Logger LOG =
      LoggerFactory.getLogger(AllocationBasedResourceTracker.class);

  private final NMAllocationPolicy overAllocationPolicy;

  UtilizationBasedResourceTracker(ContainerScheduler scheduler) {
    super(scheduler);
    this.overAllocationPolicy =
        getContainersMonitor().getContainerOverAllocationPolicy();
  }

  @Override
  public void containerLaunched(Container container) {
    super.containerLaunched(container);
    if (overAllocationPolicy != null) {
      overAllocationPolicy.containerLaunched(container);
    }
  }

  @Override
  public void containerReleased(Container container) {
    super.containerReleased(container);
    if (overAllocationPolicy != null) {
      overAllocationPolicy.containerReleased(container);
    }
  }

  @Override
  public Resource getAvailableResources() {
    Resource resourceBasedOnAllocation = getUnallocatedResources();
    Resource resourceBasedOnUtilization =
        getResourcesAvailableBasedOnUtilization();
    if (LOG.isDebugEnabled()) {
      LOG.debug("The amount of resources available based on allocation is " +
          resourceBasedOnAllocation + ", based on utilization is " +
          resourceBasedOnUtilization);
    }

    return Resources.componentwiseMax(resourceBasedOnAllocation,
        resourceBasedOnUtilization);
  }

  /**
   * Get the amount of resources based on the slack between
   * the actual utilization and desired utilization.
   * @return Resource resource available
   */
  private Resource getResourcesAvailableBasedOnUtilization() {
    if (overAllocationPolicy == null) {
      return Resources.none();
    }

    return overAllocationPolicy.getAvailableResources();
  }

  @Override
  public ResourceUtilization getCurrentUtilization() {
    return getContainersMonitor().getContainersUtilization(false)
        .getUtilization();
  }
}

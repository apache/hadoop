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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSAssignment;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.PlacementSet;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

public class ContainerAllocator extends AbstractContainerAllocator {
  private AbstractContainerAllocator increaseContainerAllocator;
  private AbstractContainerAllocator regularContainerAllocator;

  public ContainerAllocator(FiCaSchedulerApp application,
      ResourceCalculator rc, RMContext rmContext) {
    this(application, rc, rmContext, null);
  }

  public ContainerAllocator(FiCaSchedulerApp application, ResourceCalculator rc,
      RMContext rmContext, ActivitiesManager activitiesManager) {
    super(application, rc, rmContext);

    increaseContainerAllocator =
        new IncreaseContainerAllocator(application, rc, rmContext);
    regularContainerAllocator = new RegularContainerAllocator(application, rc,
        rmContext, activitiesManager);
  }

  @Override
  public CSAssignment assignContainers(Resource clusterResource,
      PlacementSet<FiCaSchedulerNode> ps, SchedulingMode schedulingMode,
      ResourceLimits resourceLimits, RMContainer reservedContainer) {
    if (reservedContainer != null) {
      if (reservedContainer.getState() == RMContainerState.RESERVED) {
        // It's a regular container
        return regularContainerAllocator.assignContainers(clusterResource,
            ps, schedulingMode, resourceLimits, reservedContainer);
      } else {
        // It's a increase container
        return increaseContainerAllocator.assignContainers(clusterResource,
            ps, schedulingMode, resourceLimits, reservedContainer);
      }
    } else {
      /*
       * Try to allocate increase container first, and if we failed to allocate
       * anything, we will try to allocate regular container
       */
      CSAssignment assign =
          increaseContainerAllocator.assignContainers(clusterResource, ps,
              schedulingMode, resourceLimits, null);
      if (Resources.greaterThan(rc, clusterResource, assign.getResource(),
          Resources.none())) {
        return assign;
      }

      return regularContainerAllocator.assignContainers(clusterResource, ps,
          schedulingMode, resourceLimits, null);
    }
  }

}

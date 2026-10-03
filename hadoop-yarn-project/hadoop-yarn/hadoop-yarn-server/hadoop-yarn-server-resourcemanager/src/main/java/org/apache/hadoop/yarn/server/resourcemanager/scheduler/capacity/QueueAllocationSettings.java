/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.UNDEFINED;

/**
 * This class determines minimum and maximum allocation settings based on the
 * {@link CapacitySchedulerConfiguration} and other queue
 * properties.
 **/
public class QueueAllocationSettings {
  private final Resource minimumAllocation;
  private Resource maximumAllocation;

  public QueueAllocationSettings(Resource minimumAllocation) {
    this.minimumAllocation = minimumAllocation;
  }

  void setupMaximumAllocation(CapacitySchedulerConfiguration configuration, QueuePath queuePath,
      CSQueue parent) {
    Resource clusterMax = ResourceUtils
        .fetchMaximumAllocationFromConfig(configuration);
    Resource queueMax = configuration.getQueueMaximumAllocation(queuePath);

    maximumAllocation = Resources.clone(
        parent == null ? clusterMax : parent.getMaximumAllocation());

    if (queueMax == Resources.none()) {
      // Handle backward compatibility
      long queueMemory = configuration.getQueueMaximumAllocationMb(queuePath);
      int queueVcores = configuration.getQueueMaximumAllocationVcores(queuePath);
      if (queueMemory != UNDEFINED) {
        maximumAllocation.setMemorySize(queueMemory);
      }

      if (queueVcores != UNDEFINED) {
        maximumAllocation.setVirtualCores(queueVcores);
      }

      String error = QueueAllocationChecks.checkLegacyQueueMaximumAllocation(
          new QueueAllocationChecks.LegacyMaximumAllocationInput(
              String.valueOf(queuePath), queueMemory, queueVcores, clusterMax,
              maximumAllocation));
      if (error != null) {
        throw new IllegalArgumentException(error);
      }
    } else {
      // Queue level maximum-allocation can't be larger than cluster setting
      String error = QueueAllocationChecks.checkQueueMaximumAllocation(
          new QueueAllocationChecks.MaximumAllocationInput(
              String.valueOf(queuePath), queueMax, clusterMax));
      if (error != null) {
        throw new IllegalArgumentException(error);
      }
      for (ResourceInformation ri : queueMax.getResources()) {
        maximumAllocation.setResourceInformation(ri.getName(), ri);
      }
    }
  }

  public Resource getMinimumAllocation() {
    return minimumAllocation;
  }

  public Resource getMaximumAllocation() {
    return maximumAllocation;
  }
}

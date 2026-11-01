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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.Resolved;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ValueSource;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * This class holds the minimum and maximum allocation settings of a queue. The maximum
 * allocation is taken from the queue's resolved configuration, in which a queue inherits the
 * maximum allocation of its parent and root starts from the cluster maximum allocation.
 **/
public class QueueAllocationSettings {
  private static final String MAXIMUM_ALLOCATION_KEY_SUFFIX =
      CapacitySchedulerConfiguration.DOT + CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION;

  private final Resource minimumAllocation;
  private Resource maximumAllocation;

  public QueueAllocationSettings(Resource minimumAllocation) {
    this.minimumAllocation = minimumAllocation;
  }

  void setupMaximumAllocation(CapacitySchedulerConfiguration configuration,
      ResolvedQueueConfig resolved, QueuePath queuePath) {
    Resource clusterMax = ResourceUtils
        .fetchMaximumAllocationFromConfig(configuration);
    Resolved<Resource> queueMax = resolved.get(QueueProperties.MAXIMUM_ALLOCATION);
    Resource maximum = Resources.clone(queueMax.getValue());

    if (!isSetByMaximumAllocationKey(queueMax)) {
      // Handle backward compatibility
      long queueMemory = resolved.get(QueueProperties.MAXIMUM_ALLOCATION_MB).getValue();
      int queueVcores = resolved.get(QueueProperties.MAXIMUM_ALLOCATION_VCORES).getValue();
      String error = QueueAllocationChecks.checkLegacyQueueMaximumAllocation(
          String.valueOf(queuePath), queueMemory, queueVcores, clusterMax,
          maximum);
      if (error != null) {
        throw new IllegalArgumentException(error);
      }
    } else {
      // Queue level maximum-allocation can't be larger than cluster setting.
      // The resolved value overlays every resource of the configured one, so
      // it equals the configured value.
      String error = QueueAllocationChecks.checkQueueMaximumAllocation(
          String.valueOf(queuePath), maximum, clusterMax);
      if (error != null) {
        throw new IllegalArgumentException(error);
      }
    }
    maximumAllocation = maximum;
  }

  /**
   * Whether the resolved maximum allocation was set by the queue's
   * {@code maximum-allocation} key, directly or through a template, rather
   * than inherited or set by the per resource keys.
   */
  private static boolean isSetByMaximumAllocationKey(Resolved<Resource> queueMax) {
    ValueSource source = queueMax.getSource();
    String sourceKey = queueMax.getSourceDetail();
    return (source == ValueSource.QUEUE || source == ValueSource.TEMPLATE_V1
        || source == ValueSource.TEMPLATE_V2) && sourceKey != null
        && sourceKey.endsWith(MAXIMUM_ALLOCATION_KEY_SUFFIX);
  }

  public Resource getMinimumAllocation() {
    return minimumAllocation;
  }

  public Resource getMaximumAllocation() {
    return maximumAllocation;
  }
}

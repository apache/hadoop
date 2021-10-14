/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.ROOT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityType.PERCENTAGE;

public class RootQueueCapacityCalculator extends
    AbstractQueueCapacityCalculator {
  @Override
  public void setup(CSQueue queue, String label) {
    queue.getQueueCapacities().setCapacity(label, 100f);
  }

  @Override
  public void calculateChildQueueResources(
      QueueHierarchyUpdateContext updateContext, CSQueue parentQueue) {
    for (String label : parentQueue.getConfiguredNodeLabels()) {
      for (ResourceInformation resource : ResourceUtils.getResourceTypesArray()) {
        updateContext.getRelativeResourceRatio(ROOT, label).setValue(
            resource.getName(), 1);

        parentQueue.getQueueResourceQuotas().getEffectiveMinResource(label)
            .setResourceValue(resource.getName(),
                updateContext.getUpdatedClusterResource(label).getResourceValue(
                    resource.getName()));
      }

      setNormalizedResourceRatio(updateContext, parentQueue, label);
    }
  }

  @Override
  public void setMetrics(
      QueueHierarchyUpdateContext updateContext, CSQueue queue, String label) {
    queue.getQueueCapacities().setAbsoluteCapacity(label, 1);
  }

  @Override
  protected QueueCapacityVector.QueueCapacityType getCapacityType() {
    return PERCENTAGE;
  }
}

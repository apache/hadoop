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

<<<<<<< HEAD
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType.PERCENTAGE;

public class RootQueueCapacityCalculator extends
    AbstractQueueCapacityCalculator {
=======
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityType.PERCENTAGE;

public class RootQueueCapacityCalculator extends
    AbstractQueueCapacityCalculator {

  @Override
  public void calculateChildQueueResources(
      QueueHierarchyUpdateContext updateContext, CSQueue parentQueue) {
    for (String label : parentQueue.getConfiguredNodeLabels()) {
      for (QueueCapacityVectorEntry capacityVectorEntry : parentQueue.getConfiguredCapacityVector(label)) {
        parentQueue.getOrCreateAbsoluteMinCapacityVector(label).setValue(
            capacityVectorEntry.getResourceName(), 1);
        parentQueue.getOrCreateAbsoluteMaxCapacityVector(label).setValue(
            capacityVectorEntry.getResourceName(), 1);

        float minimumResource = calculateMinimumResource(updateContext, parentQueue, label, capacityVectorEntry);
        float maximumResource = calculateMaximumResource(updateContext, parentQueue, label, capacityVectorEntry);
        long roundedMinResource = (long) Math.floor(minimumResource);
        long roundedMaxResource = (long) Math.floor(maximumResource);
        parentQueue.getQueueResourceQuotas().getEffectiveMinResource(label)
            .setResourceValue(capacityVectorEntry.getResourceName(), roundedMinResource);
        parentQueue.getQueueResourceQuotas().getEffectiveMaxResource(label)
            .setResourceValue(capacityVectorEntry.getResourceName(), roundedMaxResource);
      }
    }

    calculateResourcePrerequisites(updateContext, parentQueue);
  }
>>>>>>> 400f4af9c96 (YARN-11000. Fix weight queue calculation and test issues)

  @Override
  public float calculateMinimumResource(ResourceCalculationDriver resourceCalculationDriver, String label) {
    return resourceCalculationDriver.getUpdateContext().getUpdatedClusterResource(label).getResourceValue(resourceCalculationDriver.getCurrentResourceName());
  }

  @Override
  public float calculateMaximumResource(ResourceCalculationDriver resourceCalculationDriver, String label) {
    return resourceCalculationDriver.getUpdateContext().getUpdatedClusterResource(label).getResourceValue(resourceCalculationDriver.getCurrentResourceName());
  }

  @Override
  public void updateCapacitiesAfterCalculation(
      ResourceCalculationDriver resourceCalculationDriver, String label) {
    resourceCalculationDriver.getParent().getQueueCapacities().setAbsoluteCapacity(label, 1);
    if (resourceCalculationDriver.getParent().getQueueCapacities().getWeight(label) == 1) {
      resourceCalculationDriver.getParent().getQueueCapacities().setNormalizedWeight(label, 1);
    }
  }

  @Override
  public QueueCapacityVector.ResourceUnitCapacityType getCapacityType() {
    return PERCENTAGE;
  }
}

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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;

import java.util.Collection;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType.WEIGHT;

public class WeightQueueCapacityCalculator extends AbstractQueueCapacityCalculator {

  @Override
  public void calculateResourcePrerequisites(ResourceCalculationDriver resourceCalculationDriver) {
    super.calculateResourcePrerequisites(resourceCalculationDriver);

    for (CSQueue childQueue : resourceCalculationDriver.getParent().getChildQueues()) {
      for (String label : childQueue.getConfiguredNodeLabels()) {
        for (String resourceName : childQueue.getConfiguredCapacityVector(label)
            .getResourceNamesByCapacityType(getCapacityType())) {
              resourceCalculationDriver.incrementWeight(label, resourceName, childQueue
                  .getConfiguredCapacityVector(label).getResource(resourceName).getResourceValue());
        }
      }
    }
  }

  @Override
  public float calculateMinimumResource(ResourceCalculationDriver resourceCalculationDriver,
                                        String label) {
    CSQueue parentQueue = resourceCalculationDriver.getParent();
    String resourceName = resourceCalculationDriver.getCurrentResourceName();
    float normalizedWeight = resourceCalculationDriver.getCurrentMinimumCapacityEntry(label)
        .getResourceValue() / resourceCalculationDriver.getSumWeightsByResource(label, resourceName);

    float remainingResource = resourceCalculationDriver.getBatchRemainingResource(label).getValue(
        resourceName);

    // Due to rounding loss it is better to use all remaining resources if no other resource uses
    // weight
    if (normalizedWeight == 1) {
      return remainingResource;
    }

    float remainingResourceRatio = resourceCalculationDriver.getRemainingRatioOfResource(
        label, resourceName);

    float parentAbsoluteCapacity = parentQueue.getOrCreateAbsoluteMinCapacityVector(label)
        .getValue(resourceName);
    float queueAbsoluteCapacity = parentAbsoluteCapacity * remainingResourceRatio
        * normalizedWeight;

    // Weight capacity types are the last to consider, therefore it is safe to assign all remaining
    // effective resources between queues. The strategy is to round values to the closest whole
    // number.

    return resourceCalculationDriver.getUpdateContext()
        .getUpdatedClusterResource(label).getResourceValue(resourceName) * queueAbsoluteCapacity;
  }

  @Override
  public float calculateMaximumResource(ResourceCalculationDriver resourceCalculationDriver,
                                        String label) {
    throw new IllegalStateException("Resource " + resourceCalculationDriver.getCurrentMinimumCapacityEntry(
        label).getResourceName() + " has " + "WEIGHT maximum capacity type, which is not supported");
  }

  @Override
  public ResourceUnitCapacityType getCapacityType() {
    return WEIGHT;
  }

  @Override
  public void updateCapacitiesAfterCalculation(
      ResourceCalculationDriver resourceCalculationDriver, String label) {
    float sumCapacityPerResource = 0f;

    Collection<String> resourceNames = getResourceNames(resourceCalculationDriver.getCurrentChild(), label);
    for (String resourceName : resourceNames) {
      float sumBranchWeight = resourceCalculationDriver.getSumWeightsByResource(label, resourceName);
      float capacity =  resourceCalculationDriver.getCurrentChild().getConfiguredCapacityVector(
          label).getResource(resourceName).getResourceValue() / sumBranchWeight;
      sumCapacityPerResource += capacity;
    }

    resourceCalculationDriver.getCurrentChild().getQueueCapacities().setNormalizedWeight(label,
        sumCapacityPerResource / resourceNames.size());
    ((AbstractCSQueue) resourceCalculationDriver.getCurrentChild()).updateAbsoluteCapacities();
  }
}

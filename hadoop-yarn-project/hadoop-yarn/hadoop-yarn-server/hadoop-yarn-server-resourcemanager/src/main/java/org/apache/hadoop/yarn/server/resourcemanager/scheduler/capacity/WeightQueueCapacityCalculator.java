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
    // Precalculate the summary of children's weight
    for (CSQueue childQueue : resourceCalculationDriver.getChildQueues()) {
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
  public double calculateMinimumResource(ResourceCalculationDriver resourceCalculationDriver,
                                        CalculationContext context,
                                        String label) {
    String resourceName = context.getResourceName();
    double normalizedWeight = context.getCurrentMinimumCapacityEntry(label).getResourceValue() /
        resourceCalculationDriver.getSumWeightsByResource(label, resourceName);

    double remainingResource = resourceCalculationDriver.getBatchRemainingResource(label)
        .getValue(resourceName);

    // Due to rounding loss it is better to use all remaining resources if no other resource uses
    // weight
    if (normalizedWeight == 1) {
      return remainingResource;
    }

    double remainingResourceRatio = resourceCalculationDriver.getRemainingRatioOfResource(
        label, resourceName);
    double parentAbsoluteCapacity = resourceCalculationDriver.getParentAbsoluteMinCapacity(
        label, resourceName);
    double queueAbsoluteCapacity = parentAbsoluteCapacity * remainingResourceRatio
        * normalizedWeight;

    return resourceCalculationDriver.getUpdateContext()
        .getUpdatedClusterResource(label).getResourceValue(resourceName) * queueAbsoluteCapacity;
  }

  @Override
  public double calculateMaximumResource(ResourceCalculationDriver resourceCalculationDriver,
                                        CalculationContext context,
                                        String label) {
    throw new IllegalStateException("Resource " + context.getCurrentMinimumCapacityEntry(
        label).getResourceName() +
        " has " + "WEIGHT maximum capacity type, which is not supported");
  }

  @Override
  public ResourceUnitCapacityType getCapacityType() {
    return WEIGHT;
  }

  @Override
  public void updateCapacitiesAfterCalculation(
      ResourceCalculationDriver resourceCalculationDriver, CSQueue queue, String label) {
    double sumCapacityPerResource = 0f;

    Collection<String> resourceNames = getResourceNames(queue, label);
    for (String resourceName : resourceNames) {
      double sumBranchWeight = resourceCalculationDriver.getSumWeightsByResource(label,
          resourceName);
      double capacity =  queue.getConfiguredCapacityVector(
          label).getResource(resourceName).getResourceValue() / sumBranchWeight;
      sumCapacityPerResource += capacity;
    }

    queue.getQueueCapacities().setNormalizedWeight(label,
        (float) (sumCapacityPerResource / resourceNames.size()));
    ((AbstractCSQueue) queue).updateAbsoluteCapacities();
  }
}

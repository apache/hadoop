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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;

import java.util.Collection;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType.WEIGHT;

public class WeightQueueCapacityCalculator extends AbstractQueueCapacityCalculator {

  @Override
  public void calculateResourcePrerequisites(
      QueueCapacityUpdateContext updateContext, CSQueue parentQueue) {
    super.calculateResourcePrerequisites(updateContext, parentQueue);

    for (CSQueue childQueue : parentQueue.getChildQueues()) {
      for (String label : childQueue.getConfiguredNodeLabels()) {
        for (String resourceName : childQueue.getConfiguredCapacityVector(label)
            .getResourceNamesByCapacityType(getCapacityType())) {
          updateContext.getQueueBranchContext(parentQueue.getQueuePath())
              .incrementWeight(label, resourceName, childQueue.getConfiguredCapacityVector(label)
                  .getResource(resourceName).getResourceValue());
        }
      }
    }
  }

  @Override
  public float calculateMinimumResource(
      QueueCapacityUpdateContext updateContext, CSQueue childQueue, String label,
      QueueCapacityVectorEntry capacityVectorEntry) {
    CSQueue parentQueue = childQueue.getParent();
    String resourceName = capacityVectorEntry.getResourceName();
    float normalizedWeight = capacityVectorEntry.getResourceValue()
        / updateContext.getQueueBranchContext(parentQueue.getQueuePath())
        .getSumWeightsByResource(label, resourceName);

    float remainingResource = updateContext.getQueueBranchContext(
            parentQueue.getQueuePath()).getBatchRemainingResources(label)
        .getValue(resourceName);

    // Due to rounding loss it is better to use all remaining resources if no other resource uses
    // weight
    if (normalizedWeight == 1) {
      return remainingResource;
    }

    float remainingPerEffectiveResourceRatio = remainingResource / parentQueue.getEffectiveCapacity(
        label).getResourceValue(resourceName);

    float parentAbsoluteCapacity = parentQueue.getOrCreateAbsoluteMinCapacityVector(label)
        .getValue(resourceName);
    float queueAbsoluteCapacity = parentAbsoluteCapacity *
        remainingPerEffectiveResourceRatio * normalizedWeight;

    // Weight capacity types are the last to consider, therefore it is safe to assign all remaining
    // effective resources between queues. The strategy is to round values to the closest whole
    // number.
    float resource = updateContext.getUpdatedClusterResource(label).getResourceValue(resourceName)
        * queueAbsoluteCapacity;

    return Math.round(resource);
  }

  @Override
  public float calculateMaximumResource(
      QueueCapacityUpdateContext updateContext, CSQueue childQueue, String label,
      QueueCapacityVectorEntry capacityVectorEntry) {
    throw new IllegalStateException("Resource " + capacityVectorEntry.getResourceName() + " has " +
        "WEIGHT maximum capacity type, which is not supported");
  }

  @Override
  public ResourceUnitCapacityType getCapacityType() {
    return WEIGHT;
  }

  @Override
  public void updateCapacitiesAfterCalculation(
      QueueCapacityUpdateContext updateContext, CSQueue queue, String label) {
    float sumCapacityPerResource = 0f;

    Collection<String> resourceNames = getResourceNames(queue, label);
    for (String resourceName : resourceNames) {
      float sumBranchWeight = updateContext.getQueueBranchContext(queue.getParent().getQueuePath())
          .getSumWeightsByResource(label, resourceName);
      float capacity =  queue.getConfiguredCapacityVector(label).getResource(
          resourceName).getResourceValue() / sumBranchWeight;
      sumCapacityPerResource += capacity;
    }

    queue.getQueueCapacities().setNormalizedWeight(label, sumCapacityPerResource
        / resourceNames.size());
    ((AbstractCSQueue) queue).updateAbsoluteCapacities();
  }
}

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
import org.apache.hadoop.yarn.util.UnitsConversionUtil;

import java.util.Map;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueUpdateWarning.QueueUpdateWarningType.BRANCH_DOWNSCALED;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ResourceCalculationDriver.MB_UNIT;

public class AbsoluteResourceCapacityCalculator extends AbstractQueueCapacityCalculator {

  @Override
  public void calculateResourcePrerequisites(ResourceCalculationDriver resourceCalculationDriver) {
    setNormalizedResourceRatio(resourceCalculationDriver);
  }

  @Override
  public double calculateMinimumResource(
      ResourceCalculationDriver resourceCalculationDriver, CalculationContext context,
      String label) {
    String resourceName = context.getResourceName();
    double normalizedRatio = resourceCalculationDriver.getNormalizedResourceRatios().getOrDefault(
        label, ResourceVector.of(1)).getValue(resourceName);
    double remainingResourceRatio = resourceCalculationDriver.getRemainingRatioOfResource(
        label, resourceName);

    return normalizedRatio * remainingResourceRatio * context.getCurrentMinimumCapacityEntry(
        label).getResourceValue();
  }

  @Override
  public double calculateMaximumResource(
      ResourceCalculationDriver resourceCalculationDriver, CalculationContext context,
      String label) {
    return context.getCurrentMaximumCapacityEntry(label).getResourceValue();
  }

  @Override
  public void updateCapacitiesAfterCalculation(
      ResourceCalculationDriver resourceCalculationDriver, CSQueue queue, String label) {
    CapacitySchedulerQueueCapacityHandler.setQueueCapacities(
        resourceCalculationDriver.getUpdateContext()
            .getUpdatedClusterResource(label), queue, label);
  }

  @Override
  public ResourceUnitCapacityType getCapacityType() {
    return ResourceUnitCapacityType.ABSOLUTE;
  }

  /**
   * Calculates the normalized resource ratio of a parent queue, under which children are defined
   * with absolute capacity type. If the effective resource of the parent is less, than the
   * aggregated configured absolute resource of its children, the resource ratio will be less,
   * than 1.
   *
   * @param calculationDriver the driver, which contains the parent queue that will form the base
   *                          of the normalization calculation
   */
  public static void setNormalizedResourceRatio(ResourceCalculationDriver calculationDriver) {
    CSQueue queue = calculationDriver.getQueue();

    for (String label : queue.getConfiguredNodeLabels()) {
      // ManagedParents assign zero capacity to queues in case of overutilization, downscaling is
      // turned off for their children
      if (queue instanceof ManagedParentQueue) {
        return;
      }

      for (String resourceName : queue.getConfiguredCapacityVector(label).getResourceNames()) {
        long childrenConfiguredResource = 0;
        long effectiveMinResource = queue.getQueueResourceQuotas().getEffectiveMinResource(
            label).getResourceValue(resourceName);

        // Total configured min resources of direct children of the queue
        for (CSQueue childQueue : queue.getChildQueues()) {
          if (!childQueue.getConfiguredNodeLabels().contains(label)) {
            continue;
          }
          QueueCapacityVector capacityVector = childQueue.getConfiguredCapacityVector(label);
          if (capacityVector.isResourceOfType(resourceName, ResourceUnitCapacityType.ABSOLUTE)) {
            childrenConfiguredResource += capacityVector.getResource(resourceName)
                .getResourceValue();
          }
        }
        // If no children is using ABSOLUTE capacity type, normalization is not needed
        if (childrenConfiguredResource == 0) {
          continue;
        }
        // Factor to scale down effective resource: When cluster has sufficient
        // resources, effective_min_resources will be same as configured
        // min_resources.
        float numeratorForMinRatio = childrenConfiguredResource;
        if (effectiveMinResource < childrenConfiguredResource) {
          numeratorForMinRatio = queue.getQueueResourceQuotas().getEffectiveMinResource(label)
              .getResourceValue(resourceName);
          calculationDriver.getUpdateContext().addUpdateWarning(BRANCH_DOWNSCALED.ofQueue(
              queue.getQueuePath()));
        }

        String unit = resourceName.equals(MEMORY_URI) ? MB_UNIT : "";
        long convertedValue = UnitsConversionUtil.convert(unit, calculationDriver.getUpdateContext()
            .getUpdatedClusterResource(label).getResourceInformation(resourceName).getUnits(),
            childrenConfiguredResource);

        if (convertedValue != 0) {
          Map<String, ResourceVector> normalizedResourceRatios =
              calculationDriver.getNormalizedResourceRatios();
          normalizedResourceRatios.putIfAbsent(label, ResourceVector.newInstance());
          normalizedResourceRatios.get(label).setValue(resourceName, numeratorForMinRatio /
              convertedValue);
        }
      }
    }
  }
}

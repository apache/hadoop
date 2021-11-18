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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueUpdateWarning.QueueUpdateWarningType;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;

/**
 * A strategy class to encapsulate queue capacity setup and resource calculation
 * logic.
 */
public abstract class AbstractQueueCapacityCalculator {
  private static final String MB_UNIT = "Mi";

  /**
   * Sets the metrics and statistics after effective resource values calculation.
   *
   * @param label         node label
   */
  public abstract void updateCapacitiesAfterCalculation(
      ResourceCalculationDriver resourceCalculationDriver, String label);


  /**
   * Returns the capacity type the calculator could handle.
   *
   * @return capacity type
   */
  public abstract ResourceUnitCapacityType getCapacityType();

  /**
   * Calculates the minimum effective resource.
   *
   * @param resourceCalculationDriver driver that contains the current resource unit and child to
   *                                  process
   * @param label         node label
   * @return minimum effective resource
   */
  public abstract float calculateMinimumResource(ResourceCalculationDriver resourceCalculationDriver,
                                                 String label);

  /**
   * Calculates the maximum effective resource.
   *
   * @param resourceCalculationDriver driver that contains the current resource unit and child to
   *                                  process
   * @param label         node label
   * @return minimum effective resource
   */
  public abstract float calculateMaximumResource(ResourceCalculationDriver resourceCalculationDriver,
                                                 String label);

  /**
   * Executes all logic that must be called prior to the effective resource value calculations.
   *
   * @param resourceCalculationDriver driver that contains the parent queue on which the prerequisite
   *                                  calculation should be made
   */
  public void calculateResourcePrerequisites(ResourceCalculationDriver resourceCalculationDriver) {
    for (String label : resourceCalculationDriver.getParent().getConfiguredNodeLabels()) {
      // We need to set normalized resource ratio only once per parent
      if (resourceCalculationDriver.getNormalizedResourceRatios().isEmpty()) {
        setNormalizedResourceRatio(resourceCalculationDriver, label);
      }
    }
  }

  /**
   * Returns all resource names that are defined for the capacity type that is
   * handled by the calculator.
   *
   * @param queue queue for which the capacity vector is defined
   * @param label node label
   * @return resource names
   */
  protected Set<String> getResourceNames(CSQueue queue, String label) {
    return getResourceNames(queue, label, getCapacityType());
  }

  /**
   * Returns all resource names that are defined for a capacity type.
   *
   * @param queue        queue for which the capacity vector is defined
   * @param label        node label
   * @param capacityType capacity type for which the resource names are defined
   * @return resource names
   */
  protected Set<String> getResourceNames(CSQueue queue, String label,
                                         ResourceUnitCapacityType capacityType) {
    return queue.getConfiguredCapacityVector(label)
        .getResourceNamesByCapacityType(capacityType);
  }

  /**
   * Sets capacity and absolute capacity values based on minimum and maximum effective resources.
   *
   * @param clusterResource cluster resource for the corresponding label
   * @param queue child queue for which the capacities are set
   * @param label node label
   */
  public static void setQueueCapacities(
      Resource clusterResource, CSQueue queue, String label) {
    if (!(queue instanceof AbstractCSQueue)) {
      return;
    }

    AbstractCSQueue csQueue = (AbstractCSQueue) queue;
    ResourceCalculator resourceCalculator = csQueue.resourceCalculator;

    CSQueue parent = queue.getParent();
    if (parent == null) {
      return;
    }
    // Update capacity with a float calculated from the parent's minResources
    // and the recently changed queue minResources.
    // capacity = effectiveMinResource / {parent's effectiveMinResource}
    float result = resourceCalculator.divide(clusterResource,
        queue.getQueueResourceQuotas().getEffectiveMinResource(label),
        parent.getQueueResourceQuotas().getEffectiveMinResource(label));
    queue.getQueueCapacities().setCapacity(label,
        Float.isInfinite(result) ? 0 : result);

    // Update maxCapacity with a float calculated from the parent's maxResources
    // and the recently changed queue maxResources.
    // maxCapacity = effectiveMaxResource / parent's effectiveMaxResource
    result = resourceCalculator.divide(clusterResource,
        queue.getQueueResourceQuotas().getEffectiveMaxResource(label),
        parent.getQueueResourceQuotas().getEffectiveMaxResource(label));
    queue.getQueueCapacities().setMaximumCapacity(label,
        Float.isInfinite(result) ? 0 : result);

    csQueue.updateAbsoluteCapacities();
  }

  /**
   * Calculates the normalized resource ratio of a parent queue, under which children are defined
   * with absolute capacity type. If the effective resource of the parent is less, than the
   * aggregated configured absolute resource of its children, the resource ratio will be less,
   * than 1.
   *
   * @param label         node label
   */
  private void setNormalizedResourceRatio(
      ResourceCalculationDriver resourceCalculationDriver, String label) {
    // ManagedParents assign zero capacity to queues in case of overutilization, downscaling is
    // turned off for their children
    CSQueue parentQueue = resourceCalculationDriver.getParent();
    QueueCapacityUpdateContext updateContext = resourceCalculationDriver.getUpdateContext();

    if (parentQueue instanceof ManagedParentQueue) {
      return;
    }

    for (QueueCapacityVectorEntry capacityVectorEntry : parentQueue.getConfiguredCapacityVector(
        label)) {
      String resourceName = capacityVectorEntry.getResourceName();
      long childrenConfiguredResource = 0;
      long effectiveMinResource = parentQueue.getQueueResourceQuotas().getEffectiveMinResource(
          label).getResourceValue(resourceName);

      // Total configured min resources of direct children of this given parent
      // queue
      for (CSQueue childQueue : parentQueue.getChildQueues()) {
        if (!childQueue.getConfiguredNodeLabels().contains(label)) {
          continue;
        }
        QueueCapacityVector capacityVector = childQueue.getConfiguredCapacityVector(label);
        if (capacityVector.isResourceOfType(resourceName, ResourceUnitCapacityType.ABSOLUTE)) {
          childrenConfiguredResource += capacityVector.getResource(resourceName).getResourceValue();
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
        numeratorForMinRatio = parentQueue.getQueueResourceQuotas().getEffectiveMinResource(label)
            .getResourceValue(resourceName);
        updateContext.addUpdateWarning(QueueUpdateWarningType.BRANCH_DOWNSCALED.ofQueue(
            parentQueue.getQueuePath()));
      }

      String unit = resourceName.equals(MEMORY_URI) ? MB_UNIT : "";
      long convertedValue = UnitsConversionUtil.convert(unit,
          updateContext.getUpdatedClusterResource(label).getResourceInformation(resourceName)
              .getUnits(), childrenConfiguredResource);

      if (convertedValue != 0) {
        Map<String, ResourceVector> normalizedResourceRatios = resourceCalculationDriver
            .getNormalizedResourceRatios();
        normalizedResourceRatios.putIfAbsent(label, ResourceVector.newInstance());
        normalizedResourceRatios.get(label).setValue(resourceName, numeratorForMinRatio /
            convertedValue);
      }
    }
  }
}

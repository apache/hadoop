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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueUpdateWarning.QueueUpdateWarningType;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;

/**
 * A strategy class to encapsulate queue capacity setup and resource calculation
 * logic.
 */
public abstract class AbstractQueueCapacityCalculator {

  /**
   * Sets all field of the queue based on its configurations.
   *
   * @param queue queue to setup
   * @param label node label
   */
  public abstract void setup(CSQueue queue, String label);

  /**
   * Sets the metrics and statistics after effective resource calculation.
   *
   * @param updateContext context of the current update phase
   * @param queue         queue to update
   * @param label         node label
   */
  public abstract void setMetrics(QueueHierarchyUpdateContext updateContext, CSQueue queue,
                                  String label);

  /**
   * Calculate the effective resource for a specific resource.
   *
   * @param updateContext context of the current update phase
   * @param parentQueue   the parent whose children will be updated
   */
  public void calculateChildQueueResources(QueueHierarchyUpdateContext updateContext,
                                           CSQueue parentQueue) {
    calculateResourcePrerequisites(updateContext, parentQueue);

    Map<String, ResourceVector> aggregatedResources = new HashMap<>();
    for (CSQueue childQueue : parentQueue.getChildQueues()) {
      childQueue.getWriteLock().lock();
      try {
        for (String label : childQueue.getConfiguredNodeLabels()) {
          ResourceVector aggregatedUsedResource = aggregatedResources.getOrDefault(label,
              ResourceVector.newInstance());
          calculateResources(updateContext, childQueue, label, aggregatedUsedResource);
          aggregatedResources.put(label, aggregatedUsedResource);
        }
      } finally {
        childQueue.getWriteLock().unlock();
      }
    }

    for (Map.Entry<String, ResourceVector> entry : aggregatedResources.entrySet()) {
      updateContext.getQueueBranchContext(parentQueue.getQueuePath()).getRemainingResource(
          entry.getKey()).subtract(entry.getValue());
    }
  }

  /**
   * Returns the capacity type the calculator could handle.
   *
   * @return capacity type
   */
  protected abstract QueueCapacityType getCapacityType();

  protected abstract float calculateMinimumResource(
      QueueHierarchyUpdateContext updateContext, CSQueue childQueue, String label,
      QueueCapacityVectorEntry capacityVectorEntry);

  protected abstract float calculateMaximumResource(
      QueueHierarchyUpdateContext updateContext, CSQueue childQueue, String label,
      QueueCapacityVectorEntry capacityVectorEntry);

  protected void calculateResourcePrerequisites(QueueHierarchyUpdateContext updateContext,
                                                CSQueue parentQueue) {
    for (String label : parentQueue.getConfiguredNodeLabels()) {
      // We need to set normalized resource ratio only once per parent
      if (!updateContext.getQueueBranchContext(parentQueue.getQueuePath())
          .isParentAlreadyUpdated()) {
        setNormalizedResourceRatio(updateContext, parentQueue, label);
        updateContext.getQueueBranchContext(parentQueue.getQueuePath())
            .setUpdateFlag();
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
                                         QueueCapacityType capacityType) {
    return queue.getConfiguredCapacityVector(label)
        .getResourceNamesByCapacityType(capacityType);
  }

  protected float sumCapacityValues(CSQueue queue, String label) {
    float sumValue = 0f;
    QueueCapacityVector capacityVector = queue.getConfiguredCapacityVector(label);
    for (String resourceName : getResourceNames(queue, label)) {
      sumValue += capacityVector.getResource(resourceName).getResourceValue();
    }
    return sumValue;
  }

  private void setNormalizedResourceRatio(
      QueueHierarchyUpdateContext updateContext, CSQueue parentQueue, String label) {
    for (QueueCapacityVectorEntry capacityVectorEntry : parentQueue.getConfiguredCapacityVector(
        label)) {
      String resourceName = capacityVectorEntry.getResourceName();
      long childrenConfiguredResource = 0;
      long effectiveMinResource = parentQueue.getQueueResourceQuotas().getEffectiveMinResource(
          label).getResourceValue(resourceName);

      // Total configured min resources of direct children of this given parent
      // queue
      for (CSQueue childQueue : parentQueue.getChildQueues()) {
        QueueCapacityVector capacityVector = childQueue.getConfiguredCapacityVector(label);
        if (capacityVector.isResourceOfType(resourceName, QueueCapacityType.ABSOLUTE)) {
          childrenConfiguredResource += capacityVector.getResource(resourceName).getResourceValue();
        }
      }
      // If no children is using ABSOLUTE capacity type, normalization is
      // not needed
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

      String unit = resourceName.equals(MEMORY_URI) ? "Mi" : "";
      long convertedValue = UnitsConversionUtil.convert(unit,
          updateContext.getUpdatedClusterResource(label).getResourceInformation(resourceName)
              .getUnits(), childrenConfiguredResource);

      if (convertedValue != 0) {
        updateContext.getNormalizedMinResourceRatio(parentQueue.getQueuePath(), label).setValue(
            resourceName, numeratorForMinRatio / convertedValue);
      }
    }
  }

  private void calculateResources(
      QueueHierarchyUpdateContext updateContext, CSQueue childQueue, String label,
      ResourceVector aggregatedUsedResource) {
    CSQueue parentQueue = childQueue.getParent();
    for (String resourceName : getResourceNames(childQueue, label)) {
      long clusterResource = updateContext.getUpdatedClusterResource(label).getResourceValue(
          resourceName);
      float minimumResource = calculateMinimumResource(updateContext, childQueue, label,
          childQueue.getConfiguredCapacityVector(label).getResource(resourceName));
      long parentMinimumResource = parentQueue.getEffectiveCapacity(label).getResourceValue(
          resourceName);

      long parentMaximumResource = parentQueue.getEffectiveMaxCapacity(label).getResourceValue(
          resourceName);
      float maximumResource = calculateMaximumResource(updateContext, childQueue, label,
          childQueue.getConfiguredMaximumCapacityVector(label).getResource(resourceName));

      if (maximumResource != 0 && maximumResource > parentMaximumResource) {
        updateContext.addUpdateWarning(QueueUpdateWarningType.QUEUE_MAX_RESOURCE_EXCEEDS_PARENT.ofQueue(
            childQueue.getQueuePath()));
      }
      maximumResource = maximumResource == 0 ? parentMaximumResource
          : Math.min(maximumResource, parentMaximumResource);

      if (maximumResource < minimumResource) {
        updateContext.addUpdateWarning(QueueUpdateWarningType.QUEUE_EXCEEDS_MAX_RESOURCE.ofQueue(
            childQueue.getQueuePath()));
        minimumResource = maximumResource;
      }

      if (minimumResource > parentMinimumResource) {
        updateContext.addUpdateWarning(
            QueueUpdateWarningType.QUEUE_OVERUTILIZED.ofQueue(childQueue.getQueuePath()).withInfo(
                "Resource name: " + resourceName + " resource value: " + minimumResource));
        minimumResource = parentMinimumResource;
      }

      if (minimumResource == 0) {
        updateContext.addUpdateWarning(QueueUpdateWarningType.QUEUE_ZERO_RESOURCE.ofQueue(
            childQueue.getQueuePath()).withInfo("Resource name: " + resourceName));
      }

      float absoluteMinCapacity = minimumResource / clusterResource;
      float absoluteMaxCapacity = maximumResource / clusterResource;
      updateContext.getAbsoluteMinCapacity(childQueue.getQueuePath(), label).setValue(resourceName,
          absoluteMinCapacity);
      updateContext.getAbsoluteMaxCapacity(childQueue.getQueuePath(), label).setValue(resourceName,
          absoluteMaxCapacity);

      long roundedMinResource = (long) Math.floor(minimumResource);
      long roundedMaxResource = (long) Math.floor(maximumResource);
      childQueue.getQueueResourceQuotas().getEffectiveMinResource(label).setResourceValue(
          resourceName, roundedMinResource);
      childQueue.getQueueResourceQuotas().getEffectiveMaxResource(label).setResourceValue(
          resourceName, roundedMaxResource);

      aggregatedUsedResource.increment(resourceName, roundedMinResource);
    }
  }
}

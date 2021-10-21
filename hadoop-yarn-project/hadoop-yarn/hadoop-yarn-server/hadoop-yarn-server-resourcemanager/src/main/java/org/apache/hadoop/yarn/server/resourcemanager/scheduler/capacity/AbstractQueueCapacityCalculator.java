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
import org.apache.hadoop.yarn.util.UnitsConversionUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A strategy class to encapsulate queue capacity setup and resource calculation
 * logic.
 */
public abstract class AbstractQueueCapacityCalculator {

  /**
   * Sets all field of the queue based on its configurations.
   * @param queue queue to setup
   * @param label node label
   */
  public abstract void setup(
      CSQueue queue, String label);

  /**
   * Calculate the effective resource for a specific resource.
   * @param updateContext context of the current update phase
   * @param parentQueue the parent whose children will be updated
   */
  public void calculateChildQueueResources(
      QueueHierarchyUpdateContext updateContext,
      CSQueue parentQueue) {
    calculateResourcePrerequisites(updateContext, parentQueue);

    Map<String, ResourceVector> aggregatedResources = new HashMap<>();
    for (CSQueue childQueue : parentQueue.getChildQueues()) {
      for (String label : childQueue.getConfiguredNodeLabels()) {
        ResourceVector aggregatedUsedResource = aggregatedResources.getOrDefault(
            label, ResourceVector.newInstance());
        calculateMinResForAllResource(updateContext, childQueue, label, aggregatedUsedResource);
        aggregatedResources.put(label, aggregatedUsedResource);
      }
    }

    for (Map.Entry<String, ResourceVector> entry : aggregatedResources.entrySet()){
      updateContext.getQueueBranchContext(parentQueue.getQueuePath())
          .getRemainingResource(entry.getKey()).subtract(entry.getValue());
    }
  }

  private void calculateMinResForAllResource(
      QueueHierarchyUpdateContext updateContext, CSQueue childQueue, String label,
      ResourceVector aggregatedUsedResource) {
    CSQueue parentQueue = childQueue.getParent();
    for (String resourceName : getResourceNames(childQueue, label)) {
      long parentResource = parentQueue.getEffectiveCapacity(label)
          .getResourceValue(resourceName);
      long minimumResource = calculateMinimumResource(updateContext, childQueue,
          label, childQueue.getConfiguredCapacityVector(label)
              .getResource(resourceName));

      if (minimumResource > parentResource) {
        updateContext.addUpdateWarning(
            QueueUpdateWarning.QUEUE_OVERUTILIZED.ofQueue(childQueue.getQueuePath())
                .withInfo("Resource name: " + resourceName + " resource value: " + minimumResource));
        minimumResource = parentResource;
      }
      if (minimumResource == 0) {
        updateContext.addUpdateWarning(QueueUpdateWarning.
            QUEUE_ZERO_RESOURCE.ofQueue(childQueue.getQueuePath()));
      }

      childQueue.getQueueResourceQuotas().getEffectiveMinResource(label)
          .setResourceValue(resourceName, minimumResource);

      aggregatedUsedResource.increment(resourceName, minimumResource);
    }
  }

  /**
   * Sets the metrics and statistics after effective resource calculation.
   * @param updateContext context of the current update phase
   * @param queue queue to update
   * @param label node label
   */
  public abstract void setMetrics(
      QueueHierarchyUpdateContext updateContext, CSQueue queue, String label);

  /**
   * Returns the capacity type the calculator could handle.
   * @return capacity type
   */
  protected abstract QueueCapacityType getCapacityType();

  protected abstract long calculateMinimumResource(
      QueueHierarchyUpdateContext updateContext, CSQueue childQueue, String label,
      QueueCapacityVectorEntry capacityVectorEntry);

  protected abstract long calculateMaximumResource(
      QueueHierarchyUpdateContext updateContext, CSQueue childQueue, String label,
      QueueCapacityVectorEntry capacityVectorEntry);

  protected void calculateResourcePrerequisites(
      QueueHierarchyUpdateContext updateContext, CSQueue parentQueue) {
    for (String label : parentQueue.getConfiguredNodeLabels()) {
      // We need to set normalized resource ratio only once, not for each
      // resource calculator
      if (!updateContext.getQueueBranchContext(
          parentQueue.getQueuePath()).isParentAlreadyUpdated()) {
        setNormalizedResourceRatio(updateContext, parentQueue, label);
        updateContext.getQueueBranchContext(parentQueue.getQueuePath())
            .setUpdateFlag();
      }
    }
  }

  /**
   * Returns all resource names that are defined for the capacity type that is
   * handled by the calculator.
   * @param queue queue for which the capacity vector is defined
   * @param label node label
   * @return resource names
   */
  protected Set<String> getResourceNames(CSQueue queue, String label) {
    return getResourceNames(queue, label, getCapacityType());
  }

  /**
   * Returns all resource names that are defined for a capacity type.
   * @param queue queue for which the capacity vector is defined
   * @param label node label
   * @param capacityType capacity type for which the resource names are defined
   * @return resource names
   */
  protected Set<String> getResourceNames(CSQueue queue, String label,
                                         QueueCapacityType capacityType) {
    return queue.getConfiguredCapacityVector(label)
        .getResourceNamesByCapacityType(capacityType);
  }

  protected void setNormalizedResourceRatio(
      QueueHierarchyUpdateContext updateContext, CSQueue parentQueue,
      String label
  ) {
    for (QueueCapacityVectorEntry capacityVectorEntry :
        parentQueue.getConfiguredCapacityVector(label)) {
      long childrenConfiguredResource = 0;
      long effectiveMinResource = parentQueue.getQueueResourceQuotas()
          .getEffectiveMinResource(label).getResourceValue(
              capacityVectorEntry.getResourceName());

      // Total configured min resources of direct children of this given parent
      // queue
      for (CSQueue childQueue : parentQueue.getChildQueues()) {
        QueueCapacityVector capacityVector = childQueue.getConfiguredCapacityVector(label);
        if (capacityVector.isResourceOfType(
            capacityVectorEntry.getResourceName(), QueueCapacityType.ABSOLUTE)) {
          childrenConfiguredResource += capacityVector.getResource(
              capacityVectorEntry.getResourceName()).getResourceValue();
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
        numeratorForMinRatio = parentQueue.getQueueResourceQuotas()
            .getEffectiveMinResource(label).getResourceValue(
                capacityVectorEntry.getResourceName());
        updateContext.addUpdateWarning(QueueUpdateWarning.BRANCH_DOWNSCALED.ofQueue(
            parentQueue.getQueuePath()));
      }

      String unit = capacityVectorEntry.getResourceName().equals("memory-mb")
          ? "Mi" : "";
      long convertedValue = UnitsConversionUtil.convert(unit,
          updateContext.getUpdatedClusterResource(label).getResourceInformation(
              capacityVectorEntry.getResourceName()).getUnits(),
          childrenConfiguredResource);

      if (convertedValue != 0) {
        updateContext.getNormalizedMinResourceRatio(parentQueue.getQueuePath(), label)
            .setValue(capacityVectorEntry.getResourceName(),
                numeratorForMinRatio / convertedValue);
      }
    }
  }

  protected float sumCapacityValues(CSQueue queue, String label) {
    float sumValue = 0f;
    QueueCapacityVector capacityVector =
        queue.getConfiguredCapacityVector(label);
    for (String resourceName : getResourceNames(queue, label)) {
      sumValue += capacityVector.getResource(resourceName).getResourceValue();
    }

    return sumValue;
  }

  protected void setChildrenResources(
      CSQueue parentQueue, QueueHierarchyUpdateContext updateContext,
      ChildResourceCalculator callable) {
    Map<String, ResourceVector> aggregatedResources = new HashMap<>();
    for (CSQueue childQueue : parentQueue.getChildQueues()) {
      for (String label : childQueue.getConfiguredNodeLabels()) {
      ResourceVector aggregatedUsedResource = aggregatedResources.getOrDefault(
          label, ResourceVector.newInstance());
      
        for (String resourceName : getResourceNames(childQueue, label)) {
          long parentResource = parentQueue.getEffectiveCapacity(label)
              .getResourceValue(resourceName);
          long resource = callable.call(childQueue, label, childQueue
              .getConfiguredCapacityVector(label).getResource(resourceName));
          
          if (resource > parentResource) {
            updateContext.addUpdateWarning(
                QueueUpdateWarning.QUEUE_OVERUTILIZED.ofQueue(childQueue.getQueuePath()));
            resource = parentResource;
          }
          if (resource == 0) {
            updateContext.addUpdateWarning(QueueUpdateWarning.
                QUEUE_ZERO_RESOURCE.ofQueue(childQueue.getQueuePath()));
          }
          
          childQueue.getQueueResourceQuotas().getEffectiveMinResource(label)
              .setResourceValue(resourceName, resource);
          
          aggregatedUsedResource.increment(resourceName, resource);
        }
        
        aggregatedResources.put(label, aggregatedUsedResource);
      }
    }

    for (Map.Entry<String, ResourceVector> entry : aggregatedResources.entrySet()){
      updateContext.getQueueBranchContext(parentQueue.getQueuePath())
          .getRemainingResource(entry.getKey()).subtract(entry.getValue());
    }
  }

  protected interface ChildResourceCalculator {
    long call(CSQueue childQueue, String label,
              QueueCapacityVectorEntry capacityVectorEntry);
  }
}

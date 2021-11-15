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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueUpdateWarning.QueueUpdateWarningType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;
import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager.NO_LABEL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.ROOT;

/**
 * Controls how capacity and resource values are set and calculated for a queue.
 * Effective minimum and maximum resource values are set for each label and resource separately.
 */
public class CapacitySchedulerQueueCapacityHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(CapacitySchedulerQueueCapacityHandler.class);

  private static final Set<ResourceUnitCapacityType> CALCULATOR_PRECEDENCE =
      ImmutableSet.of(
          ResourceUnitCapacityType.ABSOLUTE,
          ResourceUnitCapacityType.PERCENTAGE,
          ResourceUnitCapacityType.WEIGHT);
  public static final Resource ZERO_RESOURCE = Resource.newInstance(0, 0);

  private final Map<ResourceUnitCapacityType, AbstractQueueCapacityCalculator>
      calculators;
  private final AbstractQueueCapacityCalculator rootCalculator =
      new RootQueueCapacityCalculator();
  private final RMNodeLabelsManager labelsManager;
  private QueueResourceRoundingStrategy roundingStrategy =
      new DefaultQueueResourceRoundingStrategy();

  public CapacitySchedulerQueueCapacityHandler(RMNodeLabelsManager labelsManager) {
    this.calculators = new HashMap<>();
    this.labelsManager = labelsManager;

    this.calculators.put(ResourceUnitCapacityType.ABSOLUTE,
        new AbsoluteResourceCapacityCalculator());
    this.calculators.put(ResourceUnitCapacityType.PERCENTAGE,
        new PercentageQueueCapacityCalculator());
    this.calculators.put(ResourceUnitCapacityType.WEIGHT,
        new WeightQueueCapacityCalculator());
  }

  /**
   * Updates the resource and metrics values for a queue, its siblings and descendants.
   * These values are calculated at runtime.
   *
   * @param clusterResource resource of the cluster
   * @param queue           queue to update
   */
  public QueueCapacityUpdateContext update(Resource clusterResource, CSQueue queue) {
    ResourceLimits resourceLimits = new ResourceLimits(clusterResource);
    QueueCapacityUpdateContext newContext =
        new QueueCapacityUpdateContext(clusterResource, labelsManager);

    if (queue.getQueuePath().equals(ROOT)) {
      updateRoot(queue, newContext, resourceLimits);
      updateChildren(queue, newContext, resourceLimits);
    } else {
      updateChildren(queue.getParent(), newContext, resourceLimits);
    }

    return newContext;
  }

  private void updateRoot(
      CSQueue queue, QueueCapacityUpdateContext newContext, ResourceLimits resourceLimits) {
    for (String label : queue.getConfiguredNodeLabels()) {
      for (QueueCapacityVectorEntry capacityVectorEntry : queue.getConfiguredCapacityVector(label)) {
        queue.getOrCreateAbsoluteMinCapacityVector(label).setValue(
            capacityVectorEntry.getResourceName(), 1);
        queue.getOrCreateAbsoluteMaxCapacityVector(label).setValue(
            capacityVectorEntry.getResourceName(), 1);

        float minimumResource = rootCalculator.calculateMinimumResource(newContext, queue, label,
            capacityVectorEntry);
        float maximumResource = rootCalculator.calculateMaximumResource(newContext, queue, label,
            capacityVectorEntry);
        long roundedMinResource = (long) Math.floor(minimumResource);
        long roundedMaxResource = (long) Math.floor(maximumResource);
        queue.getQueueResourceQuotas().getEffectiveMinResource(label)
            .setResourceValue(capacityVectorEntry.getResourceName(), roundedMinResource);
        queue.getQueueResourceQuotas().getEffectiveMaxResource(label)
            .setResourceValue(capacityVectorEntry.getResourceName(), roundedMaxResource);
      }
      rootCalculator.updateCapacitiesAfterCalculation(newContext, queue, label);
    }

    rootCalculator.calculateResourcePrerequisites(newContext, queue);
    queue.refreshAfterResourceCalculation(newContext.getUpdatedClusterResource(), resourceLimits);
  }


  private void updateChildren(
      CSQueue parent, QueueCapacityUpdateContext updateContext,
      ResourceLimits resourceLimits) {
    if (parent == null || CollectionUtils.isEmpty(parent.getChildQueues())) {
      return;
    }

    for (String label : parent.getConfiguredNodeLabels()) {
      updateContext.getQueueBranchContext(parent.getQueuePath()).setBatchRemainingResource(label,
          ResourceVector.of(parent.getEffectiveCapacity(label)));
    }

    calculateChildrenResources(updateContext, parent);

    for (String label : parent.getConfiguredNodeLabels()) {
      if (!updateContext.getQueueBranchContext(parent.getQueuePath()).getBatchRemainingResources(
          label).equals(ResourceVector.newInstance())) {
        updateContext.addUpdateWarning(QueueUpdateWarningType.BRANCH_UNDERUTILIZED.ofQueue(
            parent.getQueuePath()));
      }
    }

    for (CSQueue childQueue : parent.getChildQueues()) {
      for (String label : childQueue.getConfiguredNodeLabels()) {
        updateChildCapacities(updateContext, childQueue, label);
      }

      ResourceLimits childLimit = ((ParentQueue) parent).getResourceLimitsOfChild(
          childQueue, updateContext.getUpdatedClusterResource(), resourceLimits, NO_LABEL, false);
      childQueue.refreshAfterResourceCalculation(updateContext.getUpdatedClusterResource(),
          childLimit);
      updateChildren(childQueue, updateContext, childLimit);
    }
  }

  private void updateChildCapacities(
      QueueCapacityUpdateContext updateContext, CSQueue childQueue, String label) {
    QueueCapacityVector capacityVector = childQueue.getConfiguredCapacityVector(label);
    if (capacityVector.isMixedCapacityVector()) {
      // Post update capacities based on the calculated effective resource values
      AbstractQueueCapacityCalculator.setQueueCapacities(updateContext.getUpdatedClusterResource(
          label), childQueue, label);
    } else {
      // Update capacities according to the legacy logic
      for (ResourceUnitCapacityType capacityType :
          childQueue.getConfiguredCapacityVector(label).getDefinedCapacityTypes()) {
        AbstractQueueCapacityCalculator calculator = calculators.get(capacityType);
        calculator.updateCapacitiesAfterCalculation(updateContext, childQueue, label);
      }
    }

    // If memory is zero, all other resource units should be considered zero as well.
    if (childQueue.getEffectiveCapacity(label).getMemorySize() == 0) {
      childQueue.getQueueResourceQuotas().setEffectiveMinResource(label, ZERO_RESOURCE);
    }

    if (childQueue.getEffectiveMaxCapacity(label).getMemorySize() == 0) {
      childQueue.getQueueResourceQuotas().setEffectiveMaxResource(label, ZERO_RESOURCE);
    }
  }

  private void calculateChildrenResources(
      QueueCapacityUpdateContext updateContext, CSQueue parent) {
    for (ResourceUnitCapacityType capacityType : CALCULATOR_PRECEDENCE) {
      Map<String, ResourceVector> aggregatedResources = new HashMap<>();
      AbstractQueueCapacityCalculator capacityCalculator = calculators.get(capacityType);
      capacityCalculator.calculateResourcePrerequisites(updateContext, parent);

      for (CSQueue childQueue : parent.getChildQueues()) {
        childQueue.getWriteLock().lock();
        try {
          for (String label : childQueue.getConfiguredNodeLabels()) {
            ResourceVector aggregatedUsedResource = aggregatedResources.getOrDefault(label,
                ResourceVector.newInstance());
            setChildResources(updateContext, childQueue, label, aggregatedUsedResource, capacityCalculator);
            aggregatedResources.put(label, aggregatedUsedResource);
          }
        } finally {
          childQueue.getWriteLock().unlock();
        }
      }

      for (Map.Entry<String, ResourceVector> entry : aggregatedResources.entrySet()) {
        updateContext.getQueueBranchContext(parent.getQueuePath()).getBatchRemainingResources(
            entry.getKey()).subtract(entry.getValue());
      }
    }
  }

  private void setChildResources(
      QueueCapacityUpdateContext updateContext, CSQueue childQueue, String label,
      ResourceVector usedResourcesOfCalculator, AbstractQueueCapacityCalculator capacityCalculator) {
    for (String resourceName : capacityCalculator.getResourceNames(childQueue, label)) {
      long clusterResource = updateContext.getUpdatedClusterResource(label).getResourceValue(
          resourceName);
      QueueCapacityVectorEntry capacityVectorEntry = childQueue.getConfiguredCapacityVector(label)
          .getResource(resourceName);
      QueueCapacityVectorEntry maximumCapacityVectorEntry = childQueue
          .getConfiguredMaximumCapacityVector(label).getResource(resourceName);
      AbstractQueueCapacityCalculator maximumCapacityCalculator = calculators.get(
          maximumCapacityVectorEntry.getVectorResourceType());

      float minimumResource = capacityCalculator.calculateMinimumResource(
          updateContext, childQueue, label, capacityVectorEntry);
      float maximumResource = maximumCapacityCalculator.calculateMaximumResource(
          updateContext, childQueue, label, maximumCapacityVectorEntry);

      minimumResource = roundingStrategy.getRoundedResource(minimumResource, capacityVectorEntry);
      maximumResource = roundingStrategy.getRoundedResource(maximumResource,
          maximumCapacityVectorEntry);
      Pair<Float, Float> resources = validateCalculatedResources(updateContext, childQueue, resourceName, label,
          usedResourcesOfCalculator, new ImmutablePair<>(minimumResource, maximumResource));
      minimumResource = resources.getLeft();
      maximumResource = resources.getRight();

      float absoluteMinCapacity = minimumResource / clusterResource;
      float absoluteMaxCapacity = maximumResource / clusterResource;
      childQueue.getOrCreateAbsoluteMinCapacityVector(label).setValue(
          resourceName, absoluteMinCapacity);
      childQueue.getOrCreateAbsoluteMaxCapacityVector(label).setValue(
          resourceName, absoluteMaxCapacity);

      childQueue.getQueueResourceQuotas().getEffectiveMinResource(label).setResourceValue(
          resourceName, (long) minimumResource);
      childQueue.getQueueResourceQuotas().getEffectiveMaxResource(label).setResourceValue(
          resourceName, (long) maximumResource);

      usedResourcesOfCalculator.increment(resourceName, minimumResource);
    }
  }

  private Pair<Float, Float> validateCalculatedResources(
      QueueCapacityUpdateContext updateContext, CSQueue childQueue, String resourceName,
      String label, ResourceVector usedResourcesOfCalculator, Pair<Float, Float> calculatedResources) {
    CSQueue parentQueue = childQueue.getParent();

    float minimumResource = calculatedResources.getLeft();
    long minimumMemoryResource = childQueue.getQueueResourceQuotas().getEffectiveMinResource(label)
        .getMemorySize();

    float remainingResourceUnderParent = updateContext.getQueueBranchContext(
        parentQueue.getQueuePath()).getBatchRemainingResources(label).getValue(resourceName)
        - usedResourcesOfCalculator.getValue(resourceName);

    long parentMaximumResource = parentQueue.getEffectiveMaxCapacity(label).getResourceValue(
        resourceName);
    float maximumResource = calculatedResources.getRight();

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

    if (minimumResource > remainingResourceUnderParent) {
      // Legacy auto queues are assigned a zero resource if not enough resource is left
      if (parentQueue instanceof ManagedParentQueue) {
        minimumResource = 0;
      } else {
        updateContext.addUpdateWarning(
            QueueUpdateWarningType.QUEUE_OVERUTILIZED.ofQueue(childQueue.getQueuePath()).withInfo(
                "Resource name: " + resourceName + " resource value: " + minimumResource));
        minimumResource = remainingResourceUnderParent;
      }
    }

    if (minimumResource == 0) {
      updateContext.addUpdateWarning(QueueUpdateWarningType.QUEUE_ZERO_RESOURCE.ofQueue(
          childQueue.getQueuePath()).withInfo("Resource name: " + resourceName));
    }

    return new ImmutablePair<>(minimumResource, maximumResource);
  }
}
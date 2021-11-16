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
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;
import static org.apache.hadoop.yarn.api.records.ResourceInformation.VCORES_URI;
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
  private final Map<ResourceUnitCapacityType, AbstractQueueCapacityCalculator>
      calculators;
  private final AbstractQueueCapacityCalculator rootCalculator =
      new RootQueueCapacityCalculator();
  private final RMNodeLabelsManager labelsManager;
  private final Collection<String> definedResources = new LinkedHashSet<>();
  private final QueueResourceRoundingStrategy roundingStrategy =
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

    loadResourceNames();
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
      updateContext.getOrCreateQueueBranchContext(parent.getQueuePath()).setPostCalculatorRemainingResource(label,
          ResourceVector.of(parent.getEffectiveCapacity(label)));
      updateContext.getOrCreateQueueBranchContext(parent.getQueuePath()).setOverallRemainingResource(label,
          ResourceVector.of(parent.getEffectiveCapacity(label)));
    }

    for (AbstractQueueCapacityCalculator capacityCalculator : calculators.values()) {
      capacityCalculator.calculateResourcePrerequisites(updateContext, parent);
    }

    calculateChildrenResources(updateContext, parent);

    for (String label : parent.getConfiguredNodeLabels()) {
      if (!updateContext.getOrCreateQueueBranchContext(parent.getQueuePath()).getPostCalculatorRemainingResource(
          label).equals(ResourceVector.newInstance())) {
        updateContext.addUpdateWarning(QueueUpdateWarningType.BRANCH_UNDERUTILIZED.ofQueue(
            parent.getQueuePath()));
      }
    }

    updateChildrenAfterCalculation(parent, updateContext, resourceLimits);
  }

  private void calculateChildrenResources(
      QueueCapacityUpdateContext updateContext, CSQueue parent) {
    for (String resourceName : definedResources) {
      for (ResourceUnitCapacityType capacityType : CALCULATOR_PRECEDENCE) {
        Map<String, Float> effectiveResourceUsedByLabel = new HashMap<>();
        for (CSQueue childQueue : parent.getChildQueues()) {
          childQueue.getWriteLock().lock();
          try {
            for (String label : childQueue.getConfiguredNodeLabels()) {
              QueueCapacityVector capacityVector = childQueue.getConfiguredCapacityVector(label);
              if (!childQueue.getConfiguredCapacityVector(label).isResourceOfType(
                  resourceName, capacityType)) {
                continue;
              }
              float aggregatedUsedResource = effectiveResourceUsedByLabel.getOrDefault(label,
                  0f);
              float usedResourceByChild = setChildResources(updateContext, childQueue, label,
                  capacityVector.getResource(resourceName));
              float resourceUsedByLabel = aggregatedUsedResource + usedResourceByChild;
              updateContext.getOrCreateQueueBranchContext(parent.getQueuePath())
                  .getOverallRemainingResource(label).subtract(resourceName, usedResourceByChild);
              effectiveResourceUsedByLabel.put(label, resourceUsedByLabel);
            }
          } finally {
            childQueue.getWriteLock().unlock();
          }
        }

        // Only decrement post calculator remaining resource at the end of each calculator phase
        for (Map.Entry<String, Float> entry : effectiveResourceUsedByLabel.entrySet()) {
          updateContext.getOrCreateQueueBranchContext(parent.getQueuePath()).getPostCalculatorRemainingResource(
              entry.getKey()).subtract(resourceName, entry.getValue());
        }
      }
    }
  }

  private float setChildResources(
      QueueCapacityUpdateContext updateContext, CSQueue childQueue, String label,
      QueueCapacityVectorEntry capacityVectorEntry) {
    AbstractQueueCapacityCalculator capacityCalculator = calculators.get(
        capacityVectorEntry.getVectorResourceType());
    String resourceName = capacityVectorEntry.getResourceName();
    long clusterResource = updateContext.getUpdatedClusterResource(label).getResourceValue(
        resourceName);
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
    Pair<Float, Float> resources = validateCalculatedResources(updateContext, childQueue,
        resourceName, label, new ImmutablePair<>(minimumResource, maximumResource));
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

    return minimumResource;
  }

  private Pair<Float, Float> validateCalculatedResources(
      QueueCapacityUpdateContext updateContext, CSQueue childQueue, String resourceName,
      String label, Pair<Float, Float> calculatedResources) {
    CSQueue parentQueue = childQueue.getParent();

    float minimumResource = calculatedResources.getLeft();
    long minimumMemoryResource = childQueue.getQueueResourceQuotas().getEffectiveMinResource(label)
        .getMemorySize();

    float remainingResourceUnderParent = updateContext.getOrCreateQueueBranchContext(
        parentQueue.getQueuePath()).getOverallRemainingResource(label).getValue(resourceName);

    long parentMaximumResource = parentQueue.getEffectiveMaxCapacity(label).getResourceValue(
        resourceName);
    float maximumResource = calculatedResources.getRight();

    // Memory is the primary resource, if its zero, all other resource units are zero as well.
    if (!resourceName.equals(MEMORY_URI) && minimumMemoryResource == 0) {
      minimumResource = 0;
    }

    if (maximumResource != 0 && maximumResource > parentMaximumResource) {
      updateContext.addUpdateWarning(QueueUpdateWarningType.QUEUE_MAX_RESOURCE_EXCEEDS_PARENT.ofQueue(
          childQueue.getQueuePath()));
    }
    maximumResource = maximumResource == 0 ? parentMaximumResource : Math.min(maximumResource,
        parentMaximumResource);

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

  private void updateChildrenAfterCalculation(
      CSQueue parent, QueueCapacityUpdateContext updateContext, ResourceLimits resourceLimits) {
    for (CSQueue childQueue : parent.getChildQueues()) {
      updateChildCapacities(updateContext, childQueue);
      ResourceLimits childLimit = ((ParentQueue) parent).getResourceLimitsOfChild(
          childQueue, updateContext.getUpdatedClusterResource(), resourceLimits, NO_LABEL, false);
      childQueue.refreshAfterResourceCalculation(updateContext.getUpdatedClusterResource(),
          childLimit);
      updateChildren(childQueue, updateContext, childLimit);
    }
  }

  private void updateChildCapacities(
      QueueCapacityUpdateContext updateContext, CSQueue childQueue) {
    for (String label : childQueue.getConfiguredNodeLabels()) {
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
    }
  }

  private void loadResourceNames() {
    Set<String> resources = new HashSet<>(ResourceUtils.getResourceTypes().keySet());
    if (resources.contains(MEMORY_URI)) {
      resources.remove(MEMORY_URI);
      definedResources.add(MEMORY_URI);
    }

    if (resources.contains(VCORES_URI)) {
      resources.remove(VCORES_URI);
      definedResources.add(VCORES_URI);
    }

    definedResources.addAll(resources);
  }
}
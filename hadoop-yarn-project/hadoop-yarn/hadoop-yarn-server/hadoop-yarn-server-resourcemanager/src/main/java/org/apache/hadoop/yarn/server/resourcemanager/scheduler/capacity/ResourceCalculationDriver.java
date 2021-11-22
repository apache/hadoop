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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;

/**
 * Drives the main logic of resource calculation for all children under a parent queue. Acts as a
 * bookkeeper of disposable update information that is used by all children under a common parent.
 */
public class ResourceCalculationDriver {
  protected static final Set<ResourceUnitCapacityType> CALCULATOR_PRECEDENCE =
      ImmutableSet.of(
          ResourceUnitCapacityType.ABSOLUTE,
          ResourceUnitCapacityType.PERCENTAGE,
          ResourceUnitCapacityType.WEIGHT);

  protected final QueueResourceRoundingStrategy roundingStrategy =
      new DefaultQueueResourceRoundingStrategy();
  protected final CSQueue parent;
  protected final QueueCapacityUpdateContext updateContext;
  protected final Map<ResourceUnitCapacityType, AbstractQueueCapacityCalculator> calculators;
  protected final Collection<String> definedResources;

  protected final Map<String, ResourceVector> overallRemainingResource = new HashMap<>();
  protected final Map<String, ResourceVector> batchRemainingResource = new HashMap<>();
  // Used by ABSOLUTE capacity types
  protected final Map<String, ResourceVector> normalizedResourceRatio = new HashMap<>();
  // Used by WEIGHT capacity types
  protected final Map<String, Map<String, Float>> sumWeightsPerLabel = new HashMap<>();

  protected String currentResourceName;
  protected AbstractQueueCapacityCalculator currentCalculator;
  protected CSQueue currentChild;
  protected Map<String, Float> usedResourceByCurrentCalculator = new HashMap<>();

  public ResourceCalculationDriver(
      CSQueue parent, QueueCapacityUpdateContext updateContext,
      Map<ResourceUnitCapacityType, AbstractQueueCapacityCalculator> calculators,
      Collection<String> definedResources) {
    this.parent = parent;
    this.updateContext = updateContext;
    this.calculators = calculators;
    this.definedResources = definedResources;
  }

  /**
   * Returns the parent that is driving the calculation.
   *
   * @return a common parent queue
   */
  public CSQueue getParent() {
    return parent;
  }

  /**
   * Returns the context that is used throughout the whole update phase.
   *
   * @return update context
   */
  public QueueCapacityUpdateContext getUpdateContext() {
    return updateContext;
  }

  /**
   * Returns the name of the resource that is currently processed.
   *
   * @return resource name
   */
  public String getCurrentResourceName() {
    return currentResourceName;
  }

  /**
   * Returns the child that is currently processed.
   *
   * @return child queue
   */
  public CSQueue getCurrentChild() {
    return currentChild;
  }

  /**
   * Sets the currently evaluated child to a specific queue.
   *
   * @param currentChild a child queue
   */
  public void setCurrentChild(CSQueue currentChild) {
    if (currentChild.getParent() != parent) {
      throw new IllegalArgumentException("Child queue " + currentChild.getQueuePath() + " is not " +
          "a child of " + parent.getQueuePath());
    }

    this.currentChild = currentChild;
  }

  /**
   * A shorthand to return the minimum capacity vector entry for the currently evaluated child and
   * resource name.
   *
   * @param label node label
   * @return capacity vector entry
   */
  public QueueCapacityVectorEntry getCurrentMinimumCapacityEntry(String label) {
    return currentChild.getConfiguredCapacityVector(label).getResource(currentResourceName);
  }

  /**
   * A shorthand to return the maximum capacity vector entry for the currently evaluated child and
   * resource name.
   *
   * @param label node label
   * @return capacity vector entry
   */
  public QueueCapacityVectorEntry getCurrentMaximumCapacityEntry(String label) {
    return currentChild.getConfiguredMaximumCapacityVector(label).getResource(currentResourceName);
  }

  /**
   * Increments the aggregated weight.
   *
   * @param label        node label
   * @param resourceName resource unit name
   * @param value        weight value
   */
  public void incrementWeight(String label, String resourceName, float value) {
    sumWeightsPerLabel.putIfAbsent(label, new HashMap<>());
    sumWeightsPerLabel.get(label).put(resourceName,
        sumWeightsPerLabel.get(label).getOrDefault(resourceName, 0f) + value);
  }

  /**
   * Returns the aggregated children weights.
   *
   * @param label        node label
   * @param resourceName resource unit name
   * @return aggregated weights of children
   */
  public float getSumWeightsByResource(String label, String resourceName) {
    return sumWeightsPerLabel.get(label).get(resourceName);
  }

  public Map<String, ResourceVector> getNormalizedResourceRatios() {
    return normalizedResourceRatio;
  }

  /**
   * Returns the remaining resources of a parent that is still available for its
   * children. Decremented only after the calculator is finished its work on the corresponding
   * resources.
   *
   * @param label node label
   * @return remaining resources
   */
  public ResourceVector getBatchRemainingResource(String label) {
    batchRemainingResource.putIfAbsent(label, ResourceVector.newInstance());
    return batchRemainingResource.get(label);
  }

  /**
   * Calculates and sets the minimum and maximum effective resources for all children under the
   * parent queue with which this driver was initialized.
   */
  public void calculateResources() {
    // Reset both remaining resource storage to the parent's available resource
    for (String label : parent.getConfiguredNodeLabels()) {
      overallRemainingResource.put(label, ResourceVector.of(parent.getEffectiveCapacity(label)));
      batchRemainingResource.put(label, ResourceVector.of(parent.getEffectiveCapacity(label)));
    }

    for (AbstractQueueCapacityCalculator capacityCalculator : calculators.values()) {
      capacityCalculator.calculateResourcePrerequisites(this);
    }

    for (String resourceName : definedResources) {
      currentResourceName = resourceName;
      for (ResourceUnitCapacityType capacityType : CALCULATOR_PRECEDENCE) {
        currentCalculator = calculators.get(capacityType);
        for (CSQueue childQueue : parent.getChildQueues()) {
          currentChild = childQueue;
          calculateResourceOnChild(capacityType);
        }
        // Flush aggregated used resource by labels at the end of a calculator phase
        for (Map.Entry<String, Float> entry : usedResourceByCurrentCalculator.entrySet()) {
          batchRemainingResource.get(entry.getKey()).subtract(resourceName, entry.getValue());
        }
        usedResourceByCurrentCalculator = new HashMap<>();
      }
    }

    validateRemainingResource();
  }

  /**
   * Updates the capacity values of the currently evaluated child.
   */
  public void updateChildCapacities() {
    currentChild.getWriteLock().lock();
    try {
      for (String label : currentChild.getConfiguredNodeLabels()) {
        QueueCapacityVector capacityVector = currentChild.getConfiguredCapacityVector(label);
        if (capacityVector.isMixedCapacityVector()) {
          // Post update capacities based on the calculated effective resource values
          AbstractQueueCapacityCalculator.setQueueCapacities(updateContext.getUpdatedClusterResource(
              label), currentChild, label);
        } else {
          // Update capacities according to the legacy logic
          for (ResourceUnitCapacityType capacityType :
              currentChild.getConfiguredCapacityVector(label).getDefinedCapacityTypes()) {
            AbstractQueueCapacityCalculator calculator = calculators.get(capacityType);
            calculator.updateCapacitiesAfterCalculation(this, label);
          }
        }
      }
    } finally {
      currentChild.getWriteLock().unlock();
    }
  }

  private void calculateResourceOnChild(ResourceUnitCapacityType capacityType) {
    currentChild.getWriteLock().lock();
    try {
      for (String label : currentChild.getConfiguredNodeLabels()) {
        if (!currentChild.getConfiguredCapacityVector(label).isResourceOfType(currentResourceName,
            capacityType)) {
          return;
        }

        if (!overallRemainingResource.containsKey(label)) {
          continue;
        }

        float usedResourceByChild = setChildResources(label);
        float aggregatedUsedResource = usedResourceByCurrentCalculator.getOrDefault(label,
            0f);
        float resourceUsedByLabel = aggregatedUsedResource + usedResourceByChild;

        overallRemainingResource.get(label).subtract(currentResourceName, usedResourceByChild);
        usedResourceByCurrentCalculator.put(label, resourceUsedByLabel);
      }
    } finally {
      currentChild.getWriteLock().unlock();
    }
  }

  private float setChildResources(String label) {
    QueueCapacityVectorEntry capacityVectorEntry = currentChild.getConfiguredCapacityVector(
        label).getResource(currentResourceName);
    long clusterResource = updateContext.getUpdatedClusterResource(label).getResourceValue(
        currentResourceName);
    QueueCapacityVectorEntry maximumCapacityVectorEntry = currentChild
        .getConfiguredMaximumCapacityVector(label).getResource(currentResourceName);
    AbstractQueueCapacityCalculator maximumCapacityCalculator = calculators.get(
        maximumCapacityVectorEntry.getVectorResourceType());

    float minimumResource = currentCalculator.calculateMinimumResource(this, label);
    float maximumResource = maximumCapacityCalculator.calculateMaximumResource(this, label);

    minimumResource = roundingStrategy.getRoundedResource(minimumResource, capacityVectorEntry);
    maximumResource = roundingStrategy.getRoundedResource(maximumResource,
        maximumCapacityVectorEntry);
    Pair<Float, Float> resources = validateCalculatedResources(label, new ImmutablePair<>(
        minimumResource, maximumResource));
    minimumResource = resources.getLeft();
    maximumResource = resources.getRight();

    float absoluteMinCapacity = minimumResource / clusterResource;
    float absoluteMaxCapacity = maximumResource / clusterResource;
    currentChild.getOrCreateAbsoluteMinCapacityVector(label).setValue(
        currentResourceName, absoluteMinCapacity);
    currentChild.getOrCreateAbsoluteMaxCapacityVector(label).setValue(
        currentResourceName, absoluteMaxCapacity);

    currentChild.getQueueResourceQuotas().getEffectiveMinResource(label).setResourceValue(
        currentResourceName, (long) minimumResource);
    currentChild.getQueueResourceQuotas().getEffectiveMaxResource(label).setResourceValue(
        currentResourceName, (long) maximumResource);

    return minimumResource;
  }

  private Pair<Float, Float> validateCalculatedResources(
      String label, Pair<Float, Float> calculatedResources) {
    float minimumResource = calculatedResources.getLeft();
    long minimumMemoryResource = currentChild.getQueueResourceQuotas().getEffectiveMinResource(label)
        .getMemorySize();

    float remainingResourceUnderParent = overallRemainingResource.get(label).getValue(
        currentResourceName);

    long parentMaximumResource = parent.getEffectiveMaxCapacity(label).getResourceValue(
        currentResourceName);
    float maximumResource = calculatedResources.getRight();

    // Memory is the primary resource, if its zero, all other resource units are zero as well.
    if (!currentResourceName.equals(MEMORY_URI) && minimumMemoryResource == 0) {
      minimumResource = 0;
    }

    if (maximumResource != 0 && maximumResource > parentMaximumResource) {
      updateContext.addUpdateWarning(QueueUpdateWarning.QueueUpdateWarningType.QUEUE_MAX_RESOURCE_EXCEEDS_PARENT.ofQueue(
          currentChild.getQueuePath()));
    }
    maximumResource = maximumResource == 0 ? parentMaximumResource : Math.min(maximumResource,
        parentMaximumResource);

    if (maximumResource < minimumResource) {
      updateContext.addUpdateWarning(QueueUpdateWarning.QueueUpdateWarningType.QUEUE_EXCEEDS_MAX_RESOURCE.ofQueue(
          currentChild.getQueuePath()));
      minimumResource = maximumResource;
    }

    if (minimumResource > remainingResourceUnderParent) {
      // Legacy auto queues are assigned a zero resource if not enough resource is left
      if (parent instanceof ManagedParentQueue) {
        minimumResource = 0;
      } else {
        updateContext.addUpdateWarning(
            QueueUpdateWarning.QueueUpdateWarningType.QUEUE_OVERUTILIZED.ofQueue(currentChild.getQueuePath()).withInfo(
                "Resource name: " + currentResourceName + " resource value: " + minimumResource));
        minimumResource = remainingResourceUnderParent;
      }
    }

    if (minimumResource == 0) {
      updateContext.addUpdateWarning(QueueUpdateWarning.QueueUpdateWarningType.QUEUE_ZERO_RESOURCE.ofQueue(
          currentChild.getQueuePath()).withInfo("Resource name: " + currentResourceName));
    }

    return new ImmutablePair<>(minimumResource, maximumResource);
  }

  private void validateRemainingResource() {
    for (String label : parent.getConfiguredNodeLabels()) {
      if (!batchRemainingResource.get(label).equals(ResourceVector.newInstance())) {
        updateContext.addUpdateWarning(QueueUpdateWarning.QueueUpdateWarningType.BRANCH_UNDERUTILIZED.ofQueue(
            parent.getQueuePath()));
      }
    }
  }
}

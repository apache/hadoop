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
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

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
  static final String MB_UNIT = "Mi";

  protected final QueueResourceRoundingStrategy roundingStrategy =
      new DefaultQueueResourceRoundingStrategy(CALCULATOR_PRECEDENCE);
  protected final CSQueue parent;
  protected final QueueCapacityUpdateContext updateContext;
  protected final Map<ResourceUnitCapacityType, AbstractQueueCapacityCalculator> calculators;
  protected final Collection<String> definedResources;

  protected final Map<String, ResourceVector> overallRemainingResource = new HashMap<>();
  protected final Map<String, ResourceVector> batchRemainingResource = new HashMap<>();
  // Used by ABSOLUTE capacity types
  protected final Map<String, ResourceVector> normalizedResourceRatio = new HashMap<>();
  // Used by WEIGHT capacity typet js
  protected final Map<String, Map<String, Float>> sumWeightsPerLabel = new HashMap<>();
  protected Map<String, Float> usedResourceByCurrentCalculatorPerLabel = new HashMap<>();

  public ResourceCalculationDriver(
      CSQueue parent, QueueCapacityUpdateContext updateContext,
      Map<ResourceUnitCapacityType, AbstractQueueCapacityCalculator> calculators,
      Collection<String> definedResources) {
    this.parent = parent;
    this.updateContext = updateContext;
    this.calculators = calculators;
    this.definedResources = definedResources;
  }

  public static class CalculationContext {
    private String resourceName;
    private ResourceUnitCapacityType capacityType;
    private CSQueue childQueue;

    public CalculationContext(String resourceName, ResourceUnitCapacityType capacityType, CSQueue childQueue) {
      this.resourceName = resourceName;
      this.capacityType = capacityType;
      this.childQueue = childQueue;
    }

    public String getResourceName() {
      return resourceName;
    }

    public ResourceUnitCapacityType getCapacityType() {
      return capacityType;
    }

    public CSQueue getChildQueue() {
      return childQueue;
    }

    /**
     * A shorthand to return the minimum capacity vector entry for the currently evaluated child and
     * resource name.
     *
     * @param label node label
     * @return capacity vector entry
     */
    public QueueCapacityVectorEntry getCurrentMinimumCapacityEntry(String label) {
      return childQueue.getConfiguredCapacityVector(label).getResource(resourceName);
    }

    /**
     * A shorthand to return the maximum capacity vector entry for the currently evaluated child and
     * resource name.
     *
     * @param label node label
     * @return capacity vector entry
     */
    public QueueCapacityVectorEntry getCurrentMaximumCapacityEntry(String label) {
      return childQueue.getConfiguredMaxCapacityVector(label).getResource(resourceName);
    }
  }

  /**
   * Sets capacity and absolute capacity values based on minimum and maximum effective resources.
   *
   * @param queue child queue for which the capacities are set
   * @param label node label
   */
  public void setQueueCapacities(CSQueue queue, String label) {
    if (!(queue instanceof AbstractCSQueue)) {
      return;
    }

    Resource clusterResource = updateContext.getUpdatedClusterResource(label);
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

  public float getRemainingRatioOfResource(String label, String resourceName) {
    return batchRemainingResource.get(label).getValue(resourceName)
        / parent.getEffectiveCapacity(label).getResourceValue(resourceName);
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
      for (ResourceUnitCapacityType capacityType : CALCULATOR_PRECEDENCE) {
        for (CSQueue childQueue : parent.getChildQueues()) {
          CalculationContext context = new CalculationContext(resourceName, capacityType, childQueue);
          calculateResourceOnChild(context);
        }
        // Flush aggregated used resource by labels at the end of a calculator phase
        for (Map.Entry<String, Float> entry : usedResourceByCurrentCalculatorPerLabel.entrySet()) {
          batchRemainingResource.get(entry.getKey()).decrement(resourceName, entry.getValue());
        }
        usedResourceByCurrentCalculatorPerLabel = new HashMap<>();
      }
    }

    validateRemainingResource();
  }

  /**
   * Updates the capacity values of the currently evaluated child.
   */
  public void updateChildCapacities(CSQueue childQueue) {
    childQueue.getWriteLock().lock();
    try {
      for (String label : childQueue.getConfiguredNodeLabels()) {
        QueueCapacityVector capacityVector = childQueue.getConfiguredCapacityVector(label);
        if (capacityVector.isMixedCapacityVector()) {
          // Post update capacities based on the calculated effective resource values
          setQueueCapacities(childQueue, label);
        } else {
          // Update capacities according to the legacy logic
          for (ResourceUnitCapacityType capacityType :
              childQueue.getConfiguredCapacityVector(label).getDefinedCapacityTypes()) {
            AbstractQueueCapacityCalculator calculator = calculators.get(capacityType);
            calculator.updateCapacitiesAfterCalculation(this, childQueue, label);
          }
        }
      }
    } finally {
      childQueue.getWriteLock().unlock();
    }
  }

  private void calculateResourceOnChild(CalculationContext context) {
    context.getChildQueue().getWriteLock().lock();
    try {
      for (String label : context.getChildQueue().getConfiguredNodeLabels()) {
        if (!context.getChildQueue().getConfiguredCapacityVector(label).isResourceOfType(context.getResourceName(),
            context.getCapacityType())) {
          continue;
        }
        float usedResourceByChild = setChildResources(context, label);
        float aggregatedUsedResource = usedResourceByCurrentCalculatorPerLabel.getOrDefault(label,
            0f);
        float resourceUsedByLabel = aggregatedUsedResource + usedResourceByChild;

        overallRemainingResource.get(label).decrement(context.getResourceName(), usedResourceByChild);
        usedResourceByCurrentCalculatorPerLabel.put(label, resourceUsedByLabel);
      }
    } finally {
      context.getChildQueue().getWriteLock().unlock();
    }
  }

  private float setChildResources(CalculationContext context, String label) {
    QueueCapacityVectorEntry capacityVectorEntry = context.getChildQueue().getConfiguredCapacityVector(
        label).getResource(context.getResourceName());
    long clusterResource = updateContext.getUpdatedClusterResource(label).getResourceValue(
        context.getResourceName());
    QueueCapacityVectorEntry maximumCapacityVectorEntry = context.getChildQueue()
        .getConfiguredMaxCapacityVector(label).getResource(context.getResourceName());
    AbstractQueueCapacityCalculator maximumCapacityCalculator = calculators.get(
        maximumCapacityVectorEntry.getVectorResourceType());

    float minimumResource = calculators.get(context.getCapacityType()).calculateMinimumResource(this, context, label);
    float maximumResource = maximumCapacityCalculator.calculateMaximumResource(this, context, label);

    minimumResource = roundingStrategy.getRoundedResource(minimumResource, capacityVectorEntry);
    maximumResource = roundingStrategy.getRoundedResource(maximumResource,
        maximumCapacityVectorEntry);
    Pair<Float, Float> resources = validateCalculatedResources(context, label, new ImmutablePair<>(
        minimumResource, maximumResource));
    minimumResource = resources.getLeft();
    maximumResource = resources.getRight();

    float absoluteMinCapacity = minimumResource / clusterResource;
    float absoluteMaxCapacity = maximumResource / clusterResource;
    context.getChildQueue().getOrCreateAbsoluteMinCapacityVector(label).setValue(
        context.getResourceName(), absoluteMinCapacity);
    context.getChildQueue().getOrCreateAbsoluteMaxCapacityVector(label).setValue(
        context.getResourceName(), absoluteMaxCapacity);

    context.getChildQueue().getQueueResourceQuotas().getEffectiveMinResource(label).setResourceValue(
        context.getResourceName(), (long) minimumResource);
    context.getChildQueue().getQueueResourceQuotas().getEffectiveMaxResource(label).setResourceValue(
        context.getResourceName(), (long) maximumResource);

    return minimumResource;
  }

  private Pair<Float, Float> validateCalculatedResources(CalculationContext context,
      String label, Pair<Float, Float> calculatedResources) {
    float minimumResource = calculatedResources.getLeft();
    long minimumMemoryResource = context.getChildQueue().getQueueResourceQuotas().getEffectiveMinResource(label)
        .getMemorySize();

    float remainingResourceUnderParent = overallRemainingResource.get(label).getValue(
        context.getResourceName());

    long parentMaximumResource = parent.getEffectiveMaxCapacity(label).getResourceValue(
        context.getResourceName());
    float maximumResource = calculatedResources.getRight();

    // Memory is the primary resource, if its zero, all other resource units are zero as well.
    if (!context.getResourceName().equals(MEMORY_URI) && minimumMemoryResource == 0) {
      minimumResource = 0;
    }

    if (maximumResource != 0 && maximumResource > parentMaximumResource) {
      updateContext.addUpdateWarning(QueueUpdateWarning.QueueUpdateWarningType.QUEUE_MAX_RESOURCE_EXCEEDS_PARENT.ofQueue(
          context.getChildQueue().getQueuePath()));
    }
    maximumResource = maximumResource == 0 ? parentMaximumResource : Math.min(maximumResource,
        parentMaximumResource);

    if (maximumResource < minimumResource) {
      updateContext.addUpdateWarning(QueueUpdateWarning.QueueUpdateWarningType.QUEUE_EXCEEDS_MAX_RESOURCE.ofQueue(
          context.getChildQueue().getQueuePath()));
      minimumResource = maximumResource;
    }

    if (minimumResource > remainingResourceUnderParent) {
      // Legacy auto queues are assigned a zero resource if not enough resource is left
      if (parent instanceof ManagedParentQueue) {
        minimumResource = 0;
      } else {
        updateContext.addUpdateWarning(
            QueueUpdateWarning.QueueUpdateWarningType.QUEUE_OVERUTILIZED.ofQueue(context.getChildQueue().getQueuePath()).withInfo(
                "Resource name: " + context.getResourceName() + " resource value: " + minimumResource));
        minimumResource = remainingResourceUnderParent;
      }
    }

    if (minimumResource == 0) {
      updateContext.addUpdateWarning(QueueUpdateWarning.QueueUpdateWarningType.QUEUE_ZERO_RESOURCE.ofQueue(
          context.getChildQueue().getQueuePath()).withInfo("Resource name: " + context.getResourceName()));
    }

    return new ImmutablePair<>(minimumResource, maximumResource);
  }

  private void validateRemainingResource() {
    for (String label : parent.getConfiguredNodeLabels()) {
      if (!batchRemainingResource.get(label).equals(ResourceVector.newInstance())) {
        updateContext.addUpdateWarning(QueueUpdateWarning.QueueUpdateWarningType.BRANCH_UNDERUTILIZED.ofQueue(
            parent.getQueuePath()).withInfo("Label: " + label));
      }
    }
  }
}

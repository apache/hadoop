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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueUpdateWarning.QueueUpdateWarningType;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;

/**
 * Drives the main logic of resource calculation for all children under a queue. Acts as a
 * bookkeeper of disposable update information that is used by all children under the common parent.
 */
public class ResourceCalculationDriver {
  private static final ResourceUnitCapacityType[] CALCULATOR_PRECEDENCE =
      new ResourceUnitCapacityType[] {
          ResourceUnitCapacityType.ABSOLUTE,
          ResourceUnitCapacityType.PERCENTAGE,
          ResourceUnitCapacityType.WEIGHT};
  static final String MB_UNIT = "Mi";

  protected final QueueResourceRoundingStrategy roundingStrategy =
      new DefaultQueueResourceRoundingStrategy(CALCULATOR_PRECEDENCE);
  protected final CSQueue queue;
  protected final QueueCapacityUpdateContext updateContext;
  protected final Map<ResourceUnitCapacityType, AbstractQueueCapacityCalculator> calculators;
  protected final Collection<String> definedResources;

  protected final Map<String, ResourceVector> overallRemainingResourcePerLabel = new HashMap<>();
  protected final Map<String, ResourceVector> batchRemainingResourcePerLabel = new HashMap<>();
  // Used by ABSOLUTE capacity types
  protected final Map<String, ResourceVector> normalizedResourceRatioPerLabel = new HashMap<>();
  // Used by WEIGHT capacity types
  protected final Map<String, Map<String, Double>> sumWeightsPerLabel = new HashMap<>();
  protected Map<String, Double> usedResourceByCurrentCalculatorPerLabel = new HashMap<>();

  public ResourceCalculationDriver(
      CSQueue queue, QueueCapacityUpdateContext updateContext,
      Map<ResourceUnitCapacityType, AbstractQueueCapacityCalculator> calculators,
      Collection<String> definedResources) {
    this.queue = queue;
    this.updateContext = updateContext;
    this.calculators = calculators;
    this.definedResources = definedResources;
  }


  /**
   * Returns the parent that is driving the calculation.
   *
   * @return a common parent queue
   */
  public CSQueue getQueue() {
    return queue;
  }

  /**
   * Returns all the children defined under the driver parent queue.
   *
   * @return child queues
   */
  public Collection<CSQueue> getChildQueues() {
    return queue.getChildQueues();
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
  public void incrementWeight(String label, String resourceName, double value) {
    sumWeightsPerLabel.putIfAbsent(label, new HashMap<>());
    sumWeightsPerLabel.get(label).put(resourceName,
        sumWeightsPerLabel.get(label).getOrDefault(resourceName, 0d) + value);
  }

  /**
   * Returns the aggregated children weights.
   *
   * @param label        node label
   * @param resourceName resource unit name
   * @return aggregated weights of children
   */
  public double getSumWeightsByResource(String label, String resourceName) {
    return sumWeightsPerLabel.get(label).get(resourceName);
  }

  /**
   * Returns the ratio of the summary of children absolute configured resources and the parent's
   * effective minimum resource.
   *
   * @return normalized resource ratio for all labels
   */
  public Map<String, ResourceVector> getNormalizedResourceRatios() {
    return normalizedResourceRatioPerLabel;
  }

  /**
   * Returns the remaining resource ratio under the parent queue. The remaining resource is only
   * decremented after a capacity type is fully evaluated.
   *
   * @param label node label
   * @param resourceName name of resource unit
   * @return resource ratio
   */
  public double getRemainingRatioOfResource(String label, String resourceName) {
    return batchRemainingResourcePerLabel.get(label).getValue(resourceName)
        / queue.getEffectiveCapacity(label).getResourceValue(resourceName);
  }

  /**
   * Returns the ratio of the parent queue's effective minimum resource relative to the full cluster
   * resource.
   *
   * @param label node label
   * @param resourceName name of resource unit
   * @return absolute minimum capacity
   */
  public double getParentAbsoluteMinCapacity(String label, String resourceName) {
    return (double) queue.getEffectiveCapacity(label).getResourceValue(resourceName)
        / getUpdateContext().getUpdatedClusterResource(label).getResourceValue(resourceName);
  }

  /**
   * Returns the ratio of the parent queue's effective maximum resource relative to the full cluster
   * resource.
   *
   * @param label node label
   * @param resourceName name of resource unit
   * @return absolute maximum capacity
   */
  public double getParentAbsoluteMaxCapacity(String label, String resourceName) {
    return (double) queue.getEffectiveMaxCapacity(label).getResourceValue(resourceName)
        / getUpdateContext().getUpdatedClusterResource(label).getResourceValue(resourceName);
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
    batchRemainingResourcePerLabel.putIfAbsent(label, ResourceVector.newInstance());
    return batchRemainingResourcePerLabel.get(label);
  }

  /**
   * Calculates and sets the minimum and maximum effective resources for all children under the
   * parent queue with which this driver was initialized.
   */
  public void calculateResources() {
    // Reset both remaining resource storage to the parent's available resource
    for (String label : queue.getConfiguredNodeLabels()) {
      overallRemainingResourcePerLabel.put(label,
          ResourceVector.of(queue.getEffectiveCapacity(label)));
      batchRemainingResourcePerLabel.put(label,
          ResourceVector.of(queue.getEffectiveCapacity(label)));
    }

    for (AbstractQueueCapacityCalculator capacityCalculator : calculators.values()) {
      capacityCalculator.calculateResourcePrerequisites(this);
    }

    for (String resourceName : definedResources) {
      for (ResourceUnitCapacityType capacityType : CALCULATOR_PRECEDENCE) {
        for (CSQueue childQueue : getChildQueues()) {
          CalculationContext context = new CalculationContext(resourceName, capacityType,
              childQueue);
          calculateResourceOnChild(context);
        }

        // Flush aggregated used resource by labels at the end of a calculator phase
        for (Map.Entry<String, Double> entry : usedResourceByCurrentCalculatorPerLabel.entrySet()) {
          batchRemainingResourcePerLabel.get(entry.getKey()).decrement(resourceName,
              entry.getValue());
        }

        usedResourceByCurrentCalculatorPerLabel = new HashMap<>();
      }
    }

    validateRemainingResource();
  }

  private void calculateResourceOnChild(CalculationContext context) {
    context.getQueue().getWriteLock().lock();
    try {
      for (String label : context.getQueue().getConfiguredNodeLabels()) {
        if (!context.getQueue().getConfiguredCapacityVector(label).isResourceOfType(
            context.getResourceName(), context.getCapacityType())) {
          continue;
        }

        if (!overallRemainingResourcePerLabel.containsKey(label)) {
          continue;
        }

        double usedResourceByChild = setChildResources(context, label);
        double aggregatedUsedResource = usedResourceByCurrentCalculatorPerLabel.getOrDefault(label,
            0d);
        double resourceUsedByLabel = aggregatedUsedResource + usedResourceByChild;

        overallRemainingResourcePerLabel.get(label).decrement(context.getResourceName(),
            usedResourceByChild);
        usedResourceByCurrentCalculatorPerLabel.put(label, resourceUsedByLabel);
      }
    } finally {
      context.getQueue().getWriteLock().unlock();
    }
  }

  private double setChildResources(CalculationContext context, String label) {
    QueueCapacityVectorEntry capacityVectorEntry = context.getQueue().getConfiguredCapacityVector(
        label).getResource(context.getResourceName());
    QueueCapacityVectorEntry maximumCapacityVectorEntry = context.getQueue()
        .getConfiguredMaxCapacityVector(label).getResource(context.getResourceName());
    AbstractQueueCapacityCalculator maximumCapacityCalculator = calculators.get(
        maximumCapacityVectorEntry.getVectorResourceType());

    double minimumResource =
        calculators.get(context.getCapacityType()).calculateMinimumResource(this, context, label);
    double maximumResource = maximumCapacityCalculator.calculateMaximumResource(this, context,
        label);

    minimumResource = roundingStrategy.getRoundedResource(minimumResource, capacityVectorEntry);
    maximumResource = roundingStrategy.getRoundedResource(maximumResource,
        maximumCapacityVectorEntry);
    Pair<Double, Double> resources = validateCalculatedResources(context, label,
        new ImmutablePair<>(
        minimumResource, maximumResource));
    minimumResource = resources.getLeft();
    maximumResource = resources.getRight();

    context.getQueue().getQueueResourceQuotas().getEffectiveMinResource(label).setResourceValue(
        context.getResourceName(), (long) minimumResource);
    context.getQueue().getQueueResourceQuotas().getEffectiveMaxResource(label).setResourceValue(
        context.getResourceName(), (long) maximumResource);

    return minimumResource;
  }

  private Pair<Double, Double> validateCalculatedResources(CalculationContext context,
      String label, Pair<Double, Double> calculatedResources) {
    double minimumResource = calculatedResources.getLeft();
    long minimumMemoryResource =
        context.getQueue().getQueueResourceQuotas().getEffectiveMinResource(label).getMemorySize();

    double remainingResourceUnderParent = overallRemainingResourcePerLabel.get(label).getValue(
        context.getResourceName());

    long parentMaximumResource = queue.getEffectiveMaxCapacity(label).getResourceValue(
        context.getResourceName());
    double maximumResource = calculatedResources.getRight();

    // Memory is the primary resource, if its zero, all other resource units are zero as well.
    if (!context.getResourceName().equals(MEMORY_URI) && minimumMemoryResource == 0) {
      minimumResource = 0;
    }

    if (maximumResource != 0 && maximumResource > parentMaximumResource) {
      updateContext.addUpdateWarning(QueueUpdateWarningType.QUEUE_MAX_RESOURCE_EXCEEDS_PARENT
          .ofQueue(context.getQueue().getQueuePath()));
    }
    maximumResource = maximumResource == 0 ? parentMaximumResource : Math.min(maximumResource,
        parentMaximumResource);

    if (maximumResource < minimumResource) {
      updateContext.addUpdateWarning(QueueUpdateWarningType.QUEUE_EXCEEDS_MAX_RESOURCE.ofQueue(
          context.getQueue().getQueuePath()));
      minimumResource = maximumResource;
    }

    if (minimumResource > remainingResourceUnderParent) {
      // Legacy auto queues are assigned a zero resource if not enough resource is left
      if (queue instanceof ManagedParentQueue) {
        minimumResource = 0;
      } else {
        updateContext.addUpdateWarning(
            QueueUpdateWarningType.QUEUE_OVERUTILIZED.ofQueue(
                context.getQueue().getQueuePath()).withInfo(
                    "Resource name: " + context.getResourceName() +
                        " resource value: " + minimumResource));
        minimumResource = remainingResourceUnderParent;
      }
    }

    if (minimumResource == 0) {
      updateContext.addUpdateWarning(QueueUpdateWarningType.QUEUE_ZERO_RESOURCE.ofQueue(
          context.getQueue().getQueuePath())
          .withInfo("Resource name: " + context.getResourceName()));
    }

    return new ImmutablePair<>(minimumResource, maximumResource);
  }

  private void validateRemainingResource() {
    for (String label : queue.getConfiguredNodeLabels()) {
      if (!batchRemainingResourcePerLabel.get(label).equals(ResourceVector.newInstance())) {
        updateContext.addUpdateWarning(QueueUpdateWarningType.BRANCH_UNDERUTILIZED.ofQueue(
            queue.getQueuePath()).withInfo("Label: " + label));
      }
    }
  }
}

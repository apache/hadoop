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
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;
import static org.apache.hadoop.yarn.api.records.ResourceInformation.VCORES_URI;
import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager.NO_LABEL;

/**
 * Controls how capacity and resource values are set and calculated for a queue.
 * Effective minimum and maximum resource values are set for each label and resource separately.
 */
public class CapacitySchedulerQueueCapacityHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(CapacitySchedulerQueueCapacityHandler.class);

  private final Map<ResourceUnitCapacityType, AbstractQueueCapacityCalculator>
      calculators;
  private final AbstractQueueCapacityCalculator rootCalculator =
      new RootQueueCapacityCalculator();
  private final RMNodeLabelsManager labelsManager;
  private final Collection<String> definedResources = new LinkedHashSet<>();
  private final boolean isLegacyQueueMode;

  public CapacitySchedulerQueueCapacityHandler(RMNodeLabelsManager labelsManager,
                                               CapacitySchedulerConfiguration configuration) {
    this.calculators = new HashMap<>();
    this.labelsManager = labelsManager;

    this.calculators.put(ResourceUnitCapacityType.ABSOLUTE,
        new AbsoluteResourceCapacityCalculator());
    this.calculators.put(ResourceUnitCapacityType.PERCENTAGE,
        new PercentageQueueCapacityCalculator());
    this.calculators.put(ResourceUnitCapacityType.WEIGHT,
        new WeightQueueCapacityCalculator());
    this.isLegacyQueueMode = configuration.isLegacyQueueMode();

    loadResourceNames();
  }

  /**
   * Updates the resource and metrics values of all children under a specific queue.
   * These values are calculated at runtime.
   *
   * @param clusterResource resource of the cluster
   * @param queue           parent queue whose children will be updated
   * @return update context that contains information about the update phase
   */
  public QueueCapacityUpdateContext updateChildren(Resource clusterResource, CSQueue queue) {
    ResourceLimits resourceLimits = new ResourceLimits(clusterResource);
    QueueCapacityUpdateContext updateContext =
        new QueueCapacityUpdateContext(clusterResource, labelsManager);

    update(queue, updateContext, resourceLimits);
    return updateContext;
  }

  /**
   * Updates the resource and metrics value of the root queue. Root queue always has percentage
   * capacity type and is assigned the cluster resource as its minimum and maximum effective
   * resource.
   * @param rootQueue root queue
   * @param clusterResource cluster resource
   */
  public void updateRoot(CSQueue rootQueue, Resource clusterResource) {
    ResourceLimits resourceLimits = new ResourceLimits(clusterResource);
    QueueCapacityUpdateContext updateContext =
        new QueueCapacityUpdateContext(clusterResource, labelsManager);

    RootCalculationDriver rootCalculationDriver = new RootCalculationDriver(rootQueue,
        updateContext,
        rootCalculator, definedResources);
    rootCalculationDriver.calculateResources();
    rootQueue.refreshAfterResourceCalculation(updateContext.getUpdatedClusterResource(),
        resourceLimits);
  }

  private void update(
      CSQueue queue, QueueCapacityUpdateContext updateContext, ResourceLimits resourceLimits) {
    if (queue == null || CollectionUtils.isEmpty(queue.getChildQueues())) {
      return;
    }

    ResourceCalculationDriver resourceCalculationDriver = new ResourceCalculationDriver(
        queue, updateContext, calculators, definedResources);
    resourceCalculationDriver.calculateResources();

    updateChildrenAfterCalculation(resourceCalculationDriver, resourceLimits);
  }

  private void updateChildrenAfterCalculation(
      ResourceCalculationDriver resourceCalculationDriver, ResourceLimits resourceLimits) {
    AbstractParentQueue parentQueue = (AbstractParentQueue) resourceCalculationDriver.getQueue();
    for (CSQueue childQueue : parentQueue.getChildQueues()) {
      updateQueueCapacities(resourceCalculationDriver, childQueue);

      ResourceLimits childLimit = parentQueue.getResourceLimitsOfChild(childQueue,
          resourceCalculationDriver.getUpdateContext().getUpdatedClusterResource(),
          resourceLimits, NO_LABEL, false);
      childQueue.refreshAfterResourceCalculation(resourceCalculationDriver.getUpdateContext()
              .getUpdatedClusterResource(), childLimit);

      update(childQueue, resourceCalculationDriver.getUpdateContext(), childLimit);
    }
  }

  /**
   * Updates the capacity values of the currently evaluated child.
   * @param queue queue on which the capacities are set
   */
  private void updateQueueCapacities(
      ResourceCalculationDriver resourceCalculationDriver, CSQueue queue) {
    queue.getWriteLock().lock();
    try {
      for (String label : queue.getConfiguredNodeLabels()) {
        if (!isLegacyQueueMode) {
          // Post update capacities based on the calculated effective resource values
          setQueueCapacities(resourceCalculationDriver.getUpdateContext().getUpdatedClusterResource(
              label), queue, label);
        } else {
          // Update capacities according to the legacy logic
          for (ResourceUnitCapacityType capacityType :
              queue.getConfiguredCapacityVector(label).getDefinedCapacityTypes()) {
            AbstractQueueCapacityCalculator calculator = calculators.get(capacityType);
            calculator.updateCapacitiesAfterCalculation(resourceCalculationDriver, queue, label);
          }
        }
      }
    } finally {
      queue.getWriteLock().unlock();
    }
  }

  /**
   * Sets capacity and absolute capacity values of a queue based on minimum and
   * maximum effective resources.
   *
   * @param clusterResource overall cluster resource
   * @param queue child queue for which the capacities are set
   * @param label node label
   */
  public static void setQueueCapacities(Resource clusterResource, CSQueue queue, String label) {
    if (!(queue instanceof AbstractCSQueue)) {
      return;
    }

    AbstractCSQueue csQueue = (AbstractCSQueue) queue;
    // Do not override reservations when there are no cluster resources yet
    if ((csQueue instanceof ReservationQueue ||
        csQueue instanceof PlanQueue) &&
        Stream.of(clusterResource.getResources())
            .map(ResourceInformation::getValue)
            .noneMatch(num -> num > 0)) {
      return;
    }

    ResourceCalculator resourceCalculator = csQueue.resourceCalculator;

    CSQueue parent = queue.getParent();
    if (parent == null) {
      return;
    }
    // Update capacity with a double calculated from the parent's minResources
    // and the recently changed queue minResources.
    // capacity = effectiveMinResource / {parent's effectiveMinResource}
    float result = resourceCalculator.divide(clusterResource,
        queue.getQueueResourceQuotas().getEffectiveMinResource(label),
        parent.getQueueResourceQuotas().getEffectiveMinResource(label));
    queue.getQueueCapacities().setCapacity(label,
        Float.isInfinite(result) ? 0 : result);

    // Update maxCapacity with a double calculated from the parent's maxResources
    // and the recently changed queue maxResources.
    // maxCapacity = effectiveMaxResource / parent's effectiveMaxResource
    result = resourceCalculator.divide(clusterResource,
        queue.getQueueResourceQuotas().getEffectiveMaxResource(label),
        parent.getQueueResourceQuotas().getEffectiveMaxResource(label));
    queue.getQueueCapacities().setMaximumCapacity(label,
        Float.isInfinite(result) ? 0 : result);

    csQueue.updateAbsoluteCapacities();
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
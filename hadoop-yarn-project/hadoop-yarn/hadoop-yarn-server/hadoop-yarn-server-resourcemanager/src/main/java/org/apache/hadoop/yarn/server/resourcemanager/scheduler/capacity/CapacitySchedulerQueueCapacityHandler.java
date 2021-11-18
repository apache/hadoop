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
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
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

  private final Map<ResourceUnitCapacityType, AbstractQueueCapacityCalculator>
      calculators;
  private final AbstractQueueCapacityCalculator rootCalculator =
      new RootQueueCapacityCalculator();
  private final RMNodeLabelsManager labelsManager;
  private final Collection<String> definedResources = new LinkedHashSet<>();

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
    QueueCapacityUpdateContext updateContext =
        new QueueCapacityUpdateContext(clusterResource, labelsManager);

    if (queue.getQueuePath().equals(ROOT)) {
      updateRoot(queue, updateContext, resourceLimits);
      updateChildren(queue, updateContext, resourceLimits);
    } else {
      updateChildren(queue.getParent(), updateContext, resourceLimits);
    }

    return updateContext;
  }

  private void updateRoot(
      CSQueue queue, QueueCapacityUpdateContext updateContext, ResourceLimits resourceLimits) {
    RootCalculationDriver rootCalculationDriver = new RootCalculationDriver(queue, updateContext,
        rootCalculator, definedResources);
    rootCalculationDriver.calculateResources();
    queue.refreshAfterResourceCalculation(updateContext.getUpdatedClusterResource(), resourceLimits);
  }

  private void updateChildren(
      CSQueue parent, QueueCapacityUpdateContext updateContext,
      ResourceLimits resourceLimits) {
    if (parent == null || CollectionUtils.isEmpty(parent.getChildQueues())) {
      return;
    }

    ResourceCalculationDriver resourceCalculationDriver = new ResourceCalculationDriver(
        parent, updateContext, calculators, definedResources);
    resourceCalculationDriver.calculateResources();

    updateChildrenAfterCalculation(resourceCalculationDriver, resourceLimits);
  }

  private void updateChildrenAfterCalculation(
      ResourceCalculationDriver resourceCalculationDriver, ResourceLimits resourceLimits) {
    for (CSQueue childQueue : resourceCalculationDriver.getParent().getChildQueues()) {
      resourceCalculationDriver.setCurrentChild(childQueue);
      resourceCalculationDriver.updateChildCapacities();
      ResourceLimits childLimit = ((ParentQueue) resourceCalculationDriver.getParent()).getResourceLimitsOfChild(
          childQueue, resourceCalculationDriver.getUpdateContext().getUpdatedClusterResource(), resourceLimits, NO_LABEL, false);
      childQueue.refreshAfterResourceCalculation(resourceCalculationDriver.getUpdateContext()
              .getUpdatedClusterResource(), childLimit);
      updateChildren(childQueue, resourceCalculationDriver.getUpdateContext(), childLimit);
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
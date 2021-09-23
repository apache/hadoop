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
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSortedSet;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.ROOT;

/**
 * Controls how capacity and resource values are set and calculated for a queue.
 * Resources are set for each label and resource separately.
 */
public class CapacitySchedulerQueueCapacityHandler {

  private static final Set<QueueCapacityType> CALCULATOR_PRECEDENCE =
      ImmutableSortedSet.of(
          QueueCapacityType.PERCENTAGE);

  private final Map<QueueCapacityType, AbstractQueueCapacityCalculator>
      calculators;
  private final AbstractQueueCapacityCalculator rootCalculator =
      new RootQueueCapacityCalculator();
  private QueueHierarchyUpdateContext lastUpdateContext;

  public CapacitySchedulerQueueCapacityHandler() {
    this.calculators = new HashMap<>();
    this.lastUpdateContext = new QueueHierarchyUpdateContext(
        Resource.newInstance(0, 0));

    this.calculators.put(QueueCapacityType.PERCENTAGE,
        new PercentageQueueCapacityCalculator());
  }

  /**
   * Set static capacity config values (mostly due to backward compatibility).
   * These values are not calculated but defined at configuration time.
   *
   * @param queue queue to set capacity config values to
   * @param conf  configuration from which the values are derived
   */
  public void setup(CSQueue queue, CapacitySchedulerConfiguration conf) {
    for (String label : queue.getConfiguredNodeLabels()) {
      for (QueueCapacityType capacityType :
          queue.getConfiguredCapacityVector(label).getDefinedCapacityTypes()) {
        AbstractQueueCapacityCalculator calculator =
            calculators.get(capacityType);
        calculator.setup(queue, conf, label);
      }
    }
  }

  /**
   * Updates the resource and metrics values for a queue and its descendants
   * (and siblings if needed). These values are calculated at runtime.
   *
   * @param clusterResource resource of the cluster
   * @param queue           queue to update
   */
  public void update(Resource clusterResource, CSQueue queue) {
    QueueHierarchyUpdateContext newContext =
        new QueueHierarchyUpdateContext(clusterResource, lastUpdateContext);
    this.lastUpdateContext = newContext;

    if (queue.getQueuePath().equals(ROOT)) {
      calculateResources(newContext, queue, ImmutableSet.of(rootCalculator));
    }

    update(queue, newContext);
  }

  private void update(
      CSQueue parent, QueueHierarchyUpdateContext queueHierarchyContext) {
    if (parent == null || CollectionUtils.isEmpty(parent.getChildQueues())) {
      return;
    }

    collectCapacities(queueHierarchyContext, parent);
    calculateResources(queueHierarchyContext, parent,
        CALCULATOR_PRECEDENCE.stream().map((calculators::get))
            .collect(Collectors.toList()));
    updateChildren(queueHierarchyContext, parent);
  }

  private void calculateResources(
      QueueHierarchyUpdateContext queueHierarchyContext, CSQueue parent,
      Collection<AbstractQueueCapacityCalculator> usableCalculators) {
    for (String label : parent.getConfiguredNodeLabels()) {
      queueHierarchyContext.getQueueBranchContext(parent.getQueuePath())
          .setRemainingResource(label, ResourceVector.of(parent.getEffectiveCapacity(label)));
      for (AbstractQueueCapacityCalculator calculator : usableCalculators) {
        calculator.calculateChildQueueResources(queueHierarchyContext, parent, label);
      }
      setMetrics(parent, label);
    }
  }

  private void updateChildren(
      QueueHierarchyUpdateContext queueHierarchyContext, CSQueue parent) {
    if (parent.getChildQueues() != null) {
      for (CSQueue childQueue : parent.getChildQueues()) {
        update(childQueue, queueHierarchyContext);
      }
    }
  }

  /**
   * Collects capacity values of all queue on the same level for each resource.
   *
   * @param queueHierarchyContext update context of the queue hierarchy
   * @param parent parent of the branch
   */
  private void collectCapacities(
      QueueHierarchyUpdateContext queueHierarchyContext, CSQueue parent) {
    List<CSQueue> siblingsOfLevel = parent.getChildQueues();
    if (CollectionUtils.isEmpty(siblingsOfLevel)) {
      return;
    }

    for (CSQueue queue : siblingsOfLevel) {
      for (String label : queue.getConfiguredNodeLabels()) {
        for (QueueCapacityVectorEntry capacityResource :
            queue.getConfiguredCapacityVector(label)) {
          queueHierarchyContext.getQueueBranchContext(
                  parent.getQueuePath()).getSumByLabel(label)
              .increment(capacityResource);
        }
      }
    }
  }

  private void setMetrics(CSQueue parent, String label) {
    for (CSQueue childQueue : parent.getChildQueues()) {
      for (QueueCapacityType capacityType :
          childQueue.getConfiguredCapacityVector(label)
              .getDefinedCapacityTypes()) {
        AbstractQueueCapacityCalculator calculator =
            calculators.get(capacityType);
        calculator.setMetrics(lastUpdateContext, childQueue, label);
      }
    }
  }
}
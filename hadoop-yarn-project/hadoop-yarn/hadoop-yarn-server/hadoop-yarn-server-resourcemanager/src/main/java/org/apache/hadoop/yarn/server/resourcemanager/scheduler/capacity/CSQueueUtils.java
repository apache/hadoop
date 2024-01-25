/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.Set;

import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

public class CSQueueUtils {

  public final static float EPSILON = 0.001f;

  /*
   * Used only by tests
   */
  public static void checkMaxCapacity(QueuePath queuePath,
      float capacity, float maximumCapacity) {
    if (maximumCapacity < 0.0f || maximumCapacity > 1.0f) {
      throw new IllegalArgumentException(
          "Illegal value  of maximumCapacity " + maximumCapacity +
          " used in call to setMaxCapacity for queue " + queuePath);
    }
    }

  /*
   * Used only by tests
   */
  public static void checkAbsoluteCapacity(QueuePath queuePath,
      float absCapacity, float absMaxCapacity) {
    if (absMaxCapacity < (absCapacity - EPSILON)) {
      throw new IllegalArgumentException("Illegal call to setMaxCapacity. "
          + "Queue '" + queuePath + "' has "
          + "an absolute capacity (" + absCapacity
          + ") greater than its absolute maximumCapacity (" + absMaxCapacity
          + ")");
  }
  }

  public static float computeAbsoluteMaximumCapacity(
      float maximumCapacity, CSQueue parent) {
    float parentAbsMaxCapacity =
        (parent == null) ? 1.0f : parent.getAbsoluteMaximumCapacity();
    return (parentAbsMaxCapacity * maximumCapacity);
  }

  public static void loadCapacitiesByLabelsFromConf(
      QueuePath queuePath, QueueCapacities queueCapacities,
      CapacitySchedulerConfiguration csConf, Set<String> nodeLabels) {
    queueCapacities.clearConfigurableFields();

    for (String label : nodeLabels) {
      if (label.equals(CommonNodeLabelsManager.NO_LABEL)) {
        queueCapacities.setCapacity(label,
            csConf.getNonLabeledQueueCapacity(queuePath) / 100);
        queueCapacities.setMaximumCapacity(label,
            csConf.getNonLabeledQueueMaximumCapacity(queuePath) / 100);
        queueCapacities.setMaxAMResourcePercentage(
            label,
            csConf.getMaximumAMResourcePercentPerPartition(queuePath, label));
        queueCapacities.setWeight(label,
            csConf.getNonLabeledQueueWeight(queuePath.getFullPath()));
      } else{
        queueCapacities.setCapacity(label,
            csConf.getLabeledQueueCapacity(queuePath, label) / 100);
        queueCapacities.setMaximumCapacity(label,
            csConf.getLabeledQueueMaximumCapacity(queuePath, label) / 100);
        queueCapacities.setMaxAMResourcePercentage(label,
            csConf.getMaximumAMResourcePercentPerPartition(queuePath, label));
        queueCapacities.setWeight(label,
            csConf.getLabeledQueueWeight(queuePath, label));
      }
    }
  }

  /**
   * Update partitioned resource usage, if nodePartition == null, will update
   * used resource for all partitions of this queue.
   *
   * @param rc resource calculator.
   * @param totalPartitionResource total Partition Resource.
   * @param nodePartition node label.
   * @param childQueue child queue.
   */
  public static void updateUsedCapacity(final ResourceCalculator rc,
      final Resource totalPartitionResource, String nodePartition,
      AbstractCSQueue childQueue) {
    QueueCapacities queueCapacities = childQueue.getQueueCapacities();
    CSQueueMetrics queueMetrics = childQueue.getMetrics();
    ResourceUsage queueResourceUsage = childQueue.getQueueResourceUsage();
    Resource minimumAllocation = childQueue.getMinimumAllocation();
    float absoluteUsedCapacity = 0.0f;
    float usedCapacity = 0.0f;
    float reservedCapacity = 0.0f;
    float absoluteReservedCapacity = 0.0f;

    if (Resources.greaterThan(rc, totalPartitionResource,
        totalPartitionResource, Resources.none())) {

      Resource queueGuaranteedResource = childQueue
          .getEffectiveCapacity(nodePartition);

      //TODO : Modify below code to support Absolute Resource configurations
      // (YARN-5881) for AutoCreatedLeafQueue
      if (Float.compare(queueCapacities.getAbsoluteCapacity
              (nodePartition), 0f) == 0
          && childQueue instanceof AutoCreatedLeafQueue) {

        //If absolute capacity is 0 for a leaf queue (could be a managed leaf
        // queue, then use the leaf queue's template capacity to compute
        // guaranteed resource for used capacity)

        // queueGuaranteed = totalPartitionedResource *
        // absolute_capacity(partition)
        ManagedParentQueue parentQueue = (ManagedParentQueue)
            childQueue.getParent();
        QueueCapacities leafQueueTemplateCapacities = parentQueue
            .getLeafQueueTemplate()
            .getQueueCapacities();
        queueGuaranteedResource = Resources.multiply(totalPartitionResource,
            leafQueueTemplateCapacities.getAbsoluteCapacity
                (nodePartition));
      }

      // make queueGuranteed >= minimum_allocation to avoid divided by 0.
      queueGuaranteedResource =
          Resources.max(rc, totalPartitionResource, queueGuaranteedResource,
              minimumAllocation);

      Resource usedResource = queueResourceUsage.getUsed(nodePartition);
      absoluteUsedCapacity =
          Resources.divide(rc, totalPartitionResource, usedResource,
              totalPartitionResource);
      usedCapacity =
          Resources.divide(rc, totalPartitionResource, usedResource,
              queueGuaranteedResource);

      Resource resResource = queueResourceUsage.getReserved(nodePartition);
      reservedCapacity =
          Resources.divide(rc, totalPartitionResource, resResource,
              queueGuaranteedResource);
      absoluteReservedCapacity =
          Resources.divide(rc, totalPartitionResource, resResource,
              totalPartitionResource);
    }

    queueCapacities
        .setAbsoluteUsedCapacity(nodePartition, absoluteUsedCapacity);
    queueCapacities.setUsedCapacity(nodePartition, usedCapacity);
    queueCapacities.setReservedCapacity(nodePartition, reservedCapacity);
    queueCapacities
        .setAbsoluteReservedCapacity(nodePartition, absoluteReservedCapacity);

    // QueueMetrics does not support per-label capacities,
    // so we report values only for the default partition.

    queueMetrics.setUsedCapacity(nodePartition,
        queueCapacities.getUsedCapacity(RMNodeLabelsManager.NO_LABEL));
    queueMetrics.setAbsoluteUsedCapacity(nodePartition,
        queueCapacities.getAbsoluteUsedCapacity(
            RMNodeLabelsManager.NO_LABEL));

  }

  private static Resource getMaxAvailableResourceToQueuePartition(
      final ResourceCalculator rc, CSQueue queue,
      Resource cluster, String partition) {
    // Calculate guaranteed resource for a label in a queue by below logic.
    // (total label resource) * (absolute capacity of label in that queue)
    Resource queueGuaranteedResource = queue.getEffectiveCapacity(partition);

    // Available resource in queue for a specific label will be calculated as
    // {(guaranteed resource for a label in a queue) -
    // (resource usage of that label in the queue)}
    Resource available = (Resources.greaterThan(rc, cluster,
        queueGuaranteedResource,
        queue.getQueueResourceUsage().getUsed(partition))) ? Resources
        .componentwiseMax(Resources.subtractFrom(queueGuaranteedResource,
            queue.getQueueResourceUsage().getUsed(partition)), Resources
            .none()) : Resources.none();

    return available;
  }

  /**
   * <p>
   * Update Queue Statistics:
   * </p>
   *
   * <ul>
   *   <li>used-capacity/absolute-used-capacity by partition</li>
   *   <li>non-partitioned max-avail-resource to queue</li>
   * </ul>
   *
   * <p>
   * When nodePartition is null, all partition of
   * used-capacity/absolute-used-capacity will be updated.
   * </p>
   *
   * @param rc resource calculator.
   * @param cluster cluster resource.
   * @param childQueue child queue.
   * @param nlm RMNodeLabelsManager.
   * @param nodePartition node label.
   */
  @Lock(CSQueue.class)
  public static void updateQueueStatistics(
      final ResourceCalculator rc, final Resource cluster,
      final AbstractCSQueue childQueue, final RMNodeLabelsManager nlm,
      final String nodePartition) {
    QueueCapacities queueCapacities = childQueue.getQueueCapacities();
    ResourceUsage queueResourceUsage = childQueue.getQueueResourceUsage();

    if (nodePartition == null) {
      for (String partition : Sets.union(queueCapacities.getExistingNodeLabels(),
          queueResourceUsage.getExistingNodeLabels())) {
        updateUsedCapacity(rc, nlm.getResourceByLabel(partition, cluster),
            partition, childQueue);

        // Update queue metrics w.r.t node labels.
        // In QueueMetrics, null label is handled the same as NO_LABEL.
        // This is because queue metrics for partitions are not tracked.
        // In the future, will have to change this when/if queue metrics
        // for partitions also get tracked.
        childQueue.getMetrics().setAvailableResourcesToQueue(
            partition,
            getMaxAvailableResourceToQueuePartition(rc, childQueue,
                cluster, partition));
      }
    } else {
      updateUsedCapacity(rc, nlm.getResourceByLabel(nodePartition, cluster),
          nodePartition, childQueue);

      // Same as above.
      childQueue.getMetrics().setAvailableResourcesToQueue(
          nodePartition,
          getMaxAvailableResourceToQueuePartition(rc, childQueue,
              cluster, nodePartition));
    }
   }

  /**
   * Updated configured capacity/max-capacity for queue.
   * @param rc resource calculator
   * @param partitionResource total cluster resources for this partition
   * @param partition partition being updated
   * @param queue queue
   */
   public static void updateConfiguredCapacityMetrics(ResourceCalculator rc,
       Resource partitionResource, String partition, AbstractCSQueue queue) {
     queue.getMetrics().setGuaranteedResources(partition, rc.multiplyAndNormalizeDown(
         partitionResource, queue.getQueueCapacities().getAbsoluteCapacity(partition),
         queue.getMinimumAllocation()));
     queue.getMetrics().setMaxCapacityResources(partition, rc.multiplyAndNormalizeDown(
         partitionResource, queue.getQueueCapacities().getAbsoluteMaximumCapacity(partition),
         queue.getMinimumAllocation()));
    queue.getMetrics().setGuaranteedCapacities(partition,
        queue.getQueueCapacities().getCapacity(partition),
        queue.getQueueCapacities().getAbsoluteCapacity(partition));
    queue.getMetrics().setMaxCapacities(partition,
        queue.getQueueCapacities().getMaximumCapacity(partition),
        queue.getQueueCapacities().getAbsoluteMaximumCapacity(partition));
   }

  public static void updateAbsoluteCapacitiesByNodeLabels(QueueCapacities queueCapacities,
                                                          QueueCapacities parentQueueCapacities,
                                                          Set<String> nodeLabels,
                                                          boolean isLegacyQueueMode) {
    for (String label : nodeLabels) {
      if (isLegacyQueueMode) {
        // Weight will be normalized to queue.weight =
        //      queue.weight(sum({sibling-queues.weight}))
        // When weight is set, capacity will be set to 0;
        // When capacity is set, weight will be normalized to 0,
        // So get larger from normalized_weight and capacity will make sure we do
        // calculation correct
        float capacity = Math.max(
            queueCapacities.getCapacity(label),
            queueCapacities
                .getNormalizedWeight(label));

        if (capacity > 0f) {
          queueCapacities.setAbsoluteCapacity(label, capacity * (
              parentQueueCapacities == null ? 1 :
                  parentQueueCapacities.getAbsoluteCapacity(label)));
        }
      } else {
        queueCapacities.setAbsoluteCapacity(label, queueCapacities.getCapacity(label) * (
            parentQueueCapacities == null ? 1 :
                parentQueueCapacities.getAbsoluteCapacity(label)));
      }

      float maxCapacity = queueCapacities
          .getMaximumCapacity(label);
      if (maxCapacity > 0f) {
        queueCapacities.setAbsoluteMaximumCapacity(label, maxCapacity * (
            parentQueueCapacities == null ? 1 :
                parentQueueCapacities.getAbsoluteMaximumCapacity(label)));
      }
    }
  }
}

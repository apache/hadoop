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

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.collect.Sets;

class CSQueueUtils {

  final static float EPSILON = 0.0001f;
  
  /*
   * Used only by tests
   */
  public static void checkMaxCapacity(String queueName, 
      float capacity, float maximumCapacity) {
    if (maximumCapacity < 0.0f || maximumCapacity > 1.0f) {
      throw new IllegalArgumentException(
          "Illegal value  of maximumCapacity " + maximumCapacity + 
          " used in call to setMaxCapacity for queue " + queueName);
    }
    }

  /*
   * Used only by tests
   */
  public static void checkAbsoluteCapacity(String queueName,
      float absCapacity, float absMaxCapacity) {
    if (absMaxCapacity < (absCapacity - EPSILON)) {
      throw new IllegalArgumentException("Illegal call to setMaxCapacity. "
          + "Queue '" + queueName + "' has " + "an absolute capacity (" + absCapacity
          + ") greater than " + "its absolute maximumCapacity (" + absMaxCapacity
          + ")");
  }
  }
  
  /**
   * Check sanity of capacities:
   * - capacity <= maxCapacity
   * - absCapacity <= absMaximumCapacity
   */
  private static void capacitiesSanityCheck(String queueName,
      QueueCapacities queueCapacities) {
    for (String label : queueCapacities.getExistingNodeLabels()) {
      float capacity = queueCapacities.getCapacity(label);
      float maximumCapacity = queueCapacities.getMaximumCapacity(label);
      if (capacity > maximumCapacity) {
        throw new IllegalArgumentException("Illegal queue capacity setting, "
            + "(capacity=" + capacity + ") > (maximum-capacity="
            + maximumCapacity + "). When label=[" + label + "]");
      }
     
      // Actually, this may not needed since we have verified capacity <=
      // maximumCapacity. And the way we compute absolute capacity (abs(x) =
      // cap(x) * cap(x.parent) * ...) is a monotone increasing function. But
      // just keep it here to make sure our compute abs capacity method works
      // correctly. 
      float absCapacity = queueCapacities.getAbsoluteCapacity(label);
      float absMaxCapacity = queueCapacities.getAbsoluteMaximumCapacity(label);
      if (absCapacity > absMaxCapacity) {
        throw new IllegalArgumentException("Illegal queue capacity setting, "
            + "(abs-capacity=" + absCapacity + ") > (abs-maximum-capacity="
            + absMaxCapacity + "). When label=[" + label + "]");
      }
    }
  }

  public static float computeAbsoluteMaximumCapacity(
      float maximumCapacity, CSQueue parent) {
    float parentAbsMaxCapacity = 
        (parent == null) ? 1.0f : parent.getAbsoluteMaximumCapacity();
    return (parentAbsMaxCapacity * maximumCapacity);
  }
  
  /**
   * This method intends to be used by ReservationQueue, ReservationQueue will
   * not appear in configuration file, so we shouldn't do load capacities
   * settings in configuration for reservation queue.
   */
  public static void updateAndCheckCapacitiesByLabel(String queuePath,
      QueueCapacities queueCapacities, QueueCapacities parentQueueCapacities) {
    updateAbsoluteCapacitiesByNodeLabels(queueCapacities, parentQueueCapacities);

    capacitiesSanityCheck(queuePath, queueCapacities);
  }

  /**
   * Do following steps for capacities
   * - Load capacities from configuration
   * - Update absolute capacities for new capacities
   * - Check if capacities/absolute-capacities legal
   */
  public static void loadUpdateAndCheckCapacities(String queuePath,
      CapacitySchedulerConfiguration csConf,
      QueueCapacities queueCapacities, QueueCapacities parentQueueCapacities) {
    loadCapacitiesByLabelsFromConf(queuePath,
        queueCapacities, csConf);

    updateAbsoluteCapacitiesByNodeLabels(queueCapacities, parentQueueCapacities);

    capacitiesSanityCheck(queuePath, queueCapacities);
  }
  
  private static void loadCapacitiesByLabelsFromConf(String queuePath,
      QueueCapacities queueCapacities, CapacitySchedulerConfiguration csConf) {
    queueCapacities.clearConfigurableFields();
    Set<String> configuredNodelabels =
        csConf.getConfiguredNodeLabels(queuePath);

    for (String label : configuredNodelabels) {
      if (label.equals(CommonNodeLabelsManager.NO_LABEL)) {
        queueCapacities.setCapacity(CommonNodeLabelsManager.NO_LABEL,
            csConf.getNonLabeledQueueCapacity(queuePath) / 100);
        queueCapacities.setMaximumCapacity(CommonNodeLabelsManager.NO_LABEL,
            csConf.getNonLabeledQueueMaximumCapacity(queuePath) / 100);
        queueCapacities.setMaxAMResourcePercentage(
            CommonNodeLabelsManager.NO_LABEL,
            csConf.getMaximumAMResourcePercentPerPartition(queuePath, label));
      } else {
        queueCapacities.setCapacity(label,
            csConf.getLabeledQueueCapacity(queuePath, label) / 100);
        queueCapacities.setMaximumCapacity(label,
            csConf.getLabeledQueueMaximumCapacity(queuePath, label) / 100);
        queueCapacities.setMaxAMResourcePercentage(label,
            csConf.getMaximumAMResourcePercentPerPartition(queuePath, label));
      }
    }
  }
  
  // Set absolute capacities for {capacity, maximum-capacity}
  private static void updateAbsoluteCapacitiesByNodeLabels(
      QueueCapacities queueCapacities, QueueCapacities parentQueueCapacities) {
    for (String label : queueCapacities.getExistingNodeLabels()) {
      float capacity = queueCapacities.getCapacity(label);
      if (capacity > 0f) {
        queueCapacities.setAbsoluteCapacity(
            label,
            capacity
                * (parentQueueCapacities == null ? 1 : parentQueueCapacities
                    .getAbsoluteCapacity(label)));
      }

      float maxCapacity = queueCapacities.getMaximumCapacity(label);
      if (maxCapacity > 0f) {
        queueCapacities.setAbsoluteMaximumCapacity(
            label,
            maxCapacity
                * (parentQueueCapacities == null ? 1 : parentQueueCapacities
                    .getAbsoluteMaximumCapacity(label)));
      }
    }
  }
  
  /**
   * Update partitioned resource usage, if nodePartition == null, will update
   * used resource for all partitions of this queue.
   */
  public static void updateUsedCapacity(final ResourceCalculator rc,
      final Resource totalPartitionResource, final Resource minimumAllocation,
      ResourceUsage queueResourceUsage, QueueCapacities queueCapacities,
      String nodePartition) {
    float absoluteUsedCapacity = 0.0f;
    float usedCapacity = 0.0f;
    float reservedCapacity = 0.0f;
    float absoluteReservedCapacity = 0.0f;

    if (Resources.greaterThan(rc, totalPartitionResource,
        totalPartitionResource, Resources.none())) {
      // queueGuaranteed = totalPartitionedResource *
      // absolute_capacity(partition)
      Resource queueGuranteedResource =
          Resources.multiply(totalPartitionResource,
              queueCapacities.getAbsoluteCapacity(nodePartition));

      // make queueGuranteed >= minimum_allocation to avoid divided by 0.
      queueGuranteedResource =
          Resources.max(rc, totalPartitionResource, queueGuranteedResource,
              minimumAllocation);

      Resource usedResource = queueResourceUsage.getUsed(nodePartition);
      absoluteUsedCapacity =
          Resources.divide(rc, totalPartitionResource, usedResource,
              totalPartitionResource);
      usedCapacity =
          Resources.divide(rc, totalPartitionResource, usedResource,
              queueGuranteedResource);

      Resource resResource = queueResourceUsage.getReserved(nodePartition);
      reservedCapacity =
          Resources.divide(rc, totalPartitionResource, resResource,
              queueGuranteedResource);
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
  }
  
  private static Resource getNonPartitionedMaxAvailableResourceToQueue(
      final ResourceCalculator rc, Resource totalNonPartitionedResource,
      CSQueue queue) {
    Resource queueLimit = Resources.none();
    Resource usedResources = queue.getUsedResources();

    if (Resources.greaterThan(rc, totalNonPartitionedResource,
        totalNonPartitionedResource, Resources.none())) {
      queueLimit =
          Resources.multiply(totalNonPartitionedResource,
              queue.getAbsoluteCapacity());
    }

    Resource available = Resources.subtract(queueLimit, usedResources);
    return Resources.max(rc, totalNonPartitionedResource, available,
        Resources.none());
  }
  
  /**
   * <p>
   * Update Queue Statistics:
   * </p>
   *  
   * <li>used-capacity/absolute-used-capacity by partition</li> 
   * <li>non-partitioned max-avail-resource to queue</li>
   * 
   * <p>
   * When nodePartition is null, all partition of
   * used-capacity/absolute-used-capacity will be updated.
   * </p>
   */
  @Lock(CSQueue.class)
  public static void updateQueueStatistics(
      final ResourceCalculator rc, final Resource cluster, final Resource minimumAllocation,
      final CSQueue childQueue, final RMNodeLabelsManager nlm, 
      final String nodePartition) {
    QueueCapacities queueCapacities = childQueue.getQueueCapacities();
    ResourceUsage queueResourceUsage = childQueue.getQueueResourceUsage();
    
    if (nodePartition == null) {
      for (String partition : Sets.union(
          queueCapacities.getNodePartitionsSet(),
          queueResourceUsage.getNodePartitionsSet())) {
        updateUsedCapacity(rc, nlm.getResourceByLabel(partition, cluster),
            minimumAllocation, queueResourceUsage, queueCapacities, partition);
      }
    } else {
      updateUsedCapacity(rc, nlm.getResourceByLabel(nodePartition, cluster),
          minimumAllocation, queueResourceUsage, queueCapacities, nodePartition);
    }
    
    // Now in QueueMetrics, we only store available-resource-to-queue for
    // default partition.
    if (nodePartition == null
        || nodePartition.equals(RMNodeLabelsManager.NO_LABEL)) {
      childQueue.getMetrics().setAvailableResourcesToQueue(
          getNonPartitionedMaxAvailableResourceToQueue(rc,
              nlm.getResourceByLabel(RMNodeLabelsManager.NO_LABEL, cluster),
              childQueue));
    }
   }
}

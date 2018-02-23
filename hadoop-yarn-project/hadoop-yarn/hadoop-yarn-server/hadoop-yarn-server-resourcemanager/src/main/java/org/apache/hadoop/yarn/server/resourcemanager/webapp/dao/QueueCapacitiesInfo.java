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
package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;

/**
 * DAO which wraps PartitionQueueCapacitiesInfo applicable for a queue
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class QueueCapacitiesInfo {
  protected List<PartitionQueueCapacitiesInfo> queueCapacitiesByPartition =
      new ArrayList<>();

  public QueueCapacitiesInfo() {
  }

  public QueueCapacitiesInfo(QueueCapacities capacities,
      QueueResourceQuotas resourceQuotas,
      boolean considerAMUsage) {
    if (capacities == null) {
      return;
    }
    float capacity;
    float usedCapacity;
    float maxCapacity;
    float absCapacity;
    float absUsedCapacity;
    float absMaxCapacity;
    float maxAMLimitPercentage;
    for (String partitionName : capacities.getExistingNodeLabels()) {
      usedCapacity = capacities.getUsedCapacity(partitionName) * 100;
      capacity = capacities.getCapacity(partitionName) * 100;
      maxCapacity = capacities.getMaximumCapacity(partitionName);
      absCapacity = CapacitySchedulerQueueInfo
          .cap(capacities.getAbsoluteCapacity(partitionName), 0f, 1f) * 100;
      absUsedCapacity = CapacitySchedulerQueueInfo
          .cap(capacities.getAbsoluteUsedCapacity(partitionName), 0f, 1f) * 100;
      absMaxCapacity = CapacitySchedulerQueueInfo.cap(
          capacities.getAbsoluteMaximumCapacity(partitionName), 0f, 1f) * 100;
      maxAMLimitPercentage = capacities
          .getMaxAMResourcePercentage(partitionName) * 100;
      if (maxCapacity < CapacitySchedulerQueueInfo.EPSILON || maxCapacity > 1f)
        maxCapacity = 1f;
      maxCapacity = maxCapacity * 100;
      queueCapacitiesByPartition.add(new PartitionQueueCapacitiesInfo(
          partitionName, capacity, usedCapacity, maxCapacity, absCapacity,
          absUsedCapacity, absMaxCapacity,
          considerAMUsage ? maxAMLimitPercentage : 0f,
          resourceQuotas.getConfiguredMinResource(partitionName),
          resourceQuotas.getConfiguredMaxResource(partitionName),
          resourceQuotas.getEffectiveMinResource(partitionName),
          resourceQuotas.getEffectiveMaxResource(partitionName)));
    }
  }

  public QueueCapacitiesInfo(QueueCapacities capacities,
      QueueResourceQuotas resourceQuotas) {
    this(capacities, resourceQuotas, true);
  }

  public void add(PartitionQueueCapacitiesInfo partitionQueueCapacitiesInfo) {
    queueCapacitiesByPartition.add(partitionQueueCapacitiesInfo);
  }

  public List<PartitionQueueCapacitiesInfo> getQueueCapacitiesByPartition() {
    return queueCapacitiesByPartition;
  }

  public void setQueueCapacitiesByPartition(
      List<PartitionQueueCapacitiesInfo> capacities) {
    this.queueCapacitiesByPartition = capacities;
  }

  public PartitionQueueCapacitiesInfo getPartitionQueueCapacitiesInfo(
      String partitionName) {
    for (PartitionQueueCapacitiesInfo partitionQueueCapacitiesInfo : queueCapacitiesByPartition) {
      if (partitionQueueCapacitiesInfo.getPartitionName()
          .equals(partitionName)) {
        return partitionQueueCapacitiesInfo;
      }
    }
    return new PartitionQueueCapacitiesInfo();
  }
}

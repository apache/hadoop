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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ManagedParentQueue;

import java.io.IOException;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;

/**
 * Utility class for Capacity Scheduler queue PlacementRules.
 */
public final class QueuePlacementRuleUtils {

  public static final String CURRENT_USER_MAPPING = "%user";

  public static final String PRIMARY_GROUP_MAPPING = "%primary_group";

  public static final String SECONDARY_GROUP_MAPPING = "%secondary_group";

  private QueuePlacementRuleUtils() {
  }

  public static void validateQueueMappingUnderParentQueue(
            CSQueue parentQueue, String parentQueueName,
            String leafQueuePath) throws IOException {
    if (parentQueue == null) {
      throw new IOException(
          "mapping contains invalid or non-leaf queue [" + leafQueuePath
              + "] and invalid parent queue [" + parentQueueName + "]");
    } else if (!(parentQueue instanceof ManagedParentQueue)) {
      throw new IOException("mapping contains leaf queue [" + leafQueuePath
          + "] and invalid parent queue which "
          + "does not have auto creation of leaf queues enabled ["
          + parentQueueName + "]");
    } else if (!parentQueue.getQueueShortName().equals(parentQueueName)
        && !parentQueue.getQueuePath().equals(parentQueueName)) {
      throw new IOException(
          "mapping contains invalid or non-leaf queue [" + leafQueuePath
              + "] and invalid parent queue "
              + "which does not match existing leaf queue's parent : ["
              + parentQueueName + "] does not match [ " + parentQueue
              .getQueueShortName() + "]");
    }
  }

  public static QueueMapping validateAndGetAutoCreatedQueueMapping(
      CapacitySchedulerQueueManager queueManager, QueueMapping mapping)
      throws IOException {
    if (mapping.hasParentQueue()) {
      //if parent queue is specified,
      // then it should exist and be an instance of ManagedParentQueue
      validateQueueMappingUnderParentQueue(queueManager.getQueue(
          mapping.getParentQueue()), mapping.getParentQueue(),
          mapping.getFullPath());
      return mapping;
    }

    return null;
  }

  public static QueueMapping validateAndGetQueueMapping(
      CapacitySchedulerQueueManager queueManager, CSQueue queue,
      QueueMapping mapping) throws IOException {
    if (!(queue instanceof LeafQueue)) {
      throw new IOException(
          "mapping contains invalid or non-leaf queue : " +
          mapping.getFullPath());
    }

    if (queue instanceof AutoCreatedLeafQueue && queue
        .getParent() instanceof ManagedParentQueue) {

      QueueMapping newMapping = validateAndGetAutoCreatedQueueMapping(
          queueManager, mapping);
      if (newMapping == null) {
        throw new IOException(
            "mapping contains invalid or non-leaf queue " +
            mapping.getFullPath());
      }
      return newMapping;
    }
    return mapping;
  }

  public static boolean isStaticQueueMapping(QueueMapping mapping) {
    return !mapping.getQueue().contains(CURRENT_USER_MAPPING) && !mapping
        .getQueue().contains(PRIMARY_GROUP_MAPPING)
        && !mapping.getQueue().contains(SECONDARY_GROUP_MAPPING);
  }

  public static ApplicationPlacementContext getPlacementContext(
      QueueMapping mapping, CapacitySchedulerQueueManager queueManager)
      throws IOException {
    return getPlacementContext(mapping, mapping.getQueue(), queueManager);
  }

  public static ApplicationPlacementContext getPlacementContext(
      QueueMapping mapping, String leafQueueName,
      CapacitySchedulerQueueManager queueManager) throws IOException {

    //leafQueue name no longer identifies a queue uniquely checking ambiguity
    if (!mapping.hasParentQueue() && queueManager.isAmbiguous(leafQueueName)) {
      throw new IOException("mapping contains ambiguous leaf queue reference " +
          leafQueueName);
    }

    if (!org.apache.commons.lang3.StringUtils.isEmpty(mapping.getParentQueue())) {
      return new ApplicationPlacementContext(leafQueueName,
          mapping.getParentQueue());
    } else{
      return new ApplicationPlacementContext(leafQueueName);
    }
  }
}

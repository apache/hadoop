/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.helper;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ManagedParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AutoQueueTemplatePropertiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LeafQueueTemplateInfo.ConfItem;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager.NO_LABEL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType.PERCENTAGE;

/**
 * Helper class to describe a queue's type, its creation method and its
 * eligibility of having auto created children.
 *
 * queueType: a queue can be a parent or a leaf.
 *
 * creationMethod: the creation method of the queue. Can be: static,
 * dynamicLegacy or dynamicFlexible. When the legacy way of queue auto-creation
 * (before YARN-10506) is used, a parent can only be static (ManagedParent)
 * and a leaf queue can only be dynamicLegacy (no static child queues are
 * allowed under ManagedParents). When the flexible auto queue creation is used
 * both a parent and a leaf can be either static or dynamicFlexible.
 *
 * autoCreationEligibility: describes whether a queue can have dynamically
 * created children. Can be: off, legacy or flexible. Every leaf will have this
 * field with the value off, as they can't have children. When the legacy way
 * of queue auto-creation (before YARN-10506) is used a ManagedParent will have
 * the legacy value. When the flexible auto queue creation is used a static
 * parent can have the value flexible if it is configured to allow auto queue
 * creation, or off if it is not. A dynamic parent implicitly will have the
 * value flexible, as a dynamically created parent cannot have static children.
 */
public class CapacitySchedulerInfoHelper {
  private static final String PARENT_QUEUE = "parent";
  private static final String LEAF_QUEUE = "leaf";
  private static final String UNKNOWN_QUEUE = "unknown";
  private static final String STATIC_QUEUE = "static";
  private static final String LEGACY_DYNAMIC_QUEUE = "dynamicLegacy";
  private static final String FLEXIBLE_DYNAMIC_QUEUE = "dynamicFlexible";
  private static final String AUTO_CREATION_OFF = "off";
  private static final String AUTO_CREATION_LEGACY = "legacy";
  private static final String AUTO_CREATION_FLEXIBLE = "flexible";

  private CapacitySchedulerInfoHelper() {}

  public static String getMode(CSQueue queue) {
    if (((AbstractCSQueue) queue).getQueueContext().getConfiguration().isLegacyQueueMode()) {
      if (queue.getCapacityConfigType() ==
              AbstractCSQueue.CapacityConfigType.ABSOLUTE_RESOURCE) {
        return "absolute";
      } else if (queue.getCapacityConfigType() ==
              AbstractCSQueue.CapacityConfigType.PERCENTAGE) {
        float weight = queue.getQueueCapacities().getWeight();
        if (weight == -1) {
          //-1 indicates we are not in weight mode
          return "percentage";
        } else {
          return "weight";
        }
      }
    } else {
      final Set<QueueCapacityVector.ResourceUnitCapacityType> definedCapacityTypes =
              queue.getConfiguredCapacityVector(NO_LABEL).getDefinedCapacityTypes();
      if (definedCapacityTypes.size() == 1) {
        QueueCapacityVector.ResourceUnitCapacityType next = definedCapacityTypes.iterator().next();
        if (Objects.requireNonNull(next) == PERCENTAGE) {
          return "percentage";
        } else if (next == QueueCapacityVector.ResourceUnitCapacityType.ABSOLUTE) {
          return "absolute";
        } else if (next == QueueCapacityVector.ResourceUnitCapacityType.WEIGHT) {
          return "weight";
        }
      } else if (definedCapacityTypes.size() > 1) {
        return "mixed";
      }
    }

    return "unknown";
  }

  public static String getQueueType(CSQueue queue) {
    if (queue instanceof AbstractLeafQueue) {
      return LEAF_QUEUE;
    } else if (queue instanceof AbstractParentQueue) {
      return PARENT_QUEUE;
    }
    return UNKNOWN_QUEUE;
  }

  public static String getCreationMethod(CSQueue queue) {
    if (queue instanceof AutoCreatedLeafQueue) {
      return LEGACY_DYNAMIC_QUEUE;
    } else if (((AbstractCSQueue)queue).isDynamicQueue()) {
      return FLEXIBLE_DYNAMIC_QUEUE;
    } else {
      return STATIC_QUEUE;
    }
  }

  public static String getAutoCreationEligibility(CSQueue queue) {
    if (queue instanceof ManagedParentQueue) {
      return AUTO_CREATION_LEGACY;
    } else if (queue instanceof ParentQueue &&
        ((ParentQueue)queue).isEligibleForAutoQueueCreation()) {
      return AUTO_CREATION_FLEXIBLE;
    } else {
      return AUTO_CREATION_OFF;
    }
  }

  public static AutoQueueTemplatePropertiesInfo getAutoCreatedTemplate(
      Map<String, String> templateProperties) {
    AutoQueueTemplatePropertiesInfo propertiesInfo =
        new AutoQueueTemplatePropertiesInfo();
    for (Map.Entry<String, String> e :
        templateProperties.entrySet()) {
      propertiesInfo.add(new ConfItem(e.getKey(), e.getValue()));
    }

    return propertiesInfo;
  }
}

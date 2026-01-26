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

import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;

public final class QueuePrefixes {

  private QueuePrefixes() {
  }

  public static String getQueuePrefix(QueuePath queuePath) {
    return PREFIX + queuePath.getFullPath() + DOT;
  }

  public static String getNodeLabelPrefix(QueuePath queuePath, String label) {
    if (label.equals(CommonNodeLabelsManager.NO_LABEL)) {
      return getQueuePrefix(queuePath);
    }
    return getQueuePrefix(queuePath) + ACCESSIBLE_NODE_LABELS + DOT + label + DOT;
  }

  /**
   * Get the auto created leaf queue's template configuration prefix.
   * Leaf queue's template capacities are configured at the parent queue.
   *
   * @param queuePath parent queue's path
   * @return Config prefix for leaf queue template configurations
   */
  public static String getAutoCreatedQueueTemplateConfPrefix(QueuePath queuePath) {
    return queuePath.getFullPath() + DOT + AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX;
  }

  public static QueuePath getAutoCreatedQueueObjectTemplateConfPrefix(QueuePath queuePath) {
    return new QueuePath(getAutoCreatedQueueTemplateConfPrefix(queuePath));
  }
}

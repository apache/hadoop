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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.queuemanagement;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class DeactivatedLeafQueuesByLabel {
  private String nodeLabel;
  private final Map<String, QueueCapacities> deactivatedLeafQueues;
  private final float parentAbsoluteCapacity;
  private final float leafQueueTemplateAbsoluteCapacity;

  public DeactivatedLeafQueuesByLabel(
      String nodeLabel, Map<String, QueueCapacities> deactivatedLeafQueues,
      float parentAbsoluteCapacity,
      float leafQueueTemplateAbsoluteCapacity) {
    this.nodeLabel = nodeLabel;
    this.deactivatedLeafQueues = deactivatedLeafQueues;
    this.parentAbsoluteCapacity = parentAbsoluteCapacity;
    this.leafQueueTemplateAbsoluteCapacity = leafQueueTemplateAbsoluteCapacity;
  }

  float getTotalDeactivatedCapacity() {
    float deactivatedCapacity = 0;
    for (Iterator<Map.Entry<String, QueueCapacities>> iterator =
         deactivatedLeafQueues.entrySet().iterator(); iterator.hasNext(); ) {
      Map.Entry<String, QueueCapacities> deactivatedQueueCapacity = iterator.next();
      deactivatedCapacity += deactivatedQueueCapacity.getValue().getAbsoluteCapacity(nodeLabel);
    }
    return deactivatedCapacity;
  }

  public float getParentAbsoluteCapacity() {
    return parentAbsoluteCapacity;
  }

  public float getLeafQueueTemplateAbsoluteCapacity() {
    return leafQueueTemplateAbsoluteCapacity;
  }
  
  public Set<String> getQueues() {
    return deactivatedLeafQueues.keySet();
  }
}

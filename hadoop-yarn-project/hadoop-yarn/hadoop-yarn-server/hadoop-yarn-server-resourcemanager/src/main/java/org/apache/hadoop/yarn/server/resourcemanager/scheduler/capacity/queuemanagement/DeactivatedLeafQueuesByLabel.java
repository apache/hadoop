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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueueUtils.EPSILON;

public class DeactivatedLeafQueuesByLabel {
  private String parentQueuePath;
  private String nodeLabel;
  private Map<String, QueueCapacities> deactivatedLeafQueues;
  private float sumOfChildQueueActivatedCapacity;
  private float parentAbsoluteCapacity;
  private float leafQueueTemplateAbsoluteCapacity;
  private float availableCapacity;
  private float totalDeactivatedCapacity;

  @VisibleForTesting
  public DeactivatedLeafQueuesByLabel() {}

  public DeactivatedLeafQueuesByLabel(
      Map<String, QueueCapacities> deactivatedLeafQueues,
      String parentQueuePath,
      String nodeLabel,
      float sumOfChildQueueActivatedCapacity,
      float parentAbsoluteCapacity,
      float leafQueueTemplateAbsoluteCapacity) {
    this.parentQueuePath = parentQueuePath;
    this.nodeLabel = nodeLabel;
    this.deactivatedLeafQueues = deactivatedLeafQueues;
    this.sumOfChildQueueActivatedCapacity = sumOfChildQueueActivatedCapacity;
    this.parentAbsoluteCapacity = parentAbsoluteCapacity;
    this.leafQueueTemplateAbsoluteCapacity = leafQueueTemplateAbsoluteCapacity;

    this.totalDeactivatedCapacity = getTotalDeactivatedCapacity();
    this.availableCapacity = parentAbsoluteCapacity - sumOfChildQueueActivatedCapacity +
        this.totalDeactivatedCapacity + EPSILON;
  }

  float getTotalDeactivatedCapacity() {
    float deactivatedCapacity = 0;
    for (Map.Entry<String, QueueCapacities> deactivatedQueueCapacity :
        deactivatedLeafQueues.entrySet()) {
      deactivatedCapacity += deactivatedQueueCapacity.getValue().getAbsoluteCapacity(nodeLabel);
    }
    return deactivatedCapacity;
  }

  public Set<String> getQueues() {
    return deactivatedLeafQueues.keySet();
  }

  public void printToDebug(Logger logger) {
    if (logger.isDebugEnabled()) {
      logger.debug("Parent queue = {}, nodeLabel = {}, absCapacity = {}, " +
              "leafQueueAbsoluteCapacity = {}, deactivatedCapacity = {}, " +
              "absChildActivatedCapacity = {}, availableCapacity = {}",
          parentQueuePath, nodeLabel, parentAbsoluteCapacity,
          leafQueueTemplateAbsoluteCapacity, getTotalDeactivatedCapacity(),
          sumOfChildQueueActivatedCapacity, availableCapacity);
    }
  }

  @VisibleForTesting
  public int getMaxLeavesToBeActivated(int numPendingApps) {
    float childQueueAbsoluteCapacity = leafQueueTemplateAbsoluteCapacity;
    if (childQueueAbsoluteCapacity > 0) {
      int numLeafQueuesNeeded = (int) Math.floor(availableCapacity / childQueueAbsoluteCapacity);
      return Math.min(numLeafQueuesNeeded, numPendingApps);
    }
    return 0;
  }

  public boolean canActivateLeafQueues() {
    return availableCapacity >= leafQueueTemplateAbsoluteCapacity;
  }

  @VisibleForTesting
  public void setAvailableCapacity(float availableCapacity) {
    this.availableCapacity = availableCapacity;
  }

  @VisibleForTesting
  public void setLeafQueueTemplateAbsoluteCapacity(float leafQueueTemplateAbsoluteCapacity) {
    this.leafQueueTemplateAbsoluteCapacity = leafQueueTemplateAbsoluteCapacity;
  }
}

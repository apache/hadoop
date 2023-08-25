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

import java.io.IOException;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
@Private
@Evolving
public class ParentQueue extends AbstractParentQueue {

  private static final Logger LOG =
      LoggerFactory.getLogger(ParentQueue.class);

  public ParentQueue(CapacitySchedulerQueueContext queueContext,
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    this(queueContext, queueName, parent, old, false);
  }

  public ParentQueue(CapacitySchedulerQueueContext queueContext,
      String queueName, CSQueue parent, CSQueue old, boolean isDynamic)
      throws IOException {
    super(queueContext, queueName, parent, old, isDynamic);
    super.setupQueueConfigs(queueContext.getClusterResource());
  }

  public ParentQueue addDynamicParentQueue(String queuePath)
      throws SchedulerDynamicEditException {
    return (ParentQueue) addDynamicChildQueue(queuePath, false);
  }

  public LeafQueue addDynamicLeafQueue(String queuePath)
      throws SchedulerDynamicEditException {
    return (LeafQueue) addDynamicChildQueue(queuePath, true);
  }

  // New method to add child queue
  private CSQueue addDynamicChildQueue(String childQueuePath, boolean isLeaf)
      throws SchedulerDynamicEditException {
    writeLock.lock();
    try {
      // Check if queue exists, if queue exists, write a warning message (this
      // should not happen, since it will be handled before calling this method)
      // , but we will move on.
      CSQueue queue =
          queueContext.getQueueManager().getQueueByFullName(
              childQueuePath);
      if (queue != null) {
        LOG.warn(
            "This should not happen, trying to create queue=" + childQueuePath
                + ", however the queue already exists");
        return queue;
      }

      // Check if the max queue limit is exceeded.
      int maxQueues = queueContext.getConfiguration().
          getAutoCreatedQueuesV2MaxChildQueuesLimit(getQueuePath());
      if (childQueues.size() >= maxQueues) {
        throw new SchedulerDynamicEditException(
            "Cannot auto create queue " + childQueuePath + ". Max Child "
                + "Queue limit exceeded which is configured as: " + maxQueues
                + " and number of child queues is: " + childQueues.size());
      }

      // First, check if we allow creation or not
      boolean weightsAreUsed = false;
      try {
        weightsAreUsed = getCapacityConfigurationTypeForQueues(childQueues)
            == QueueCapacityType.WEIGHT;
      } catch (IOException e) {
        LOG.warn("Caught Exception during auto queue creation", e);
      }
      if (!weightsAreUsed && queueContext.getConfiguration().isLegacyQueueMode()) {
        throw new SchedulerDynamicEditException(
            "Trying to create new queue=" + childQueuePath
                + " but not all the queues under parent=" + this.getQueuePath()
                + " are using weight-based capacity. Failed to created queue");
      }

      CSQueue newQueue = createNewQueue(childQueuePath, isLeaf);
      this.childQueues.add(newQueue);
      updateLastSubmittedTimeStamp();

      // Call updateClusterResource.
      // Which will deal with all effectiveMin/MaxResource
      // Calculation
      this.updateClusterResource(queueContext.getClusterResource(),
          new ResourceLimits(queueContext.getClusterResource()));

      return newQueue;
    } finally {
      writeLock.unlock();
    }
  }
}

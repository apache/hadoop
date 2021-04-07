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

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.placement.ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Manages the validation and the creation of a Capacity Scheduler
 * queue at runtime.
 */
public class CapacitySchedulerAutoQueueHandler {
  private final CapacitySchedulerQueueManager queueManager;
  private static final int MAXIMUM_DEPTH_ALLOWED = 2;

  public CapacitySchedulerAutoQueueHandler(
      CapacitySchedulerQueueManager queueManager) {
    this.queueManager = queueManager;
  }

  /**
   * Creates a LeafQueue and its upper hierarchy given a path. A parent is
   * eligible for creation if either the placement context creation flags are
   * set, or the auto queue creation is enabled for the first static parent in
   * the hierarchy.
   *
   * @param queue the application placement information of the queue
   * @return LeafQueue part of a given queue path
   * @throws YarnException if the given path is not eligible to be auto created
   */
  public LeafQueue autoCreateQueue(ApplicationPlacementContext queue)
      throws YarnException {
    ApplicationPlacementContext parentContext =
        CSQueueUtils.extractQueuePath(queue.getParentQueue());
    List<ApplicationPlacementContext> parentsToCreate = new ArrayList<>();

    ApplicationPlacementContext queueCandidateContext = parentContext;
    CSQueue firstExistingQueue = getQueue(
        queueCandidateContext.getFullQueuePath());

    while (firstExistingQueue == null) {
      parentsToCreate.add(queueCandidateContext);
      queueCandidateContext = CSQueueUtils.extractQueuePath(
          queueCandidateContext.getParentQueue());
      firstExistingQueue = getQueue(
          queueCandidateContext.getFullQueuePath());
    }

    CSQueue firstExistingStaticQueue = firstExistingQueue;
    // Include the LeafQueue in the distance
    int firstStaticParentDistance = parentsToCreate.size() + 1;

    while(isNonStaticParent(firstExistingStaticQueue)) {
      queueCandidateContext = CSQueueUtils.extractQueuePath(
          queueCandidateContext.getParentQueue());
      firstExistingStaticQueue = getQueue(
          queueCandidateContext.getFullQueuePath());
      ++firstStaticParentDistance;
    }

    // Reverse the collection to to represent the hierarchy to be created
    // from highest to lowest level
    Collections.reverse(parentsToCreate);

    if (!(firstExistingQueue instanceof ParentQueue)) {
      throw new SchedulerDynamicEditException(
          "Could not auto create hierarchy of "
              + queue.getFullQueuePath() + ". Queue "
              + firstExistingQueue.getQueuePath() +
              " is not a ParentQueue."
      );
    }
    ParentQueue existingParentQueue = (ParentQueue) firstExistingQueue;
    int depthLimit = extractDepthLimit(existingParentQueue);

    if (depthLimit == 0) {
      throw new SchedulerDynamicEditException("Auto creation of queue " +
          queue.getFullQueuePath() + " is not enabled under parent "
          + existingParentQueue.getQueuePath());
    }

    if (firstStaticParentDistance > depthLimit) {
      throw new SchedulerDynamicEditException(
          "Could not auto create queue " + queue.getFullQueuePath()
              + ". The distance of the LeafQueue from the first static " +
              "ParentQueue is" + firstStaticParentDistance + ", which is " +
              "above the limit.");
    }

    for (ApplicationPlacementContext current : parentsToCreate) {
      existingParentQueue = existingParentQueue
          .addDynamicParentQueue(current.getFullQueuePath());
      queueManager.addQueue(existingParentQueue.getQueuePath(),
          existingParentQueue);
    }

    LeafQueue leafQueue = existingParentQueue.addDynamicLeafQueue(
        queue.getFullQueuePath());
    queueManager.addQueue(leafQueue.getQueuePath(), leafQueue);

    return leafQueue;
  }

  private int extractDepthLimit(ParentQueue parentQueue) {
    if (parentQueue.isEligibleForAutoQueueCreation()) {
      return MAXIMUM_DEPTH_ALLOWED;
    } else {
      return 0;
    }
  }

  private CSQueue getQueue(String queue) {
    return queue != null ? queueManager.getQueue(queue) : null;
  }

  private boolean isNonStaticParent(CSQueue queue) {
    return (!(queue instanceof AbstractCSQueue)
        || ((AbstractCSQueue) queue).isDynamicQueue());
  }
}

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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This represents a dynamic queue managed by the {@link ReservationSystem}.
 * From the user perspective this is equivalent to a LeafQueue that respect
 * reservations, but functionality wise is a sub-class of ParentQueue
 *
 */
public class PlanQueue extends AbstractManagedParentQueue {

  private static final Logger LOG = LoggerFactory.getLogger(PlanQueue.class);

  private boolean showReservationsAsQueues;

  public PlanQueue(CapacitySchedulerContext cs, String queueName,
      CSQueue parent, CSQueue old) throws IOException {
    super(cs, queueName, parent, old);
    this.leafQueueTemplate = initializeLeafQueueConfigs(getQueuePath()).build();

    StringBuffer queueInfo = new StringBuffer();
    queueInfo.append("Created Plan Queue: ").append(queueName).append(
        "]\nwith capacity: [").append(super.getCapacity()).append(
        "]\nwith max capacity: [").append(super.getMaximumCapacity()).append(
        "\nwith max apps: [").append(leafQueueTemplate.getMaxApps()).append(
        "]\nwith max apps per user: [").append(
        leafQueueTemplate.getMaxAppsPerUser()).append("]\nwith user limit: [")
        .append(leafQueueTemplate.getUserLimit()).append(
        "]\nwith user limit factor: [").append(
        leafQueueTemplate.getUserLimitFactor()).append("].");
    LOG.info(queueInfo.toString());
  }

  @Override
  public void reinitialize(CSQueue newlyParsedQueue, Resource clusterResource)
      throws IOException {
    validate(newlyParsedQueue);
    super.reinitialize(newlyParsedQueue, clusterResource);
    this.leafQueueTemplate = initializeLeafQueueConfigs(getQueuePath()).build();
  }

  @Override
  protected AutoCreatedLeafQueueTemplate.Builder initializeLeafQueueConfigs
      (String queuePath) {
    AutoCreatedLeafQueueTemplate.Builder leafQueueTemplate = super
        .initializeLeafQueueConfigs
        (queuePath);
    showReservationsAsQueues = csContext.getConfiguration()
        .getShowReservationAsQueues(queuePath);
    return leafQueueTemplate;
  }

  protected void validate(final CSQueue newlyParsedQueue) throws IOException {
    // Sanity check
    if (!(newlyParsedQueue instanceof PlanQueue) || !newlyParsedQueue
        .getQueuePath().equals(getQueuePath())) {
      throw new IOException(
          "Trying to reinitialize " + getQueuePath() + " from "
              + newlyParsedQueue.getQueuePath());
    }

    PlanQueue newlyParsedParentQueue = (PlanQueue) newlyParsedQueue;

    if (newlyParsedParentQueue.getChildQueues().size() != 1) {
      throw new IOException(
          "Reservable Queue should not have sub-queues in the"
              + "configuration expect the default reservation queue");
    }
  }

  /**
   * Determine whether to hide/show the ReservationQueues
   */
  public boolean showReservationsAsQueues() {
    return showReservationsAsQueues;
  }
}

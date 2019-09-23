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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This represents a dynamic {@link LeafQueue} managed by the
 * {@link ReservationSystem}
 *
 */
public class ReservationQueue extends AbstractAutoCreatedLeafQueue {

  private static final Logger LOG = LoggerFactory
      .getLogger(ReservationQueue.class);

  private PlanQueue parent;

  public ReservationQueue(CapacitySchedulerContext cs, String queueName,
      PlanQueue parent) throws IOException {
    super(cs, queueName, parent, null);
    // the following parameters are common to all reservation in the plan
    updateQuotas(parent.getUserLimitForReservation(),
        parent.getUserLimitFactor(),
        parent.getMaxApplicationsForReservations(),
        parent.getMaxApplicationsPerUserForReservation());
    this.parent = parent;
  }

  @Override
  public void reinitialize(CSQueue newlyParsedQueue,
      Resource clusterResource) throws IOException {
    writeLock.lock();
    try {
      // Sanity check
      if (!(newlyParsedQueue instanceof ReservationQueue) || !newlyParsedQueue
          .getQueuePath().equals(getQueuePath())) {
        throw new IOException(
            "Trying to reinitialize " + getQueuePath() + " from "
                + newlyParsedQueue.getQueuePath());
      }
      super.reinitialize(newlyParsedQueue, clusterResource);
      CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
          this, labelManager, null);

      updateQuotas(parent.getUserLimitForReservation(),
          parent.getUserLimitFactor(),
          parent.getMaxApplicationsForReservations(),
          parent.getMaxApplicationsPerUserForReservation());
    } finally {
      writeLock.unlock();
    }
  }

  private void updateQuotas(int userLimit, float userLimitFactor,
      int maxAppsForReservation, int maxAppsPerUserForReservation) {
    setUserLimit(userLimit);
    setUserLimitFactor(userLimitFactor);
    setMaxApplications(maxAppsForReservation);
    maxApplicationsPerUser = maxAppsPerUserForReservation;
  }

  @Override
  protected void setupConfigurableCapacities(CapacitySchedulerConfiguration
      configuration) {
    super.setupConfigurableCapacities(queueCapacities);
  }
}
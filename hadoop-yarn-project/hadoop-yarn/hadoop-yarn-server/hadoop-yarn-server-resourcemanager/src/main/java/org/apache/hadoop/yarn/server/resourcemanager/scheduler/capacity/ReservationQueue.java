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
public class ReservationQueue extends LeafQueue {

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
  public synchronized void reinitialize(CSQueue newlyParsedQueue,
      Resource clusterResource) throws IOException {
    // Sanity check
    if (!(newlyParsedQueue instanceof ReservationQueue)
        || !newlyParsedQueue.getQueuePath().equals(getQueuePath())) {
      throw new IOException("Trying to reinitialize " + getQueuePath()
          + " from " + newlyParsedQueue.getQueuePath());
    }
    super.reinitialize(newlyParsedQueue, clusterResource);
    CSQueueUtils.updateQueueStatistics(
        parent.schedulerContext.getResourceCalculator(), newlyParsedQueue,
        parent, parent.schedulerContext.getClusterResource(),
        parent.schedulerContext.getMinimumResourceCapability());
    updateQuotas(parent.getUserLimitForReservation(),
        parent.getUserLimitFactor(),
        parent.getMaxApplicationsForReservations(),
        parent.getMaxApplicationsPerUserForReservation());
  }

  /**
   * This methods to change capacity for a queue and adjusts its
   * absoluteCapacity
   * 
   * @param entitlement the new entitlement for the queue (capacity,
   *          maxCapacity, etc..)
   * @throws SchedulerDynamicEditException
   */
  public synchronized void setEntitlement(QueueEntitlement entitlement)
      throws SchedulerDynamicEditException {
    float capacity = entitlement.getCapacity();
    if (capacity < 0 || capacity > 1.0f) {
      throw new SchedulerDynamicEditException(
          "Capacity demand is not in the [0,1] range: " + capacity);
    }
    setCapacity(capacity);
    setAbsoluteCapacity(getParent().getAbsoluteCapacity() * getCapacity());
    // note: we currently set maxCapacity to capacity
    // this might be revised later
    setMaxCapacity(entitlement.getMaxCapacity());
    if (LOG.isDebugEnabled()) {
      LOG.debug("successfully changed to " + capacity + " for queue "
          + this.getQueueName());
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
  protected void setupConfigurableCapacities() {
    CSQueueUtils.updateAndCheckCapacitiesByLabel(getQueuePath(),
        queueCapacities, parent == null ? null : parent.getQueueCapacities());
  }
}

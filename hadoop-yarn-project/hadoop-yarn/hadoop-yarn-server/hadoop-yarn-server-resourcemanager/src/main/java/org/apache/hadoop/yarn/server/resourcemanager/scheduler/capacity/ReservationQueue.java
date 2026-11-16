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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This represents a dynamic {@link LeafQueue} managed by the
 * {@link ReservationSystem}
 *
 */
public class ReservationQueue extends AbstractAutoCreatedLeafQueue {
  private static final Logger LOG =
      LoggerFactory.getLogger(ReservationQueue.class);

  private PlanQueue parent;

  public ReservationQueue(CapacitySchedulerQueueContext queueContext, String queueName,
      PlanQueue parent) throws IOException {
    super(queueContext, queueName, parent, null);
    super.setupQueueConfigs(queueContext.getClusterResource());

    // the following parameters are common to all reservation in the plan
    updateQuotas(parent);
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

      updateQuotas(parent);
    } finally {
      writeLock.unlock();
    }
  }

  public void initializeEntitlements() throws SchedulerDynamicEditException {
    setEntitlement(new QueueEntitlement(1.0f, 1.0f));
  }

  private void updateQuotas(PlanQueue plan) {
    // The resolved user limits of a reservation queue are the plan's. The
    // plan's application limits scale with its absolute capacity, so they
    // are taken from the plan queue.
    ResolvedQueueConfig resolved = getResolvedQueueConfig();
    setUserLimit(resolved.get(QueueProperties.USER_LIMIT).getValue());
    setUserLimitFactor(
        resolved.get(QueueProperties.USER_LIMIT_FACTOR).getValue());
    setMaxApplications(plan.getMaxApplicationsForReservations());
    maxApplicationsPerUser = plan.getMaxApplicationsPerUserForReservation();
  }

  // The plan's user limits replace the ones the queue is set up with, which
  // are read for the queue's own path first, as the queue setup always did;
  // its resolved configuration holds the plan's values.

  @Override
  protected float getConfiguredUserLimit(ResolvedQueueConfig resolved) {
    return queueContext.getConfiguration().getUserLimit(getQueuePathObject());
  }

  @Override
  protected float getConfiguredUserLimitFactor(ResolvedQueueConfig resolved) {
    return queueContext.getConfiguration().getUserLimitFactor(
        getQueuePathObject());
  }

  @Override
  protected void setupConfigurableCapacities() {
    super.updateAbsoluteCapacities();
  }
}
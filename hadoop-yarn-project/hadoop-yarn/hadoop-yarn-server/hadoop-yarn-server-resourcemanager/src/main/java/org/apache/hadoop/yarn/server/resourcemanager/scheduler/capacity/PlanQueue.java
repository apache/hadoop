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
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationConstants;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
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

  private int maxAppsForReservation;
  private int maxAppsPerUserForReservation;
  private float userLimit;
  private float userLimitFactor;
  private boolean showReservationsAsQueues;

  public PlanQueue(CapacitySchedulerQueueContext queueContext, String queueName,
      CSQueue parent, CSQueue old) throws IOException {
    super(queueContext, queueName, parent, old);
    super.setupQueueConfigs(queueContext.getClusterResource());
    updateAbsoluteCapacities();

    // Set the reservation queue attributes for the Plan
    CapacitySchedulerConfiguration conf = queueContext.getConfiguration();
    String queuePath = super.getQueuePath();
    int maxAppsForReservation = conf.getMaximumApplicationsPerQueue(queuePath);
    showReservationsAsQueues = conf.getShowReservationAsQueues(queuePath);
    if (maxAppsForReservation < 0) {
      maxAppsForReservation =
          (int) (CapacitySchedulerConfiguration.
              DEFAULT_MAXIMUM_SYSTEM_APPLICATIIONS * super
              .getAbsoluteCapacity());
    }
    float configuredUserLimit = conf.getUserLimit(queuePath);
    float configuredUserLimitFactor = conf.getUserLimitFactor(queuePath);
    int configuredMaxAppsPerUserForReservation =
        (int) (maxAppsForReservation * (configuredUserLimit / 100.0f) *
            configuredUserLimitFactor);
    if (configuredUserLimitFactor == -1) {
      configuredMaxAppsPerUserForReservation = maxAppsForReservation;
    }
    updateQuotas(configuredUserLimit, configuredUserLimitFactor,
        maxAppsForReservation, configuredMaxAppsPerUserForReservation);

    StringBuffer queueInfo = new StringBuffer();
    queueInfo.append("Created Plan Queue: ").append(queueName)
        .append("\nwith capacity: [").append(super.getCapacity())
        .append("]\nwith max capacity: [").append(super.getMaximumCapacity())
        .append("\nwith max reservation apps: [").append(maxAppsForReservation)
        .append("]\nwith max reservation apps per user: [")
        .append(configuredMaxAppsPerUserForReservation)
        .append("]\nwith user limit: [")
        .append(configuredUserLimit).append("]\nwith user limit factor: [")
        .append(configuredUserLimitFactor).append("].");
    LOG.info(queueInfo.toString());
  }

  @Override
  public void reinitialize(CSQueue newlyParsedQueue,
      Resource clusterResource) throws IOException {
    writeLock.lock();
    try {
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

      // Set new configs
      setupQueueConfigs(clusterResource);

      updateQuotas(newlyParsedParentQueue.userLimit,
          newlyParsedParentQueue.userLimitFactor,
          newlyParsedParentQueue.maxAppsForReservation,
          newlyParsedParentQueue.maxAppsPerUserForReservation);

      // run reinitialize on each existing queue, to trigger absolute cap
      // recomputations
      for (CSQueue res : this.getChildQueues()) {
        res.reinitialize(res, clusterResource);
      }
      showReservationsAsQueues =
          newlyParsedParentQueue.showReservationsAsQueues;
    } finally {
      writeLock.unlock();
    }
  }

  public ReservationQueue initializeDefaultInternalQueue() throws IOException {
    //initializing the "internal" default queue, for SLS compatibility
    String defReservationId =
        getQueueName() + ReservationConstants.DEFAULT_QUEUE_SUFFIX;

    ReservationQueue resQueue = new ReservationQueue(queueContext,
        defReservationId, this);
    try {
      resQueue.initializeEntitlements();
    } catch (SchedulerDynamicEditException e) {
      throw new IllegalStateException(e);
    }
    childQueues.add(resQueue);

    return resQueue;
  }

  private void updateQuotas(float newUserLimit, float newUserLimitFactor,
      int newMaxAppsForReservation, int newMaxAppsPerUserForReservation) {
    this.userLimit = newUserLimit;
    this.userLimitFactor = newUserLimitFactor;
    this.maxAppsForReservation = newMaxAppsForReservation;
    this.maxAppsPerUserForReservation = newMaxAppsPerUserForReservation;
  }

  /**
   * Number of maximum applications for each of the reservations in this Plan.
   *
   * @return maxAppsForreservation
   */
  public int getMaxApplicationsForReservations() {
    return maxAppsForReservation;
  }

  /**
   * Number of maximum applications per user for each of the reservations in
   * this Plan.
   *
   * @return maxAppsPerUserForreservation
   */
  public int getMaxApplicationsPerUserForReservation() {
    return maxAppsPerUserForReservation;
  }

  /**
   * User limit value for each of the reservations in this Plan.
   *
   * @return userLimit
   */
  public float getUserLimitForReservation() {
    return userLimit;
  }

  /**
   * User limit factor value for each of the reservations in this Plan.
   *
   * @return userLimitFactor
   */
  public float getUserLimitFactor() {
    return userLimitFactor;
  }

  /**
   * Determine whether to hide/show the ReservationQueues.
   * @return true, show ReservationQueues; false, hide ReservationQueues.
   */
  public boolean showReservationsAsQueues() {
    return showReservationsAsQueues;
  }
}
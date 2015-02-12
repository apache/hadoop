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
import java.util.Iterator;

import org.apache.hadoop.yarn.api.records.Resource;
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
public class PlanQueue extends ParentQueue {

  private static final Logger LOG = LoggerFactory.getLogger(PlanQueue.class);

  private int maxAppsForReservation;
  private int maxAppsPerUserForReservation;
  private int userLimit;
  private float userLimitFactor;
  protected CapacitySchedulerContext schedulerContext;
  private boolean showReservationsAsQueues;

  public PlanQueue(CapacitySchedulerContext cs, String queueName,
      CSQueue parent, CSQueue old) throws IOException {
    super(cs, queueName, parent, old);

    this.schedulerContext = cs;
    // Set the reservation queue attributes for the Plan
    CapacitySchedulerConfiguration conf = cs.getConfiguration();
    String queuePath = super.getQueuePath();
    int maxAppsForReservation = conf.getMaximumApplicationsPerQueue(queuePath);
    showReservationsAsQueues = conf.getShowReservationAsQueues(queuePath);
    if (maxAppsForReservation < 0) {
      maxAppsForReservation =
          (int) (CapacitySchedulerConfiguration.DEFAULT_MAXIMUM_SYSTEM_APPLICATIIONS * super
              .getAbsoluteCapacity());
    }
    int userLimit = conf.getUserLimit(queuePath);
    float userLimitFactor = conf.getUserLimitFactor(queuePath);
    int maxAppsPerUserForReservation =
        (int) (maxAppsForReservation * (userLimit / 100.0f) * userLimitFactor);
    updateQuotas(userLimit, userLimitFactor, maxAppsForReservation,
        maxAppsPerUserForReservation);

    StringBuffer queueInfo = new StringBuffer();
    queueInfo.append("Created Plan Queue: ").append(queueName)
        .append("\nwith capacity: [").append(super.getCapacity())
        .append("]\nwith max capacity: [").append(super.getMaximumCapacity())
        .append("\nwith max reservation apps: [").append(maxAppsForReservation)
        .append("]\nwith max reservation apps per user: [")
        .append(maxAppsPerUserForReservation).append("]\nwith user limit: [")
        .append(userLimit).append("]\nwith user limit factor: [")
        .append(userLimitFactor).append("].");
    LOG.info(queueInfo.toString());
  }

  @Override
  public synchronized void reinitialize(CSQueue newlyParsedQueue,
      Resource clusterResource) throws IOException {
    // Sanity check
    if (!(newlyParsedQueue instanceof PlanQueue)
        || !newlyParsedQueue.getQueuePath().equals(getQueuePath())) {
      throw new IOException("Trying to reinitialize " + getQueuePath()
          + " from " + newlyParsedQueue.getQueuePath());
    }

    PlanQueue newlyParsedParentQueue = (PlanQueue) newlyParsedQueue;

    if (newlyParsedParentQueue.getChildQueues().size() > 0) {
      throw new IOException(
          "Reservable Queue should not have sub-queues in the"
              + "configuration");
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
    showReservationsAsQueues = newlyParsedParentQueue.showReservationsAsQueues;
  }

  synchronized void addChildQueue(CSQueue newQueue)
      throws SchedulerDynamicEditException {
    if (newQueue.getCapacity() > 0) {
      throw new SchedulerDynamicEditException("Queue " + newQueue
          + " being added has non zero capacity.");
    }
    boolean added = this.childQueues.add(newQueue);
    if (LOG.isDebugEnabled()) {
      LOG.debug("updateChildQueues (action: add queue): " + added + " "
          + getChildQueuesToPrint());
    }
  }

  synchronized void removeChildQueue(CSQueue remQueue)
      throws SchedulerDynamicEditException {
    if (remQueue.getCapacity() > 0) {
      throw new SchedulerDynamicEditException("Queue " + remQueue
          + " being removed has non zero capacity.");
    }
    Iterator<CSQueue> qiter = childQueues.iterator();
    while (qiter.hasNext()) {
      CSQueue cs = qiter.next();
      if (cs.equals(remQueue)) {
        qiter.remove();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Removed child queue: {}", cs.getQueueName());
        }
      }
    }
  }

  protected synchronized float sumOfChildCapacities() {
    float ret = 0;
    for (CSQueue l : childQueues) {
      ret += l.getCapacity();
    }
    return ret;
  }

  private void updateQuotas(int userLimit, float userLimitFactor,
      int maxAppsForReservation, int maxAppsPerUserForReservation) {
    this.userLimit = userLimit;
    this.userLimitFactor = userLimitFactor;
    this.maxAppsForReservation = maxAppsForReservation;
    this.maxAppsPerUserForReservation = maxAppsPerUserForReservation;
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
  public int getUserLimitForReservation() {
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
   * Determine whether to hide/show the ReservationQueues
   */
  public boolean showReservationsAsQueues() {
    return showReservationsAsQueues;
  }
}

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

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ReservationACL;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;

import java.util.Map;

public abstract class ReservationSchedulerConfiguration extends Configuration {

  @InterfaceAudience.Private
  public static final long DEFAULT_RESERVATION_WINDOW = 24*60*60*1000; // 1 day in msec

  @InterfaceAudience.Private
  public static final String DEFAULT_RESERVATION_ADMISSION_POLICY =
      "org.apache.hadoop.yarn.server.resourcemanager.reservation.CapacityOverTimePolicy";

  @InterfaceAudience.Private
  public static final String DEFAULT_RESERVATION_AGENT_NAME =
      "org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.AlignedPlannerWithGreedy";

  @InterfaceAudience.Private
  public static final String DEFAULT_RESERVATION_PLANNER_NAME =
      "org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.SimpleCapacityReplanner";

  @InterfaceAudience.Private
  public static final boolean DEFAULT_RESERVATION_MOVE_ON_EXPIRY = true;

  // default to 1h lookahead enforcement
  @InterfaceAudience.Private
  public static final long DEFAULT_RESERVATION_ENFORCEMENT_WINDOW = 60*60*1000;
  // 1 hour

  @InterfaceAudience.Private
  public static final boolean DEFAULT_SHOW_RESERVATIONS_AS_QUEUES = false;

  @InterfaceAudience.Private
  public static final float DEFAULT_CAPACITY_OVER_TIME_MULTIPLIER = 1;

  public ReservationSchedulerConfiguration() { super(); }

  public ReservationSchedulerConfiguration(
      Configuration configuration) {
    super(configuration);
  }

  /**
   * Checks if the queue participates in reservation based scheduling
   * @param queue name of the queue
   * @return true if the queue participates in reservation based scheduling
   */
  public abstract boolean isReservable(String queue);

  /**
   * Gets a map containing the {@link AccessControlList} of users for each
   * {@link ReservationACL} acl on thee specified queue.
   *
   * @param queue the queue with which to check a user's permissions.
   * @return The a Map of {@link ReservationACL} to {@link AccessControlList}
   * which contains a list of users that have the specified permission level.
   */
  public abstract Map<ReservationACL, AccessControlList> getReservationAcls(
          String queue);

  /**
   * Gets the length of time in milliseconds for which the {@link SharingPolicy}
   * checks for validity
   * @param queue name of the queue
   * @return length in time in milliseconds for which to check the
   * {@link SharingPolicy}
   */
  public long getReservationWindow(String queue) {
    return DEFAULT_RESERVATION_WINDOW;
  }

  /**
   * Gets the average allowed capacity which will aggregated over the
   * {@link ReservationSchedulerConfiguration#getReservationWindow} by the
   * the {@link SharingPolicy} to check aggregate used capacity
   * @param queue name of the queue
   * @return average capacity allowed by the {@link SharingPolicy}
   */
  public float getAverageCapacity(String queue) {
    return DEFAULT_CAPACITY_OVER_TIME_MULTIPLIER;
  }

  /**
   * Gets the maximum capacity at any time that the {@link SharingPolicy} allows
   * @param queue name of the queue
   * @return maximum allowed capacity at any time
   */
  public float getInstantaneousMaxCapacity(String queue) {
    return DEFAULT_CAPACITY_OVER_TIME_MULTIPLIER;
  }

  /**
   * Gets the name of the {@link SharingPolicy} class associated with the queue
   * @param queue name of the queue
   * @return the class name of the {@link SharingPolicy}
   */
  public String getReservationAdmissionPolicy(String queue) {
    return DEFAULT_RESERVATION_ADMISSION_POLICY;
  }

  /**
   * Gets the name of the {@code ReservationAgent} class associated with the
   * queue
   * @param queue name of the queue
   * @return the class name of the {@code ReservationAgent}
   */
  public String getReservationAgent(String queue) {
    return DEFAULT_RESERVATION_AGENT_NAME;
  }

  /**
   * Checks whether the reservation queues be hidden or visible
   * @param queuePath name of the queue
   * @return true if reservation queues should be visible
   */
  public boolean getShowReservationAsQueues(String queuePath) {
    return DEFAULT_SHOW_RESERVATIONS_AS_QUEUES;
  }

  /**
   * Gets the name of the {@code Planner} class associated with the
   * queue
   * @param queue name of the queue
   * @return the class name of the {@code Planner}
   */
  public String getReplanner(String queue) {
    return DEFAULT_RESERVATION_PLANNER_NAME;
  }

  /**
   * Gets whether the applications should be killed or moved to the parent queue
   * when the {@link ReservationDefinition} expires
   * @param queue name of the queue
   * @return true if application should be moved, false if they need to be
   * killed
   */
  public boolean getMoveOnExpiry(String queue) {
    return DEFAULT_RESERVATION_MOVE_ON_EXPIRY;
  }

  /**
   * Gets the time in milliseconds for which the {@code Planner} will verify
   * the {@link Plan}s satisfy the constraints
   * @param queue name of the queue
   * @return the time in milliseconds for which to check constraints
   */
  public long getEnforcementWindow(String queue) {
    return DEFAULT_RESERVATION_ENFORCEMENT_WINDOW;
  }
}

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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ReservationQueueConfiguration {
  private long reservationWindow;
  private long enforcementWindow;
  private String reservationAdmissionPolicy;
  private String reservationAgent;
  private String planner;
  private boolean showReservationAsQueues;
  private boolean moveOnExpiry;
  private float avgOverTimeMultiplier;
  private float maxOverTimeMultiplier;

  public ReservationQueueConfiguration() {
    this.reservationWindow = ReservationSchedulerConfiguration
        .DEFAULT_RESERVATION_WINDOW;
    this.enforcementWindow = ReservationSchedulerConfiguration
        .DEFAULT_RESERVATION_ENFORCEMENT_WINDOW;
    this.reservationAdmissionPolicy = ReservationSchedulerConfiguration
        .DEFAULT_RESERVATION_ADMISSION_POLICY;
    this.reservationAgent = ReservationSchedulerConfiguration
        .DEFAULT_RESERVATION_AGENT_NAME;
    this.planner = ReservationSchedulerConfiguration
        .DEFAULT_RESERVATION_PLANNER_NAME;
    this.showReservationAsQueues = ReservationSchedulerConfiguration
        .DEFAULT_SHOW_RESERVATIONS_AS_QUEUES;
    this.moveOnExpiry = ReservationSchedulerConfiguration
        .DEFAULT_RESERVATION_MOVE_ON_EXPIRY;
    this.avgOverTimeMultiplier = ReservationSchedulerConfiguration
        .DEFAULT_CAPACITY_OVER_TIME_MULTIPLIER;
    this.maxOverTimeMultiplier = ReservationSchedulerConfiguration
        .DEFAULT_CAPACITY_OVER_TIME_MULTIPLIER;
  }

  public long getReservationWindowMsec() {
    return reservationWindow;
  }

  public long getEnforcementWindowMsec() {
    return enforcementWindow;
  }

  public boolean shouldShowReservationAsQueues() {
    return showReservationAsQueues;
  }

  public boolean shouldMoveOnExpiry() {
    return moveOnExpiry;
  }

  public String getReservationAdmissionPolicy() {
    return reservationAdmissionPolicy;
  }

  public String getReservationAgent() {
    return reservationAgent;
  }

  public String getPlanner() {
    return planner;
  }

  public float getAvgOverTimeMultiplier() {
    return avgOverTimeMultiplier;
  }

  public float getMaxOverTimeMultiplier() {
    return maxOverTimeMultiplier;
  }

  public void setPlanner(String planner) {
    this.planner = planner;
  }

  public void setReservationAdmissionPolicy(String reservationAdmissionPolicy) {
    this.reservationAdmissionPolicy = reservationAdmissionPolicy;
  }

  public void setReservationAgent(String reservationAgent) {
    this.reservationAgent = reservationAgent;
  }

  @VisibleForTesting
  public void setReservationWindow(long reservationWindow) {
    this.reservationWindow = reservationWindow;
  }

  @VisibleForTesting
  public void setAverageCapacity(int averageCapacity) {
    this.avgOverTimeMultiplier = averageCapacity;
  }
}

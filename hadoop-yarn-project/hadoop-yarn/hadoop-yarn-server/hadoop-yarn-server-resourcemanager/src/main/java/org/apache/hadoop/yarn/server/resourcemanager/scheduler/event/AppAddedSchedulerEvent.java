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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.placement
    .ApplicationPlacementContext;

public class AppAddedSchedulerEvent extends SchedulerEvent {

  private final ApplicationId applicationId;
  private final String queue;
  private final String user;
  private final ReservationId reservationID;
  private final boolean isAppRecovering;
  private final Priority appPriority;
  private final ApplicationPlacementContext placementContext;

  public AppAddedSchedulerEvent(ApplicationId applicationId, String queue,
      String user) {
    this(applicationId, queue, user, false, null, Priority.newInstance(0),
        null);
  }

  public AppAddedSchedulerEvent(ApplicationId applicationId, String queue,
      String user, ApplicationPlacementContext placementContext) {
    this(applicationId, queue, user, false, null, Priority.newInstance(0),
        placementContext);
  }

  public AppAddedSchedulerEvent(ApplicationId applicationId, String queue,
      String user, ReservationId reservationID, Priority appPriority) {
    this(applicationId, queue, user, false, reservationID, appPriority, null);
  }

  public AppAddedSchedulerEvent(String user,
      ApplicationSubmissionContext submissionContext, boolean isAppRecovering,
      Priority appPriority) {
    this(submissionContext.getApplicationId(), submissionContext.getQueue(),
        user, isAppRecovering, submissionContext.getReservationID(),
        appPriority, null);
  }

  public AppAddedSchedulerEvent(String user,
      ApplicationSubmissionContext submissionContext, boolean isAppRecovering,
      Priority appPriority, ApplicationPlacementContext placementContext) {
    this(submissionContext.getApplicationId(), submissionContext.getQueue(),
        user, isAppRecovering, submissionContext.getReservationID(),
        appPriority, placementContext);
  }

  public AppAddedSchedulerEvent(ApplicationId applicationId, String queue,
      String user, boolean isAppRecovering, ReservationId reservationID,
      Priority appPriority, ApplicationPlacementContext placementContext) {
    super(SchedulerEventType.APP_ADDED);
    this.applicationId = applicationId;
    this.queue = queue;
    this.user = user;
    this.reservationID = reservationID;
    this.isAppRecovering = isAppRecovering;
    this.appPriority = appPriority;
    this.placementContext = placementContext;
  }

  public ApplicationId getApplicationId() {
    return applicationId;
  }

  public String getQueue() {
    return queue;
  }

  public String getUser() {
    return user;
  }

  public boolean getIsAppRecovering() {
    return isAppRecovering;
  }

  public ReservationId getReservationID() {
    return reservationID;
  }

  public Priority getApplicatonPriority() {
    return appPriority;
  }

  public ApplicationPlacementContext getPlacementContext() {
    return placementContext;
  }
}

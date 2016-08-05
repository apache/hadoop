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

package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * {@link ReservationListRequest} captures the set of requirements the
 * user has to list reservations.
 */
@Public
@Unstable
public abstract class ReservationListRequest {

  /**
   * The {@link ReservationListRequest} will use the reservationId to search for
   * reservations to list if it is provided. Otherwise, it will select active
   * reservations within the startTime and endTime (inclusive).
   *
   * @param queue Required. Cannot be null or empty. Refers to the reservable
   *              queue in the scheduler that was selected when creating a
   *              reservation submission {@link ReservationSubmissionRequest}.
   * @param reservationId Optional. String representation of
   *                     {@code ReservationId} If provided, other fields will
   *                     be ignored.
   * @param startTime Optional. If provided, only reservations that
   *                end after the startTime will be selected. This defaults
   *                to 0 if an invalid number is used.
   * @param endTime Optional. If provided, only reservations that
   *                start on or before endTime will be selected. This defaults
   *                to Long.MAX_VALUE if an invalid number is used.
   * @param includeReservationAllocations Optional. Flag that
   *                determines whether the entire reservation allocations are
   *                to be returned. Reservation allocations are subject to
   *                change in the event of re-planning as described by
   *                {@code ReservationDefinition}.
   * @return the list of reservations via  {@link ReservationListRequest}
   */
  @Public
  @Unstable
  public static ReservationListRequest newInstance(
      String queue, String reservationId, long startTime, long endTime,
      boolean includeReservationAllocations) {
    ReservationListRequest request =
        Records.newRecord(ReservationListRequest.class);
    request.setQueue(queue);
    request.setReservationId(reservationId);
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    request.setIncludeResourceAllocations(includeReservationAllocations);
    return request;
  }

  /**
   * The {@link ReservationListRequest} will use the reservationId to search for
   * reservations to list if it is provided. Otherwise, it will select active
   * reservations within the startTime and endTime (inclusive).
   *
   * @param queue Required. Cannot be null or empty. Refers to the reservable
   *              queue in the scheduler that was selected when creating a
   *              reservation submission {@link ReservationSubmissionRequest}.
   * @param reservationId Optional. String representation of
   *                     {@code ReservationId} If provided, other fields will
   *                     be ignored.
   * @param includeReservationAllocations Optional. Flag that
   *                determines whether the entire reservation allocations are
   *                to be returned. Reservation allocations are subject to
   *                change in the event of re-planning as described by
   *                {@code ReservationDefinition}.
   * @return the list of reservations via {@link ReservationListRequest}
   */
  @Public
  @Unstable
  public static ReservationListRequest newInstance(
          String queue, String reservationId, boolean
          includeReservationAllocations) {
    return newInstance(queue, reservationId, -1, -1,
            includeReservationAllocations);
  }

  /**
   * The {@link ReservationListRequest} will use the reservationId to search for
   * reservations to list if it is provided. Otherwise, it will select active
   * reservations within the startTime and endTime (inclusive).
   *
   * @param queue Required. Cannot be null or empty. Refers to the reservable
   *              queue in the scheduler that was selected when creating a
   *              reservation submission {@link ReservationSubmissionRequest}.
   * @param reservationId Optional. String representation of
   *                     {@code ReservationId} If provided, other fields will
   *                     be ignored.
   * @return the list of reservations via {@link ReservationListRequest}
   */
  @Public
  @Unstable
  public static ReservationListRequest newInstance(
          String queue, String reservationId) {
    return newInstance(queue, reservationId, -1, -1, false);
  }

  /**
   * Get queue name to use to find reservations.
   *
   * @return the queue name to use to find reservations.
   */
  @Public
  @Unstable
  public abstract String getQueue();

  /**
   * Set queue name to use to find resource allocations.
   *
   * @param queue Required. Cannot be null or empty.
   */
  @Public
  @Unstable
  public abstract void setQueue(String queue);

  /**
   * Get the reservation id to use to find a reservation.
   *
   * @return the reservation id of the reservation.
   */
  @Public
  @Unstable
  public abstract String getReservationId();

  /**
   * Set the reservation id to use to find a reservation.
   *
   * @param reservationId Optional. String representation of
   *                     {@code ReservationId} If provided, other fields will
   *                     be ignored.
   */
  @Public
  @Unstable
  public abstract void setReservationId(String reservationId);

  /**
   * Get the start time to use to search for reservations.
   * When this is set, reservations that start before this start
   * time are ignored.
   *
   * @return the start time to use to search for reservations.
   */
  @Public
  @Unstable
  public abstract long getStartTime();

  /**
   * Set the start time to use to search for reservations.
   * When this is set, reservations that start before this start
   * time are ignored.
   *
   * @param startTime Optional. If provided, only reservations that
   *                end after the startTime will be selected. This defaults
   *                to 0 if an invalid number is used.
   */
  @Public
  @Unstable
  public abstract void setStartTime(long startTime);

  /**
   * Get the end time to use to search for reservations.
   * When this is set, reservations that start after this end
   * time are ignored.
   *
   * @return the end time to use to search for reservations.
   */
  @Public
  @Unstable
  public abstract long getEndTime();

  /**
   * Set the end time to use to search for reservations.
   * When this is set, reservations that start after this end
   * time are ignored.
   *
   * @param endTime Optional. If provided, only reservations that
   *                start before endTime will be selected. This defaults
   *                to Long.MAX_VALUE if an invalid number is used.
   */
  @Public
  @Unstable
  public abstract void setEndTime(long endTime);

  /**
   * Get the boolean representing whether or not the user
   * is requesting the full resource allocation.
   * If this is true, the full resource allocation will
   * be included in the response.
   *
   * @return the end time to use to search for reservations.
   */
  @Public
  @Unstable
  public abstract boolean getIncludeResourceAllocations();

  /**
   * Set the boolean representing whether or not the user
   * is requesting the full resource allocation.
   * If this is true, the full resource allocation will
   * be included in the response.
   *
   * @param includeReservationAllocations Optional. Flag that
   *                determines whether the entire list of
   *                {@code ResourceAllocationRequest} will be returned.
   */
  @Public
  @Unstable
  public abstract void setIncludeResourceAllocations(
          boolean includeReservationAllocations);
}

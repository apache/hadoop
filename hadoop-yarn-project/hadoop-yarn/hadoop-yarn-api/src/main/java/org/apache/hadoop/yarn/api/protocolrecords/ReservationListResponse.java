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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ReservationAllocationState;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

/**
 * {@link ReservationListResponse} captures the list of reservations that the
 * user has queried.
 *
 * The resulting list of {@link ReservationAllocationState} contains a list of
 * {@code ResourceAllocationRequest} representing the current state of the
 * reservation resource allocations will be returned. This is subject to change
 * in the event of re-planning a described by {@code ReservationDefinition}
 *
 * @see ReservationAllocationState
 *
 */
@Public
@Unstable
public abstract class ReservationListResponse {

  @Private
  @Unstable
  public static ReservationListResponse newInstance(
      List<ReservationAllocationState> reservationAllocationState) {
    ReservationListResponse response =
        Records.newRecord(ReservationListResponse.class);
    response.setReservationAllocationState(reservationAllocationState);
    return response;
  }

  /**
   * Get the list of {@link ReservationAllocationState}, that corresponds
   * to a reservation in the scheduler.
   *
   * @return the list of {@link ReservationAllocationState} which holds
   * information of a particular reservation
   */
  @Public
  @Unstable
  public abstract List<ReservationAllocationState>
          getReservationAllocationState();

  /**
   * Set the list of {@link ReservationAllocationState}, that correspond
   * to a reservation in the scheduler.
   *
   * @param reservationAllocationState the list of
   * {@link ReservationAllocationState} which holds information of a
   *                                   particular reservation.
   */
  @Private
  @Unstable
  public abstract void setReservationAllocationState(
          List<ReservationAllocationState> reservationAllocationState);
}

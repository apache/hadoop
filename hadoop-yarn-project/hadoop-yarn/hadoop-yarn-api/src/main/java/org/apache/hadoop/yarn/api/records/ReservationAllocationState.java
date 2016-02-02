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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

/**
 * {@code ReservationAllocationState} represents the reservation that is
 * made by a user.
 * <p>
 * It includes:
 * <ul>
 *   <li>Duration of the reservation.</li>
 *   <li>Acceptance time of the duration.</li>
 *   <li>
 *       List of {@link ResourceAllocationRequest}, which includes the time
 *       interval, and capability of the allocation.
 *       {@code ResourceAllocationRequest} represents an allocation
 *       made for a reservation for the current state of the queue. This can be
 *       changed for reasons such as re-planning, but will always be subject to
 *       the constraints of the user contract as described by
 *       {@link ReservationDefinition}
 *   </li>
 *   <li>{@link ReservationId} of the reservation.</li>
 *   <li>{@link ReservationDefinition} used to make the reservation.</li>
 * </ul>
 *
 * @see ResourceAllocationRequest
 * @see ReservationId
 * @see ReservationDefinition
 */
@Public
@Stable
public abstract class ReservationAllocationState {

  /**
   *
   * @param acceptanceTime The acceptance time of the reservation.
   * @param user The username of the user who made the reservation.
   * @param resourceAllocations List of {@link ResourceAllocationRequest}
   *                            representing the current state of the
   *                            reservation resource allocations. This is
   *                            subject to change in the event of re-planning.
   * @param reservationId {@link ReservationId } of the reservation being
   *                                            listed.
   * @param reservationDefinition {@link ReservationDefinition} used to make
   *                              the reservation.
   * @return {@code ReservationAllocationState} that represents the state of
   * the reservation.
   */
  @Public
  @Stable
  public static ReservationAllocationState newInstance(long acceptanceTime,
           String user, List<ResourceAllocationRequest> resourceAllocations,
           ReservationId reservationId,
           ReservationDefinition reservationDefinition) {
    ReservationAllocationState ri = Records.newRecord(
            ReservationAllocationState.class);
    ri.setAcceptanceTime(acceptanceTime);
    ri.setUser(user);
    ri.setResourceAllocationRequests(resourceAllocations);
    ri.setReservationId(reservationId);
    ri.setReservationDefinition(reservationDefinition);
    return ri;
  }

  /**
   * Get the acceptance time of the reservation.
   *
   * @return the time that the reservation was accepted.
   */
  @Public
  @Unstable
  public abstract long getAcceptanceTime();

  /**
   * Set the time that the reservation was accepted.
   *
   * @param acceptanceTime The acceptance time of the reservation.
   */
  @Private
  @Unstable
  public abstract void setAcceptanceTime(long acceptanceTime);

  /**
   * Get the user who made the reservation.
   *
   * @return the name of the user who made the reservation.
   */
  @Public
  @Unstable
  public abstract String getUser();

  /**
   * Set the user who made the reservation.
   *
   * @param user The username of the user who made the reservation.
   */
  @Private
  @Unstable
  public abstract void setUser(String user);

  /**
   * Get the Resource allocations of the reservation based on the current state
   * of the plan. This is subject to change in the event of re-planning.
   * The allocations will be constraint to the user contract as described by
   * the {@link ReservationDefinition}
   *
   * @return a list of resource allocations for the reservation.
   */
  @Public
  @Unstable
  public abstract List<ResourceAllocationRequest>
          getResourceAllocationRequests();

  /**
   * Set the list of resource allocations made for the reservation.
   *
   * @param resourceAllocations List of {@link ResourceAllocationRequest}
   *                            representing the current state of the
   *                            reservation resource allocations. This is
   *                            subject to change in the event of re-planning.
   */
  @Private
  @Unstable
  public abstract void setResourceAllocationRequests(
          List<ResourceAllocationRequest> resourceAllocations);

  /**
   * Get the id of the reservation.
   *
   * @return the reservation id corresponding to the reservation.
   */
  @Public
  @Unstable
  public abstract ReservationId getReservationId();

  /**
   * Set the id corresponding to the reservation.
   * `
   * @param reservationId {@link ReservationId } of the reservation being
   *                                            listed.
   */
  @Private
  @Unstable
  public abstract void setReservationId(ReservationId reservationId);

  /**
   * Get the reservation definition used to make the reservation.
   *
   * @return the reservation definition used to make the reservation.
   */
  @Public
  @Unstable
  public abstract ReservationDefinition getReservationDefinition();

  /**
   * Set the definition of the reservation.
   *
   * @param reservationDefinition {@link ReservationDefinition} used to make
   *                              the reservation.
   */
  @Private
  @Unstable
  public abstract void setReservationDefinition(ReservationDefinition
                                                      reservationDefinition);


}

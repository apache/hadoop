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

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;

/**
 * This interface groups the methods used to modify the state of a Plan.
 */
public interface PlanEdit extends PlanContext, PlanView {

  /**
   * Add a new {@link ReservationAllocation} to the plan
   * 
   * @param reservation the {@link ReservationAllocation} to be added to the
   *          plan
   * @return true if addition is successful, false otherwise
   */
  public boolean addReservation(ReservationAllocation reservation)
      throws PlanningException;

  /**
   * Updates an existing {@link ReservationAllocation} in the plan. This is
   * required for re-negotiation
   * 
   * @param reservation the {@link ReservationAllocation} to be updated the plan
   * @return true if update is successful, false otherwise
   */
  public boolean updateReservation(ReservationAllocation reservation)
      throws PlanningException;

  /**
   * Delete an existing {@link ReservationAllocation} from the plan identified
   * uniquely by its {@link ReservationId}. This will generally be used for
   * garbage collection
   * 
   * @param reservationID the {@link ReservationAllocation} to be deleted from
   *          the plan identified uniquely by its {@link ReservationId}
   * @return true if delete is successful, false otherwise
   */
  public boolean deleteReservation(ReservationId reservationID)
      throws PlanningException;

  /**
   * Method invoked to garbage collect old reservations. It cleans up expired
   * reservations that have fallen out of the sliding archival window
   * 
   * @param tick the current time from which the archival window is computed
   */
  public void archiveCompletedReservations(long tick) throws PlanningException;

  /**
   * Sets the overall capacity in terms of {@link Resource} assigned to this
   * plan
   * 
   * @param capacity the overall capacity in terms of {@link Resource} assigned
   *          to this plan
   */
  public void setTotalCapacity(Resource capacity);

}

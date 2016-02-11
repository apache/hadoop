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

import java.util.Set;

/**
 * This interface provides a read-only view on the allocations made in this
 * plan. This methods are used for example by {@code ReservationAgent}s to
 * determine the free resources in a certain point in time, and by
 * PlanFollowerPolicy to publish this plan to the scheduler.
 */
public interface PlanView extends PlanContext {

  /**
   * Return a set of {@link ReservationAllocation} identified by the user who
   * made the reservation.
   *
   * @param reservationID the unqiue id to identify the
   * {@link ReservationAllocation}
   * @param interval the time interval used to retrieve the reservation
   *                 allocations from. Only reservations with start time no
   *                 greater than the interval end time, and end time no less
   *                 than the interval start time will be selected.
   * @param user the user to retrieve the reservation allocation from.
   * @return a set of {@link ReservationAllocation} identified by the user who
   * made the reservation
   */
  Set<ReservationAllocation> getReservations(ReservationId
                    reservationID, ReservationInterval interval, String user);

  /**
   * Return a set of {@link ReservationAllocation} identified by any user.
   *
   * @param reservationID the unqiue id to identify the
   * {@link ReservationAllocation}
   * @param interval the time interval used to retrieve the reservation
   *                 allocations from. Only reservations with start time no
   *                 greater than the interval end time, and end time no less
   *                 than the interval start time will be selected.
   * @return a set of {@link ReservationAllocation} identified by any user
   */
  Set<ReservationAllocation> getReservations(ReservationId reservationID,
                    ReservationInterval interval);

  /**
   * Return a {@link ReservationAllocation} identified by its
   * {@link ReservationId}
   * 
   * @param reservationID the unique id to identify the
   *          {@link ReservationAllocation}
   * @return {@link ReservationAllocation} identified by the specified id
   */
  public ReservationAllocation getReservationById(ReservationId reservationID);

  /**
   * Return a set of {@link ReservationAllocation} that belongs to a certain
   * user and overlaps time t.
   *
   * @param user the user being considered
   * @param t the instant in time being considered
   * @return set of active {@link ReservationAllocation}s for this
   *         user at this time
   */
  public Set<ReservationAllocation> getReservationByUserAtTime(String user,
      long t);

  /**
   * Gets all the active reservations at the specified point of time
   * 
   * @param tick the time (UTC in ms) for which the active reservations are
   *          requested
   * @return set of active reservations at the specified time
   */
  public Set<ReservationAllocation> getReservationsAtTime(long tick);

  /**
   * Gets all the reservations in the plan
   * 
   * @return set of all reservations handled by this Plan
   */
  public Set<ReservationAllocation> getAllReservations();

  /**
   * Returns the total {@link Resource} reserved for all users at the specified
   * time
   * 
   * @param tick the time (UTC in ms) for which the reserved resources are
   *          requested
   * @return the total {@link Resource} reserved for all users at the specified
   *         time
   */
  Resource getTotalCommittedResources(long tick);

  /**
   * Returns the overall capacity in terms of {@link Resource} assigned to this
   * plan (typically will correspond to the absolute capacity of the
   * corresponding queue).
   * 
   * @return the overall capacity in terms of {@link Resource} assigned to this
   *         plan
   */
  Resource getTotalCapacity();

  /**
   * Gets the time (UTC in ms) at which the first reservation starts
   * 
   * @return the time (UTC in ms) at which the first reservation starts
   */
  public long getEarliestStartTime();

  /**
   * Returns the time (UTC in ms) at which the last reservation terminates
   *
   * @return the time (UTC in ms) at which the last reservation terminates
   */
  public long getLastEndTime();

  /**
   * This method returns the amount of resources available to a given user
   * (optionally if removing a certain reservation) over the start-end time
   * range.
   *
   * @param user
   * @param oldId
   * @param start
   * @param end
   * @return a view of the plan as it is available to this user
   * @throws PlanningException
   */
  public RLESparseResourceAllocation getAvailableResourceOverTime(String user,
      ReservationId oldId, long start, long end) throws PlanningException;

  /**
   * This method returns a RLE encoded view of the user reservation count
   * utilization between start and end time.
   *
   * @param user
   * @param start
   * @param end
   * @return RLE encoded view of reservation used over time
   */
  public RLESparseResourceAllocation getReservationCountForUserOverTime(
      String user, long start, long end);

  /**
   * This method returns a RLE encoded view of the user reservation utilization
   * between start and end time.
   *
   * @param user
   * @param start
   * @param end
   * @return RLE encoded view of resources used over time
   */
  public RLESparseResourceAllocation getConsumptionForUserOverTime(String user,
      long start, long end);

}

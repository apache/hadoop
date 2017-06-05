/*******************************************************************************
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *******************************************************************************/
package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;

/**
 * An entity that seeks to acquire resources to satisfy an user's contract
 */
public interface ReservationAgent {

  /**
   * Constant defining the preferential treatment of time for equally valid
   * allocations.
   */
  final static String FAVOR_EARLY_ALLOCATION =
      "yarn.resourcemanager.reservation-system.favor-early-allocation";
  /**
   * By default favor early allocations.
   */
  final static boolean DEFAULT_GREEDY_FAVOR_EARLY_ALLOCATION = true;

  /**
   * Create a reservation for the user that abides by the specified contract
   *
   * @param reservationId the identifier of the reservation to be created.
   * @param user the user who wants to create the reservation
   * @param plan the Plan to which the reservation must be fitted
   * @param contract encapsulates the resources the user requires for his
   *          session
   *
   * @return whether the create operation was successful or not
   * @throws PlanningException if the session cannot be fitted into the plan
   */
  public boolean createReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException;

  /**
   * Update a reservation for the user that abides by the specified contract
   *
   * @param reservationId the identifier of the reservation to be updated
   * @param user the user who wants to create the session
   * @param plan the Plan to which the reservation must be fitted
   * @param contract encapsulates the resources the user requires for his
   *          reservation
   *
   * @return whether the update operation was successful or not
   * @throws PlanningException if the reservation cannot be fitted into the plan
   */
  public boolean updateReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException;

  /**
   * Delete an user reservation
   *
   * @param reservationId the identifier of the reservation to be deleted
   * @param user the user who wants to create the reservation
   * @param plan the Plan to which the session must be fitted
   *
   * @return whether the delete operation was successful or not
   * @throws PlanningException if the reservation cannot be fitted into the plan
   */
  public boolean deleteReservation(ReservationId reservationId, String user,
      Plan plan) throws PlanningException;

  /**
   * Init configuration.
   *
   * @param conf Configuration
   */
  void init(Configuration conf);

}

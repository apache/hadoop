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

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;

/**
 * A planning algorithm that invokes several other planning algorithms according
 * to a given order. If one of the planners succeeds, the allocation it
 * generates is returned.
 */
public class TryManyReservationAgents implements ReservationAgent {

  // Planning algorithms
  private final List<ReservationAgent> algs;

  // Constructor
  public TryManyReservationAgents(List<ReservationAgent> algs) {
    this.algs = new LinkedList<ReservationAgent>(algs);
  }

  @Override
  public boolean createReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {

    // Save the planning exception
    PlanningException planningException = null;

    // Try all of the algorithms, in order
    for (ReservationAgent alg : algs) {

      try {
        if (alg.createReservation(reservationId, user, plan, contract)) {
          return true;
        }
      } catch (PlanningException e) {
        planningException = e;
      }

    }

    // If all of the algorithms failed and one of the algorithms threw an
    // exception, throw the last planning exception
    if (planningException != null) {
      throw planningException;
    }

    // If all of the algorithms failed, return false
    return false;

  }

  @Override
  public boolean updateReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {

    // Save the planning exception
    PlanningException planningException = null;

    // Try all of the algorithms, in order
    for (ReservationAgent alg : algs) {

      try {
        if (alg.updateReservation(reservationId, user, plan, contract)) {
          return true;
        }
      } catch (PlanningException e) {
        planningException = e;
      }

    }

    // If all of the algorithms failed and one of the algorithms threw an
    // exception, throw the last planning exception
    if (planningException != null) {
      throw planningException;
    }

    // If all of the algorithms failed, return false
    return false;

  }

  @Override
  public boolean deleteReservation(ReservationId reservationId, String user,
      Plan plan) throws PlanningException {

    return plan.deleteReservation(reservationId);

  }
  @Override
  public void init(Configuration conf) {
  }
}

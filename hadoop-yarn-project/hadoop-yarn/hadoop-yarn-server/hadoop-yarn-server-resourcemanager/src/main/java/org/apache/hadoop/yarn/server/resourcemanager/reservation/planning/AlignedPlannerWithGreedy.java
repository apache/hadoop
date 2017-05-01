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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A planning algorithm that first runs LowCostAligned, and if it fails runs
 * Greedy.
 */
public class AlignedPlannerWithGreedy implements ReservationAgent {

  // Default smoothness factor
  public static final int DEFAULT_SMOOTHNESS_FACTOR = 10;
  public static final String SMOOTHNESS_FACTOR =
      "yarn.resourcemanager.reservation-system.smoothness-factor";
  private boolean allocateLeft = false;


  // Log
  private static final Logger LOG = LoggerFactory
      .getLogger(AlignedPlannerWithGreedy.class);

  // Smoothness factor
  private ReservationAgent planner;

  // Constructor
  public AlignedPlannerWithGreedy() {

  }

  @Override
  public void init(Configuration conf) {
    int smoothnessFactor =
        conf.getInt(SMOOTHNESS_FACTOR, DEFAULT_SMOOTHNESS_FACTOR);
    allocateLeft = conf.getBoolean(FAVOR_EARLY_ALLOCATION,
            DEFAULT_GREEDY_FAVOR_EARLY_ALLOCATION);

    // List of algorithms
    List<ReservationAgent> listAlg = new LinkedList<ReservationAgent>();

    // LowCostAligned planning algorithm
    ReservationAgent algAligned =
        new IterativePlanner(new StageExecutionIntervalByDemand(),
            new StageAllocatorLowCostAligned(smoothnessFactor, allocateLeft),
            allocateLeft);

    listAlg.add(algAligned);

    // Greedy planning algorithm
    ReservationAgent algGreedy =
        new IterativePlanner(new StageExecutionIntervalUnconstrained(),
            new StageAllocatorGreedyRLE(allocateLeft), allocateLeft);
    listAlg.add(algGreedy);

    // Set planner:
    // 1. Attempt to execute algAligned
    // 2. If failed, fall back to algGreedy
    planner = new TryManyReservationAgents(listAlg);
  }

  @Override
  public boolean createReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {

    LOG.info("placing the following ReservationRequest: " + contract);

    try {
      boolean res =
          planner.createReservation(reservationId, user, plan, contract);

      if (res) {
        LOG.info("OUTCOME: SUCCESS, Reservation ID: "
            + reservationId.toString() + ", Contract: " + contract.toString());
      } else {
        LOG.info("OUTCOME: FAILURE, Reservation ID: "
            + reservationId.toString() + ", Contract: " + contract.toString());
      }
      return res;
    } catch (PlanningException e) {
      LOG.info("OUTCOME: FAILURE, Reservation ID: " + reservationId.toString()
          + ", Contract: " + contract.toString());
      throw e;
    }

  }

  @Override
  public boolean updateReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {

    LOG.info("updating the following ReservationRequest: " + contract);

    return planner.updateReservation(reservationId, user, plan, contract);

  }

  @Override
  public boolean deleteReservation(ReservationId reservationId, String user,
      Plan plan) throws PlanningException {

    LOG.info("removing the following ReservationId: " + reservationId);

    return planner.deleteReservation(reservationId, user, plan);

  }
}

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Agent employs a simple greedy placement strategy, placing the various
 * stages of a {@link ReservationDefinition} from the deadline moving backward
 * towards the arrival. This allows jobs with earlier deadline to be scheduled
 * greedily as well. Combined with an opportunistic anticipation of work if the
 * cluster is not fully utilized also seems to provide good latency for
 * best-effort jobs (i.e., jobs running without a reservation).
 *
 * This agent does not account for locality and only consider container
 * granularity for validation purposes (i.e., you can't exceed max-container
 * size).
 */

public class GreedyReservationAgent implements ReservationAgent {

  // Log
  private static final Logger LOG = LoggerFactory
      .getLogger(GreedyReservationAgent.class);

  // Greedy planner
  private final ReservationAgent planner;

  public final static String GREEDY_FAVOR_EARLY_ALLOCATION =
      "yarn.resourcemanager.reservation-system.favor-early-allocation";

  public final static boolean DEFAULT_GREEDY_FAVOR_EARLY_ALLOCATION = true;

  private final boolean allocateLeft;

  public GreedyReservationAgent() {
    this(new Configuration());
  }

  public GreedyReservationAgent(Configuration yarnConfiguration) {

    allocateLeft =
        yarnConfiguration.getBoolean(GREEDY_FAVOR_EARLY_ALLOCATION,
            DEFAULT_GREEDY_FAVOR_EARLY_ALLOCATION);

    if (allocateLeft) {
      LOG.info("Initializing the GreedyReservationAgent to favor \"early\""
          + " (left) allocations (controlled by parameter: "
          + GREEDY_FAVOR_EARLY_ALLOCATION + ")");
    } else {
      LOG.info("Initializing the GreedyReservationAgent to favor \"late\""
          + " (right) allocations (controlled by parameter: "
          + GREEDY_FAVOR_EARLY_ALLOCATION + ")");
    }

    planner =
        new IterativePlanner(new StageEarliestStartByJobArrival(),
            new StageAllocatorGreedyRLE(allocateLeft), allocateLeft);

  }

  public boolean isAllocateLeft(){
    return allocateLeft;
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
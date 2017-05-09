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

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;

/**
 * An implementation of {@link StageExecutionInterval} which gives each stage
 * the maximal possible time interval, given the job constraints. Specifically,
 * for ANY and ALL jobs, the interval would be [jobArrival, jobDeadline). For
 * ORDER jobs, the stage cannot start before its predecessors (if allocateLeft
 * == true) or cannot end before its successors (if allocateLeft == false)
 */
public class StageExecutionIntervalUnconstrained implements
    StageExecutionInterval {

  @Override
  public ReservationInterval computeExecutionInterval(Plan plan,
      ReservationDefinition reservation,
      ReservationRequest currentReservationStage, boolean allocateLeft,
      RLESparseResourceAllocation allocations) {

    Long stageArrival = reservation.getArrival();
    Long stageDeadline = reservation.getDeadline();

    ReservationRequestInterpreter jobType =
        reservation.getReservationRequests().getInterpreter();

    // Left to right
    if (allocateLeft) {
      // If ORDER job, change the stage arrival time
      if ((jobType == ReservationRequestInterpreter.R_ORDER)
          || (jobType == ReservationRequestInterpreter.R_ORDER_NO_GAP)) {
        Long allocationEndTime = allocations.getLatestNonNullTime();
        if (allocationEndTime != -1) {
          stageArrival = allocationEndTime;
        }
      }
      // Right to left
    } else {
      // If ORDER job, change the stage deadline
      if ((jobType == ReservationRequestInterpreter.R_ORDER)
          || (jobType == ReservationRequestInterpreter.R_ORDER_NO_GAP)) {
        Long allocationStartTime = allocations.getEarliestStartTime();
        if (allocationStartTime != -1) {
          stageDeadline = allocationStartTime;
        }
      }
    }
    return new ReservationInterval(stageArrival, stageDeadline);
  }
}

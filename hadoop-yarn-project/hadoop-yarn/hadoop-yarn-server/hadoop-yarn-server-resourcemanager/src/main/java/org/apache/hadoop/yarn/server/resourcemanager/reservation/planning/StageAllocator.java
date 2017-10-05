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

import java.util.Map;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;

/**
 * Interface for allocating a single stage in IterativePlanner.
 */
public interface StageAllocator {

  /**
   * Computes the allocation of a stage inside a defined time interval.
   *
   * @param plan the Plan to which the reservation must be fitted
   * @param planLoads a 'dirty' read of the plan loads at each time
   * @param planModifications the allocations performed by the planning
   *          algorithm which are not yet reflected by plan
   * @param rr the stage
   * @param stageArrival the arrival time (earliest starting time) set for
   *          the stage by the two phase planning algorithm
   * @param stageDeadline the deadline of the stage set by the two phase
   *          planning algorithm
   * @param period the periodicity with which this stage appears
   * @param user name of the user
   * @param oldId identifier of the old reservation
   *
   * @return The computed allocation (or null if the stage could not be
   *         allocated)
   * @throws PlanningException if operation is unsuccessful
   */
  Map<ReservationInterval, Resource> computeStageAllocation(Plan plan,
      RLESparseResourceAllocation planLoads,
      RLESparseResourceAllocation planModifications, ReservationRequest rr,
      long stageArrival, long stageDeadline, long period, String user,
      ReservationId oldId) throws PlanningException;

}

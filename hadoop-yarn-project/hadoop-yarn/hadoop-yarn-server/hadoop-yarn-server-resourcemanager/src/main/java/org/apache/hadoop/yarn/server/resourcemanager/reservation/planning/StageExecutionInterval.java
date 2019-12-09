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
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;

/**
 * An auxiliary class used to compute the time interval in which the stage can
 * be allocated resources by {@link IterativePlanner}.
 */
public interface StageExecutionInterval {
  /**
   * Computes the earliest allowed starting time for a given stage.
   *
   * @param plan the Plan to which the reservation must be fitted
   * @param reservation the job contract
   * @param currentReservationStage the stage
   * @param allocateLeft is the job allocated from left to right
   * @param allocations Existing resource assignments for the job
   * @return the time interval in which the stage can get resources.
   */
  ReservationInterval computeExecutionInterval(Plan plan,
      ReservationDefinition reservation,
      ReservationRequest currentReservationStage, boolean allocateLeft,
      RLESparseResourceAllocation allocations);

}

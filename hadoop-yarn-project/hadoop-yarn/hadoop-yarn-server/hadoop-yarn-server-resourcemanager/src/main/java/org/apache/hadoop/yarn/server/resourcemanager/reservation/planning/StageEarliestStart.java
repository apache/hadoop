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

/**
 * Interface for setting the earliest start time of a stage in IterativePlanner.
 */
public interface StageEarliestStart {

  /**
   * Computes the earliest allowed starting time for a given stage.
   *
   * @param plan the Plan to which the reservation must be fitted
   * @param reservation the job contract
   * @param index the index of the stage in the job contract
   * @param currentReservationStage the stage
   * @param stageDeadline the deadline of the stage set by the two phase
   *          planning algorithm
   *
   * @return the earliest allowed starting time for the stage.
   */
  long setEarliestStartTime(Plan plan, ReservationDefinition reservation,
          int index, ReservationRequest currentReservationStage,
          long stageDeadline);

}

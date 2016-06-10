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

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.MismatchedUserException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.ResourceOverCommitException;

/**
 * This policy enforce a simple physical cluster capacity constraints, by
 * validating that the allocation proposed fits in the current plan. This
 * validation is compatible with "updates" and in verifying the capacity
 * constraints it conceptually remove the prior version of the reservation.
 */
@LimitedPrivate("yarn")
@Unstable
public class NoOverCommitPolicy implements SharingPolicy {

  @Override
  public void validate(Plan plan, ReservationAllocation reservation)
      throws PlanningException {

    ReservationAllocation oldReservation =
        plan.getReservationById(reservation.getReservationId());

    // check updates are using same name
    if (oldReservation != null
        && !oldReservation.getUser().equals(reservation.getUser())) {
      throw new MismatchedUserException(
          "Updating an existing reservation with mismatching user:"
              + oldReservation.getUser() + " != " + reservation.getUser());
    }

    RLESparseResourceAllocation available = plan.getAvailableResourceOverTime(
        reservation.getUser(), reservation.getReservationId(),
        reservation.getStartTime(), reservation.getEndTime());

    // test the reservation does not exceed what is available
    try {
      RLESparseResourceAllocation
          .merge(plan.getResourceCalculator(), plan.getTotalCapacity(),
              available, reservation.getResourcesOverTime(),
              RLESparseResourceAllocation.RLEOperator.subtractTestNonNegative,
              reservation.getStartTime(), reservation.getEndTime());
    } catch (PlanningException p) {
      throw new ResourceOverCommitException(
          "Resources at time " + " would be overcommitted by "
              + "accepting reservation: " + reservation.getReservationId());
    }
  }

  @Override
  public long getValidWindow() {
    // this policy has no "memory" so the valid window is set to zero
    return 0;
  }

  @Override
  public void init(String planQueuePath,
      ReservationSchedulerConfiguration conf) {
    // nothing to do for this policy
  }

  @Override
  public RLESparseResourceAllocation availableResources(
      RLESparseResourceAllocation available, Plan plan, String user,
      ReservationId oldId, long start, long end) throws PlanningException {
    return available;
  }

}

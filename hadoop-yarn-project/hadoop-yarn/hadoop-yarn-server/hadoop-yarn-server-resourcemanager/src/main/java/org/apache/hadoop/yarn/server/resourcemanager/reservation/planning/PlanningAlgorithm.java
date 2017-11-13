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
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.InMemoryReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.PeriodicRLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.ContractValidationException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;

/**
 * An abstract class that follows the general behavior of planning algorithms.
 */
public abstract class PlanningAlgorithm implements ReservationAgent {

  /**
   * Performs the actual allocation for a ReservationDefinition within a Plan.
   *
   * @param reservationId the identifier of the reservation
   * @param user the user who owns the reservation
   * @param plan the Plan to which the reservation must be fitted
   * @param contract encapsulates the resources required by the user for his
   *          session
   * @param oldReservation the existing reservation (null if none)
   * @return whether the allocateUser function was successful or not
   *
   * @throws PlanningException if the session cannot be fitted into the plan
   * @throws ContractValidationException if validation fails
   */
  protected boolean allocateUser(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract,
      ReservationAllocation oldReservation) throws PlanningException,
      ContractValidationException {

    // Adjust the ResourceDefinition to account for system "imperfections"
    // (e.g., scheduling delays for large containers).
    ReservationDefinition adjustedContract = adjustContract(plan, contract);

    // Compute the job allocation
    RLESparseResourceAllocation allocation =
            computeJobAllocation(plan, reservationId, adjustedContract, user);

    long period = Long.parseLong(contract.getRecurrenceExpression());

    // Make allocation periodic if request is periodic
    if (contract.getRecurrenceExpression() != null) {
      if (period > 0) {
        allocation =
            new PeriodicRLESparseResourceAllocation(allocation, period);
      }
    }

    // If no job allocation was found, fail
    if (allocation == null) {
      throw new PlanningException(
              "The planning algorithm could not find a valid allocation"
                      + " for your request");
    }

    // Translate the allocation to a map (with zero paddings)
    long step = plan.getStep();

    long jobArrival = stepRoundUp(adjustedContract.getArrival(), step);
    long jobDeadline = stepRoundUp(adjustedContract.getDeadline(), step);

    Map<ReservationInterval, Resource> mapAllocations =
        allocationsToPaddedMap(allocation, jobArrival, jobDeadline, period);

    // Create the reservation
    ReservationAllocation capReservation =
        new InMemoryReservationAllocation(reservationId, // ID
            adjustedContract, // Contract
            user, // User name
            plan.getQueueName(), // Queue name
            adjustedContract.getArrival(), adjustedContract.getDeadline(),
            mapAllocations, // Allocations
            plan.getResourceCalculator(), // Resource calculator
            plan.getMinimumAllocation()); // Minimum allocation

    // Add (or update) the reservation allocation
    if (oldReservation != null) {
      return plan.updateReservation(capReservation);
    } else {
      return plan.addReservation(capReservation, false);
    }

  }

  private Map<ReservationInterval, Resource> allocationsToPaddedMap(
      RLESparseResourceAllocation allocation, long jobArrival, long jobDeadline,
      long period) {

    // Zero allocation
    Resource zeroResource = Resource.newInstance(0, 0);

    if (period > 0) {
      if ((jobDeadline - jobArrival) >= period) {
        allocation.addInterval(new ReservationInterval(0L, period),
            zeroResource);
      }
      jobArrival = jobArrival % period;
      jobDeadline = jobDeadline % period;

      if (jobArrival <= jobDeadline) {
        allocation.addInterval(new ReservationInterval(0, jobArrival),
            zeroResource);
        allocation.addInterval(new ReservationInterval(jobDeadline, period),
            zeroResource);
      } else {
        allocation.addInterval(new ReservationInterval(jobDeadline, jobArrival),
            zeroResource);
      }
    } else {
      // Pad at the beginning
      long earliestStart = findEarliestTime(allocation.toIntervalMap());
      if (jobArrival < earliestStart) {
        allocation.addInterval(
            new ReservationInterval(jobArrival, earliestStart), zeroResource);
      }

      // Pad at the beginning
      long latestEnd = findLatestTime(allocation.toIntervalMap());
      if (latestEnd < jobDeadline) {
        allocation.addInterval(new ReservationInterval(latestEnd, jobDeadline),
            zeroResource);
      }
    }
    return allocation.toIntervalMap();
  }

  public abstract RLESparseResourceAllocation computeJobAllocation(Plan plan,
      ReservationId reservationId, ReservationDefinition reservation,
      String user) throws PlanningException, ContractValidationException;

  @Override
  public boolean createReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {

    // Allocate
    return allocateUser(reservationId, user, plan, contract, null);

  }

  @Override
  public boolean updateReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {

    // Get the old allocation
    ReservationAllocation oldAlloc = plan.getReservationById(reservationId);

    // Allocate (ignores the old allocation)
    return allocateUser(reservationId, user, plan, contract, oldAlloc);

  }

  @Override
  public boolean deleteReservation(ReservationId reservationId, String user,
      Plan plan) throws PlanningException {

    // Delete the existing reservation
    return plan.deleteReservation(reservationId);

  }

  protected static long findEarliestTime(
      Map<ReservationInterval, Resource> sesInt) {

    long ret = Long.MAX_VALUE;
    for (Entry<ReservationInterval, Resource> s : sesInt.entrySet()) {
      if (s.getKey().getStartTime() < ret && s.getValue() != null) {
        ret = s.getKey().getStartTime();
      }
    }
    return ret;

  }

  protected static long findLatestTime(Map<ReservationInterval,
      Resource> sesInt) {

    long ret = Long.MIN_VALUE;
    for (Entry<ReservationInterval, Resource> s : sesInt.entrySet()) {
      if (s.getKey().getEndTime() > ret && s.getValue() != null) {
        ret = s.getKey().getEndTime();
      }
    }
    return ret;

  }

  protected static long stepRoundDown(long t, long step) {
    return (t / step) * step;
  }

  protected static long stepRoundUp(long t, long step) {
    return ((t + step - 1) / step) * step;
  }

  private ReservationDefinition adjustContract(Plan plan,
      ReservationDefinition originalContract) {

    // Place here adjustment. For example using QueueMetrics we can track
    // large container delays per YARN-YARN-1990

    return originalContract;

  }

  @Override
  public void init(Configuration conf) {
  }
}

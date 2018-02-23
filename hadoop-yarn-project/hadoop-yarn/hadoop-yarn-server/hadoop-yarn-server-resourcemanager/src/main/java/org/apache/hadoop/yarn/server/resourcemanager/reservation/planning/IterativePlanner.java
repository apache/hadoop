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

import java.util.HashSet;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation.RLEOperator;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.ContractValidationException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * A planning algorithm consisting of two main phases. The algorithm iterates
 * over the job stages in ascending/descending order, depending on the flag
 * allocateLeft. For each stage, the algorithm: 1. Determines an interval
 * [stageArrival, stageDeadline) in which the stage is allocated. 2. Computes an
 * allocation for the stage inside the interval. For ANY and ALL jobs, phase 1
 * sets the allocation window of each stage to be [jobArrival, jobDeadline]. For
 * ORDER and ORDER_NO_GAP jobs, the deadline of each stage is set as
 * succcessorStartTime - the starting time of its succeeding stage (or
 * jobDeadline if it is the last stage). The phases are set using the two
 * functions: 1. setAlgStageExecutionInterval 2.setAlgStageAllocator
 */
public class IterativePlanner extends PlanningAlgorithm {

  // Modifications performed by the algorithm that are not been reflected in the
  // actual plan while a request is still pending.
  private RLESparseResourceAllocation planModifications;

  // Data extracted from plan
  private RLESparseResourceAllocation planLoads;
  private Resource capacity;
  private long step;

  // Job parameters
  private ReservationRequestInterpreter jobType;
  private long jobArrival;
  private long jobDeadline;

  // Phase algorithms
  private StageExecutionInterval algStageExecutionInterval = null;
  private StageAllocator algStageAllocator = null;
  private final boolean allocateLeft;

  // Constructor
  public IterativePlanner(StageExecutionInterval algStageExecutionInterval,
      StageAllocator algStageAllocator, boolean allocateLeft) {

    this.allocateLeft = allocateLeft;
    setAlgStageExecutionInterval(algStageExecutionInterval);
    setAlgStageAllocator(algStageAllocator);

  }

  @Override
  public RLESparseResourceAllocation computeJobAllocation(Plan plan,
      ReservationId reservationId, ReservationDefinition reservation,
      String user) throws PlanningException {

    // Initialize
    initialize(plan, reservationId, reservation);

    // Create the allocations data structure
    RLESparseResourceAllocation allocations =
        new RLESparseResourceAllocation(plan.getResourceCalculator());

    StageProvider stageProvider = new StageProvider(allocateLeft, reservation);

    // Current stage
    ReservationRequest currentReservationStage;

    // initialize periodicity
    long period = 0;
    if(reservation.getRecurrenceExpression() != null){
      period = Long.parseLong(reservation.getRecurrenceExpression());
    }

    // Iterate the stages in reverse order
    while (stageProvider.hasNext()) {

      // Get current stage
      currentReservationStage = stageProvider.next();

      // Validate that the ReservationRequest respects basic constraints
      validateInputStage(plan, currentReservationStage);

      // Set the stageArrival and stageDeadline
      ReservationInterval stageInterval =
          setStageExecutionInterval(plan, reservation, currentReservationStage,
              allocations);
      Long stageArrival = stageInterval.getStartTime();
      Long stageDeadline = stageInterval.getEndTime();

      // Compute stage allocation
      Map<ReservationInterval, Resource> curAlloc =
          computeStageAllocation(plan, currentReservationStage, stageArrival,
              stageDeadline, period, user, reservationId);

      // If we did not find an allocation, return NULL
      // (unless it's an ANY job, then we simply continue).
      if (curAlloc == null) {

        // If it's an ANY job, we can move to the next possible request
        if (jobType == ReservationRequestInterpreter.R_ANY) {
          continue;
        }

        // Otherwise, the job cannot be allocated
        throw new PlanningException("The request cannot be satisfied");

      }

      // Validate ORDER_NO_GAP
      if (jobType == ReservationRequestInterpreter.R_ORDER_NO_GAP) {
        if (!validateOrderNoGap(allocations, curAlloc, allocateLeft)) {
          throw new PlanningException(
              "The allocation found does not respect ORDER_NO_GAP");
        }
      }

      // If we did find an allocation for the stage, add it
      for (Entry<ReservationInterval, Resource> entry : curAlloc.entrySet()) {
        allocations.addInterval(entry.getKey(), entry.getValue());
      }

      // If this is an ANY clause, we have finished
      if (jobType == ReservationRequestInterpreter.R_ANY) {
        break;
      }
    }

    // If the allocation is empty, return an error
    if (allocations.isEmpty()) {
      throw new PlanningException("The request cannot be satisfied");
    }

    return allocations;
  }

  protected static boolean validateOrderNoGap(
      RLESparseResourceAllocation allocations,
      Map<ReservationInterval, Resource> curAlloc, boolean allocateLeft) {

    // Left to right
    if (allocateLeft) {
      Long stageStartTime = findEarliestTime(curAlloc);
      Long allocationEndTime = allocations.getLatestNonNullTime();

      // Check that there is no gap between stages
      if ((allocationEndTime != -1) && (allocationEndTime < stageStartTime)) {
        return false;
      }
      // Right to left
    } else {
      Long stageEndTime = findLatestTime(curAlloc);
      Long allocationStartTime = allocations.getEarliestStartTime();

      // Check that there is no gap between stages
      if ((allocationStartTime != -1) && (stageEndTime < allocationStartTime)) {
        return false;
      }
    }

    // Check that the stage allocation does not violate ORDER_NO_GAP
    if (!isNonPreemptiveAllocation(curAlloc)) {
      return false;
    }

    // The allocation is legal
    return true;
  }

  protected void initialize(Plan plan, ReservationId reservationId,
      ReservationDefinition reservation) throws PlanningException {

    // Get plan step & capacity
    capacity = plan.getTotalCapacity();
    step = plan.getStep();

    // Get job parameters (type, arrival time & deadline)
    jobType = reservation.getReservationRequests().getInterpreter();
    jobArrival = stepRoundUp(reservation.getArrival(), step);
    jobDeadline = stepRoundDown(reservation.getDeadline(), step);

    // Initialize the plan modifications
    planModifications =
        new RLESparseResourceAllocation(plan.getResourceCalculator());

    // Dirty read of plan load

    // planLoads are not used by other StageAllocators... and don't deal
    // well with huge reservation ranges
    planLoads = plan.getCumulativeLoadOverTime(jobArrival, jobDeadline);
    ReservationAllocation oldRes = plan.getReservationById(reservationId);
    if (oldRes != null) {
      planLoads = RLESparseResourceAllocation.merge(
          plan.getResourceCalculator(), plan.getTotalCapacity(), planLoads,
          oldRes.getResourcesOverTime(jobArrival, jobDeadline),
          RLEOperator.subtract, jobArrival, jobDeadline);
    }
  }

  private void validateInputStage(Plan plan, ReservationRequest rr)
      throws ContractValidationException {

    // Validate concurrency
    if (rr.getConcurrency() < 1) {
      throw new ContractValidationException("Gang Size should be >= 1");
    }

    // Validate number of containers
    if (rr.getNumContainers() <= 0) {
      throw new ContractValidationException("Num containers should be > 0");
    }

    // Check that gangSize and numContainers are compatible
    if (rr.getNumContainers() % rr.getConcurrency() != 0) {
      throw new ContractValidationException(
          "Parallelism must be an exact multiple of gang size");
    }

    // Check that the largest container request does not exceed the cluster-wide
    // limit for container sizes
    if (Resources.greaterThan(plan.getResourceCalculator(), capacity,
        rr.getCapability(), plan.getMaximumAllocation())) {

      throw new ContractValidationException(
          "Individual capability requests should not exceed cluster's "
              + "maxAlloc");

    }

  }

  private static boolean isNonPreemptiveAllocation(
      Map<ReservationInterval, Resource> curAlloc) {

    // Checks whether a stage allocation is non preemptive or not.
    // Assumption: the intervals are non-intersecting (as returned by
    // computeStageAllocation()).
    // For a non-preemptive allocation, only two end points appear exactly once

    Set<Long> endPoints = new HashSet<Long>(2 * curAlloc.size());
    for (Entry<ReservationInterval, Resource> entry : curAlloc.entrySet()) {

      ReservationInterval interval = entry.getKey();
      Resource resource = entry.getValue();

      // Ignore intervals with no allocation
      if (Resources.equals(resource, Resource.newInstance(0, 0))) {
        continue;
      }

      // Get endpoints
      Long left = interval.getStartTime();
      Long right = interval.getEndTime();

      // Add left endpoint if we haven't seen it before, remove otherwise
      if (!endPoints.contains(left)) {
        endPoints.add(left);
      } else {
        endPoints.remove(left);
      }

      // Add right endpoint if we haven't seen it before, remove otherwise
      if (!endPoints.contains(right)) {
        endPoints.add(right);
      } else {
        endPoints.remove(right);
      }
    }

    // Non-preemptive only if endPoints is of size 2
    return (endPoints.size() == 2);

  }

  // Call setStageExecutionInterval()
  protected ReservationInterval setStageExecutionInterval(Plan plan,
      ReservationDefinition reservation,
      ReservationRequest currentReservationStage,
      RLESparseResourceAllocation allocations) {
    return algStageExecutionInterval.computeExecutionInterval(plan,
        reservation, currentReservationStage, allocateLeft, allocations);
  }

  // Call algStageAllocator
  protected Map<ReservationInterval, Resource> computeStageAllocation(Plan plan,
      ReservationRequest rr, long stageArrivalTime, long stageDeadline,
      long period, String user, ReservationId oldId) throws PlanningException {

    return algStageAllocator.computeStageAllocation(plan, planLoads,
        planModifications, rr, stageArrivalTime, stageDeadline, period, user,
        oldId);

  }

  // Set the algorithm: algStageExecutionInterval
  public IterativePlanner setAlgStageExecutionInterval(
      StageExecutionInterval alg) {

    this.algStageExecutionInterval = alg;
    return this; // To allow concatenation of setAlg() functions

  }

  // Set the algorithm: algStageAllocator
  public IterativePlanner setAlgStageAllocator(StageAllocator alg) {

    this.algStageAllocator = alg;
    return this; // To allow concatenation of setAlg() functions

  }

  /**
   * Helper class that provide a list of ReservationRequests and iterates
   * forward or backward depending whether we are allocating left-to-right or
   * right-to-left.
   */
  public static class StageProvider {

    private final boolean allocateLeft;

    private final ListIterator<ReservationRequest> li;

    public StageProvider(boolean allocateLeft,
        ReservationDefinition reservation) {

      this.allocateLeft = allocateLeft;
      int startingIndex;
      if (allocateLeft) {
        startingIndex = 0;
      } else {
        startingIndex =
            reservation.getReservationRequests().getReservationResources()
                .size();
      }
      // Get a reverse iterator for the set of stages
      li =
          reservation.getReservationRequests().getReservationResources()
              .listIterator(startingIndex);

    }

    public boolean hasNext() {
      if (allocateLeft) {
        return li.hasNext();
      } else {
        return li.hasPrevious();
      }
    }

    public ReservationRequest next() {
      if (allocateLeft) {
        return li.next();
      } else {
        return li.previous();
      }
    }

    public int getCurrentIndex() {
      if (allocateLeft) {
        return li.nextIndex() - 1;
      } else {
        return li.previousIndex() + 1;
      }
    }

  }

}

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

import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.ContractValidationException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Agent employs a simple greedy placement strategy, placing the various
 * stages of a {@link ReservationRequest} from the deadline moving backward
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

  private static final Logger LOG = LoggerFactory
      .getLogger(GreedyReservationAgent.class);

  @Override
  public boolean createReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {
    return computeAllocation(reservationId, user, plan, contract, null);
  }

  @Override
  public boolean updateReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {
    return computeAllocation(reservationId, user, plan, contract,
        plan.getReservationById(reservationId));
  }

  @Override
  public boolean deleteReservation(ReservationId reservationId, String user,
      Plan plan) throws PlanningException {
    return plan.deleteReservation(reservationId);
  }

  private boolean computeAllocation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract,
      ReservationAllocation oldReservation) throws PlanningException,
      ContractValidationException {
    LOG.info("placing the following ReservationRequest: " + contract);

    Resource totalCapacity = plan.getTotalCapacity();

    // Here we can addd logic to adjust the ResourceDefinition to account for
    // system "imperfections" (e.g., scheduling delays for large containers).

    // Align with plan step conservatively (i.e., ceil arrival, and floor
    // deadline)
    long earliestStart = contract.getArrival();
    long step = plan.getStep();
    if (earliestStart % step != 0) {
      earliestStart = earliestStart + (step - (earliestStart % step));
    }
    long deadline =
        contract.getDeadline() - contract.getDeadline() % plan.getStep();

    // setup temporary variables to handle time-relations between stages and
    // intermediate answers
    long curDeadline = deadline;
    long oldDeadline = -1;

    Map<ReservationInterval, ReservationRequest> allocations =
        new HashMap<ReservationInterval, ReservationRequest>();
    RLESparseResourceAllocation tempAssigned =
        new RLESparseResourceAllocation(plan.getResourceCalculator(),
            plan.getMinimumAllocation());

    List<ReservationRequest> stages = contract.getReservationRequests()
        .getReservationResources();
    ReservationRequestInterpreter type = contract.getReservationRequests()
        .getInterpreter();

    // Iterate the stages in backward from deadline
    for (ListIterator<ReservationRequest> li = 
        stages.listIterator(stages.size()); li.hasPrevious();) {

      ReservationRequest currentReservationStage = li.previous();

      // validate the RR respect basic constraints
      validateInput(plan, currentReservationStage, totalCapacity);

      // run allocation for a single stage
      Map<ReservationInterval, ReservationRequest> curAlloc =
          placeSingleStage(plan, tempAssigned, currentReservationStage,
              earliestStart, curDeadline, oldReservation, totalCapacity);

      if (curAlloc == null) {
        // if we did not find an allocation for the currentReservationStage
        // return null, unless the ReservationDefinition we are placing is of
        // type ANY
        if (type != ReservationRequestInterpreter.R_ANY) {
          throw new PlanningException("The GreedyAgent"
              + " couldn't find a valid allocation for your request");
        } else {
          continue;
        }
      } else {

        // if we did find an allocation add it to the set of allocations
        allocations.putAll(curAlloc);

        // if this request is of type ANY we are done searching (greedy)
        // and can return the current allocation (break-out of the search)
        if (type == ReservationRequestInterpreter.R_ANY) {
          break;
        }

        // if the request is of ORDER or ORDER_NO_GAP we constraint the next
        // round of allocation to precede the current allocation, by setting
        // curDeadline
        if (type == ReservationRequestInterpreter.R_ORDER
            || type == ReservationRequestInterpreter.R_ORDER_NO_GAP) {
          curDeadline = findEarliestTime(curAlloc.keySet());

          // for ORDER_NO_GAP verify that the allocation found so far has no
          // gap, return null otherwise (the greedy procedure failed to find a
          // no-gap
          // allocation)
          if (type == ReservationRequestInterpreter.R_ORDER_NO_GAP
              && oldDeadline > 0) {
            if (oldDeadline - findLatestTime(curAlloc.keySet()) > plan
                .getStep()) {
              throw new PlanningException("The GreedyAgent"
                  + " couldn't find a valid allocation for your request");
            }
          }
          // keep the variable oldDeadline pointing to the last deadline we
          // found
          oldDeadline = curDeadline;
        }
      }
    }

    // / If we got here is because we failed to find an allocation for the
    // ReservationDefinition give-up and report failure to the user
    if (allocations.isEmpty()) {
      throw new PlanningException("The GreedyAgent"
          + " couldn't find a valid allocation for your request");
    }

    // create reservation with above allocations if not null/empty

    ReservationRequest ZERO_RES =
        ReservationRequest.newInstance(Resource.newInstance(0, 0), 0);

    long firstStartTime = findEarliestTime(allocations.keySet());
    
    // add zero-padding from arrival up to the first non-null allocation
    // to guarantee that the reservation exists starting at arrival
    if (firstStartTime > earliestStart) {
      allocations.put(new ReservationInterval(earliestStart,
          firstStartTime), ZERO_RES);
      firstStartTime = earliestStart;
      // consider to add trailing zeros at the end for simmetry
    }

    // Actually add/update the reservation in the plan.
    // This is subject to validation as other agents might be placing
    // in parallel and there might be sharing policies the agent is not
    // aware off.
    ReservationAllocation capReservation =
        new InMemoryReservationAllocation(reservationId, contract, user,
            plan.getQueueName(), firstStartTime,
            findLatestTime(allocations.keySet()), allocations,
            plan.getResourceCalculator(), plan.getMinimumAllocation());
    if (oldReservation != null) {
      return plan.updateReservation(capReservation);
    } else {
      return plan.addReservation(capReservation);
    }
  }

  private void validateInput(Plan plan, ReservationRequest rr,
      Resource totalCapacity) throws ContractValidationException {

    if (rr.getConcurrency() < 1) {
      throw new ContractValidationException("Gang Size should be >= 1");
    }

    if (rr.getNumContainers() <= 0) {
      throw new ContractValidationException("Num containers should be >= 0");
    }

    // check that gangSize and numContainers are compatible
    if (rr.getNumContainers() % rr.getConcurrency() != 0) {
      throw new ContractValidationException(
          "Parallelism must be an exact multiple of gang size");
    }

    // check that the largest container request does not exceed
    // the cluster-wide limit for container sizes
    if (Resources.greaterThan(plan.getResourceCalculator(), totalCapacity,
        rr.getCapability(), plan.getMaximumAllocation())) {
      throw new ContractValidationException("Individual"
          + " capability requests should not exceed cluster's maxAlloc");
    }
  }

  /**
   * This method actually perform the placement of an atomic stage of the
   * reservation. The key idea is to traverse the plan backward for a
   * "lease-duration" worth of time, and compute what is the maximum multiple of
   * our concurrency (gang) parameter we can fit. We do this and move towards
   * previous instant in time until the time-window is exhausted or we placed
   * all the user request.
   */
  private Map<ReservationInterval, ReservationRequest> placeSingleStage(
      Plan plan, RLESparseResourceAllocation tempAssigned,
      ReservationRequest rr, long earliestStart, long curDeadline,
      ReservationAllocation oldResAllocation, final Resource totalCapacity) {

    Map<ReservationInterval, ReservationRequest> allocationRequests =
        new HashMap<ReservationInterval, ReservationRequest>();

    // compute the gang as a resource and get the duration
    Resource gang = Resources.multiply(rr.getCapability(), rr.getConcurrency());
    long dur = rr.getDuration();
    long step = plan.getStep();

    // ceil the duration to the next multiple of the plan step
    if (dur % step != 0) {
      dur += (step - (dur % step));
    }

    // we know for sure that this division has no remainder (part of contract
    // with user, validate before
    int gangsToPlace = rr.getNumContainers() / rr.getConcurrency();

    int maxGang = 0;

    // loop trying to place until we are done, or we are considering
    // an invalid range of times
    while (gangsToPlace > 0 && curDeadline - dur >= earliestStart) {

      // as we run along we remember how many gangs we can fit, and what
      // was the most constraining moment in time (we will restart just
      // after that to place the next batch)
      maxGang = gangsToPlace;
      long minPoint = curDeadline;
      int curMaxGang = maxGang;

      // start placing at deadline (excluded due to [,) interval semantics and
      // move backward
      for (long t = curDeadline - plan.getStep(); t >= curDeadline - dur
          && maxGang > 0; t = t - plan.getStep()) {

        // As we run along we will logically remove the previous allocation for
        // this reservation
        // if one existed
        Resource oldResCap = Resource.newInstance(0, 0);
        if (oldResAllocation != null) {
          oldResCap = oldResAllocation.getResourcesAtTime(t);
        }

        // compute net available resources
        Resource netAvailableRes = Resources.clone(totalCapacity);
        Resources.addTo(netAvailableRes, oldResCap);
        Resources.subtractFrom(netAvailableRes,
            plan.getTotalCommittedResources(t));
        Resources.subtractFrom(netAvailableRes,
            tempAssigned.getCapacityAtTime(t));

        // compute maximum number of gangs we could fit
        curMaxGang =
            (int) Math.floor(Resources.divide(plan.getResourceCalculator(),
                totalCapacity, netAvailableRes, gang));

        // pick the minimum between available resources in this instant, and how
        // many gangs we have to place
        curMaxGang = Math.min(gangsToPlace, curMaxGang);

        // compare with previous max, and set it. also remember *where* we found
        // the minimum (useful for next attempts)
        if (curMaxGang <= maxGang) {
          maxGang = curMaxGang;
          minPoint = t;
        }
      }

      // if we were able to place any gang, record this, and decrement
      // gangsToPlace
      if (maxGang > 0) {
        gangsToPlace -= maxGang;

        ReservationInterval reservationInt =
            new ReservationInterval(curDeadline - dur, curDeadline);
        ReservationRequest reservationRes =
            ReservationRequest.newInstance(rr.getCapability(),
                rr.getConcurrency() * maxGang, rr.getConcurrency(),
                rr.getDuration());
        // remember occupied space (plan is read-only till we find a plausible
        // allocation for the entire request). This is needed since we might be
        // placing other ReservationRequest within the same
        // ReservationDefinition,
        // and we must avoid double-counting the available resources
        tempAssigned.addInterval(reservationInt, reservationRes);
        allocationRequests.put(reservationInt, reservationRes);

      }

      // reset our new starting point (curDeadline) to the most constraining
      // point so far, we will look "left" of that to find more places where
      // to schedule gangs (for sure nothing on the "right" of this point can
      // fit a full gang.
      curDeadline = minPoint;
    }

    // if no gangs are left to place we succeed and return the allocation
    if (gangsToPlace == 0) {
      return allocationRequests;
    } else {
      // If we are here is becasue we did not manage to satisfy this request.
      // So we need to remove unwanted side-effect from tempAssigned (needed
      // for ANY).
      for (Map.Entry<ReservationInterval, ReservationRequest> tempAllocation :
        allocationRequests.entrySet()) {
        tempAssigned.removeInterval(tempAllocation.getKey(),
            tempAllocation.getValue());
      }
      // and return null to signal failure in this allocation
      return null;
    }
  }

  // finds the leftmost point of this set of ReservationInterval
  private long findEarliestTime(Set<ReservationInterval> resInt) {
    long ret = Long.MAX_VALUE;
    for (ReservationInterval s : resInt) {
      if (s.getStartTime() < ret) {
        ret = s.getStartTime();
      }
    }
    return ret;
  }

  // finds the rightmost point of this set of ReservationIntervals
  private long findLatestTime(Set<ReservationInterval> resInt) {
    long ret = Long.MIN_VALUE;
    for (ReservationInterval s : resInt) {
      if (s.getEndTime() > ret) {
        ret = s.getEndTime();
      }
    }
    return ret;
  }

}

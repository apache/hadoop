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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation.RLEOperator;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Computes the stage allocation according to the greedy allocation rule. The
 * greedy rule repeatedly allocates requested containers at the rightmost
 * (latest) free interval.
 */

public class StageAllocatorGreedy implements StageAllocator {

  @Override
  public Map<ReservationInterval, Resource> computeStageAllocation(Plan plan,
      RLESparseResourceAllocation planLoads,
      RLESparseResourceAllocation planModifications, ReservationRequest rr,
      long stageEarliestStart, long stageDeadline, String user,
      ReservationId oldId) throws PlanningException {

    Resource totalCapacity = plan.getTotalCapacity();

    Map<ReservationInterval, Resource> allocationRequests =
        new HashMap<ReservationInterval, Resource>();

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

    RLESparseResourceAllocation netAvailable =
        plan.getAvailableResourceOverTime(user, oldId, stageEarliestStart,
            stageDeadline, 0);

    netAvailable =
        RLESparseResourceAllocation.merge(plan.getResourceCalculator(),
            plan.getTotalCapacity(), netAvailable, planModifications,
            RLEOperator.subtract, stageEarliestStart, stageDeadline);

    // loop trying to place until we are done, or we are considering
    // an invalid range of times
    while (gangsToPlace > 0 && stageDeadline - dur >= stageEarliestStart) {

      // as we run along we remember how many gangs we can fit, and what
      // was the most constraining moment in time (we will restart just
      // after that to place the next batch)
      maxGang = gangsToPlace;
      long minPoint = stageDeadline;
      int curMaxGang = maxGang;

      // start placing at deadline (excluded due to [,) interval semantics and
      // move backward
      for (long t = stageDeadline - plan.getStep(); t >= stageDeadline - dur
          && maxGang > 0; t = t - plan.getStep()) {

        Resource netAvailableRes = netAvailable.getCapacityAtTime(t);

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
            new ReservationInterval(stageDeadline - dur, stageDeadline);
        Resource reservationRes =
            Resources.multiply(rr.getCapability(), rr.getConcurrency()
                * maxGang);
        // remember occupied space (plan is read-only till we find a plausible
        // allocation for the entire request). This is needed since we might be
        // placing other ReservationRequest within the same
        // ReservationDefinition,
        // and we must avoid double-counting the available resources
        planModifications.addInterval(reservationInt, reservationRes);
        allocationRequests.put(reservationInt, reservationRes);

      }

      // reset our new starting point (curDeadline) to the most constraining
      // point so far, we will look "left" of that to find more places where
      // to schedule gangs (for sure nothing on the "right" of this point can
      // fit a full gang.
      stageDeadline = minPoint;
    }

    // if no gangs are left to place we succeed and return the allocation
    if (gangsToPlace == 0) {
      return allocationRequests;
    } else {
      // If we are here is becasue we did not manage to satisfy this request.
      // So we need to remove unwanted side-effect from tempAssigned (needed
      // for ANY).
      for (Map.Entry<ReservationInterval, Resource> tempAllocation
          : allocationRequests.entrySet()) {
        planModifications.removeInterval(tempAllocation.getKey(),
            tempAllocation.getValue());
      }
      // and return null to signal failure in this allocation
      return null;
    }

  }

}

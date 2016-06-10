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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation.RLEOperator;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Computes the stage allocation according to the greedy allocation rule. The
 * greedy rule repeatedly allocates requested containers at the leftmost or
 * rightmost possible interval. This implementation leverages the
 * run-length-encoding of the time-series we operate on and proceed more quickly
 * than the baseline.
 */

public class StageAllocatorGreedyRLE implements StageAllocator {

  private final boolean allocateLeft;

  public StageAllocatorGreedyRLE(boolean allocateLeft) {
    this.allocateLeft = allocateLeft;
  }

  @Override
  public Map<ReservationInterval, Resource> computeStageAllocation(Plan plan,
      Map<Long, Resource> planLoads,
      RLESparseResourceAllocation planModifications, ReservationRequest rr,
      long stageEarliestStart, long stageDeadline, String user,
      ReservationId oldId) throws PlanningException {

    // abort early if the interval is not satisfiable
    if (stageEarliestStart + rr.getDuration() > stageDeadline) {
      return null;
    }

    Map<ReservationInterval, Resource> allocationRequests =
        new HashMap<ReservationInterval, Resource>();

    Resource totalCapacity = plan.getTotalCapacity();

    // compute the gang as a resource and get the duration
    Resource sizeOfGang =
        Resources.multiply(rr.getCapability(), rr.getConcurrency());
    long dur = rr.getDuration();
    long step = plan.getStep();

    // ceil the duration to the next multiple of the plan step
    if (dur % step != 0) {
      dur += (step - (dur % step));
    }

    // we know for sure that this division has no remainder (part of contract
    // with user, validate before
    int gangsToPlace = rr.getNumContainers() / rr.getConcurrency();

    // get available resources from plan
    RLESparseResourceAllocation netRLERes =
        plan.getAvailableResourceOverTime(user, oldId, stageEarliestStart,
            stageDeadline);

    // remove plan modifications
    netRLERes =
        RLESparseResourceAllocation.merge(plan.getResourceCalculator(),
            totalCapacity, netRLERes, planModifications, RLEOperator.subtract,
            stageEarliestStart, stageDeadline);

    // loop trying to place until we are done, or we are considering
    // an invalid range of times
    while (gangsToPlace > 0 && stageEarliestStart + dur <= stageDeadline) {

      // as we run along we remember how many gangs we can fit, and what
      // was the most constraining moment in time (we will restart just
      // after that to place the next batch)
      int maxGang = gangsToPlace;
      long minPoint = -1;

      // focus our attention to a time-range under consideration
      NavigableMap<Long, Resource> partialMap =
          netRLERes.getRangeOverlapping(stageEarliestStart, stageDeadline)
              .getCumulative();

      // revert the map for right-to-left allocation
      if (!allocateLeft) {
        partialMap = partialMap.descendingMap();
      }

      Iterator<Entry<Long, Resource>> netIt = partialMap.entrySet().iterator();

      long oldT = stageDeadline;

      // internal loop, tries to allocate as many gang as possible starting
      // at a given point in time, if it fails we move to the next time
      // interval (with outside loop)
      while (maxGang > 0 && netIt.hasNext()) {

        long t;
        Resource curAvailRes;

        Entry<Long, Resource> e = netIt.next();
        if (allocateLeft) {
          t = Math.max(e.getKey(), stageEarliestStart);
          curAvailRes = e.getValue();
        } else {
          t = oldT;
          oldT = e.getKey();
          //attention: higher means lower, because we reversed the map direction
          curAvailRes = partialMap.higherEntry(t).getValue();
        }

        // check exit/skip conditions/
        if (curAvailRes == null) {
          //skip undefined regions (should not happen beside borders)
          continue;
        }
        if (exitCondition(t, stageEarliestStart, stageDeadline, dur)) {
          break;
        }

        // compute maximum number of gangs we could fit
        int curMaxGang =
            (int) Math.floor(Resources.divide(plan.getResourceCalculator(),
                totalCapacity, curAvailRes, sizeOfGang));
        curMaxGang = Math.min(gangsToPlace, curMaxGang);

        // compare with previous max, and set it. also remember *where* we found
        // the minimum (useful for next attempts)
        if (curMaxGang <= maxGang) {
          maxGang = curMaxGang;
          minPoint = t;
        }
      }

      // update data structures that retain the progress made so far
      gangsToPlace =
          trackProgress(planModifications, rr, stageEarliestStart,
              stageDeadline, allocationRequests, dur, gangsToPlace, maxGang);

      // reset the next range of time-intervals to deal with
      if (allocateLeft) {
        // set earliest start to the min of the constraining "range" or my the
        // end of this allocation
        if(partialMap.higherKey(minPoint) == null){
          stageEarliestStart = stageEarliestStart + dur;
        } else {
          stageEarliestStart =
             Math.min(partialMap.higherKey(minPoint), stageEarliestStart + dur);
        }
      } else {
        // same as above moving right-to-left
        if(partialMap.higherKey(minPoint) == null){
          stageDeadline = stageDeadline - dur;
        } else {
          stageDeadline =
              Math.max(partialMap.higherKey(minPoint), stageDeadline - dur);
        }
      }
    }

    // if no gangs are left to place we succeed and return the allocation
    if (gangsToPlace == 0) {
      return allocationRequests;
    } else {
      // If we are here is because we did not manage to satisfy this request.
      // So we need to remove unwanted side-effect from tempAssigned (needed
      // for ANY).
      for (Map.Entry<ReservationInterval, Resource> tempAllocation :
          allocationRequests.entrySet()) {
        planModifications.removeInterval(tempAllocation.getKey(),
            tempAllocation.getValue());
      }
      // and return null to signal failure in this allocation
      return null;
    }

  }

  private int trackProgress(RLESparseResourceAllocation planModifications,
      ReservationRequest rr, long stageEarliestStart, long stageDeadline,
      Map<ReservationInterval, Resource> allocationRequests, long dur,
      int gangsToPlace, int maxGang) {
    // if we were able to place any gang, record this, and decrement
    // gangsToPlace
    if (maxGang > 0) {
      gangsToPlace -= maxGang;

      ReservationInterval reservationInt =
          computeReservationInterval(stageEarliestStart, stageDeadline, dur);
      Resource reservationRes =
          Resources.multiply(rr.getCapability(), rr.getConcurrency() * maxGang);
      // remember occupied space (plan is read-only till we find a plausible
      // allocation for the entire request). This is needed since we might be
      // placing other ReservationRequest within the same
      // ReservationDefinition,
      // and we must avoid double-counting the available resources
      planModifications.addInterval(reservationInt, reservationRes);
      allocationRequests.put(reservationInt, reservationRes);

    }
    return gangsToPlace;
  }

  private ReservationInterval computeReservationInterval(
      long stageEarliestStart, long stageDeadline, long dur) {
    ReservationInterval reservationInt;
    if (allocateLeft) {
      reservationInt =
          new ReservationInterval(stageEarliestStart, stageEarliestStart + dur);
    } else {
      reservationInt =
          new ReservationInterval(stageDeadline - dur, stageDeadline);
    }
    return reservationInt;
  }


  private boolean exitCondition(long t, long stageEarliestStart,
      long stageDeadline, long dur) {
    if (allocateLeft) {
      return t >= stageEarliestStart + dur;
    } else {
      return t < stageDeadline - dur;
    }
  }
}

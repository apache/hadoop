/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *******************************************************************************/
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation.RLEOperator;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningQuotaException;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * This policy enforces a time-extended notion of Capacity. In particular it
 * guarantees that the allocation received in input when combined with all
 * previous allocation for the user does not violate an instantaneous max limit
 * on the resources received, and that for every window of time of length
 * validWindow, the integral of the allocations for a user (sum of the currently
 * submitted allocation and all prior allocations for the user) does not exceed
 * validWindow * maxAvg.
 *
 * This allows flexibility, in the sense that an allocation can instantaneously
 * use large portions of the available capacity, but prevents abuses by bounding
 * the average use over time.
 *
 * By controlling maxInst, maxAvg, validWindow the administrator configuring
 * this policy can obtain a behavior ranging from instantaneously enforced
 * capacity (akin to existing queues), or fully flexible allocations (likely
 * reserved to super-users, or trusted systems).
 */
@LimitedPrivate("yarn")
@Unstable
public class CapacityOverTimePolicy extends NoOverCommitPolicy {

  private ReservationSchedulerConfiguration conf;
  private long validWindow;
  private float maxInst;
  private float maxAvg;

  @Override
  public void init(String reservationQueuePath,
      ReservationSchedulerConfiguration conf) {
    this.conf = conf;
    validWindow = this.conf.getReservationWindow(reservationQueuePath);
    maxInst = this.conf.getInstantaneousMaxCapacity(reservationQueuePath) / 100;
    maxAvg = this.conf.getAverageCapacity(reservationQueuePath) / 100;
  }

  /**
   * The validation algorithm walks over the RLE encoded allocation and
   * checks that for all transition points (when the start or end of the
   * checking window encounters a value in the RLE). At this point it
   * checkes whether the integral computed exceeds the quota limit. Note that
   * this might not find the exact time of a violation, but if a violation
   * exists it will find it. The advantage is a much lower number of checks
   * as compared to time-slot by time-slot checks.
   *
   * @param plan the plan to validate against
   * @param reservation the reservation allocation to test.
   * @throws PlanningException if the validation fails.
   */
  @Override
  public void validate(Plan plan, ReservationAllocation reservation)
      throws PlanningException {


    // rely on NoOverCommitPolicy to check for: 1) user-match, 2) physical
    // cluster limits, and 3) maxInst (via override of available)
    try {
      super.validate(plan, reservation);
    } catch (PlanningException p) {
      //wrap it in proper quota exception
      throw new PlanningQuotaException(p);
    }

    long checkStart = reservation.getStartTime() - validWindow;
    long checkEnd = reservation.getEndTime() + validWindow;

    //---- check for integral violations of capacity --------

    // Gather a view of what to check (curr allocation of user, minus old
    // version of this reservation, plus new version)
    RLESparseResourceAllocation consumptionForUserOverTime =
        plan.getConsumptionForUserOverTime(reservation.getUser(),
            checkStart, checkEnd);

    ReservationAllocation old =
        plan.getReservationById(reservation.getReservationId());
    if (old != null) {
      consumptionForUserOverTime =
          RLESparseResourceAllocation.merge(plan.getResourceCalculator(),
              plan.getTotalCapacity(), consumptionForUserOverTime,
              old.getResourcesOverTime(checkStart, checkEnd), RLEOperator.add,
              checkStart, checkEnd);
    }

    RLESparseResourceAllocation resRLE =
        reservation.getResourcesOverTime(checkStart, checkEnd);

    RLESparseResourceAllocation toCheck = RLESparseResourceAllocation
        .merge(plan.getResourceCalculator(), plan.getTotalCapacity(),
            consumptionForUserOverTime, resRLE, RLEOperator.add, Long.MIN_VALUE,
            Long.MAX_VALUE);

    NavigableMap<Long, Resource> integralUp = new TreeMap<>();
    NavigableMap<Long, Resource> integralDown = new TreeMap<>();

    long prevTime = toCheck.getEarliestStartTime();
    IntegralResource prevResource = new IntegralResource(0L, 0L);
    IntegralResource runningTot = new IntegralResource(0L, 0L);

    // add intermediate points
    Map<Long, Resource> temp = new TreeMap<>();
    for (Map.Entry<Long, Resource> pointToCheck : toCheck.getCumulative()
        .entrySet()) {

      Long timeToCheck = pointToCheck.getKey();
      Resource resourceToCheck = pointToCheck.getValue();

      Long nextPoint = toCheck.getCumulative().higherKey(timeToCheck);
      if (nextPoint == null || toCheck.getCumulative().get(nextPoint) == null) {
        continue;
      }
      for (int i = 1; i <= (nextPoint - timeToCheck) / validWindow; i++) {
        temp.put(timeToCheck + (i * validWindow), resourceToCheck);
      }
    }
    temp.putAll(toCheck.getCumulative());

    // compute point-wise integral for the up-fronts and down-fronts
    for (Map.Entry<Long, Resource> currPoint : temp.entrySet()) {

      Long currTime = currPoint.getKey();
      Resource currResource = currPoint.getValue();

      //add to running total current contribution
      prevResource.multiplyBy(currTime - prevTime);
      runningTot.add(prevResource);
      integralUp.put(currTime, normalizeToResource(runningTot, validWindow));
      integralDown.put(currTime + validWindow,
          normalizeToResource(runningTot, validWindow));

      if (currResource != null) {
        prevResource.memory = currResource.getMemorySize();
        prevResource.vcores = currResource.getVirtualCores();
      } else {
        prevResource.memory = 0L;
        prevResource.vcores = 0L;
      }
      prevTime = currTime;
    }

    // compute final integral as delta of up minus down transitions
    RLESparseResourceAllocation intUp =
        new RLESparseResourceAllocation(integralUp,
            plan.getResourceCalculator());
    RLESparseResourceAllocation intDown =
        new RLESparseResourceAllocation(integralDown,
            plan.getResourceCalculator());

    RLESparseResourceAllocation integral = RLESparseResourceAllocation
        .merge(plan.getResourceCalculator(), plan.getTotalCapacity(), intUp,
            intDown, RLEOperator.subtract, Long.MIN_VALUE, Long.MAX_VALUE);

    // define over-time integral limit
    // note: this is aligned with the normalization done above
    NavigableMap<Long, Resource> tlimit = new TreeMap<>();
    Resource maxAvgRes = Resources.multiply(plan.getTotalCapacity(), maxAvg);
    tlimit.put(toCheck.getEarliestStartTime() - validWindow, maxAvgRes);
    RLESparseResourceAllocation targetLimit =
        new RLESparseResourceAllocation(tlimit, plan.getResourceCalculator());

    // compare using merge() limit with integral
    try {

      RLESparseResourceAllocation.merge(plan.getResourceCalculator(),
          plan.getTotalCapacity(), targetLimit, integral,
          RLEOperator.subtractTestNonNegative, checkStart, checkEnd);

    } catch (PlanningException p) {
      throw new PlanningQuotaException(
          "Integral (avg over time) quota capacity " + maxAvg
              + " over a window of " + validWindow / 1000 + " seconds, "
              + " would be exceeded by accepting reservation: " + reservation
              .getReservationId(), p);
    }
  }

  private Resource normalizeToResource(IntegralResource runningTot,
      long window) {
    // normalize to fit in windows. Rounding should not impact more than
    // sub 1 core average allocations. This will all be removed once
    // Resource moves to long.
    int memory = (int) Math.round((double) runningTot.memory / window);
    int vcores = (int) Math.round((double) runningTot.vcores / window);
    return Resource.newInstance(memory, vcores);
  }

  @Override
  public RLESparseResourceAllocation availableResources(
      RLESparseResourceAllocation available, Plan plan, String user,
      ReservationId oldId, long start, long end) throws PlanningException {

    // this only propagates the instantaneous maxInst properties, while
    // the time-varying one depends on the current allocation as well
    // and are not easily captured here
    Resource planTotalCapacity = plan.getTotalCapacity();
    Resource maxInsRes = Resources.multiply(planTotalCapacity, maxInst);
    NavigableMap<Long, Resource> instQuota = new TreeMap<Long, Resource>();
    instQuota.put(start, maxInsRes);

    RLESparseResourceAllocation instRLEQuota =
        new RLESparseResourceAllocation(instQuota,
            plan.getResourceCalculator());

    RLESparseResourceAllocation used =
        plan.getConsumptionForUserOverTime(user, start, end);

    // add back in old reservation used resources if any
    ReservationAllocation old = plan.getReservationById(oldId);
    if (old != null) {
      used = RLESparseResourceAllocation.merge(plan.getResourceCalculator(),
          Resources.clone(plan.getTotalCapacity()), used,
          old.getResourcesOverTime(start, end), RLEOperator.subtract, start,
          end);
    }

    instRLEQuota = RLESparseResourceAllocation
        .merge(plan.getResourceCalculator(), planTotalCapacity, instRLEQuota,
            used, RLEOperator.subtract, start, end);

    instRLEQuota = RLESparseResourceAllocation
        .merge(plan.getResourceCalculator(), planTotalCapacity, available,
            instRLEQuota, RLEOperator.min, start, end);

    return instRLEQuota;
  }

  @Override
  public long getValidWindow() {
    return validWindow;
  }

  /**
   * This class provides support for Resource-like book-keeping, based on
   * long(s), as using Resource to store the "integral" of the allocation over
   * time leads to integer overflows for large allocations/clusters. (Evolving
   * Resource to use long is too disruptive at this point.)
   *
   * The comparison/multiplication behaviors of IntegralResource are consistent
   * with the DefaultResourceCalculator.
   */
  private static class IntegralResource {
    long memory;
    long vcores;

    public IntegralResource(Resource resource) {
      this.memory = resource.getMemorySize();
      this.vcores = resource.getVirtualCores();
    }

    public IntegralResource(long mem, long vcores) {
      this.memory = mem;
      this.vcores = vcores;
    }

    public void add(Resource r) {
      memory += r.getMemorySize();
      vcores += r.getVirtualCores();
    }

    public void add(IntegralResource r) {
      memory += r.memory;
      vcores += r.vcores;
    }

    public void subtract(Resource r) {
      memory -= r.getMemorySize();
      vcores -= r.getVirtualCores();
    }

    public IntegralResource negate() {
      return new IntegralResource(-memory, -vcores);
    }

    public void multiplyBy(long window) {
      memory = memory * window;
      vcores = vcores * window;
    }

    public long compareTo(IntegralResource other) {
      long diff = memory - other.memory;
      if (diff == 0) {
        diff = vcores - other.vcores;
      }
      return diff;
    }

    @Override
    public String toString() {
      return "<memory:" + memory + ", vCores:" + vcores + ">";
    }

  }

}

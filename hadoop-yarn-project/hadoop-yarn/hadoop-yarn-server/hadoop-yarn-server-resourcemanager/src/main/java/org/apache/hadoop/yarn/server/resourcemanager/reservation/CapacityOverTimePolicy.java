/*******************************************************************************
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *******************************************************************************/
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import java.util.Date;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation.RLEOperator;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningQuotaException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.ResourceOverCommitException;
import org.apache.hadoop.yarn.util.resource.Resources;

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
public class CapacityOverTimePolicy implements SharingPolicy {

  private ReservationSchedulerConfiguration conf;
  private long validWindow;
  private float maxInst;
  private float maxAvg;

  // For now this is CapacityScheduler specific, but given a hierarchy in the
  // configuration structure of the schedulers (e.g., SchedulerConfiguration)
  // it should be easy to remove this limitation
  @Override
  public void init(String reservationQueuePath,
      ReservationSchedulerConfiguration conf) {
    this.conf = conf;
    validWindow = this.conf.getReservationWindow(reservationQueuePath);
    maxInst = this.conf.getInstantaneousMaxCapacity(reservationQueuePath) / 100;
    maxAvg = this.conf.getAverageCapacity(reservationQueuePath) / 100;
  };

  @Override
  public void validate(Plan plan, ReservationAllocation reservation)
      throws PlanningException {

    // this is entire method invoked under a write-lock on the plan, no need
    // to synchronize accesses to the plan further

    // Try to verify whether there is already a reservation with this ID in
    // the system (remove its contribution during validation to simulate a
    // try-n-swap
    // update).
    ReservationAllocation oldReservation =
        plan.getReservationById(reservation.getReservationId());

    long startTime = reservation.getStartTime();
    long endTime = reservation.getEndTime();
    long step = plan.getStep();

    Resource planTotalCapacity = plan.getTotalCapacity();

    Resource maxAvgRes = Resources.multiply(planTotalCapacity, maxAvg);
    Resource maxInsRes = Resources.multiply(planTotalCapacity, maxInst);

    // define variable that will store integral of resources (need diff class to
    // avoid overflow issues for long/large allocations)
    IntegralResource runningTot = new IntegralResource(0L, 0L);
    IntegralResource maxAllowed = new IntegralResource(maxAvgRes);
    maxAllowed.multiplyBy(validWindow / step);

    RLESparseResourceAllocation userCons =
        plan.getConsumptionForUserOverTime(reservation.getUser(), startTime
            - validWindow, endTime + validWindow);

    // check that the resources offered to the user during any window of length
    // "validWindow" overlapping this allocation are within maxAllowed
    // also enforce instantaneous and physical constraints during this pass
    for (long t = startTime - validWindow; t < endTime + validWindow; t += step) {

      Resource currExistingAllocTot = plan.getTotalCommittedResources(t);
      Resource currExistingAllocForUser = userCons.getCapacityAtTime(t);
      Resource currNewAlloc = reservation.getResourcesAtTime(t);
      Resource currOldAlloc = Resources.none();
      if (oldReservation != null) {
        currOldAlloc = oldReservation.getResourcesAtTime(t);
      }

      // throw exception if the cluster is overcommitted
      // tot_allocated - old + new > capacity
      Resource inst =
          Resources.subtract(Resources.add(currExistingAllocTot, currNewAlloc),
              currOldAlloc);
      if (Resources.greaterThan(plan.getResourceCalculator(),
          planTotalCapacity, inst, planTotalCapacity)) {
        throw new ResourceOverCommitException(" Resources at time " + t
            + " would be overcommitted (" + inst + " over "
            + plan.getTotalCapacity() + ") by accepting reservation: "
            + reservation.getReservationId());
      }

      // throw exception if instantaneous limits are violated
      // tot_alloc_to_this_user - old + new > inst_limit
      if (Resources.greaterThan(plan.getResourceCalculator(),
          planTotalCapacity, Resources.subtract(
              Resources.add(currExistingAllocForUser, currNewAlloc),
              currOldAlloc), maxInsRes)) {
        throw new PlanningQuotaException("Instantaneous quota capacity "
            + maxInst + " would be passed at time " + t
            + " by accepting reservation: " + reservation.getReservationId());
      }

      // throw exception if the running integral of utilization over validWindow
      // is violated. We perform a delta check, adding/removing instants at the
      // boundary of the window from runningTot.

      // runningTot = previous_runningTot + currExistingAllocForUser +
      // currNewAlloc - currOldAlloc - pastNewAlloc - pastOldAlloc;

      // Where:
      // 1) currNewAlloc, currExistingAllocForUser represent the contribution of
      // the instant in time added in this pass.
      // 2) pastNewAlloc, pastOldAlloc are the contributions relative to time
      // instants that are being retired from the the window
      // 3) currOldAlloc is the contribution (if any) of the previous version of
      // this reservation (the one we are updating)

      runningTot.add(currExistingAllocForUser);
      runningTot.add(currNewAlloc);
      runningTot.subtract(currOldAlloc);

      // expire contributions from instant in time before (t - validWindow)
      if (t > startTime) {
        Resource pastOldAlloc = userCons.getCapacityAtTime(t - validWindow);
        Resource pastNewAlloc = reservation.getResourcesAtTime(t - validWindow);

        // runningTot = runningTot - pastExistingAlloc - pastNewAlloc;
        runningTot.subtract(pastOldAlloc);
        runningTot.subtract(pastNewAlloc);
      }

      // check integral
      // runningTot > maxAvg * validWindow
      // NOTE: we need to use comparator of IntegralResource directly, as
      // Resource and ResourceCalculator assume "int" amount of resources,
      // which is not sufficient when comparing integrals (out-of-bound)
      if (maxAllowed.compareTo(runningTot) < 0) {
        throw new PlanningQuotaException(
            "Integral (avg over time) quota capacity " + maxAvg
                + " over a window of " + validWindow / 1000 + " seconds, "
                + " would be passed at time " + t + "(" + new Date(t)
                + ") by accepting reservation: "
                + reservation.getReservationId());
      }
    }
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
      used =
          RLESparseResourceAllocation.merge(plan.getResourceCalculator(),
              Resources.clone(plan.getTotalCapacity()), used,
              old.getResourcesOverTime(), RLEOperator.subtract, start, end);
    }

    instRLEQuota =
        RLESparseResourceAllocation.merge(plan.getResourceCalculator(),
            planTotalCapacity, instRLEQuota, used, RLEOperator.subtract, start,
            end);

    instRLEQuota =
        RLESparseResourceAllocation.merge(plan.getResourceCalculator(),
            planTotalCapacity, available, instRLEQuota, RLEOperator.min, start,
            end);

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

    public void subtract(Resource r) {
      memory -= r.getMemorySize();
      vcores -= r.getVirtualCores();
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

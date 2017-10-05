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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeSet;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation.RLEOperator;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * A stage allocator that iteratively allocates containers in the
 * {@link DurationInterval} with lowest overall cost. The algorithm only
 * considers non-overlapping intervals of length 'duration'. This guarantees
 * that the allocations are aligned. If 'allocateLeft == true', the intervals
 * considered by the algorithm are aligned to stageArrival; otherwise, they are
 * aligned to stageDeadline. The smoothnessFactor parameter controls the number
 * of containers that are simultaneously allocated in each iteration of the
 * algorithm.
 */

public class StageAllocatorLowCostAligned implements StageAllocator {

  private final boolean allocateLeft;
  // Smoothness factor
  private int smoothnessFactor = 10;

  // Constructor
  public StageAllocatorLowCostAligned(boolean allocateLeft) {
    this.allocateLeft = allocateLeft;
  }

  // Constructor
  public StageAllocatorLowCostAligned(int smoothnessFactor,
      boolean allocateLeft) {
    this.allocateLeft = allocateLeft;
    this.smoothnessFactor = smoothnessFactor;
  }

  @Override
  public Map<ReservationInterval, Resource> computeStageAllocation(Plan plan,
      RLESparseResourceAllocation planLoads,
      RLESparseResourceAllocation planModifications, ReservationRequest rr,
      long stageArrival, long stageDeadline, long period, String user,
      ReservationId oldId) throws PlanningException {

    // Initialize
    ResourceCalculator resCalc = plan.getResourceCalculator();
    Resource capacity = plan.getTotalCapacity();

    RLESparseResourceAllocation netRLERes = plan.getAvailableResourceOverTime(
        user, oldId, stageArrival, stageDeadline, period);

    long step = plan.getStep();

    // Create allocationRequestsearlies
    RLESparseResourceAllocation allocationRequests =
        new RLESparseResourceAllocation(plan.getResourceCalculator());

    // Initialize parameters
    long duration = stepRoundUp(rr.getDuration(), step);
    int windowSizeInDurations =
        (int) ((stageDeadline - stageArrival) / duration);
    int totalGangs = rr.getNumContainers() / rr.getConcurrency();
    int numContainersPerGang = rr.getConcurrency();
    Resource gang =
        Resources.multiply(rr.getCapability(), numContainersPerGang);

    // Set maxGangsPerUnit
    int maxGangsPerUnit = (int) Math
        .max(Math.floor(((double) totalGangs) / windowSizeInDurations), 1);
    maxGangsPerUnit = Math.max(maxGangsPerUnit / smoothnessFactor, 1);

    // If window size is too small, return null
    if (windowSizeInDurations <= 0) {
      return null;
    }

    final int preferLeft = allocateLeft ? 1 : -1;

    // Initialize tree sorted by costs
    TreeSet<DurationInterval> durationIntervalsSortedByCost =
        new TreeSet<DurationInterval>(new Comparator<DurationInterval>() {
          @Override
          public int compare(DurationInterval val1, DurationInterval val2) {

            int cmp = Double.compare(val1.getTotalCost(), val2.getTotalCost());
            if (cmp != 0) {
              return cmp;
            }

            return preferLeft
                * Long.compare(val1.getEndTime(), val2.getEndTime());
          }
        });

    List<Long> intervalEndTimes =
        computeIntervalEndTimes(stageArrival, stageDeadline, duration);

    // Add durationIntervals that end at (endTime - n*duration) for some n.
    for (long intervalEnd : intervalEndTimes) {

      long intervalStart = intervalEnd - duration;

      // Get duration interval [intervalStart,intervalEnd)
      DurationInterval durationInterval =
          getDurationInterval(intervalStart, intervalEnd, planLoads,
              planModifications, capacity, netRLERes, resCalc, step, gang);

      // If the interval can fit a gang, add it to the tree
      if (durationInterval.canAllocate()) {
        durationIntervalsSortedByCost.add(durationInterval);
      }
    }

    // Allocate
    int remainingGangs = totalGangs;
    while (remainingGangs > 0) {

      // If no durationInterval can fit a gang, break and return null
      if (durationIntervalsSortedByCost.isEmpty()) {
        break;
      }

      // Get best duration interval
      DurationInterval bestDurationInterval =
          durationIntervalsSortedByCost.first();
      int numGangsToAllocate = Math.min(maxGangsPerUnit, remainingGangs);
      numGangsToAllocate =
          Math.min(numGangsToAllocate, bestDurationInterval.numCanFit());
      // Add it
      remainingGangs -= numGangsToAllocate;

      ReservationInterval reservationInt =
          new ReservationInterval(bestDurationInterval.getStartTime(),
              bestDurationInterval.getEndTime());

      Resource reservationRes = Resources.multiply(rr.getCapability(),
          rr.getConcurrency() * numGangsToAllocate);

      planModifications.addInterval(reservationInt, reservationRes);
      allocationRequests.addInterval(reservationInt, reservationRes);

      // Remove from tree
      durationIntervalsSortedByCost.remove(bestDurationInterval);

      // Get updated interval
      DurationInterval updatedDurationInterval =
          getDurationInterval(bestDurationInterval.getStartTime(),
              bestDurationInterval.getStartTime() + duration, planLoads,
              planModifications, capacity, netRLERes, resCalc, step, gang);

      // Add to tree, if possible
      if (updatedDurationInterval.canAllocate()) {
        durationIntervalsSortedByCost.add(updatedDurationInterval);
      }

    }

    // Get the final allocation
    Map<ReservationInterval, Resource> allocations =
        allocationRequests.toIntervalMap();

    // If no gangs are left to place we succeed and return the allocation
    if (remainingGangs <= 0) {
      return allocations;
    } else {

      // If we are here is because we did not manage to satisfy this
      // request.
      // We remove unwanted side-effect from planModifications (needed for
      // ANY).
      for (Map.Entry<ReservationInterval, Resource> tempAllocation : allocations
          .entrySet()) {

        planModifications.removeInterval(tempAllocation.getKey(),
            tempAllocation.getValue());

      }
      // Return null to signal failure in this allocation
      return null;

    }

  }

  private List<Long> computeIntervalEndTimes(long stageEarliestStart,
      long stageDeadline, long duration) {

    List<Long> intervalEndTimes = new ArrayList<Long>();
    if (!allocateLeft) {
      for (long intervalEnd = stageDeadline; intervalEnd >= stageEarliestStart
          + duration; intervalEnd -= duration) {
        intervalEndTimes.add(intervalEnd);
      }
    } else {
      for (long intervalStart =
          stageEarliestStart; intervalStart <= stageDeadline
              - duration; intervalStart += duration) {
        intervalEndTimes.add(intervalStart + duration);
      }
    }

    return intervalEndTimes;
  }

  protected static DurationInterval getDurationInterval(long startTime,
      long endTime, RLESparseResourceAllocation planLoads,
      RLESparseResourceAllocation planModifications, Resource capacity,
      RLESparseResourceAllocation netRLERes, ResourceCalculator resCalc,
      long step, Resource requestedResources) throws PlanningException {

    // Get the total cost associated with the duration interval
    double totalCost = getDurationIntervalTotalCost(startTime, endTime,
        planLoads, planModifications, capacity, resCalc, step);

    // Calculate how many gangs can fit, i.e., how many times can 'capacity'
    // be allocated within the duration interval [startTime, endTime)
    int gangsCanFit = getDurationIntervalGangsCanFit(startTime, endTime,
        planModifications, capacity, netRLERes, resCalc, requestedResources);

    // Return the desired durationInterval
    return new DurationInterval(startTime, endTime, totalCost, gangsCanFit);

  }

  protected static double getDurationIntervalTotalCost(long startTime,
      long endTime, RLESparseResourceAllocation planLoads,
      RLESparseResourceAllocation planModifications, Resource capacity,
      ResourceCalculator resCalc, long step) throws PlanningException {

    // Compute the current resource load within the interval [startTime,endTime)
    // by adding planLoads (existing load) and planModifications (load that
    // corresponds to the current job).
    RLESparseResourceAllocation currentLoad =
        RLESparseResourceAllocation.merge(resCalc, capacity, planLoads,
            planModifications, RLEOperator.add, startTime, endTime);

    // Convert load from RLESparseResourceAllocation to a Map representation
    NavigableMap<Long, Resource> mapCurrentLoad = currentLoad.getCumulative();

    // Initialize auxiliary variables
    double totalCost = 0.0;
    Long tPrev = -1L;
    Resource loadPrev = Resources.none();
    double cost = 0.0;

    // Iterate over time points. For each point 't', accumulate the total cost
    // that corresponds to the interval [tPrev, t). The cost associated within
    // this interval is fixed for each of the time steps, therefore the cost of
    // a single step is multiplied by (t - tPrev) / step.
    for (Entry<Long, Resource> e : mapCurrentLoad.entrySet()) {
      Long t = e.getKey();
      Resource load = e.getValue();
      if (tPrev != -1L) {
        tPrev = Math.max(tPrev, startTime);
        cost = calcCostOfLoad(loadPrev, capacity, resCalc);
        totalCost = totalCost + cost * (t - tPrev) / step;
      }

      tPrev = t;
      loadPrev = load;
    }

    // Add the cost associated with the last interval (the for loop does not
    // calculate it).
    if (loadPrev != null) {

      // This takes care of the corner case of a single entry
      tPrev = Math.max(tPrev, startTime);
      cost = calcCostOfLoad(loadPrev, capacity, resCalc);
      totalCost = totalCost + cost * (endTime - tPrev) / step;
    }

    // Return the overall cost
    return totalCost;
  }

  protected static int getDurationIntervalGangsCanFit(long startTime,
      long endTime, RLESparseResourceAllocation planModifications,
      Resource capacity, RLESparseResourceAllocation netRLERes,
      ResourceCalculator resCalc, Resource requestedResources)
      throws PlanningException {

    // Initialize auxiliary variables
    int gangsCanFit = Integer.MAX_VALUE;
    int curGangsCanFit;

    // Calculate the total amount of available resources between startTime
    // and endTime, by subtracting planModifications from netRLERes
    RLESparseResourceAllocation netAvailableResources =
        RLESparseResourceAllocation.merge(resCalc, capacity, netRLERes,
            planModifications, RLEOperator.subtractTestNonNegative, startTime,
            endTime);

    // Convert result to a map
    NavigableMap<Long, Resource> mapAvailableCapacity =
        netAvailableResources.getCumulative();

    // Iterate over the map representation.
    // At each point, calculate how many times does 'requestedResources' fit.
    // The result is the minimum over all time points.
    for (Entry<Long, Resource> e : mapAvailableCapacity.entrySet()) {
      Long t = e.getKey();
      Resource curAvailable = e.getValue();
      if (t >= endTime) {
        break;
      }

      if (curAvailable == null) {
        gangsCanFit = 0;
      } else {
        curGangsCanFit = (int) Math.floor(Resources.divide(resCalc, capacity,
            curAvailable, requestedResources));
        if (curGangsCanFit < gangsCanFit) {
          gangsCanFit = curGangsCanFit;
        }
      }
    }
    return gangsCanFit;
  }

  protected double calcCostOfInterval(long startTime, long endTime,
      RLESparseResourceAllocation planLoads,
      RLESparseResourceAllocation planModifications, Resource capacity,
      ResourceCalculator resCalc, long step) {

    // Sum costs in the interval [startTime,endTime)
    double totalCost = 0.0;
    for (long t = startTime; t < endTime; t += step) {
      totalCost += calcCostOfTimeSlot(t, planLoads, planModifications, capacity,
          resCalc);
    }

    // Return sum
    return totalCost;

  }

  protected double calcCostOfTimeSlot(long t,
      RLESparseResourceAllocation planLoads,
      RLESparseResourceAllocation planModifications, Resource capacity,
      ResourceCalculator resCalc) {

    // Get the current load at time t
    Resource load = getLoadAtTime(t, planLoads, planModifications);

    // Return cost
    return calcCostOfLoad(load, capacity, resCalc);

  }

  protected Resource getLoadAtTime(long t,
      RLESparseResourceAllocation planLoads,
      RLESparseResourceAllocation planModifications) {

    Resource planLoad = planLoads.getCapacityAtTime(t);

    return Resources.add(planLoad, planModifications.getCapacityAtTime(t));

  }

  protected static double calcCostOfLoad(Resource load, Resource capacity,
      ResourceCalculator resCalc) {

    return resCalc.ratio(load, capacity);

  }

  protected static long stepRoundDown(long t, long step) {
    return (t / step) * step;
  }

  protected static long stepRoundUp(long t, long step) {
    return ((t + step - 1) / step) * step;
  }

  /**
   * An inner class that represents an interval, typically of length duration.
   * The class holds the total cost of the interval and the maximal load inside
   * the interval in each dimension (both calculated externally).
   */
  protected static class DurationInterval {

    private long startTime;
    private long endTime;
    private double cost;
    private final int gangsCanFit;

    // Constructor
    public DurationInterval(long startTime, long endTime, double cost,
        int gangsCanfit) {
      this.startTime = startTime;
      this.endTime = endTime;
      this.cost = cost;
      this.gangsCanFit = gangsCanfit;
    }

    // canAllocate() - boolean function, returns whether requestedResources
    // can be allocated during the durationInterval without
    // violating capacity constraints
    public boolean canAllocate() {
      return (gangsCanFit > 0);
    }

    // numCanFit() - returns the maximal number of requestedResources can be
    // allocated during the durationInterval without violating
    // capacity constraints

    public int numCanFit() {
      return gangsCanFit;
    }

    public long getStartTime() {
      return this.startTime;
    }

    public void setStartTime(long value) {
      this.startTime = value;
    }

    public long getEndTime() {
      return this.endTime;
    }

    public void setEndTime(long value) {
      this.endTime = value;
    }

    public double getTotalCost() {
      return this.cost;
    }

    public void setTotalCost(double value) {
      this.cost = value;
    }

    @Override
    public String toString() {

      StringBuilder sb = new StringBuilder();

      sb.append(" start: " + startTime).append(" end: " + endTime)
          .append(" cost: " + cost).append(" gangsCanFit: " + gangsCanFit);

      return sb.toString();

    }

  }
}

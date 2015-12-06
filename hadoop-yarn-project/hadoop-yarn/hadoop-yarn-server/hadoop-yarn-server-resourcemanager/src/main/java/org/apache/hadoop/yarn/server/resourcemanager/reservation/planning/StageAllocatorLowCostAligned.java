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

import java.util.Comparator;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * A stage allocator that iteratively allocates containers in the
 * {@link DurationInterval} with lowest overall cost. The algorithm only
 * considers intervals of the form: [stageDeadline - (n+1)*duration,
 * stageDeadline - n*duration) for an integer n. This guarantees that the
 * allocations are aligned (as opposed to overlapping duration intervals).
 *
 * The smoothnessFactor parameter controls the number of containers that are
 * simultaneously allocated in each iteration of the algorithm.
 */

public class StageAllocatorLowCostAligned implements StageAllocator {

  // Smoothness factor
  private int smoothnessFactor = 10;

  // Constructor
  public StageAllocatorLowCostAligned() {
  }

  // Constructor
  public StageAllocatorLowCostAligned(int smoothnessFactor) {
    this.smoothnessFactor = smoothnessFactor;
  }

  // computeJobAllocation()
  @Override
  public Map<ReservationInterval, Resource> computeStageAllocation(
      Plan plan, Map<Long, Resource> planLoads,
      RLESparseResourceAllocation planModifications, ReservationRequest rr,
      long stageEarliestStart, long stageDeadline, String user,
      ReservationId oldId) {

    // Initialize
    ResourceCalculator resCalc = plan.getResourceCalculator();
    Resource capacity = plan.getTotalCapacity();
    long step = plan.getStep();

    // Create allocationRequestsearlies
    RLESparseResourceAllocation allocationRequests =
        new RLESparseResourceAllocation(plan.getResourceCalculator());

    // Initialize parameters
    long duration = stepRoundUp(rr.getDuration(), step);
    int windowSizeInDurations =
        (int) ((stageDeadline - stageEarliestStart) / duration);
    int totalGangs = rr.getNumContainers() / rr.getConcurrency();
    int numContainersPerGang = rr.getConcurrency();
    Resource gang =
        Resources.multiply(rr.getCapability(), numContainersPerGang);

    // Set maxGangsPerUnit
    int maxGangsPerUnit =
        (int) Math.max(
            Math.floor(((double) totalGangs) / windowSizeInDurations), 1);
    maxGangsPerUnit = Math.max(maxGangsPerUnit / smoothnessFactor, 1);

    // If window size is too small, return null
    if (windowSizeInDurations <= 0) {
      return null;
    }

    // Initialize tree sorted by costs
    TreeSet<DurationInterval> durationIntervalsSortedByCost =
        new TreeSet<DurationInterval>(new Comparator<DurationInterval>() {
          @Override
          public int compare(DurationInterval val1, DurationInterval val2) {

            int cmp = Double.compare(val1.getTotalCost(), val2.getTotalCost());
            if (cmp != 0) {
              return cmp;
            }

            return (-1) * Long.compare(val1.getEndTime(), val2.getEndTime());
          }
        });

    // Add durationIntervals that end at (endTime - n*duration) for some n.
    for (long intervalEnd = stageDeadline; intervalEnd >= stageEarliestStart
        + duration; intervalEnd -= duration) {

      long intervalStart = intervalEnd - duration;

      // Get duration interval [intervalStart,intervalEnd)
      DurationInterval durationInterval =
          getDurationInterval(intervalStart, intervalEnd, planLoads,
              planModifications, capacity, resCalc, step);

      // If the interval can fit a gang, add it to the tree
      if (durationInterval.canAllocate(gang, capacity, resCalc)) {
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
          Math.min(numGangsToAllocate,
              bestDurationInterval.numCanFit(gang, capacity, resCalc));
      // Add it
      remainingGangs -= numGangsToAllocate;

      ReservationInterval reservationInt =
          new ReservationInterval(bestDurationInterval.getStartTime(),
              bestDurationInterval.getEndTime());

      Resource reservationRes =
          Resources.multiply(rr.getCapability(), rr.getConcurrency()
              * numGangsToAllocate);

      planModifications.addInterval(reservationInt, reservationRes);
      allocationRequests.addInterval(reservationInt, reservationRes);

      // Remove from tree
      durationIntervalsSortedByCost.remove(bestDurationInterval);

      // Get updated interval
      DurationInterval updatedDurationInterval =
          getDurationInterval(bestDurationInterval.getStartTime(),
              bestDurationInterval.getStartTime() + duration, planLoads,
              planModifications, capacity, resCalc, step);

      // Add to tree, if possible
      if (updatedDurationInterval.canAllocate(gang, capacity, resCalc)) {
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

      // If we are here is because we did not manage to satisfy this request.
      // We remove unwanted side-effect from planModifications (needed for ANY).
      for (Map.Entry<ReservationInterval, Resource> tempAllocation
          : allocations.entrySet()) {

        planModifications.removeInterval(tempAllocation.getKey(),
            tempAllocation.getValue());

      }
      // Return null to signal failure in this allocation
      return null;

    }

  }

  protected DurationInterval getDurationInterval(long startTime, long endTime,
      Map<Long, Resource> planLoads,
      RLESparseResourceAllocation planModifications, Resource capacity,
      ResourceCalculator resCalc, long step) {

    // Initialize the dominant loads structure
    Resource dominantResources = Resource.newInstance(0, 0);

    // Calculate totalCost and maxLoad
    double totalCost = 0.0;
    for (long t = startTime; t < endTime; t += step) {

      // Get the load
      Resource load = getLoadAtTime(t, planLoads, planModifications);

      // Increase the total cost
      totalCost += calcCostOfLoad(load, capacity, resCalc);

      // Update the dominant resources
      dominantResources = Resources.componentwiseMax(dominantResources, load);

    }

    // Return the corresponding durationInterval
    return new DurationInterval(startTime, endTime, totalCost,
        dominantResources);

  }

  protected double calcCostOfInterval(long startTime, long endTime,
      Map<Long, Resource> planLoads,
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

  protected double calcCostOfTimeSlot(long t, Map<Long, Resource> planLoads,
      RLESparseResourceAllocation planModifications, Resource capacity,
      ResourceCalculator resCalc) {

    // Get the current load at time t
    Resource load = getLoadAtTime(t, planLoads, planModifications);

    // Return cost
    return calcCostOfLoad(load, capacity, resCalc);

  }

  protected Resource getLoadAtTime(long t, Map<Long, Resource> planLoads,
      RLESparseResourceAllocation planModifications) {

    Resource planLoad = planLoads.get(t);
    planLoad = (planLoad == null) ? Resource.newInstance(0, 0) : planLoad;

    return Resources.add(planLoad, planModifications.getCapacityAtTime(t));

  }

  protected double calcCostOfLoad(Resource load, Resource capacity,
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
    private Resource maxLoad;

    // Constructor
    public DurationInterval(long startTime, long endTime, double cost,
        Resource maxLoad) {
      this.startTime = startTime;
      this.endTime = endTime;
      this.cost = cost;
      this.maxLoad = maxLoad;
    }

    // canAllocate() - boolean function, returns whether requestedResources
    // can be allocated during the durationInterval without
    // violating capacity constraints
    public boolean canAllocate(Resource requestedResources, Resource capacity,
        ResourceCalculator resCalc) {

      Resource updatedMaxLoad = Resources.add(maxLoad, requestedResources);
      return (resCalc.compare(capacity, updatedMaxLoad, capacity) <= 0);

    }

    // numCanFit() - returns the maximal number of requestedResources can be
    // allocated during the durationInterval without violating
    // capacity constraints
    public int numCanFit(Resource requestedResources, Resource capacity,
        ResourceCalculator resCalc) {

      // Represents the largest resource demand that can be satisfied throughout
      // the entire DurationInterval (i.e., during [startTime,endTime))
      Resource availableResources = Resources.subtract(capacity, maxLoad);

      // Maximal number of requestedResources that fit inside the interval
      return (int) Math.floor(Resources.divide(resCalc, capacity,
          availableResources, requestedResources));

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

    public Resource getMaxLoad() {
      return this.maxLoad;
    }

    public void setMaxLoad(Resource value) {
      this.maxLoad = value;
    }

    public double getTotalCost() {
      return this.cost;
    }

    public void setTotalCost(double value) {
      this.cost = value;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(" start: " + startTime).append(" end: " + endTime)
          .append(" cost: " + cost).append(" maxLoad: " + maxLoad);
      return sb.toString();
    }
  }
}

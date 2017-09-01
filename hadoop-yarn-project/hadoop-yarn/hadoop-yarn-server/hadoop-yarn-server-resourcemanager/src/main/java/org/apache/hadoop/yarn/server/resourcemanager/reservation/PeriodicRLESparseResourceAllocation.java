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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * This data structure stores a periodic {@link RLESparseResourceAllocation}.
 * Default period is 1 day (86400000ms).
 */
public class PeriodicRLESparseResourceAllocation
    extends RLESparseResourceAllocation {

  // Log
  private static final Logger LOG =
      LoggerFactory.getLogger(PeriodicRLESparseResourceAllocation.class);

  private long timePeriod;

  /**
   * Constructor.
   *
   * @param resourceCalculator {@link ResourceCalculator} the resource
   *          calculator to use.
   * @param timePeriod Time period in milliseconds.
   */
  public PeriodicRLESparseResourceAllocation(
      ResourceCalculator resourceCalculator, Long timePeriod) {
    super(resourceCalculator);
    this.timePeriod = timePeriod;
  }

  /**
   * Constructor. Default time period set to 1 day.
   *
   * @param resourceCalculator {@link ResourceCalculator} the resource
   *          calculator to use..
   */
  public PeriodicRLESparseResourceAllocation(
      ResourceCalculator resourceCalculator) {
    this(resourceCalculator,
        YarnConfiguration.DEFAULT_RM_RESERVATION_SYSTEM_MAX_PERIODICITY);
  }

  /**
   * Constructor.
   *
   * @param rleVector {@link RLESparseResourceAllocation} with the run-length
   *          encoded data.
   * @param timePeriod Time period in milliseconds.
   */
  @VisibleForTesting
  public PeriodicRLESparseResourceAllocation(
      RLESparseResourceAllocation rleVector, Long timePeriod) {
    super(rleVector.getCumulative(), rleVector.getResourceCalculator());
    this.timePeriod = timePeriod;

    // make sure the PeriodicRLE is zero-based, and handles wrap-around
    long delta = (getEarliestStartTime() % timePeriod - getEarliestStartTime());
    shift(delta);

    List<Long> toRemove = new ArrayList<>();
    Map<Long, Resource> toAdd = new TreeMap<>();

    for (Map.Entry<Long, Resource> entry : cumulativeCapacity.entrySet()) {
      if (entry.getKey() > timePeriod) {
        toRemove.add(entry.getKey());
        if (entry.getValue() != null) {
          toAdd.put(timePeriod, entry.getValue());
          long prev = entry.getKey() % timePeriod;
          toAdd.put(prev, this.getCapacityAtTime(prev));
          toAdd.put(0L, entry.getValue());
        }
      }
    }
    for (Long l : toRemove) {
      cumulativeCapacity.remove(l);
    }
    cumulativeCapacity.putAll(toAdd);
  }

  /**
   * Get capacity at time based on periodic repetition.
   *
   * @param tick UTC time for which the allocated {@link Resource} is queried.
   * @return {@link Resource} allocated at specified time
   */
  public Resource getCapacityAtTime(long tick) {
    long convertedTime = (tick % timePeriod);
    return super.getCapacityAtTime(convertedTime);
  }

  /**
   * Add resource for the specified interval. This function will be used by
   * {@link InMemoryPlan} while placing reservations between 0 and timePeriod.
   * The interval may include 0, but the end time must be strictly less than
   * timePeriod.
   *
   * @param interval {@link ReservationInterval} to which the specified resource
   *          is to be added.
   * @param resource {@link Resource} to be added to the interval specified.
   * @return true if addition is successful, false otherwise
   */
  public boolean addInterval(ReservationInterval interval, Resource resource) {
    long startTime = interval.getStartTime();
    long endTime = interval.getEndTime();

    if (startTime >= 0 && endTime > startTime && endTime <= timePeriod) {
      return super.addInterval(interval, resource);
    } else {
      LOG.info("Cannot set capacity beyond end time: " + timePeriod + " was ("
          + interval.toString() + ")");
      return false;
    }
  }

  /**
   * Removes a resource for the specified interval.
   *
   * @param interval the {@link ReservationInterval} for which the resource is
   *          to be removed.
   * @param resource the {@link Resource} to be removed.
   * @return true if removal is successful, false otherwise
   */
  public boolean removeInterval(ReservationInterval interval,
      Resource resource) {
    long startTime = interval.getStartTime();
    long endTime = interval.getEndTime();
    // If the resource to be subtracted is less than the minimum resource in
    // the range, abort removal to avoid negative capacity.
    // TODO revesit decrementing endTime
    if (!Resources.fitsIn(resource, getMinimumCapacityInInterval(
        new ReservationInterval(startTime, endTime - 1)))) {
      LOG.info("Request to remove more resources than what is available");
      return false;
    }
    if (startTime >= 0 && endTime > startTime && endTime <= timePeriod) {
      return super.removeInterval(interval, resource);
    } else {
      LOG.info("Interval extends beyond the end time " + timePeriod);
      return false;
    }
  }

  /**
   * Get maximum capacity at periodic offsets from the specified time.
   *
   * @param tick UTC time base from which offsets are specified for finding the
   *          maximum capacity.
   * @param period periodic offset at which capacities are evaluated.
   * @return the maximum {@link Resource} across the specified time instants.
   * @return true if removal is successful, false otherwise
   */
  public Resource getMaximumPeriodicCapacity(long tick, long period) {
    Resource maxResource;
    if (period < timePeriod) {
      maxResource = super.getMaximumPeriodicCapacity(tick % timePeriod, period);
    } else {
      // if period is greater than the length of PeriodicRLESparseAllocation,
      // only a single value exists in this interval.
      maxResource = super.getCapacityAtTime(tick % timePeriod);
    }
    return maxResource;
  }

  /**
   * Get time period of PeriodicRLESparseResourceAllocation.
   *
   * @return timePeriod time period represented in ms.
   */
  public long getTimePeriod() {
    return this.timePeriod;
  }

  @Override
  public String toString() {
    StringBuilder ret = new StringBuilder();
    ret.append("Period: ").append(timePeriod).append("\n")
        .append(super.toString());
    if (super.isEmpty()) {
      ret.append(" no allocations\n");
    }
    return ret.toString();
  }

  @Override
  public RLESparseResourceAllocation getRangeOverlapping(long start, long end) {
    NavigableMap<Long, Resource> unrolledMap = new TreeMap<>();
    readLock.lock();
    try {
      long relativeStart = (start >= 0) ? start % timePeriod : 0;
      NavigableMap<Long, Resource> cumulativeMap = this.getCumulative();
      Long previous = cumulativeMap.floorKey(relativeStart);
      previous = (previous != null) ? previous : 0;
      for (long i = 0; i <= (end - start) / timePeriod; i++) {
        for (Map.Entry<Long, Resource> e : cumulativeMap.entrySet()) {
          long curKey = e.getKey() + (i * timePeriod);
          if (curKey >= previous && (start + curKey - relativeStart) <= end) {
            unrolledMap.put(curKey, e.getValue());
          }
        }
      }
      RLESparseResourceAllocation rle =
          new RLESparseResourceAllocation(unrolledMap, getResourceCalculator());
      rle.shift(start - relativeStart);
      return rle;
    } finally {
      readLock.unlock();
    }
  }

}

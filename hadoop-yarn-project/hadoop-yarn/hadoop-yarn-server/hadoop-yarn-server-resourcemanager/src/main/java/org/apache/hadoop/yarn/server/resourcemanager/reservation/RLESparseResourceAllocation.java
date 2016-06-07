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

import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.gson.stream.JsonWriter;

/**
 * This is a run length encoded sparse data structure that maintains resource
 * allocations over time.
 */
public class RLESparseResourceAllocation {

  private static final int THRESHOLD = 100;
  private static final Resource ZERO_RESOURCE = Resources.none();

  private NavigableMap<Long, Resource> cumulativeCapacity =
      new TreeMap<Long, Resource>();

  private final ReentrantReadWriteLock readWriteLock =
      new ReentrantReadWriteLock();
  private final Lock readLock = readWriteLock.readLock();
  private final Lock writeLock = readWriteLock.writeLock();

  private final ResourceCalculator resourceCalculator;

  public RLESparseResourceAllocation(ResourceCalculator resourceCalculator) {
    this.resourceCalculator = resourceCalculator;
  }

  public RLESparseResourceAllocation(NavigableMap<Long, Resource> out,
      ResourceCalculator resourceCalculator) {
    // miss check for repeated entries
    this.cumulativeCapacity = out;
    this.resourceCalculator = resourceCalculator;
  }

  /**
   * Add a resource for the specified interval.
   *
   * @param reservationInterval the interval for which the resource is to be
   *          added
   * @param totCap the resource to be added
   * @return true if addition is successful, false otherwise
   */
  public boolean addInterval(ReservationInterval reservationInterval,
      Resource totCap) {
    if (totCap.equals(ZERO_RESOURCE)) {
      return true;
    }
    writeLock.lock();
    try {
      NavigableMap<Long, Resource> addInt = new TreeMap<Long, Resource>();
      addInt.put(reservationInterval.getStartTime(), totCap);
      addInt.put(reservationInterval.getEndTime(), ZERO_RESOURCE);
      try {
        cumulativeCapacity =
            merge(resourceCalculator, totCap, cumulativeCapacity, addInt,
                Long.MIN_VALUE, Long.MAX_VALUE, RLEOperator.add);
      } catch (PlanningException e) {
        // never happens for add
      }
      return true;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Removes a resource for the specified interval.
   *
   * @param reservationInterval the interval for which the resource is to be
   *          removed
   * @param totCap the resource to be removed
   * @return true if removal is successful, false otherwise
   */
  public boolean removeInterval(ReservationInterval reservationInterval,
      Resource totCap) {
    if (totCap.equals(ZERO_RESOURCE)) {
      return true;
    }
    writeLock.lock();
    try {

      NavigableMap<Long, Resource> removeInt = new TreeMap<Long, Resource>();
      removeInt.put(reservationInterval.getStartTime(), totCap);
      removeInt.put(reservationInterval.getEndTime(), ZERO_RESOURCE);
      try {
        cumulativeCapacity =
            merge(resourceCalculator, totCap, cumulativeCapacity, removeInt,
                Long.MIN_VALUE, Long.MAX_VALUE, RLEOperator.subtract);
      } catch (PlanningException e) {
        // never happens for subtract
      }
      return true;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Returns the capacity, i.e. total resources allocated at the specified point
   * of time.
   *
   * @return the resources allocated at the specified time
   */
  public Resource getCapacityAtTime(long tick) {
    readLock.lock();
    try {
      Entry<Long, Resource> closestStep = cumulativeCapacity.floorEntry(tick);
      if (closestStep != null) {
        return Resources.clone(closestStep.getValue());
      }
      return Resources.clone(ZERO_RESOURCE);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get the timestamp of the earliest resource allocation.
   *
   * @return the timestamp of the first resource allocation
   */
  public long getEarliestStartTime() {
    readLock.lock();
    try {
      if (cumulativeCapacity.isEmpty()) {
        return -1;
      } else {
        return cumulativeCapacity.firstKey();
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get the timestamp of the latest non-null resource allocation.
   *
   * @return the timestamp of the last resource allocation
   */
  public long getLatestNonNullTime() {
    readLock.lock();
    try {
      if (cumulativeCapacity.isEmpty()) {
        return -1;
      } else {
        // the last entry might contain null (to terminate
        // the sequence)... return previous one.
        Entry<Long, Resource> last = cumulativeCapacity.lastEntry();
        if (last.getValue() == null) {
          return cumulativeCapacity.floorKey(last.getKey() - 1);
        } else {
          return last.getKey();
        }
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Returns true if there are no non-zero entries.
   *
   * @return true if there are no allocations or false otherwise
   */
  public boolean isEmpty() {
    readLock.lock();
    try {
      if (cumulativeCapacity.isEmpty()) {
        return true;
      }
      // Deletion leaves a single zero entry with a null at the end so check for
      // that
      if (cumulativeCapacity.size() == 2) {
        return cumulativeCapacity.firstEntry().getValue().equals(ZERO_RESOURCE)
            && cumulativeCapacity.lastEntry().getValue() == null;
      }
      return false;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public String toString() {
    StringBuilder ret = new StringBuilder();
    readLock.lock();
    try {
      if (cumulativeCapacity.size() > THRESHOLD) {
        ret.append("Number of steps: ").append(cumulativeCapacity.size())
            .append(" earliest entry: ").append(cumulativeCapacity.firstKey())
            .append(" latest entry: ").append(cumulativeCapacity.lastKey());
      } else {
        for (Map.Entry<Long, Resource> r : cumulativeCapacity.entrySet()) {
          ret.append(r.getKey()).append(": ").append(r.getValue())
              .append("\n ");
        }
      }
      return ret.toString();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Returns the JSON string representation of the current resources allocated
   * over time.
   *
   * @return the JSON string representation of the current resources allocated
   *         over time
   */
  public String toMemJSONString() {
    StringWriter json = new StringWriter();
    JsonWriter jsonWriter = new JsonWriter(json);
    readLock.lock();
    try {
      jsonWriter.beginObject();
      // jsonWriter.name("timestamp").value("resource");
      for (Map.Entry<Long, Resource> r : cumulativeCapacity.entrySet()) {
        jsonWriter.name(r.getKey().toString()).value(r.getValue().toString());
      }
      jsonWriter.endObject();
      jsonWriter.close();
      return json.toString();
    } catch (IOException e) {
      // This should not happen
      return "";
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Returns the representation of the current resources allocated over time as
   * an interval map (in the defined non-null range).
   *
   * @return the representation of the current resources allocated over time as
   *         an interval map.
   */
  public Map<ReservationInterval, Resource> toIntervalMap() {

    readLock.lock();
    try {
      Map<ReservationInterval, Resource> allocations =
          new TreeMap<ReservationInterval, Resource>();

      // Empty
      if (isEmpty()) {
        return allocations;
      }

      Map.Entry<Long, Resource> lastEntry = null;
      for (Map.Entry<Long, Resource> entry : cumulativeCapacity.entrySet()) {

        if (lastEntry != null && entry.getValue() != null) {
          ReservationInterval interval =
              new ReservationInterval(lastEntry.getKey(), entry.getKey());
          Resource resource = lastEntry.getValue();

          allocations.put(interval, resource);
        }

        lastEntry = entry;
      }
      return allocations;
    } finally {
      readLock.unlock();
    }
  }

  public NavigableMap<Long, Resource> getCumulative() {
    readLock.lock();
    try {
      return cumulativeCapacity;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Merges the range start to end of two {@code RLESparseResourceAllocation}
   * using a given {@code RLEOperator}.
   *
   * @param resCalc the resource calculator
   * @param clusterResource the total cluster resources (for DRF)
   * @param a the left operand
   * @param b the right operand
   * @param operator the operator to be applied during merge
   * @param start the start-time of the range to be considered
   * @param end the end-time of the range to be considered
   * @return the a merged RLESparseResourceAllocation, produced by applying
   *         "operator" to "a" and "b"
   * @throws PlanningException in case the operator is subtractTestPositive and
   *           the result would contain a negative value
   */
  public static RLESparseResourceAllocation merge(ResourceCalculator resCalc,
      Resource clusterResource, RLESparseResourceAllocation a,
      RLESparseResourceAllocation b, RLEOperator operator, long start, long end)
      throws PlanningException {
    NavigableMap<Long, Resource> cumA =
        a.getRangeOverlapping(start, end).getCumulative();
    NavigableMap<Long, Resource> cumB =
        b.getRangeOverlapping(start, end).getCumulative();
    NavigableMap<Long, Resource> out =
        merge(resCalc, clusterResource, cumA, cumB, start, end, operator);
    return new RLESparseResourceAllocation(out, resCalc);
  }

  private static NavigableMap<Long, Resource> merge(ResourceCalculator resCalc,
      Resource clusterResource, NavigableMap<Long, Resource> a,
      NavigableMap<Long, Resource> b, long start, long end,
      RLEOperator operator) throws PlanningException {

    // handle special cases of empty input
    if (a == null || a.isEmpty()) {
      if (operator == RLEOperator.subtract
          || operator == RLEOperator.subtractTestNonNegative) {
        return negate(operator, b);
      } else {
        return b;
      }
    }
    if (b == null || b.isEmpty()) {
      return a;
    }

    // define iterators and support variables
    Iterator<Entry<Long, Resource>> aIt = a.entrySet().iterator();
    Iterator<Entry<Long, Resource>> bIt = b.entrySet().iterator();
    Entry<Long, Resource> curA = aIt.next();
    Entry<Long, Resource> curB = bIt.next();
    Entry<Long, Resource> lastA = null;
    Entry<Long, Resource> lastB = null;
    boolean aIsDone = false;
    boolean bIsDone = false;

    TreeMap<Long, Resource> out = new TreeMap<Long, Resource>();

    while (!(curA.equals(lastA) && curB.equals(lastB))) {

      Resource outRes;
      long time = -1;

      // curA is smaller than curB
      if (bIsDone || (curA.getKey() < curB.getKey() && !aIsDone)) {
        outRes = combineValue(operator, resCalc, clusterResource, curA, lastB);
        time = (curA.getKey() < start) ? start : curA.getKey();
        lastA = curA;
        if (aIt.hasNext()) {
          curA = aIt.next();
        } else {
          aIsDone = true;
        }

      } else {
        // curB is smaller than curA
        if (aIsDone || (curA.getKey() > curB.getKey() && !bIsDone)) {
          outRes =
              combineValue(operator, resCalc, clusterResource, lastA, curB);
          time = (curB.getKey() < start) ? start : curB.getKey();
          lastB = curB;
          if (bIt.hasNext()) {
            curB = bIt.next();
          } else {
            bIsDone = true;
          }

        } else {
          // curA is equal to curB
          outRes = combineValue(operator, resCalc, clusterResource, curA, curB);
          time = (curA.getKey() < start) ? start : curA.getKey();
          lastA = curA;
          if (aIt.hasNext()) {
            curA = aIt.next();
          } else {
            aIsDone = true;
          }
          lastB = curB;
          if (bIt.hasNext()) {
            curB = bIt.next();
          } else {
            bIsDone = true;
          }
        }
      }

      // add to out if not redundant
      addIfNeeded(out, time, outRes);
    }
    addIfNeeded(out, end, null);

    return out;
  }

  private static NavigableMap<Long, Resource> negate(RLEOperator operator,
      NavigableMap<Long, Resource> a) throws PlanningException {

    TreeMap<Long, Resource> out = new TreeMap<Long, Resource>();
    for (Entry<Long, Resource> e : a.entrySet()) {
      Resource val = Resources.negate(e.getValue());
      // test for negative value and throws
      if (operator == RLEOperator.subtractTestNonNegative
          && (Resources.fitsIn(val, ZERO_RESOURCE) &&
              !Resources.equals(val, ZERO_RESOURCE))) {
        throw new PlanningException(
            "RLESparseResourceAllocation: merge failed as the "
                + "resulting RLESparseResourceAllocation would be negative");
      }
      out.put(e.getKey(), val);
    }

    return out;
  }

  private static void addIfNeeded(TreeMap<Long, Resource> out, long time,
      Resource outRes) {

    if (out.isEmpty() || (out.lastEntry() != null && outRes == null)
        || !Resources.equals(out.lastEntry().getValue(), outRes)) {
      out.put(time, outRes);
    }

  }

  private static Resource combineValue(RLEOperator op,
      ResourceCalculator resCalc, Resource clusterResource,
      Entry<Long, Resource> eA, Entry<Long, Resource> eB)
      throws PlanningException {

    // deal with nulls
    if (eA == null || eA.getValue() == null) {
      if (eB == null || eB.getValue() == null) {
        return null;
      }
      if (op == RLEOperator.subtract) {
        return Resources.negate(eB.getValue());
      } else {
        return eB.getValue();
      }
    }
    if (eB == null || eB.getValue() == null) {
      return eA.getValue();
    }

    Resource a = eA.getValue();
    Resource b = eB.getValue();
    switch (op) {
    case add:
      return Resources.add(a, b);
    case subtract:
      return Resources.subtract(a, b);
    case subtractTestNonNegative:
      if (!Resources.fitsIn(b, a)) {
        throw new PlanningException(
            "RLESparseResourceAllocation: merge failed as the "
                + "resulting RLESparseResourceAllocation would be negative");
      } else {
        return Resources.subtract(a, b);
      }
    case min:
      return Resources.min(resCalc, clusterResource, a, b);
    case max:
      return Resources.max(resCalc, clusterResource, a, b);
    default:
      return null;
    }

  }

  public RLESparseResourceAllocation getRangeOverlapping(long start, long end) {
    readLock.lock();
    try {
      NavigableMap<Long, Resource> a = this.getCumulative();

      if (a != null && !a.isEmpty()) {
        // include the portion of previous entry that overlaps start
        if (start > a.firstKey()) {
          long previous = a.floorKey(start);
          a = a.tailMap(previous, true);
        }

        if (end < a.lastKey()) {
          a = a.headMap(end, true);
        }

      }
      RLESparseResourceAllocation ret =
          new RLESparseResourceAllocation(a, resourceCalculator);
      return ret;
    } finally {
      readLock.unlock();
    }

  }

  /**
   * The set of operators that can be applied to two
   * {@code RLESparseResourceAllocation} during a merge operation.
   */
  public enum RLEOperator {
    add, subtract, min, max, subtractTestNonNegative
  }

}

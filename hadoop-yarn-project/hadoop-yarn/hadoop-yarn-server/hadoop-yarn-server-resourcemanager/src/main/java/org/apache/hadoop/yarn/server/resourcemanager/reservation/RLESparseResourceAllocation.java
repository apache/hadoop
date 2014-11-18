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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.gson.stream.JsonWriter;

/**
 * This is a run length encoded sparse data structure that maintains resource
 * allocations over time
 */
public class RLESparseResourceAllocation {

  private static final int THRESHOLD = 100;
  private static final Resource ZERO_RESOURCE = Resource.newInstance(0, 0);

  private TreeMap<Long, Resource> cumulativeCapacity =
      new TreeMap<Long, Resource>();

  private final ReentrantReadWriteLock readWriteLock =
      new ReentrantReadWriteLock();
  private final Lock readLock = readWriteLock.readLock();
  private final Lock writeLock = readWriteLock.writeLock();

  private final ResourceCalculator resourceCalculator;
  private final Resource minAlloc;

  public RLESparseResourceAllocation(ResourceCalculator resourceCalculator,
      Resource minAlloc) {
    this.resourceCalculator = resourceCalculator;
    this.minAlloc = minAlloc;
  }

  private boolean isSameAsPrevious(Long key, Resource capacity) {
    Entry<Long, Resource> previous = cumulativeCapacity.lowerEntry(key);
    return (previous != null && previous.getValue().equals(capacity));
  }

  private boolean isSameAsNext(Long key, Resource capacity) {
    Entry<Long, Resource> next = cumulativeCapacity.higherEntry(key);
    return (next != null && next.getValue().equals(capacity));
  }

  /**
   * Add a resource for the specified interval
   * 
   * @param reservationInterval the interval for which the resource is to be
   *          added
   * @param capacity the resource to be added
   * @return true if addition is successful, false otherwise
   */
  public boolean addInterval(ReservationInterval reservationInterval,
      ReservationRequest capacity) {
    Resource totCap =
        Resources.multiply(capacity.getCapability(),
            (float) capacity.getNumContainers());
    if (totCap.equals(ZERO_RESOURCE)) {
      return true;
    }
    writeLock.lock();
    try {
      long startKey = reservationInterval.getStartTime();
      long endKey = reservationInterval.getEndTime();
      NavigableMap<Long, Resource> ticks =
          cumulativeCapacity.headMap(endKey, false);
      if (ticks != null && !ticks.isEmpty()) {
        Resource updatedCapacity = Resource.newInstance(0, 0);
        Entry<Long, Resource> lowEntry = ticks.floorEntry(startKey);
        if (lowEntry == null) {
          // This is the earliest starting interval
          cumulativeCapacity.put(startKey, totCap);
        } else {
          updatedCapacity = Resources.add(lowEntry.getValue(), totCap);
          // Add a new tick only if the updated value is different
          // from the previous tick
          if ((startKey == lowEntry.getKey())
              && (isSameAsPrevious(lowEntry.getKey(), updatedCapacity))) {
            cumulativeCapacity.remove(lowEntry.getKey());
          } else {
            cumulativeCapacity.put(startKey, updatedCapacity);
          }
        }
        // Increase all the capacities of overlapping intervals
        Set<Entry<Long, Resource>> overlapSet =
            ticks.tailMap(startKey, false).entrySet();
        for (Entry<Long, Resource> entry : overlapSet) {
          updatedCapacity = Resources.add(entry.getValue(), totCap);
          entry.setValue(updatedCapacity);
        }
      } else {
        // This is the first interval to be added
        cumulativeCapacity.put(startKey, totCap);
      }
      Resource nextTick = cumulativeCapacity.get(endKey);
      if (nextTick != null) {
        // If there is overlap, remove the duplicate entry
        if (isSameAsPrevious(endKey, nextTick)) {
          cumulativeCapacity.remove(endKey);
        }
      } else {
        // Decrease capacity as this is end of the interval
        cumulativeCapacity.put(endKey, Resources.subtract(cumulativeCapacity
            .floorEntry(endKey).getValue(), totCap));
      }
      return true;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Add multiple resources for the specified interval
   * 
   * @param reservationInterval the interval for which the resource is to be
   *          added
   * @param ReservationRequests the resources to be added
   * @param clusterResource the total resources in the cluster
   * @return true if addition is successful, false otherwise
   */
  public boolean addCompositeInterval(ReservationInterval reservationInterval,
      List<ReservationRequest> ReservationRequests, Resource clusterResource) {
    ReservationRequest aggregateReservationRequest =
        Records.newRecord(ReservationRequest.class);
    Resource capacity = Resource.newInstance(0, 0);
    for (ReservationRequest ReservationRequest : ReservationRequests) {
      Resources.addTo(capacity, Resources.multiply(
          ReservationRequest.getCapability(),
          ReservationRequest.getNumContainers()));
    }
    aggregateReservationRequest.setNumContainers((int) Math.ceil(Resources
        .divide(resourceCalculator, clusterResource, capacity, minAlloc)));
    aggregateReservationRequest.setCapability(minAlloc);

    return addInterval(reservationInterval, aggregateReservationRequest);
  }

  /**
   * Removes a resource for the specified interval
   * 
   * @param reservationInterval the interval for which the resource is to be
   *          removed
   * @param capacity the resource to be removed
   * @return true if removal is successful, false otherwise
   */
  public boolean removeInterval(ReservationInterval reservationInterval,
      ReservationRequest capacity) {
    Resource totCap =
        Resources.multiply(capacity.getCapability(),
            (float) capacity.getNumContainers());
    if (totCap.equals(ZERO_RESOURCE)) {
      return true;
    }
    writeLock.lock();
    try {
      long startKey = reservationInterval.getStartTime();
      long endKey = reservationInterval.getEndTime();
      // update the start key
      NavigableMap<Long, Resource> ticks =
          cumulativeCapacity.headMap(endKey, false);
      // Decrease all the capacities of overlapping intervals
      SortedMap<Long, Resource> overlapSet = ticks.tailMap(startKey);
      if (overlapSet != null && !overlapSet.isEmpty()) {
        Resource updatedCapacity = Resource.newInstance(0, 0);
        long currentKey = -1;
        for (Iterator<Entry<Long, Resource>> overlapEntries =
            overlapSet.entrySet().iterator(); overlapEntries.hasNext();) {
          Entry<Long, Resource> entry = overlapEntries.next();
          currentKey = entry.getKey();
          updatedCapacity = Resources.subtract(entry.getValue(), totCap);
          // update each entry between start and end key
          cumulativeCapacity.put(currentKey, updatedCapacity);
        }
        // Remove the first overlap entry if it is same as previous after
        // updation
        Long firstKey = overlapSet.firstKey();
        if (isSameAsPrevious(firstKey, overlapSet.get(firstKey))) {
          cumulativeCapacity.remove(firstKey);
        }
        // Remove the next entry if it is same as end entry after updation
        if ((currentKey != -1) && (isSameAsNext(currentKey, updatedCapacity))) {
          cumulativeCapacity.remove(cumulativeCapacity.higherKey(currentKey));
        }
      }
      return true;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Returns the capacity, i.e. total resources allocated at the specified point
   * of time
   * 
   * @param tick the time (UTC in ms) at which the capacity is requested
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
   * Get the timestamp of the earliest resource allocation
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
   * Get the timestamp of the latest resource allocation
   * 
   * @return the timestamp of the last resource allocation
   */
  public long getLatestEndTime() {
    readLock.lock();
    try {
      if (cumulativeCapacity.isEmpty()) {
        return -1;
      } else {
        return cumulativeCapacity.lastKey();
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Returns true if there are no non-zero entries
   * 
   * @return true if there are no allocations or false otherwise
   */
  public boolean isEmpty() {
    readLock.lock();
    try {
      if (cumulativeCapacity.isEmpty()) {
        return true;
      }
      // Deletion leaves a single zero entry so check for that
      if (cumulativeCapacity.size() == 1) {
        return cumulativeCapacity.firstEntry().getValue().equals(ZERO_RESOURCE);
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
   * over time
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

}

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

import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * An in memory implementation of a reservation allocation using the
 * {@link RLESparseResourceAllocation}
 *
 */
public class InMemoryReservationAllocation implements ReservationAllocation {

  private final String planName;
  private final ReservationId reservationID;
  private final String user;
  private final ReservationDefinition contract;
  private final long startTime;
  private final long endTime;
  private final Map<ReservationInterval, Resource> allocationRequests;
  private boolean hasGang = false;
  private long acceptedAt = -1;
  private long periodicity = 0;

  private RLESparseResourceAllocation resourcesOverTime;

  public InMemoryReservationAllocation(ReservationId reservationID,
      ReservationDefinition contract, String user, String planName,
      long startTime, long endTime,
      Map<ReservationInterval, Resource> allocations,
      ResourceCalculator calculator, Resource minAlloc) {
    this(reservationID, contract, user, planName, startTime, endTime,
        allocations, calculator, minAlloc, false);
  }

  public InMemoryReservationAllocation(ReservationId reservationID,
      ReservationDefinition contract, String user, String planName,
      long startTime, long endTime,
      Map<ReservationInterval, Resource> allocations,
      ResourceCalculator calculator, Resource minAlloc, boolean hasGang) {
    this.contract = contract;
    this.startTime = startTime;
    this.endTime = endTime;
    this.reservationID = reservationID;
    this.user = user;
    this.allocationRequests = allocations;
    this.planName = planName;
    this.hasGang = hasGang;
    if (contract != null && contract.getRecurrenceExpression() != null) {
      this.periodicity = Long.parseLong(contract.getRecurrenceExpression());
    }
    if (periodicity > 0) {
      resourcesOverTime =
          new PeriodicRLESparseResourceAllocation(calculator, periodicity);
    } else {
      resourcesOverTime = new RLESparseResourceAllocation(calculator);
    }
    for (Map.Entry<ReservationInterval, Resource> r : allocations.entrySet()) {
      resourcesOverTime.addInterval(r.getKey(), r.getValue());
    }
  }

  @Override
  public ReservationId getReservationId() {
    return reservationID;
  }

  @Override
  public ReservationDefinition getReservationDefinition() {
    return contract;
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  @Override
  public long getEndTime() {
    return endTime;
  }

  @Override
  public Map<ReservationInterval, Resource> getAllocationRequests() {
    return Collections.unmodifiableMap(allocationRequests);
  }

  @Override
  public String getPlanName() {
    return planName;
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public boolean containsGangs() {
    return hasGang;
  }

  @Override
  public void setAcceptanceTimestamp(long acceptedAt) {
    this.acceptedAt = acceptedAt;
  }

  @Override
  public long getAcceptanceTime() {
    return acceptedAt;
  }

  @Override
  public Resource getResourcesAtTime(long tick) {
    if (tick < startTime || tick >= endTime) {
      return Resource.newInstance(0, 0);
    }
    return Resources.clone(resourcesOverTime.getCapacityAtTime(tick));
  }

  @Override
  public RLESparseResourceAllocation getResourcesOverTime() {
    return resourcesOverTime;
  }

  @Override
  public RLESparseResourceAllocation getResourcesOverTime(long start,
      long end) {
    return resourcesOverTime.getRangeOverlapping(start, end);
  }

  @Override
  public long getPeriodicity() {
    return periodicity;
  }

  @Override
  public void setPeriodicity(long period) {
    periodicity = period;
  }

  @Override
  public String toString() {
    StringBuilder sBuf = new StringBuilder();
    sBuf.append(getReservationId()).append(" user:").append(getUser())
        .append(" startTime: ").append(getStartTime()).append(" endTime: ")
        .append(getEndTime()).append(" Periodiciy: ").append(periodicity)
        .append(" alloc:\n[").append(resourcesOverTime.toString()).append("] ");
    return sBuf.toString();
  }

  @Override
  public int compareTo(ReservationAllocation other) {
    // reverse order of acceptance
    if (this.getAcceptanceTime() > other.getAcceptanceTime()) {
      return -1;
    }
    if (this.getAcceptanceTime() < other.getAcceptanceTime()) {
      return 1;
    }
    if (this.getReservationId().getId() > other.getReservationId().getId()) {
      return -1;
    }
    if (this.getReservationId().getId() < other.getReservationId().getId()) {
      return 1;
    }
    return 0;
  }

  @Override
  public int hashCode() {
    return reservationID.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    InMemoryReservationAllocation other = (InMemoryReservationAllocation) obj;
    return this.reservationID.equals(other.getReservationId());
  }

}

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
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * An in memory implementation of a reservation allocation using the
 * {@link RLESparseResourceAllocation}
 * 
 */
class InMemoryReservationAllocation implements ReservationAllocation {

  private final String planName;
  private final ReservationId reservationID;
  private final String user;
  private final ReservationDefinition contract;
  private final long startTime;
  private final long endTime;
  private final Map<ReservationInterval, ReservationRequest> allocationRequests;
  private boolean hasGang = false;
  private long acceptedAt = -1;

  private RLESparseResourceAllocation resourcesOverTime;

  InMemoryReservationAllocation(ReservationId reservationID,
      ReservationDefinition contract, String user, String planName,
      long startTime, long endTime,
      Map<ReservationInterval, ReservationRequest> allocationRequests,
      ResourceCalculator calculator, Resource minAlloc) {
    this.contract = contract;
    this.startTime = startTime;
    this.endTime = endTime;
    this.reservationID = reservationID;
    this.user = user;
    this.allocationRequests = allocationRequests;
    this.planName = planName;
    resourcesOverTime = new RLESparseResourceAllocation(calculator, minAlloc);
    for (Map.Entry<ReservationInterval, ReservationRequest> r : allocationRequests
        .entrySet()) {
      resourcesOverTime.addInterval(r.getKey(), r.getValue());
      if (r.getValue().getConcurrency() > 1) {
        hasGang = true;
      }
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
  public Map<ReservationInterval, ReservationRequest> getAllocationRequests() {
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
  public String toString() {
    StringBuilder sBuf = new StringBuilder();
    sBuf.append(getReservationId()).append(" user:").append(getUser())
        .append(" startTime: ").append(getStartTime()).append(" endTime: ")
        .append(getEndTime()).append(" alloc:[")
        .append(resourcesOverTime.toString()).append("] ");
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

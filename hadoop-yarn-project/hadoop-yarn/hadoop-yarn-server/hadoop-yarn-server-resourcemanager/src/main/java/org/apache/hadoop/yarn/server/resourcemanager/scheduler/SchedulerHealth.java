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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SchedulerHealth {

  static public class DetailedInformation {
    long timestamp;
    NodeId nodeId;
    ContainerId containerId;
    String queue;

    public DetailedInformation(long timestamp, NodeId nodeId,
        ContainerId containerId, String queue) {
      this.timestamp = timestamp;
      this.nodeId = nodeId;
      this.containerId = containerId;
      this.queue = queue;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public NodeId getNodeId() {
      return nodeId;
    }

    public ContainerId getContainerId() {
      return containerId;
    }

    public String getQueue() {
      return queue;
    }
  }

  enum Operation {
    ALLOCATION, RELEASE, PREEMPTION, RESERVATION, FULFILLED_RESERVATION
  }

  long lastSchedulerRunTime;
  Map<Operation, Resource> lastSchedulerRunDetails;
  Map<Operation, DetailedInformation> schedulerHealthDetails;
  Map<Operation, Long> schedulerOperationCounts;
  // this is for counts since the RM started, never reset
  Map<Operation, Long> schedulerOperationAggregateCounts;

  public SchedulerHealth() {
    lastSchedulerRunDetails = new ConcurrentHashMap<>();
    schedulerHealthDetails = new ConcurrentHashMap<>();
    schedulerOperationCounts = new ConcurrentHashMap<>();
    schedulerOperationAggregateCounts = new ConcurrentHashMap<>();
    for (Operation op : Operation.values()) {
      lastSchedulerRunDetails.put(op, Resource.newInstance(0, 0));
      schedulerOperationCounts.put(op, 0L);
      schedulerHealthDetails.put(op, new DetailedInformation(0, null, null,
        null));
      schedulerOperationAggregateCounts.put(op, 0L);
    }

  }

  public void updateAllocation(long timestamp, NodeId nodeId,
      ContainerId containerId, String queue) {
    DetailedInformation di =
        new DetailedInformation(timestamp, nodeId, containerId, queue);
    schedulerHealthDetails.put(Operation.ALLOCATION, di);
  }

  public void updateRelease(long timestamp, NodeId nodeId,
      ContainerId containerId, String queue) {
    DetailedInformation di =
        new DetailedInformation(timestamp, nodeId, containerId, queue);
    schedulerHealthDetails.put(Operation.RELEASE, di);
  }

  public void updatePreemption(long timestamp, NodeId nodeId,
      ContainerId containerId, String queue) {
    DetailedInformation di =
        new DetailedInformation(timestamp, nodeId, containerId, queue);
    schedulerHealthDetails.put(Operation.PREEMPTION, di);
  }

  public void updateReservation(long timestamp, NodeId nodeId,
      ContainerId containerId, String queue) {
    DetailedInformation di =
        new DetailedInformation(timestamp, nodeId, containerId, queue);
    schedulerHealthDetails.put(Operation.RESERVATION, di);
  }

  public void updateSchedulerRunDetails(long timestamp, Resource allocated,
      Resource reserved) {
    lastSchedulerRunTime = timestamp;
    lastSchedulerRunDetails.put(Operation.ALLOCATION, allocated);
    lastSchedulerRunDetails.put(Operation.RESERVATION, reserved);
  }

  public void updateSchedulerReleaseDetails(long timestamp, Resource released) {
    lastSchedulerRunTime = timestamp;
    lastSchedulerRunDetails.put(Operation.RELEASE, released);
  }

  public void updateSchedulerReleaseCounts(long count) {
    updateCounts(Operation.RELEASE, count);
  }

  public void updateSchedulerAllocationCounts(long count) {
    updateCounts(Operation.ALLOCATION, count);
  }

  public void updateSchedulerReservationCounts(long count) {
    updateCounts(Operation.RESERVATION, count);
  }

  public void updateSchedulerFulfilledReservationCounts(long count) {
    updateCounts(Operation.FULFILLED_RESERVATION, count);
  }

  public void updateSchedulerPreemptionCounts(long count) {
    updateCounts(Operation.PREEMPTION, count);
  }

  private void updateCounts(Operation op, long count) {
    schedulerOperationCounts.put(op, count);
    Long tmp = schedulerOperationAggregateCounts.get(op);
    schedulerOperationAggregateCounts.put(op, tmp + count);
  }

  public long getLastSchedulerRunTime() {
    return lastSchedulerRunTime;
  }

  private Resource getResourceDetails(Operation op) {
    return lastSchedulerRunDetails.get(op);
  }

  public Resource getResourcesAllocated() {
    return getResourceDetails(Operation.ALLOCATION);
  }

  public Resource getResourcesReserved() {
    return getResourceDetails(Operation.RESERVATION);
  }

  public Resource getResourcesReleased() {
    return getResourceDetails(Operation.RELEASE);
  }

  private DetailedInformation getDetailedInformation(Operation op) {
    return schedulerHealthDetails.get(op);
  }

  public DetailedInformation getLastAllocationDetails() {
    return getDetailedInformation(Operation.ALLOCATION);
  }

  public DetailedInformation getLastReleaseDetails() {
    return getDetailedInformation(Operation.RELEASE);
  }

  public DetailedInformation getLastReservationDetails() {
    return getDetailedInformation(Operation.RESERVATION);
  }

  public DetailedInformation getLastPreemptionDetails() {
    return getDetailedInformation(Operation.PREEMPTION);
  }

  private Long getOperationCount(Operation op) {
    return schedulerOperationCounts.get(op);
  }

  public Long getAllocationCount() {
    return getOperationCount(Operation.ALLOCATION);
  }

  public Long getReleaseCount() {
    return getOperationCount(Operation.RELEASE);
  }

  public Long getReservationCount() {
    return getOperationCount(Operation.RESERVATION);
  }

  public Long getPreemptionCount() {
    return getOperationCount(Operation.PREEMPTION);
  }

  private Long getAggregateOperationCount(Operation op) {
    return schedulerOperationAggregateCounts.get(op);
  }

  public Long getAggregateAllocationCount() {
    return getAggregateOperationCount(Operation.ALLOCATION);
  }

  public Long getAggregateReleaseCount() {
    return getAggregateOperationCount(Operation.RELEASE);
  }

  public Long getAggregateReservationCount() {
    return getAggregateOperationCount(Operation.RESERVATION);
  }

  public Long getAggregatePreemptionCount() {
    return getAggregateOperationCount(Operation.PREEMPTION);
  }

  public Long getAggregateFulFilledReservationsCount() {
    return getAggregateOperationCount(Operation.FULFILLED_RESERVATION);
  }
}

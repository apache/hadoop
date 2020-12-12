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

/**
 * SchedulerHealth class holds the details of the schedulers operations.
 *
 * <p><code>SchedulerHealth</code> provides clients with information such as:
 * <ol>
 *   <li>
 *   scheduler's latest timestamp
 *   </li>
 *   <li>
 *   resources allocated, reserved, released in the last scheduler run
 *   </li>
 *   <li>
 *   latest allocation, release, reservation, preemption details
 *   </li>
 *   <li>
 *   count of latest allocation, release, reservation, preemption
 *   </li>
 *   <li>
 *   aggregate count of latest allocation, release, reservation, preemption,
 *   fulfilled reservation
 *   </li>
 *</ol>
 *
 */

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

  private long lastSchedulerRunTime;
  private Map<Operation, Resource> lastSchedulerRunDetails;
  private Map<Operation, DetailedInformation> lastSchedulerHealthDetails;
  private Map<Operation, Long> schedulerOperationCounts;
  // this is for counts since the RM started, never reset
  private Map<Operation, Long> schedulerOperationAggregateCounts;

  SchedulerHealth() {
    lastSchedulerRunDetails = new ConcurrentHashMap<>();
    lastSchedulerHealthDetails = new ConcurrentHashMap<>();
    schedulerOperationCounts = new ConcurrentHashMap<>();
    schedulerOperationAggregateCounts = new ConcurrentHashMap<>();
    for (Operation op : Operation.values()) {
      lastSchedulerRunDetails.put(op, Resource.newInstance(0, 0));
      schedulerOperationCounts.put(op, 0L);
      lastSchedulerHealthDetails.put(op, new DetailedInformation(0, null, null,
        null));
      schedulerOperationAggregateCounts.put(op, 0L);
    }

  }

  public void updateAllocation(long timestamp, NodeId nodeId,
      ContainerId containerId, String queue) {
    DetailedInformation di =
        new DetailedInformation(timestamp, nodeId, containerId, queue);
    lastSchedulerHealthDetails.put(Operation.ALLOCATION, di);
  }

  public void updateRelease(long timestamp, NodeId nodeId,
      ContainerId containerId, String queue) {
    DetailedInformation di =
        new DetailedInformation(timestamp, nodeId, containerId, queue);
    lastSchedulerHealthDetails.put(Operation.RELEASE, di);
  }

  public void updatePreemption(long timestamp, NodeId nodeId,
      ContainerId containerId, String queue) {
    DetailedInformation di =
        new DetailedInformation(timestamp, nodeId, containerId, queue);
    lastSchedulerHealthDetails.put(Operation.PREEMPTION, di);
  }

  public void updateReservation(long timestamp, NodeId nodeId,
      ContainerId containerId, String queue) {
    DetailedInformation di =
        new DetailedInformation(timestamp, nodeId, containerId, queue);
    lastSchedulerHealthDetails.put(Operation.RESERVATION, di);
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

  /**
   * Get the timestamp of the latest scheduler operation.
   *
   * @return the scheduler's latest timestamp
   */
  public long getLastSchedulerRunTime() {
    return lastSchedulerRunTime;
  }

  private Resource getResourceDetails(Operation op) {
    return lastSchedulerRunDetails.get(op);
  }

  /**
   * Get the resources allocated in the last scheduler run.
   *
   * @return resources allocated
   */
  public Resource getResourcesAllocated() {
    return getResourceDetails(Operation.ALLOCATION);
  }

  /**
   * Get the resources reserved in the last scheduler run.
   *
   * @return resources reserved
   */
  public Resource getResourcesReserved() {
    return getResourceDetails(Operation.RESERVATION);
  }

  /**
   * Get the resources released in the last scheduler run.
   *
   * @return resources released
   */
  public Resource getResourcesReleased() {
    return getResourceDetails(Operation.RELEASE);
  }

  private DetailedInformation getDetailedInformation(Operation op) {
    return lastSchedulerHealthDetails.get(op);
  }

  /**
   * Get the details of last allocation.
   *
   * @return last allocation details
   */
  public DetailedInformation getLastAllocationDetails() {
    return getDetailedInformation(Operation.ALLOCATION);
  }

  /**
   * Get the details of last release.
   *
   * @return last release details
   */
  public DetailedInformation getLastReleaseDetails() {
    return getDetailedInformation(Operation.RELEASE);
  }

  /**
   * Get the details of last reservation.
   *
   * @return last reservation details
   */
  public DetailedInformation getLastReservationDetails() {
    return getDetailedInformation(Operation.RESERVATION);
  }

  /**
   * Get the details of last preemption.
   *
   * @return last preemption details
   */
  public DetailedInformation getLastPreemptionDetails() {
    return getDetailedInformation(Operation.PREEMPTION);
  }

  private Long getOperationCount(Operation op) {
    return schedulerOperationCounts.get(op);
  }

  /**
   * Get the count of allocation from the latest scheduler health report.
   *
   * @return allocation count
   */
  public Long getAllocationCount() {
    return getOperationCount(Operation.ALLOCATION);
  }

  /**
   * Get the count of release from the latest scheduler health report.
   *
   * @return release count
   */
  public Long getReleaseCount() {
    return getOperationCount(Operation.RELEASE);
  }

  /**
   * Get the count of reservation from the latest scheduler health report.
   *
   * @return reservation count
   */
  public Long getReservationCount() {
    return getOperationCount(Operation.RESERVATION);
  }

  /**
   * Get the count of preemption from the latest scheduler health report.
   *
   * @return preemption count
   */
  public Long getPreemptionCount() {
    return getOperationCount(Operation.PREEMPTION);
  }

  private Long getAggregateOperationCount(Operation op) {
    return schedulerOperationAggregateCounts.get(op);
  }

  /**
   * Get the aggregate of all the allocations count.
   *
   * @return aggregate allocation count
   */
  public Long getAggregateAllocationCount() {
    return getAggregateOperationCount(Operation.ALLOCATION);
  }

  /**
   * Get the aggregate of all the release count.
   *
   * @return aggregate release count
   */
  public Long getAggregateReleaseCount() {
    return getAggregateOperationCount(Operation.RELEASE);
  }

  /**
   * Get the aggregate of all the reservations count.
   *
   * @return aggregate reservation count
   */
  public Long getAggregateReservationCount() {
    return getAggregateOperationCount(Operation.RESERVATION);
  }

  /**
   * Get the aggregate of all the preemption count.
   *
   * @return aggregate preemption count
   */
  public Long getAggregatePreemptionCount() {
    return getAggregateOperationCount(Operation.PREEMPTION);
  }

  /**
   * Get the aggregate of all the fulfilled reservations count.
   *
   * @return aggregate fulfilled reservations count
   */
  public Long getAggregateFulFilledReservationsCount() {
    return getAggregateOperationCount(Operation.FULFILLED_RESERVATION);
  }
}

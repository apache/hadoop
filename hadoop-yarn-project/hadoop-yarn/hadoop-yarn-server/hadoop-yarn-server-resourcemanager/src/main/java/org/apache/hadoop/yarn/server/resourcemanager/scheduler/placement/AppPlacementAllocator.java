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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement;

import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * This class has the following functionality:
 * 1) Keeps track of pending resource requests when following events happen:
 * - New ResourceRequests are added to scheduler.
 * - New containers get allocated.
 *
 * 2) Determines the order that the nodes given in the {@link CandidateNodeSet}
 * will be used for allocating containers.
 * </p>
 *
 * <p>
 * And different set of resource requests (E.g., resource requests with the
 * same schedulerKey) can have one instance of AppPlacementAllocator, each
 * AppPlacementAllocator can have different ways to order nodes depends on
 * requests.
 * </p>
 */
public interface AppPlacementAllocator<N extends SchedulerNode> {
  /**
   * Get iterator of preferred node depends on requirement and/or availability
   * @param candidateNodeSet input CandidateNodeSet
   * @return iterator of preferred node
   */
  Iterator<N> getPreferredNodeIterator(CandidateNodeSet<N> candidateNodeSet);

  /**
   * Replace existing ResourceRequest by the new requests
   *
   * @param requests new ResourceRequests
   * @param recoverPreemptedRequestForAContainer if we're recovering resource
   * requests for preempted container
   * @return true if total pending resource changed
   */
  ResourceRequestUpdateResult updateResourceRequests(
      Collection<ResourceRequest> requests,
      boolean recoverPreemptedRequestForAContainer);

  /**
   * Get pending ResourceRequests by given schedulerRequestKey
   * @return Map of resourceName to ResourceRequest
   */
  Map<String, ResourceRequest> getResourceRequests();

  /**
   * Get pending ask for given resourceName. If there's no such pendingAsk,
   * returns {@link PendingAsk#ZERO}
   *
   * @param resourceName resourceName
   * @return PendingAsk
   */
  PendingAsk getPendingAsk(String resourceName);

  /**
   * Get #pending-allocations for given resourceName. If there's no such
   * pendingAsk, returns 0
   *
   * @param resourceName resourceName
   * @return #pending-allocations
   */
  int getOutstandingAsksCount(String resourceName);

  /**
   * Notify container allocated.
   * @param schedulerKey SchedulerRequestKey for this ResourceRequest
   * @param type Type of the allocation
   * @param node Which node this container allocated on
   * @return list of ResourceRequests deducted
   */
  List<ResourceRequest> allocate(SchedulerRequestKey schedulerKey,
      NodeType type, SchedulerNode node);

  /**
   * Returns list of accepted resourceNames.
   * @return Iterator of accepted resourceNames
   */
  Iterator<String> getAcceptedResouceNames();

  /**
   * We can still have pending requirement for a given NodeType and node
   * @param type Locality Type
   * @param node which node we will allocate on
   * @return true if we has pending requirement
   */
  boolean canAllocate(NodeType type, SchedulerNode node);

  /**
   * Can delay to give locality?
   * TODO: This should be moved out of AppPlacementAllocator
   * and should belong to specific delay scheduling policy impl.
   * See YARN-7457 for more details.
   *
   * @param resourceName resourceName
   * @return can/cannot
   */
  boolean canDelayTo(String resourceName);

  /**
   * Does this {@link AppPlacementAllocator} accept resources on nodePartition?
   *
   * @param nodePartition nodePartition
   * @param schedulingMode schedulingMode
   * @return accepted/not
   */
  boolean acceptNodePartition(String nodePartition,
      SchedulingMode schedulingMode);

  /**
   * It is possible that one request can accept multiple node partition,
   * So this method returns primary node partition for pending resource /
   * headroom calculation.
   *
   * @return primary requested node partition
   */
  String getPrimaryRequestedNodePartition();

  /**
   * @return number of unique location asks with #pending greater than 0,
   * (like /rack1, host1, etc.).
   *
   * TODO: This should be moved out of AppPlacementAllocator
   * and should belong to specific delay scheduling policy impl.
   * See YARN-7457 for more details.
   */
  int getUniqueLocationAsks();

  /**
   * Print human-readable requests to LOG debug.
   */
  void showRequests();
}

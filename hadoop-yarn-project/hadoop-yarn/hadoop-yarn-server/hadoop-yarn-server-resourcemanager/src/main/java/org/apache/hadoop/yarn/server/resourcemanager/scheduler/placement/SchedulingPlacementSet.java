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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerRequestKey;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * In addition to {@link PlacementSet}, this also maintains
 * pending ResourceRequests:
 * - When new ResourceRequest(s) added to scheduler, or,
 * - Or new container allocated, scheduler can notify corresponding
 * PlacementSet.
 * </p>
 *
 * <p>
 * Different set of resource requests (E.g., resource requests with the
 * same schedulerKey) can have one instance of PlacementSet, each PlacementSet
 * can have different ways to order nodes depends on requests.
 * </p>
 */
public interface SchedulingPlacementSet<N extends SchedulerNode>
    extends PlacementSet<N> {
  /**
   * Get iterator of preferred node depends on requirement and/or availability
   * @param clusterPlacementSet input cluster PlacementSet
   * @return iterator of preferred node
   */
  Iterator<N> getPreferredNodeIterator(PlacementSet<N> clusterPlacementSet);

  /**
   * Replace existing ResourceRequest by the new requests
   *
   * @param requests new ResourceRequests
   * @param recoverPreemptedRequestForAContainer if we're recovering resource
   * requests for preempted container
   * @return true if total pending resource changed
   */
  ResourceRequestUpdateResult updateResourceRequests(
      List<ResourceRequest> requests,
      boolean recoverPreemptedRequestForAContainer);

  /**
   * Get pending ResourceRequests by given schedulerRequestKey
   * @return Map of resourceName to ResourceRequest
   */
  Map<String, ResourceRequest> getResourceRequests();

  /**
   * Get ResourceRequest by given schedulerKey and resourceName
   * @param resourceName resourceName
   * @param schedulerRequestKey schedulerRequestKey
   * @return ResourceRequest
   */
  ResourceRequest getResourceRequest(String resourceName,
      SchedulerRequestKey schedulerRequestKey);

  /**
   * Notify container allocated.
   * @param type Type of the allocation
   * @param node Which node this container allocated on
   * @param request resource request
   * @return list of ResourceRequests deducted
   */
  List<ResourceRequest> allocate(NodeType type, SchedulerNode node,
      ResourceRequest request);
}

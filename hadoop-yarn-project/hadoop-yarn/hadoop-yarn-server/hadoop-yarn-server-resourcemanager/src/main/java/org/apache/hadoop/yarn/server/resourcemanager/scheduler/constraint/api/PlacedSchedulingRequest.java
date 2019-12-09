/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api;

import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Class to encapsulate a Placed scheduling Request.
 * It has the original Scheduling Request and a list of SchedulerNodes (one
 * for each 'numAllocation' field in the corresponding ResourceSizing object.
 *
 * NOTE: Clients of this class SHOULD NOT rely on the value of
 *       resourceSizing.numAllocations and instead should use the
 *       size of collection returned by getNodes() instead.
 */
public class PlacedSchedulingRequest {

  // The number of times the Algorithm tried to place the SchedulingRequest
  // after it was rejected by the commit phase of the Scheduler (due to some
  // transient state of the cluster. For eg: no space on Node / user limit etc.)
  // The Algorithm can then try to probably place on a different node.
  private int placementAttempt = 0;
  private final SchedulingRequest request;
  // One Node per numContainers in the SchedulingRequest;
  private final List<SchedulerNode> nodes = new ArrayList<>();

  public PlacedSchedulingRequest(SchedulingRequest request) {
    this.request = request;
  }

  public SchedulingRequest getSchedulingRequest() {
    return request;
  }

  /**
   * List of Node locations on which this Scheduling Request can be placed.
   * The size of this list = schedulingRequest.resourceSizing.numAllocations.
   * @return List of Scheduler nodes.
   */
  public List<SchedulerNode> getNodes() {
    return nodes;
  }

  public int getPlacementAttempt() {
    return placementAttempt;
  }

  public void setPlacementAttempt(int attempt) {
    this.placementAttempt = attempt;
  }

  @Override
  public String toString() {
    return "PlacedSchedulingRequest{" +
        "placementAttempt=" + placementAttempt +
        ", request=" + request +
        ", nodes=" + nodes +
        '}';
  }
}

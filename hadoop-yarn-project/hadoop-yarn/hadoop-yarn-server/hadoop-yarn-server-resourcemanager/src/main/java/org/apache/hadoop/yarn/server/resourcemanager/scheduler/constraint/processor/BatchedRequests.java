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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithmInput;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A grouping of Scheduling Requests which are sent to the PlacementAlgorithm
 * to place as a batch. The placement algorithm tends to give more optimal
 * placements if more requests are batched together.
 */
class BatchedRequests implements ConstraintPlacementAlgorithmInput {

  // PlacementAlgorithmOutput attempt - the number of times the requests in this
  // batch has been placed but was rejected by the scheduler.
  private final int placementAttempt;

  private final ApplicationId applicationId;
  private final Collection<SchedulingRequest> requests;
  private final Map<String, Set<NodeId>> blacklist = new HashMap<>();

  BatchedRequests(ApplicationId applicationId,
      Collection<SchedulingRequest> requests, int attempt) {
    this.applicationId = applicationId;
    this.requests = requests;
    this.placementAttempt = attempt;
  }

  /**
   * Get Application Id.
   * @return Application Id.
   */
  ApplicationId getApplicationId() {
    return applicationId;
  }

  /**
   * Get Collection of SchedulingRequests in this batch.
   * @return Collection of Scheduling Requests.
   */
  @Override
  public Collection<SchedulingRequest> getSchedulingRequests() {
    return requests;
  }

  /**
   * Add a Scheduling request to the batch.
   * @param req Scheduling Request.
   */
  void addToBatch(SchedulingRequest req) {
    requests.add(req);
  }

  void addToBlacklist(Set<String> tags, SchedulerNode node) {
    if (tags != null && !tags.isEmpty()) {
      // We are currently assuming a single allocation tag
      // per scheduler request currently.
      blacklist.computeIfAbsent(tags.iterator().next(),
          k -> new HashSet<>()).add(node.getNodeID());
    }
  }

  /**
   * Get placement attempt.
   * @return PlacementAlgorithmOutput placement Attempt.
   */
  int getPlacementAttempt() {
    return placementAttempt;
  }

  /**
   * Get any blacklisted nodes associated with tag.
   * @param tag Tag.
   * @return Set of blacklisted Nodes.
   */
  Set<NodeId> getBlacklist(String tag) {
    return blacklist.getOrDefault(tag, Collections.EMPTY_SET);
  }
}

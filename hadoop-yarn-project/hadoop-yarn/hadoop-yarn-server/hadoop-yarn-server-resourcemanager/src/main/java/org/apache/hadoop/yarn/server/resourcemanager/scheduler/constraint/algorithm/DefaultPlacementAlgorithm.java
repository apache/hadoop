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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.InvalidAllocationTagsQueryException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintsUtil;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithm;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithmInput;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithmOutput;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithmOutputCollector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.PlacedSchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.BatchedRequests;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.NodeCandidateSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic placement algorithm.
 * Supports different Iterators at SchedulingRequest level including:
 * Serial, PopularTags
 */
public class DefaultPlacementAlgorithm implements ConstraintPlacementAlgorithm {

  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultPlacementAlgorithm.class);

  // Number of times to re-attempt placing a single scheduling request.
  private static final int RE_ATTEMPT_COUNT = 2;

  private LocalAllocationTagsManager tagsManager;
  private PlacementConstraintManager constraintManager;
  private NodeCandidateSelector nodeSelector;

  @Override
  public void init(RMContext rmContext) {
    this.tagsManager = new LocalAllocationTagsManager(
        rmContext.getAllocationTagsManager());
    this.constraintManager = rmContext.getPlacementConstraintManager();
    this.nodeSelector =
        filter -> ((AbstractYarnScheduler) (rmContext).getScheduler())
            .getNodes(filter);
  }

  public boolean attemptPlacementOnNode(ApplicationId appId,
      SchedulingRequest schedulingRequest, SchedulerNode schedulerNode)
      throws InvalidAllocationTagsQueryException {
    if (PlacementConstraintsUtil.canSatisfyConstraints(appId,
        schedulingRequest, schedulerNode, constraintManager, tagsManager)) {
      return true;
    }
    return false;
  }


  @Override
  public void place(ConstraintPlacementAlgorithmInput input,
      ConstraintPlacementAlgorithmOutputCollector collector) {
    BatchedRequests requests = (BatchedRequests) input;
    ConstraintPlacementAlgorithmOutput resp =
        new ConstraintPlacementAlgorithmOutput(requests.getApplicationId());
    List<SchedulerNode> allNodes = nodeSelector.selectNodes(null);

    List<SchedulingRequest> rejectedRequests = new ArrayList<>();
    int rePlacementCount = RE_ATTEMPT_COUNT;
    while (rePlacementCount > 0) {
      doPlacement(requests, resp, allNodes, rejectedRequests);
      // Double check if placement constraints are really satisfied
      validatePlacement(requests.getApplicationId(), resp,
          rejectedRequests);
      if (rejectedRequests.size() == 0 || rePlacementCount == 1) {
        break;
      }
      requests = new BatchedRequests(requests.getIteratorType(),
          requests.getApplicationId(), rejectedRequests,
          requests.getPlacementAttempt());
      rejectedRequests = new ArrayList<>();
      rePlacementCount--;
    }

    resp.getRejectedRequests().addAll(rejectedRequests);
    collector.collect(resp);
    // Clean current temp-container tags
    this.tagsManager.cleanTempContainers(requests.getApplicationId());
  }

  private void doPlacement(BatchedRequests requests,
      ConstraintPlacementAlgorithmOutput resp,
      List<SchedulerNode> allNodes,
      List<SchedulingRequest> rejectedRequests) {
    Iterator<SchedulingRequest> requestIterator = requests.iterator();
    Iterator<SchedulerNode> nIter = allNodes.iterator();
    SchedulerNode lastSatisfiedNode = null;
    while (requestIterator.hasNext()) {
      if (allNodes.isEmpty()) {
        LOG.warn("No nodes available for placement at the moment !!");
        break;
      }
      SchedulingRequest schedulingRequest = requestIterator.next();
      PlacedSchedulingRequest placedReq =
          new PlacedSchedulingRequest(schedulingRequest);
      placedReq.setPlacementAttempt(requests.getPlacementAttempt());
      resp.getPlacedRequests().add(placedReq);
      CircularIterator<SchedulerNode> nodeIter =
          new CircularIterator(lastSatisfiedNode, nIter, allNodes);
      int numAllocs =
          schedulingRequest.getResourceSizing().getNumAllocations();
      while (nodeIter.hasNext() && numAllocs > 0) {
        SchedulerNode node = nodeIter.next();
        try {
          String tag = schedulingRequest.getAllocationTags() == null ? "" :
              schedulingRequest.getAllocationTags().iterator().next();
          if (!requests.getBlacklist(tag).contains(node.getNodeID()) &&
              attemptPlacementOnNode(
                  requests.getApplicationId(), schedulingRequest, node)) {
            schedulingRequest.getResourceSizing()
                .setNumAllocations(--numAllocs);
            placedReq.getNodes().add(node);
            numAllocs =
                schedulingRequest.getResourceSizing().getNumAllocations();
            // Add temp-container tags for current placement cycle
            this.tagsManager.addTempTags(node.getNodeID(),
                requests.getApplicationId(),
                schedulingRequest.getAllocationTags());
            lastSatisfiedNode = node;
          }
        } catch (InvalidAllocationTagsQueryException e) {
          LOG.warn("Got exception from TagManager !", e);
        }
      }
    }
    // Add all requests whose numAllocations still > 0 to rejected list.
    requests.getSchedulingRequests().stream()
        .filter(sReq -> sReq.getResourceSizing().getNumAllocations() > 0)
        .forEach(rejReq -> rejectedRequests.add(cloneReq(rejReq)));
  }

  /**
   * During the placement phase, allocation tags are added to the node if the
   * constraint is satisfied, But depending on the order in which the
   * algorithm sees the request, it is possible that a constraint that happened
   * to be valid during placement of an earlier-seen request, might not be
   * valid after all subsequent requests have been placed.
   *
   * For eg:
   *   Assume nodes n1, n2, n3, n4 and n5
   *
   *   Consider the 2 constraints:
   *   1) "foo", anti-affinity with "foo"
   *   2) "bar", anti-affinity with "foo"
   *
   *   And 2 requests
   *   req1: NumAllocations = 4, allocTags = [foo]
   *   req2: NumAllocations = 1, allocTags = [bar]
   *
   *   If "req1" is seen first, the algorithm can place the 4 containers in
   *   n1, n2, n3 and n4. And when it gets to "req2", it will see that 4 nodes
   *   with the "foo" tag and will place on n5.
   *   But if "req2" is seem first, then "bar" will be placed on any node,
   *   since no node currently has "foo", and when it gets to "req1", since
   *   "foo" has not anti-affinity with "bar", the algorithm can end up placing
   *   "foo" on a node with "bar" violating the second constraint.
   *
   * To prevent the above, we need a validation step: after the placements for a
   * batch of requests are made, for each req, we remove its tags from the node
   * and try to see of constraints are still satisfied if the tag were to be
   * added back on the node.
   *
   *   When applied to the example above, after "req2" and "req1" are placed,
   *   we remove the "bar" tag from the node and try to add it back on the node.
   *   This time, constraint satisfaction will fail, since there is now a "foo"
   *   tag on the node and "bar" cannot be added. The algorithm will then
   *   retry placing "req2" on another node.
   *
   * @param applicationId
   * @param resp
   * @param rejectedRequests
   */
  private void validatePlacement(ApplicationId applicationId,
      ConstraintPlacementAlgorithmOutput resp,
      List<SchedulingRequest> rejectedRequests) {
    Iterator<PlacedSchedulingRequest> pReqIter =
        resp.getPlacedRequests().iterator();
    while (pReqIter.hasNext()) {
      PlacedSchedulingRequest pReq = pReqIter.next();
      Iterator<SchedulerNode> nodeIter = pReq.getNodes().iterator();
      // Assuming all reqs were satisfied.
      int num = 0;
      while (nodeIter.hasNext()) {
        SchedulerNode node = nodeIter.next();
        try {
          // Remove just the tags for this placement.
          this.tagsManager.removeTempTags(node.getNodeID(),
              applicationId, pReq.getSchedulingRequest().getAllocationTags());
          if (!attemptPlacementOnNode(
              applicationId, pReq.getSchedulingRequest(), node)) {
            nodeIter.remove();
            num++;
          } else {
            // Add back the tags if everything is fine.
            this.tagsManager.addTempTags(node.getNodeID(),
                applicationId, pReq.getSchedulingRequest().getAllocationTags());
          }
        } catch (InvalidAllocationTagsQueryException e) {
          LOG.warn("Got exception from TagManager !", e);
        }
      }
      if (num > 0) {
        SchedulingRequest sReq = cloneReq(pReq.getSchedulingRequest());
        sReq.getResourceSizing().setNumAllocations(num);
        rejectedRequests.add(sReq);
      }
      if (pReq.getNodes().isEmpty()) {
        pReqIter.remove();
      }
    }
  }

  private static SchedulingRequest cloneReq(SchedulingRequest sReq) {
    return SchedulingRequest.newInstance(
        sReq.getAllocationRequestId(), sReq.getPriority(),
        sReq.getExecutionType(), sReq.getAllocationTags(),
        ResourceSizing.newInstance(
            sReq.getResourceSizing().getNumAllocations(),
            sReq.getResourceSizing().getResources()),
        sReq.getPlacementConstraint());
  }

}

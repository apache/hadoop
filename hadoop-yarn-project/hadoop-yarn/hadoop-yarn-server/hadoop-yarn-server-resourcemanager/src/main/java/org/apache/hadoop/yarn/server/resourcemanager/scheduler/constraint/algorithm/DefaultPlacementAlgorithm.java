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
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager;
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

  private AllocationTagsManager tagsManager;
  private PlacementConstraintManager constraintManager;
  private NodeCandidateSelector nodeSelector;

  @Override
  public void init(RMContext rmContext) {
    this.tagsManager = rmContext.getAllocationTagsManager();
    this.constraintManager = rmContext.getPlacementConstraintManager();
    this.nodeSelector =
        filter -> ((AbstractYarnScheduler) (rmContext).getScheduler())
            .getNodes(filter);
  }

  public boolean attemptPlacementOnNode(ApplicationId appId,
      SchedulingRequest schedulingRequest, SchedulerNode schedulerNode)
      throws InvalidAllocationTagsQueryException {
    int numAllocs = schedulingRequest.getResourceSizing().getNumAllocations();
    if (numAllocs > 0) {
      if (PlacementConstraintsUtil.canSatisfyConstraints(appId,
          schedulingRequest, schedulerNode,
          constraintManager, tagsManager)) {
        return true;
      }
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
      CircularIterator<SchedulerNode> nodeIter =
          new CircularIterator(lastSatisfiedNode, nIter, allNodes);
      int numAllocs = schedulingRequest.getResourceSizing().getNumAllocations();
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
            PlacedSchedulingRequest placedReq =
                new PlacedSchedulingRequest(schedulingRequest);
            placedReq.setPlacementAttempt(requests.getPlacementAttempt());
            placedReq.getNodes().add(node);
            resp.getPlacedRequests().add(placedReq);
            numAllocs =
                schedulingRequest.getResourceSizing().getNumAllocations();
            // Add temp-container tags for current placement cycle
            this.tagsManager.addTempContainer(node.getNodeID(),
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
        .forEach(rejReq -> rejectedRequests.add(rejReq));
  }
}

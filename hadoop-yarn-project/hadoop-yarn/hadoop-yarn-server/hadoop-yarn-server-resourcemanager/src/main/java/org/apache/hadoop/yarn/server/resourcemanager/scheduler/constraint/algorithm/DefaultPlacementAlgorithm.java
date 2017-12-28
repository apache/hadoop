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

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraintTransformations;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.InvalidAllocationTagsQueryException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintManager;
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

  /**
   * TODO: Method will be moved to PlacementConstraintsUtil class (YARN-7682)
   * @param applicationId
   * @param allocationTags
   * @param nodeId
   * @param tagsManager
   * @return boolean
   * @throws InvalidAllocationTagsQueryException
   */
  public boolean canAssign(ApplicationId applicationId,
      Set<String> allocationTags, NodeId nodeId,
      AllocationTagsManager tagsManager)
      throws InvalidAllocationTagsQueryException {
    PlacementConstraint constraint =
        constraintManager.getConstraint(applicationId, allocationTags);
    if (constraint == null) {
      return true;
    }
    // TODO: proper transformations
    // Currently works only for simple anti-affinity
    // NODE scope target expressions
    PlacementConstraintTransformations.SpecializedConstraintTransformer transformer =
        new PlacementConstraintTransformations.SpecializedConstraintTransformer(
            constraint);
    PlacementConstraint transform = transformer.transform();
    PlacementConstraint.TargetConstraint targetConstraint =
        (PlacementConstraint.TargetConstraint) transform.getConstraintExpr();
    // Assume a single target expression tag;
    // The Sample Algorithm assumes a constraint will always be a simple
    // Target Constraint with a single entry in the target set.
    // As mentioned in the class javadoc - This algorithm should be
    // used mostly for testing and validating end-2-end workflow.
    String targetTag = targetConstraint.getTargetExpressions().iterator().next()
        .getTargetValues().iterator().next();
    // TODO: Assuming anti-affinity constraint
    long nodeCardinality =
        tagsManager.getNodeCardinality(nodeId, applicationId, targetTag);
    if (nodeCardinality != 0) {
      return false;
    }
    // return true if it is a valid placement
    return true;
  }

  public boolean attemptPlacementOnNode(ApplicationId appId,
      SchedulingRequest schedulingRequest, SchedulerNode schedulerNode)
      throws InvalidAllocationTagsQueryException {
    int numAllocs = schedulingRequest.getResourceSizing().getNumAllocations();
    if (numAllocs > 0) {
      if (canAssign(appId,
          schedulingRequest.getAllocationTags(), schedulerNode.getNodeID(),
          tagsManager)) {
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

    Iterator<SchedulingRequest> requestIterator = requests.iterator();
    while (requestIterator.hasNext()) {
      SchedulingRequest schedulingRequest = requestIterator.next();
      Iterator<SchedulerNode> nodeIter = allNodes.iterator();
      int numAllocs = schedulingRequest.getResourceSizing().getNumAllocations();
      while (nodeIter.hasNext() && numAllocs > 0) {
        SchedulerNode node = nodeIter.next();
        try {
          if (attemptPlacementOnNode(requests.getApplicationId(),
              schedulingRequest, node)) {
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
          }
        } catch (InvalidAllocationTagsQueryException e) {
          LOG.warn("Got exception from TagManager !", e);
        }
      }
    }
    // Add all requests whose numAllocations still > 0 to rejected list.
    requests.getSchedulingRequests().stream()
        .filter(sReq -> sReq.getResourceSizing().getNumAllocations() > 0)
        .forEach(rejReq -> resp.getRejectedRequests().add(rejReq));
    collector.collect(resp);
    // Clean current temp-container tags
    this.tagsManager.cleanTempContainers(requests.getApplicationId());
  }
}

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

import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TargetConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraintTransformations.SpecializedConstraintTransformer;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.InvalidAllocationTagsQueryException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithm;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithmInput;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithmOutput;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithmOutputCollector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.PlacedSchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Sample Test algorithm. Assumes anti-affinity always
 * It also assumes the numAllocations in resource sizing is always = 1
 *
 * NOTE: This is just a sample implementation. Not be actually used
 */
public class SamplePlacementAlgorithm implements ConstraintPlacementAlgorithm {

  private static final Logger LOG =
      LoggerFactory.getLogger(SamplePlacementAlgorithm.class);

  private AllocationTagsManager tagsManager;
  private PlacementConstraintManager constraintManager;
  private NodeCandidateSelector nodeSelector;

  @Override
  public void init(RMContext rmContext) {
    this.tagsManager = rmContext.getAllocationTagsManager();
    this.constraintManager = rmContext.getPlacementConstraintManager();
    this.nodeSelector =
        filter -> ((AbstractYarnScheduler)(rmContext)
            .getScheduler()).getNodes(filter);
  }

  @Override
  public void place(ConstraintPlacementAlgorithmInput input,
      ConstraintPlacementAlgorithmOutputCollector collector) {
    BatchedRequests requests = (BatchedRequests)input;
    ConstraintPlacementAlgorithmOutput resp =
        new ConstraintPlacementAlgorithmOutput(requests.getApplicationId());
    List<SchedulerNode> allNodes = nodeSelector.selectNodes(null);
    Map<String, List<SchedulingRequest>> tagIndexedRequests = new HashMap<>();
    requests.getSchedulingRequests()
        .stream()
        .filter(r -> r.getAllocationTags() != null)
        .forEach(
            req -> req.getAllocationTags().forEach(
                tag -> tagIndexedRequests.computeIfAbsent(tag,
                    k -> new ArrayList<>()).add(req))
        );
    for (Map.Entry<String, List<SchedulingRequest>> entry :
        tagIndexedRequests.entrySet()) {
      String tag = entry.getKey();
      PlacementConstraint constraint =
          constraintManager.getConstraint(requests.getApplicationId(),
              Collections.singleton(tag));
      if (constraint != null) {
        // Currently works only for simple anti-affinity
        // NODE scope target expressions
        SpecializedConstraintTransformer transformer =
            new SpecializedConstraintTransformer(constraint);
        PlacementConstraint transform = transformer.transform();
        TargetConstraint targetConstraint =
            (TargetConstraint) transform.getConstraintExpr();
        // Assume a single target expression tag;
        // The Sample Algorithm assumes a constraint will always be a simple
        // Target Constraint with a single entry in the target set.
        // As mentioned in the class javadoc - This algorithm should be
        // used mostly for testing and validating end-2-end workflow.
        String targetTag =
            targetConstraint.getTargetExpressions().iterator().next()
            .getTargetValues().iterator().next();
        // iterate over all nodes
        Iterator<SchedulerNode> nodeIter = allNodes.iterator();
        List<SchedulingRequest> schedulingRequests = entry.getValue();
        Iterator<SchedulingRequest> reqIter = schedulingRequests.iterator();
        while (reqIter.hasNext()) {
          SchedulingRequest sReq = reqIter.next();
          int numAllocs = sReq.getResourceSizing().getNumAllocations();
          while (numAllocs > 0 && nodeIter.hasNext()) {
            SchedulerNode node = nodeIter.next();
            long nodeCardinality = 0;
            try {
              nodeCardinality = tagsManager.getNodeCardinality(
                  node.getNodeID(), requests.getApplicationId(),
                  targetTag);
              if (nodeCardinality == 0 &&
                  !requests.getBlacklist(tag).contains(node.getNodeID())) {
                numAllocs--;
                sReq.getResourceSizing().setNumAllocations(numAllocs);
                PlacedSchedulingRequest placedReq =
                    new PlacedSchedulingRequest(sReq);
                placedReq.setPlacementAttempt(requests.getPlacementAttempt());
                placedReq.getNodes().add(node);
                resp.getPlacedRequests().add(placedReq);
              }
            } catch (InvalidAllocationTagsQueryException e) {
              LOG.warn("Got exception from TagManager !", e);
            }
          }
        }
      }
    }
    // Add all requests whose numAllocations still > 0 to rejected list.
    requests.getSchedulingRequests().stream()
        .filter(sReq -> sReq.getResourceSizing().getNumAllocations() > 0)
        .forEach(rejReq -> resp.getRejectedRequests().add(rejReq));
    collector.collect(resp);
  }
}

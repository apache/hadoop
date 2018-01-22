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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.records.impl.pb.SchedulingRequestPBImpl;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints;
import org.apache.hadoop.yarn.exceptions.SchedulerInvalidResoureRequestException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.InvalidAllocationTagsQueryException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintsUtil;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.APPLICATION_LABEL_INTRA_APPLICATION;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.NODE_PARTITION;

/**
 * This is a simple implementation to do affinity or anti-affinity for
 * inter/intra apps.
 */
public class SingleConstraintAppPlacementAllocator<N extends SchedulerNode>
    extends AppPlacementAllocator<N> {
  private static final Log LOG =
      LogFactory.getLog(SingleConstraintAppPlacementAllocator.class);

  private ReentrantReadWriteLock.ReadLock readLock;
  private ReentrantReadWriteLock.WriteLock writeLock;

  private SchedulingRequest schedulingRequest = null;
  private String targetNodePartition;
  private Set<String> targetAllocationTags;
  private AllocationTagsManager allocationTagsManager;
  private PlacementConstraintManager placementConstraintManager;

  public SingleConstraintAppPlacementAllocator() {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<N> getPreferredNodeIterator(
      CandidateNodeSet<N> candidateNodeSet) {
    // Now only handle the case that single node in the candidateNodeSet
    // TODO, Add support to multi-hosts inside candidateNodeSet which is passed
    // in.

    N singleNode = CandidateNodeSetUtils.getSingleNode(candidateNodeSet);
    if (null != singleNode) {
      return IteratorUtils.singletonIterator(singleNode);
    }

    return IteratorUtils.emptyIterator();
  }

  @Override
  public PendingAskUpdateResult updatePendingAsk(
      Collection<ResourceRequest> requests,
      boolean recoverPreemptedRequestForAContainer) {
    if (requests != null && !requests.isEmpty()) {
      throw new SchedulerInvalidResoureRequestException(
          this.getClass().getName()
              + " not be able to handle ResourceRequest, there exists a "
              + "SchedulingRequest with the same scheduler key="
              + SchedulerRequestKey.create(requests.iterator().next())
              + ", please send ResourceRequest with a different allocationId and "
              + "priority");
    }

    // Do nothing
    return null;
  }

  private PendingAskUpdateResult internalUpdatePendingAsk(
      SchedulingRequest newSchedulingRequest, boolean recoverContainer) {
    // When it is a recover container, there must exists an schedulingRequest.
    if (recoverContainer && schedulingRequest == null) {
      throw new SchedulerInvalidResoureRequestException("Trying to recover a "
          + "container request=" + newSchedulingRequest.toString() + ", however"
          + "there's no existing scheduling request, this should not happen.");
    }

    if (schedulingRequest != null) {
      // If we have an old scheduling request, we will make sure that no changes
      // made except sizing.
      // To avoid unnecessary copy of the data structure, we do this by
      // replacing numAllocations with old numAllocations in the
      // newSchedulingRequest#getResourceSizing, and compare the two objects.
      ResourceSizing sizing = newSchedulingRequest.getResourceSizing();
      int existingNumAllocations =
          schedulingRequest.getResourceSizing().getNumAllocations();

      // When it is a recovered container request, just set
      // #newAllocations = #existingAllocations + 1;
      int newNumAllocations;
      if (recoverContainer) {
        newNumAllocations = existingNumAllocations + 1;
      } else {
        newNumAllocations = sizing.getNumAllocations();
      }
      sizing.setNumAllocations(existingNumAllocations);

      // Compare two objects
      if (!schedulingRequest.equals(newSchedulingRequest)) {
        // Rollback #numAllocations
        sizing.setNumAllocations(newNumAllocations);
        throw new SchedulerInvalidResoureRequestException(
            "Invalid updated SchedulingRequest added to scheduler, "
                + " we only allows changing numAllocations for the updated "
                + "SchedulingRequest. Old=" + schedulingRequest.toString()
                + " new=" + newSchedulingRequest.toString()
                + ", if any fields need to be updated, please cancel the "
                + "old request (by setting numAllocations to 0) and send a "
                + "SchedulingRequest with different combination of "
                + "priority/allocationId");
      } else {
        if (newNumAllocations == existingNumAllocations) {
          // No update on pending asks, return null.
          return null;
        }
      }

      // Rollback #numAllocations
      sizing.setNumAllocations(newNumAllocations);

      // Basic sanity check
      if (newNumAllocations < 0) {
        throw new SchedulerInvalidResoureRequestException(
            "numAllocation in ResourceSizing field must be >= 0, "
                + "updating schedulingRequest failed.");
      }

      PendingAskUpdateResult updateResult = new PendingAskUpdateResult(
          new PendingAsk(schedulingRequest.getResourceSizing()),
          new PendingAsk(newSchedulingRequest.getResourceSizing()),
          targetNodePartition, targetNodePartition);

      // Ok, now everything is same except numAllocation, update numAllocation.
      this.schedulingRequest.getResourceSizing().setNumAllocations(
          newNumAllocations);
      LOG.info(
          "Update numAllocation from old=" + existingNumAllocations + " to new="
              + newNumAllocations);

      return updateResult;
    }

    // For a new schedulingRequest, we need to validate if we support its asks.
    // This will update internal partitions, etc. after the SchedulingRequest is
    // valid.
    validateAndSetSchedulingRequest(newSchedulingRequest);

    return new PendingAskUpdateResult(null,
        new PendingAsk(newSchedulingRequest.getResourceSizing()), null,
        targetNodePartition);
  }

  @Override
  public PendingAskUpdateResult updatePendingAsk(
      SchedulerRequestKey schedulerRequestKey,
      SchedulingRequest newSchedulingRequest,
      boolean recoverPreemptedRequestForAContainer) {
    writeLock.lock();
    try {
      return internalUpdatePendingAsk(newSchedulingRequest,
          recoverPreemptedRequestForAContainer);
    } finally {
      writeLock.unlock();
    }
  }

  private String throwExceptionWithMetaInfo(String message) {
    StringBuilder sb = new StringBuilder();
    sb.append("AppId=").append(appSchedulingInfo.getApplicationId()).append(
        " Key=").append(this.schedulerRequestKey).append(". Exception message:")
        .append(message);
    throw new SchedulerInvalidResoureRequestException(sb.toString());
  }

  private void validateAndSetSchedulingRequest(SchedulingRequest newSchedulingRequest)
      throws SchedulerInvalidResoureRequestException {
    // Check sizing exists
    if (newSchedulingRequest.getResourceSizing() == null
        || newSchedulingRequest.getResourceSizing().getResources() == null) {
      throwExceptionWithMetaInfo(
          "No ResourceSizing found in the scheduling request, please double "
              + "check");
    }

    // Check execution type == GUARANTEED
    if (newSchedulingRequest.getExecutionType() != null
        && newSchedulingRequest.getExecutionType().getExecutionType()
        != ExecutionType.GUARANTEED) {
      throwExceptionWithMetaInfo(
          "Only GUARANTEED execution type is supported.");
    }

    PlacementConstraint constraint =
        newSchedulingRequest.getPlacementConstraint();

    // We only accept SingleConstraint
    PlacementConstraint.AbstractConstraint ac = constraint.getConstraintExpr();
    if (!(ac instanceof PlacementConstraint.SingleConstraint)) {
      throwExceptionWithMetaInfo(
          "Only accepts " + PlacementConstraint.SingleConstraint.class.getName()
              + " as constraint-expression. Rejecting the new added "
              + "constraint-expression.class=" + ac.getClass().getName());
    }

    PlacementConstraint.SingleConstraint singleConstraint =
        (PlacementConstraint.SingleConstraint) ac;

    // Make sure it is an anti-affinity request (actually this implementation
    // should be able to support both affinity / anti-affinity without much
    // effort. Considering potential test effort required. Limit to
    // anti-affinity to intra-app and scope is node.
    if (!singleConstraint.getScope().equals(PlacementConstraints.NODE)) {
      throwExceptionWithMetaInfo(
          "Only support scope=" + PlacementConstraints.NODE
              + "now. PlacementConstraint=" + singleConstraint);
    }

    if (singleConstraint.getMinCardinality() != 0
        || singleConstraint.getMaxCardinality() != 0) {
      throwExceptionWithMetaInfo(
          "Only support anti-affinity, which is: minCardinality=0, "
              + "maxCardinality=1");
    }

    Set<PlacementConstraint.TargetExpression> targetExpressionSet =
        singleConstraint.getTargetExpressions();
    if (targetExpressionSet == null || targetExpressionSet.isEmpty()) {
      throwExceptionWithMetaInfo(
          "TargetExpression should not be null or empty");
    }

    // Set node partition
    String nodePartition = null;

    // Target allocation tags
    Set<String> targetAllocationTags = null;

    for (PlacementConstraint.TargetExpression targetExpression : targetExpressionSet) {
      // Handle node partition
      if (targetExpression.getTargetType().equals(
          PlacementConstraint.TargetExpression.TargetType.NODE_ATTRIBUTE)) {
        // For node attribute target, we only support Partition now. And once
        // YARN-3409 is merged, we will support node attribute.
        if (!targetExpression.getTargetKey().equals(NODE_PARTITION)) {
          throwExceptionWithMetaInfo("When TargetType="
              + PlacementConstraint.TargetExpression.TargetType.NODE_ATTRIBUTE
              + " only " + NODE_PARTITION + " is accepted as TargetKey.");
        }

        if (nodePartition != null) {
          // This means we have duplicated node partition entry inside placement
          // constraint, which might be set by mistake.
          throwExceptionWithMetaInfo(
              "Only one node partition targetExpression is allowed");
        }

        Set<String> values = targetExpression.getTargetValues();
        if (values == null || values.isEmpty()) {
          nodePartition = RMNodeLabelsManager.NO_LABEL;
          continue;
        }

        if (values.size() > 1) {
          throwExceptionWithMetaInfo("Inside one targetExpression, we only "
              + "support affinity to at most one node partition now");
        }

        nodePartition = values.iterator().next();
      } else if (targetExpression.getTargetType().equals(
          PlacementConstraint.TargetExpression.TargetType.ALLOCATION_TAG)) {
        // Handle allocation tags
        if (targetAllocationTags != null) {
          // This means we have duplicated AllocationTag expressions entries
          // inside placement constraint, which might be set by mistake.
          throwExceptionWithMetaInfo(
              "Only one AllocationTag targetExpression is allowed");
        }

        if (targetExpression.getTargetValues() == null || targetExpression
            .getTargetValues().isEmpty()) {
          throwExceptionWithMetaInfo("Failed to find allocation tags from "
              + "TargetExpressions or couldn't find self-app target.");
        }

        targetAllocationTags = new HashSet<>(
            targetExpression.getTargetValues());

        if (targetExpression.getTargetKey() == null || !targetExpression
            .getTargetKey().equals(APPLICATION_LABEL_INTRA_APPLICATION)) {
          throwExceptionWithMetaInfo(
              "As of now, the only accepted target key for targetKey of "
                  + "allocation_tag target expression is: ["
                  + APPLICATION_LABEL_INTRA_APPLICATION
                  + "]. Please make changes to placement constraints "
                  + "accordingly.");
        }
      }
    }

    if (targetAllocationTags == null) {
      // That means we don't have ALLOCATION_TAG specified
      throwExceptionWithMetaInfo(
          "Couldn't find target expression with type == ALLOCATION_TAG, it is "
              + "required to include one and only one target expression with "
              + "type == ALLOCATION_TAG");

    }

    if (nodePartition == null) {
      nodePartition = RMNodeLabelsManager.NO_LABEL;
    }

    // Validation is done. set local results:
    this.targetNodePartition = nodePartition;
    this.targetAllocationTags = targetAllocationTags;

    this.schedulingRequest = new SchedulingRequestPBImpl(
        ((SchedulingRequestPBImpl) newSchedulingRequest).getProto());

    LOG.info("Successfully added SchedulingRequest to app=" + appSchedulingInfo
        .getApplicationAttemptId() + " targetAllocationTags=[" + StringUtils
        .join(",", targetAllocationTags) + "]. nodePartition="
        + targetNodePartition);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<String, ResourceRequest> getResourceRequests() {
    return Collections.EMPTY_MAP;
  }

  @Override
  public PendingAsk getPendingAsk(String resourceName) {
    readLock.lock();
    try {
      if (resourceName.equals("*") && schedulingRequest != null) {
        return new PendingAsk(schedulingRequest.getResourceSizing());
      }
      return PendingAsk.ZERO;
    } finally {
      readLock.unlock();
    }

  }

  @Override
  public int getOutstandingAsksCount(String resourceName) {
    readLock.lock();
    try {
      if (resourceName.equals("*") && schedulingRequest != null) {
        return schedulingRequest.getResourceSizing().getNumAllocations();
      }
      return 0;
    } finally {
      readLock.unlock();
    }
  }

  private void decreasePendingNumAllocation() {
    // Deduct pending #allocations by 1
    ResourceSizing sizing = schedulingRequest.getResourceSizing();
    sizing.setNumAllocations(sizing.getNumAllocations() - 1);
  }

  @Override
  public ContainerRequest allocate(SchedulerRequestKey schedulerKey,
      NodeType type, SchedulerNode node) {
    writeLock.lock();
    try {
      // Per container scheduling request, it is just a copy of existing
      // scheduling request with #allocations=1
      SchedulingRequest containerSchedulingRequest = new SchedulingRequestPBImpl(
          ((SchedulingRequestPBImpl) schedulingRequest).getProto());
      containerSchedulingRequest.getResourceSizing().setNumAllocations(1);

      // Deduct sizing
      decreasePendingNumAllocation();

      return new ContainerRequest(containerSchedulingRequest);
    } finally {
      writeLock.unlock();
    }
  }

  private boolean checkCardinalityAndPending(SchedulerNode node) {
    // Do we still have pending resource?
    if (schedulingRequest.getResourceSizing().getNumAllocations() <= 0) {
      return false;
    }

    // node type will be ignored.
    try {
      return PlacementConstraintsUtil.canSatisfyConstraints(
          appSchedulingInfo.getApplicationId(), schedulingRequest, node,
          placementConstraintManager, allocationTagsManager);
    } catch (InvalidAllocationTagsQueryException e) {
      LOG.warn("Failed to query node cardinality:", e);
      return false;
    }
  }

  @Override
  public boolean canAllocate(NodeType type, SchedulerNode node) {
    try {
      readLock.lock();
      return checkCardinalityAndPending(node);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public boolean canDelayTo(String resourceName) {
    return true;
  }

  @Override
  public boolean precheckNode(SchedulerNode schedulerNode,
      SchedulingMode schedulingMode) {
    // We will only look at node label = nodeLabelToLookAt according to
    // schedulingMode and partition of node.
    String nodePartitionToLookAt;
    if (schedulingMode == SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY) {
      nodePartitionToLookAt = schedulerNode.getPartition();
    } else{
      nodePartitionToLookAt = RMNodeLabelsManager.NO_LABEL;
    }

    readLock.lock();
    try {
      // Check node partition as well as cardinality/pending resources.
      return this.targetNodePartition.equals(nodePartitionToLookAt)
          && checkCardinalityAndPending(schedulerNode);
    } finally {
      readLock.unlock();
    }

  }

  @Override
  public String getPrimaryRequestedNodePartition() {
    return targetNodePartition;
  }

  @Override
  public int getUniqueLocationAsks() {
    return 1;
  }

  @Override
  public void showRequests() {
    try {
      readLock.lock();
      if (schedulingRequest != null) {
        LOG.info(schedulingRequest.toString());
      }
    } finally {
      readLock.unlock();
    }
  }

  @VisibleForTesting
  SchedulingRequest getSchedulingRequest() {
    return schedulingRequest;
  }

  @VisibleForTesting
  String getTargetNodePartition() {
    return targetNodePartition;
  }

  @VisibleForTesting
  Set<String> getTargetAllocationTags() {
    return targetAllocationTags;
  }

  @Override
  public void initialize(AppSchedulingInfo appSchedulingInfo,
      SchedulerRequestKey schedulerRequestKey, RMContext rmContext) {
    super.initialize(appSchedulingInfo, schedulerRequestKey, rmContext);
    this.allocationTagsManager = rmContext.getAllocationTagsManager();
    this.placementConstraintManager = rmContext.getPlacementConstraintManager();
  }
}

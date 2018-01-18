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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint;

import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.AbstractConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.SingleConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TargetExpression;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TargetExpression.TargetType;
import org.apache.hadoop.yarn.api.resource.PlacementConstraintTransformations.SingleConstraintTransformer;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm.DefaultPlacementAlgorithm;

import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.NODE_PARTITION;

/**
 * This class contains various static methods used by the Placement Algorithms
 * to simplify constrained placement.
 * (see also {@link DefaultPlacementAlgorithm}).
 */
@Public
@Unstable
public final class PlacementConstraintsUtil {
  private static final Log LOG =
      LogFactory.getLog(PlacementConstraintsUtil.class);

  // Suppresses default constructor, ensuring non-instantiability.
  private PlacementConstraintsUtil() {
  }

  /**
   * Returns true if **single** placement constraint with associated
   * allocationTags and scope is satisfied by a specific scheduler Node.
   *
   * @param targetApplicationId the application id, which could be override by
   *                           target application id specified inside allocation
   *                           tags.
   * @param sc the placement constraint
   * @param te the target expression
   * @param node the scheduler node
   * @param tm the allocation tags store
   * @return true if single application constraint is satisfied by node
   * @throws InvalidAllocationTagsQueryException
   */
  private static boolean canSatisfySingleConstraintExpression(
      ApplicationId targetApplicationId, SingleConstraint sc,
      TargetExpression te, SchedulerNode node, AllocationTagsManager tm)
      throws InvalidAllocationTagsQueryException {
    long minScopeCardinality = 0;
    long maxScopeCardinality = 0;
    
    // Optimizations to only check cardinality if necessary.
    int desiredMinCardinality = sc.getMinCardinality();
    int desiredMaxCardinality = sc.getMaxCardinality();
    boolean checkMinCardinality = desiredMinCardinality > 0;
    boolean checkMaxCardinality = desiredMaxCardinality < Integer.MAX_VALUE;

    if (sc.getScope().equals(PlacementConstraints.NODE)) {
      if (checkMinCardinality) {
        minScopeCardinality = tm.getNodeCardinalityByOp(node.getNodeID(),
            targetApplicationId, te.getTargetValues(), Long::max);
      }
      if (checkMaxCardinality) {
        maxScopeCardinality = tm.getNodeCardinalityByOp(node.getNodeID(),
            targetApplicationId, te.getTargetValues(), Long::min);
      }
    } else if (sc.getScope().equals(PlacementConstraints.RACK)) {
      if (checkMinCardinality) {
        minScopeCardinality = tm.getRackCardinalityByOp(node.getRackName(),
            targetApplicationId, te.getTargetValues(), Long::max);
      }
      if (checkMaxCardinality) {
        maxScopeCardinality = tm.getRackCardinalityByOp(node.getRackName(),
            targetApplicationId, te.getTargetValues(), Long::min);
      }
    }
    // Make sure Anti-affinity satisfies hard upper limit
    maxScopeCardinality = desiredMaxCardinality == 0 ? maxScopeCardinality - 1
        : maxScopeCardinality;

    return (desiredMinCardinality <= 0
        || minScopeCardinality >= desiredMinCardinality) && (
        desiredMaxCardinality == Integer.MAX_VALUE
            || maxScopeCardinality < desiredMaxCardinality);
  }

  private static boolean canSatisfyNodePartitionConstraintExpresssion(
      TargetExpression targetExpression, SchedulerNode schedulerNode) {
    Set<String> values = targetExpression.getTargetValues();
    if (values == null || values.isEmpty()) {
      return schedulerNode.getPartition().equals(
          RMNodeLabelsManager.NO_LABEL);
    } else{
      String nodePartition = values.iterator().next();
      if (!nodePartition.equals(schedulerNode.getPartition())) {
        return false;
      }
    }

    return true;
  }

  private static boolean canSatisfySingleConstraint(ApplicationId applicationId,
      SingleConstraint singleConstraint, SchedulerNode schedulerNode,
      AllocationTagsManager tagsManager)
      throws InvalidAllocationTagsQueryException {
    // Iterate through TargetExpressions
    Iterator<TargetExpression> expIt =
        singleConstraint.getTargetExpressions().iterator();
    while (expIt.hasNext()) {
      TargetExpression currentExp = expIt.next();
      // Supporting AllocationTag Expressions for now
      if (currentExp.getTargetType().equals(TargetType.ALLOCATION_TAG)) {
        // Check if conditions are met
        if (!canSatisfySingleConstraintExpression(applicationId,
            singleConstraint, currentExp, schedulerNode, tagsManager)) {
          return false;
        }
      } else if (currentExp.getTargetType().equals(TargetType.NODE_ATTRIBUTE)
          && currentExp.getTargetKey().equals(NODE_PARTITION)) {
        // This is a node partition expression, check it.
        canSatisfyNodePartitionConstraintExpresssion(currentExp, schedulerNode);
      }
    }
    // return true if all targetExpressions are satisfied
    return true;
  }

  /**
   * Returns true if all placement constraints are **currently** satisfied by a
   * specific scheduler Node..
   *
   * To do so the method retrieves and goes through all application constraint
   * expressions and checks if the specific allocation is between the allowed
   * min-max cardinality values under the constraint scope (Node/Rack/etc).
   *
   * @param applicationId applicationId,
   * @param placementConstraint placement constraint.
   * @param node the scheduler node
   * @param tagsManager the allocation tags store
   * @return true if all application constraints are satisfied by node
   * @throws InvalidAllocationTagsQueryException
   */
  public static boolean canSatisfySingleConstraint(ApplicationId applicationId,
      PlacementConstraint placementConstraint, SchedulerNode node,
      AllocationTagsManager tagsManager)
      throws InvalidAllocationTagsQueryException {
    if (placementConstraint == null) {
      return true;
    }
    // Transform to SimpleConstraint
    SingleConstraintTransformer singleTransformer =
        new SingleConstraintTransformer(placementConstraint);
    placementConstraint = singleTransformer.transform();
    AbstractConstraint sConstraintExpr = placementConstraint.getConstraintExpr();
    SingleConstraint single = (SingleConstraint) sConstraintExpr;

    return canSatisfySingleConstraint(applicationId, single, node, tagsManager);
  }

  /**
   * Returns true if all placement constraints with associated allocationTags
   * are **currently** satisfied by a specific scheduler Node.
   * To do so the method retrieves and goes through all application constraint
   * expressions and checks if the specific allocation is between the allowed
   * min-max cardinality values under the constraint scope (Node/Rack/etc).
   *
   * @param appId the application id
   * @param allocationTags the allocation tags set
   * @param node the scheduler node
   * @param pcm the placement constraints store
   * @param tagsManager the allocation tags store
   * @return true if all application constraints are satisfied by node
   * @throws InvalidAllocationTagsQueryException
   */
  public static boolean canSatisfySingleConstraint(ApplicationId appId,
      Set<String> allocationTags, SchedulerNode node,
      PlacementConstraintManager pcm, AllocationTagsManager tagsManager)
      throws InvalidAllocationTagsQueryException {
    PlacementConstraint constraint = pcm.getConstraint(appId, allocationTags);
    return canSatisfySingleConstraint(appId, constraint, node, tagsManager);
  }

}

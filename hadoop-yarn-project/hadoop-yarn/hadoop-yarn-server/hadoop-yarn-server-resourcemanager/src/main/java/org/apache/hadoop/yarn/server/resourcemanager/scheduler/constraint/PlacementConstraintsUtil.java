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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm.DefaultPlacementAlgorithm;

/**
 * This class contains various static methods used by the Placement Algorithms
 * to simplify constrained placement.
 * (see also {@link DefaultPlacementAlgorithm}).
 */
@Public
@Unstable
public final class PlacementConstraintsUtil {

  // Suppresses default constructor, ensuring non-instantiability.
  private PlacementConstraintsUtil() {
  }

  /**
   * Returns true if **single** application constraint with associated
   * allocationTags and scope is satisfied by a specific scheduler Node.
   *
   * @param appId the application id
   * @param sc the placement constraint
   * @param te the target expression
   * @param node the scheduler node
   * @param tm the allocation tags store
   * @return true if single application constraint is satisfied by node
   * @throws InvalidAllocationTagsQueryException
   */
  private static boolean canSatisfySingleConstraintExpression(
      ApplicationId appId, SingleConstraint sc, TargetExpression te,
      SchedulerNode node, AllocationTagsManager tm)
      throws InvalidAllocationTagsQueryException {
    long minScopeCardinality = 0;
    long maxScopeCardinality = 0;
    if (sc.getScope().equals(PlacementConstraints.NODE)) {
      minScopeCardinality = tm.getNodeCardinalityByOp(node.getNodeID(), appId,
          te.getTargetValues(), Long::max);
      maxScopeCardinality = tm.getNodeCardinalityByOp(node.getNodeID(), appId,
          te.getTargetValues(), Long::min);
    } else if (sc.getScope().equals(PlacementConstraints.RACK)) {
      minScopeCardinality = tm.getRackCardinalityByOp(node.getRackName(), appId,
          te.getTargetValues(), Long::max);
      maxScopeCardinality = tm.getRackCardinalityByOp(node.getRackName(), appId,
          te.getTargetValues(), Long::min);
    }
    // Make sure Anti-affinity satisfies hard upper limit
    maxScopeCardinality = sc.getMaxCardinality() == 0 ? maxScopeCardinality - 1
        : maxScopeCardinality;

    return (minScopeCardinality >= sc.getMinCardinality()
        && maxScopeCardinality < sc.getMaxCardinality());
  }

  /**
   * Returns true if all application constraints with associated allocationTags
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
  public static boolean canSatisfyConstraints(ApplicationId appId,
      Set<String> allocationTags, SchedulerNode node,
      PlacementConstraintManager pcm, AllocationTagsManager tagsManager)
      throws InvalidAllocationTagsQueryException {
    PlacementConstraint constraint = pcm.getConstraint(appId, allocationTags);
    if (constraint == null) {
      return true;
    }
    // Transform to SimpleConstraint
    SingleConstraintTransformer singleTransformer =
        new SingleConstraintTransformer(constraint);
    constraint = singleTransformer.transform();
    AbstractConstraint sConstraintExpr = constraint.getConstraintExpr();
    SingleConstraint single = (SingleConstraint) sConstraintExpr;
    // Iterate through TargetExpressions
    Iterator<TargetExpression> expIt = single.getTargetExpressions().iterator();
    while (expIt.hasNext()) {
      TargetExpression currentExp = expIt.next();
      // Supporting AllocationTag Expressions for now
      if (currentExp.getTargetType().equals(TargetType.ALLOCATION_TAG)) {
        // If source and tag allocation tags are the same, we do not enforce
        // constraints with minimum cardinality.
        if (currentExp.getTargetValues().equals(allocationTags)
            && single.getMinCardinality() > 0) {
          return true;
        }
        // Check if conditions are met
        if (!canSatisfySingleConstraintExpression(appId, single, currentExp,
            node, tagsManager)) {
          return false;
        }
      }
    }
    // return true if all targetExpressions are satisfied
    return true;
  }

}

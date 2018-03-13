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
import org.apache.hadoop.yarn.api.records.AllocationTagNamespace;
import org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.records.TargetApplications;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.AbstractConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.And;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.Or;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.SingleConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TargetExpression;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TargetExpression.TargetType;
import org.apache.hadoop.yarn.api.resource.PlacementConstraintTransformations.SingleConstraintTransformer;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints;
import org.apache.hadoop.yarn.exceptions.InvalidAllocationTagException;
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
   * Try to the namespace of the allocation tags from the given target key.
   *
   * @param targetKey
   * @return allocation tag namespace.
   * @throws InvalidAllocationTagsQueryException
   * if fail to parse the target key to a valid namespace.
   */
  private static AllocationTagNamespace getAllocationTagNamespace(
      ApplicationId currentAppId, String targetKey, AllocationTagsManager atm)
      throws InvalidAllocationTagException{
    // Parse to a valid namespace.
    AllocationTagNamespace namespace = AllocationTagNamespace.parse(targetKey);

    // TODO remove such check once we support all forms of namespaces
    if (!namespace.isIntraApp() && !namespace.isSingleInterApp()) {
      throw new InvalidAllocationTagException(
          "Only support " + AllocationTagNamespaceType.SELF.toString()
              + " and "+ AllocationTagNamespaceType.APP_ID + " now,"
              + namespace.toString() + " is not supported yet!");
    }

    // Evaluate the namespace according to the given target
    // before it can be consumed.
    TargetApplications ta = new TargetApplications(currentAppId,
        atm.getAllApplicationIds());
    namespace.evaluate(ta);
    return namespace;
  }

  // We return a single app Id now, because at present,
  // only self and app-id namespace is supported. But moving on,
  // this will return a set of application IDs.
  // TODO support other forms of namespaces
  private static ApplicationId getNamespaceScope(
      AllocationTagNamespace namespace)
      throws InvalidAllocationTagException {
    if (namespace.getNamespaceScope() == null
        || namespace.getNamespaceScope().size() != 1) {
      throw new InvalidAllocationTagException(
          "Invalid allocation tag namespace " + namespace.toString()
              + ", expecting it is not null and only 1 application"
              + " ID in the scope.");
    }
    return namespace.getNamespaceScope().iterator().next();
  }

  /**
   * Returns true if <b>single</b> placement constraint with associated
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
    // Parse the allocation tag's namespace from the given target key,
    // then evaluate the namespace and get its scope,
    // which is represented by one or more application IDs.
    ApplicationId effectiveAppID;
    try {
      AllocationTagNamespace namespace = getAllocationTagNamespace(
          targetApplicationId, te.getTargetKey(), tm);
      effectiveAppID = getNamespaceScope(namespace);
    } catch (InvalidAllocationTagException e) {
      throw new InvalidAllocationTagsQueryException(e);
    }

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
            effectiveAppID, te.getTargetValues(), Long::max);
      }
      if (checkMaxCardinality) {
        maxScopeCardinality = tm.getNodeCardinalityByOp(node.getNodeID(),
            effectiveAppID, te.getTargetValues(), Long::min);
      }
    } else if (sc.getScope().equals(PlacementConstraints.RACK)) {
      if (checkMinCardinality) {
        minScopeCardinality = tm.getRackCardinalityByOp(node.getRackName(),
            effectiveAppID, te.getTargetValues(), Long::max);
      }
      if (checkMaxCardinality) {
        maxScopeCardinality = tm.getRackCardinalityByOp(node.getRackName(),
            effectiveAppID, te.getTargetValues(), Long::min);
      }
    }

    return (desiredMinCardinality <= 0
        || minScopeCardinality >= desiredMinCardinality) && (
        desiredMaxCardinality == Integer.MAX_VALUE
            || maxScopeCardinality <= desiredMaxCardinality);
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
   * Returns true if all child constraints are satisfied.
   * @param appId application id
   * @param constraint Or constraint
   * @param node node
   * @param atm allocation tags manager
   * @return true if all child constraints are satisfied, false otherwise
   * @throws InvalidAllocationTagsQueryException
   */
  private static boolean canSatisfyAndConstraint(ApplicationId appId,
      And constraint, SchedulerNode node, AllocationTagsManager atm)
      throws InvalidAllocationTagsQueryException {
    // Iterate over the constraints tree, if found any child constraint
    // isn't satisfied, return false.
    for (AbstractConstraint child : constraint.getChildren()) {
      if(!canSatisfyConstraints(appId, child.build(), node, atm)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns true as long as any of child constraint is satisfied.
   * @param appId application id
   * @param constraint Or constraint
   * @param node node
   * @param atm allocation tags manager
   * @return true if any child constraint is satisfied, false otherwise
   * @throws InvalidAllocationTagsQueryException
   */
  private static boolean canSatisfyOrConstraint(ApplicationId appId,
      Or constraint, SchedulerNode node, AllocationTagsManager atm)
      throws InvalidAllocationTagsQueryException {
    for (AbstractConstraint child : constraint.getChildren()) {
      if (canSatisfyConstraints(appId, child.build(), node, atm)) {
        return true;
      }
    }
    return false;
  }

  private static boolean canSatisfyConstraints(ApplicationId appId,
      PlacementConstraint constraint, SchedulerNode node,
      AllocationTagsManager atm)
      throws InvalidAllocationTagsQueryException {
    if (constraint == null) {
      return true;
    }

    // If this is a single constraint, transform to SingleConstraint
    SingleConstraintTransformer singleTransformer =
        new SingleConstraintTransformer(constraint);
    constraint = singleTransformer.transform();
    AbstractConstraint sConstraintExpr = constraint.getConstraintExpr();

    // TODO handle other type of constraints, e.g CompositeConstraint
    if (sConstraintExpr instanceof SingleConstraint) {
      SingleConstraint single = (SingleConstraint) sConstraintExpr;
      return canSatisfySingleConstraint(appId, single, node, atm);
    } else if (sConstraintExpr instanceof And) {
      And and = (And) sConstraintExpr;
      return canSatisfyAndConstraint(appId, and, node, atm);
    } else if (sConstraintExpr instanceof Or) {
      Or or = (Or) sConstraintExpr;
      return canSatisfyOrConstraint(appId, or, node, atm);
    } else {
      throw new InvalidAllocationTagsQueryException(
          "Unsupported type of constraint: "
              + sConstraintExpr.getClass().getSimpleName());
    }
  }

  /**
   * Returns true if the placement constraint for a given scheduling request
   * is <b>currently</b> satisfied by the specific scheduler node. This method
   * first validates the constraint specified in the request; if not specified,
   * then it validates application level constraint if exists; otherwise, it
   * validates the global constraint if exists.
   *
   * This method only checks whether a scheduling request can be placed
   * on a node with respect to the certain placement constraint. It gives no
   * guarantee that asked allocations can be eventually allocated because
   * it doesn't check resource, that needs to be further decided by a scheduler.
   *
   * @param applicationId application id
   * @param request scheduling request
   * @param schedulerNode node
   * @param pcm placement constraint manager
   * @param atm allocation tags manager
   * @return true if the given node satisfies the constraint of the request
   * @throws InvalidAllocationTagsQueryException
   */
  public static boolean canSatisfyConstraints(ApplicationId applicationId,
      SchedulingRequest request, SchedulerNode schedulerNode,
      PlacementConstraintManager pcm, AllocationTagsManager atm)
      throws InvalidAllocationTagsQueryException {
    Set<String> sourceTags = null;
    PlacementConstraint pc = null;
    if (request != null) {
      sourceTags = request.getAllocationTags();
      pc = request.getPlacementConstraint();
    }
    return canSatisfyConstraints(applicationId,
        pcm.getMultilevelConstraint(applicationId, sourceTags, pc),
        schedulerNode, atm);
  }
}

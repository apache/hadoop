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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.AbstractConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.And;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.Or;
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
  private static final Logger LOG =
      LoggerFactory.getLogger(PlacementConstraintsUtil.class);

  // Suppresses default constructor, ensuring non-instantiability.
  private PlacementConstraintsUtil() {
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
    // Creates AllocationTags that will be further consumed by allocation
    // tags manager for cardinality check.
    AllocationTags allocationTags = AllocationTags.createAllocationTags(
        targetApplicationId, te.getTargetKey(), te.getTargetValues());

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
            allocationTags, Long::min);
      }
      if (checkMaxCardinality) {
        maxScopeCardinality = tm.getNodeCardinalityByOp(node.getNodeID(),
            allocationTags, Long::max);
      }
    } else if (sc.getScope().equals(PlacementConstraints.RACK)) {
      if (checkMinCardinality) {
        minScopeCardinality = tm.getRackCardinalityByOp(node.getRackName(),
            allocationTags, Long::min);
      }
      if (checkMaxCardinality) {
        maxScopeCardinality = tm.getRackCardinalityByOp(node.getRackName(),
            allocationTags, Long::max);
      }
    }

    return (desiredMinCardinality <= 0
        || minScopeCardinality >= desiredMinCardinality) && (
        desiredMaxCardinality == Integer.MAX_VALUE
            || maxScopeCardinality <= desiredMaxCardinality);
  }

  private static boolean canSatisfyNodeConstraintExpression(
      SingleConstraint sc, TargetExpression targetExpression,
      SchedulerNode schedulerNode) {
    Set<String> values = targetExpression.getTargetValues();

    if (targetExpression.getTargetKey().equals(NODE_PARTITION)) {
      if (values == null || values.isEmpty()) {
        return schedulerNode.getPartition()
            .equals(RMNodeLabelsManager.NO_LABEL);
      } else {
        String nodePartition = values.iterator().next();
        if (!nodePartition.equals(schedulerNode.getPartition())) {
          return false;
        }
      }
    } else {
      NodeAttributeOpCode opCode = sc.getNodeAttributeOpCode();
      // compare attributes.
      String inputAttribute = values.iterator().next();
      NodeAttribute requestAttribute = getNodeConstraintFromRequest(
          targetExpression.getTargetKey(), inputAttribute);
      if (requestAttribute == null) {
        return true;
      }

      return getNodeConstraintEvaluatedResult(schedulerNode, opCode,
          requestAttribute);
    }
    return true;
  }

  private static boolean getNodeConstraintEvaluatedResult(
      SchedulerNode schedulerNode,
      NodeAttributeOpCode opCode, NodeAttribute requestAttribute) {
    // In case, attributes in a node is empty or incoming attributes doesn't
    // exist on given node, accept such nodes for scheduling if opCode is
    // equals to NE. (for eg. java != 1.8 could be scheduled on a node
    // where java is not configured.)
    if (schedulerNode.getNodeAttributes() == null ||
        !schedulerNode.getNodeAttributes().contains(requestAttribute)) {
      if (opCode == NodeAttributeOpCode.NE) {
        LOG.debug("Incoming requestAttribute:{} is not present in {},"
            + " however opcode is NE. Hence accept this node.",
            requestAttribute, schedulerNode.getNodeID());
        return true;
      }
      LOG.debug("Incoming requestAttribute:{} is not present in {},"
          + " skip such node.", requestAttribute, schedulerNode.getNodeID());
      return false;
    }

    boolean found = false;
    for (Iterator<NodeAttribute> it = schedulerNode.getNodeAttributes()
        .iterator(); it.hasNext();) {
      NodeAttribute nodeAttribute = it.next();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Starting to compare Incoming requestAttribute :"
            + requestAttribute
            + " with requestAttribute value= " + requestAttribute
            .getAttributeValue()
            + ", stored nodeAttribute value=" + nodeAttribute
            .getAttributeValue());
      }
      if (requestAttribute.equals(nodeAttribute)) {
        if (isOpCodeMatches(requestAttribute, nodeAttribute, opCode)) {
          LOG.debug("Incoming requestAttribute:{} matches with node:{}",
              requestAttribute, schedulerNode.getNodeID());
          found = true;
          return found;
        }
      }
    }
    if (!found) {
      LOG.debug("skip this node:{} for requestAttribute:{}",
          schedulerNode.getNodeID(), requestAttribute);
      return false;
    }
    return true;
  }

  private static boolean isOpCodeMatches(NodeAttribute requestAttribute,
      NodeAttribute nodeAttribute, NodeAttributeOpCode opCode) {
    boolean retCode = false;
    switch (opCode) {
    case EQ:
      retCode = requestAttribute.getAttributeValue()
          .equals(nodeAttribute.getAttributeValue());
      break;
    case NE:
      retCode = !(requestAttribute.getAttributeValue()
          .equals(nodeAttribute.getAttributeValue()));
      break;
    default:
      break;
    }
    return retCode;
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
      } else if (currentExp.getTargetType().equals(TargetType.NODE_ATTRIBUTE)) {
        // This is a node attribute expression, check it.
        if (!canSatisfyNodeConstraintExpression(singleConstraint, currentExp,
            schedulerNode)) {
          return false;
        }
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
      LOG.debug("Constraint is found empty during constraint validation for"
          + " app:{}", appId);
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

  private static NodeAttribute getNodeConstraintFromRequest(String attrKey,
      String attrString) {
    NodeAttribute nodeAttribute = null;
    LOG.debug("Incoming node attribute: {}={}", attrKey, attrString);

    // Input node attribute could be like 1.8
    String[] name = attrKey.split("/");
    if (name == null || name.length == 1) {
      nodeAttribute = NodeAttribute
          .newInstance(attrKey, NodeAttributeType.STRING, attrString);
    } else {
      nodeAttribute = NodeAttribute
          .newInstance(name[0], name[1], NodeAttributeType.STRING, attrString);
    }

    return nodeAttribute;
  }
}

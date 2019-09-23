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

package org.apache.hadoop.yarn.api.resource;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType;
import org.apache.hadoop.yarn.api.records.NodeAttributeOpCode;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.AbstractConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.And;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.DelayedOr;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.Or;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.SingleConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TargetExpression;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TargetExpression.TargetType;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TimedPlacementConstraint;

/**
 * This class contains various static methods for the applications to create
 * placement constraints (see also {@link PlacementConstraint}).
 */
@Public
@Unstable
public final class PlacementConstraints {

  // Suppresses default constructor, ensuring non-instantiability.
  private PlacementConstraints() {
  }

  // Creation of simple constraints.

  public static final String NODE = PlacementConstraint.NODE_SCOPE;
  public static final String RACK = PlacementConstraint.RACK_SCOPE;
  public static final String NODE_PARTITION = "yarn_node_partition/";

  /**
   * Creates a constraint that requires allocations to be placed on nodes that
   * satisfy all target expressions within the given scope (e.g., node or rack).
   *
   * For example, {@code targetIn(RACK, allocationTag("hbase-m"))}, allows
   * allocations on nodes that belong to a rack that has at least one tag with
   * value "hbase-m".
   *
   * @param scope the scope within which the target expressions should be
   *          satisfied
   * @param targetExpressions the expressions that need to be satisfied within
   *          the scope
   * @return the resulting placement constraint
   */
  public static AbstractConstraint targetIn(String scope,
      TargetExpression... targetExpressions) {
    return new SingleConstraint(scope, 1, Integer.MAX_VALUE, targetExpressions);
  }

  /**
   * Creates a constraint that requires allocations to be placed on nodes that
   * belong to a scope (e.g., node or rack) that does not satisfy any of the
   * target expressions.
   *
   * @param scope the scope within which the target expressions should not be
   *          true
   * @param targetExpressions the expressions that need to not be true within
   *          the scope
   * @return the resulting placement constraint
   */
  public static AbstractConstraint targetNotIn(String scope,
      TargetExpression... targetExpressions) {
    return new SingleConstraint(scope, 0, 0, targetExpressions);
  }

  /**
   * Creates a constraint that requires allocations to be placed on nodes that
   * belong to a scope (e.g., node or rack) that satisfy any of the
   * target expressions based on node attribute op code.
   *
   * @param scope the scope within which the target expressions should not be
   *          true
   * @param opCode Node Attribute code which could be equals, not equals.
   * @param targetExpressions the expressions that need to not be true within
   *          the scope
   * @return the resulting placement constraint
   */
  public static AbstractConstraint targetNodeAttribute(String scope,
      NodeAttributeOpCode opCode,
      TargetExpression... targetExpressions) {
    return new SingleConstraint(scope, -1, -1, opCode, targetExpressions);
  }

  /**
   * Creates a constraint that restricts the number of allocations within a
   * given scope (e.g., node or rack).
   *
   * For example, {@code cardinality(NODE, 3, 10, "zk")} is satisfied on nodes
   * where there are no less than 3 allocations with tag "zk" and no more than
   * 10.
   *
   * @param scope the scope of the constraint
   * @param minCardinality determines the minimum number of allocations within
   *          the scope
   * @param maxCardinality determines the maximum number of allocations within
   *          the scope
   * @param allocationTags the constraint targets allocations with these tags
   * @return the resulting placement constraint
   */
  public static AbstractConstraint cardinality(String scope, int minCardinality,
      int maxCardinality, String... allocationTags) {
    return new SingleConstraint(scope, minCardinality, maxCardinality,
        PlacementTargets.allocationTag(allocationTags));
  }

  /**
   * Similar to {@link #cardinality(String, int, int, String...)}, but let you
   * attach a namespace to the given allocation tags.
   *
   * @param scope the scope of the constraint
   * @param namespace the namespace of the allocation tags
   * @param minCardinality determines the minimum number of allocations within
   *                       the scope
   * @param maxCardinality determines the maximum number of allocations within
   *                       the scope
   * @param allocationTags allocation tags
   * @return the resulting placement constraint
   */
  public static AbstractConstraint cardinality(String scope, String namespace,
      int minCardinality, int maxCardinality, String... allocationTags) {
    return new SingleConstraint(scope, minCardinality, maxCardinality,
        PlacementTargets.allocationTagWithNamespace(namespace, allocationTags));
  }

  /**
   * Similar to {@link #cardinality(String, int, int, String...)}, but
   * determines only the minimum cardinality (the maximum cardinality is
   * unbound).
   *
   * @param scope the scope of the constraint
   * @param minCardinality determines the minimum number of allocations within
   *          the scope
   * @param allocationTags the constraint targets allocations with these tags
   * @return the resulting placement constraint
   */
  public static AbstractConstraint minCardinality(String scope,
      int minCardinality, String... allocationTags) {
    return cardinality(scope, minCardinality, Integer.MAX_VALUE,
        allocationTags);
  }

  /**
   * Similar to {@link #minCardinality(String, int, String...)}, but let you
   * attach a namespace to the allocation tags.
   *
   * @param scope the scope of the constraint
   * @param namespace the namespace of these tags
   * @param minCardinality determines the minimum number of allocations within
   *                       the scope
   * @param allocationTags the constraint targets allocations with these tags
   * @return the resulting placement constraint
   */
  public static AbstractConstraint minCardinality(String scope,
      String namespace, int minCardinality, String... allocationTags) {
    return cardinality(scope, namespace, minCardinality, Integer.MAX_VALUE,
        allocationTags);
  }

  /**
   * Similar to {@link #cardinality(String, int, int, String...)}, but
   * determines only the maximum cardinality (the minimum cardinality is 0).
   *
   * @param scope the scope of the constraint
   * @param maxCardinality determines the maximum number of allocations within
   *          the scope
   * @param allocationTags the constraint targets allocations with these tags
   * @return the resulting placement constraint
   */
  public static AbstractConstraint maxCardinality(String scope,
      int maxCardinality, String... allocationTags) {
    return cardinality(scope, 0, maxCardinality, allocationTags);
  }

  /**
   * Similar to {@link #maxCardinality(String, int, String...)}, but let you
   * specify a namespace for the tags, see supported namespaces in
   * {@link AllocationTagNamespaceType}.
   *
   * @param scope the scope of the constraint
   * @param tagNamespace the namespace of these tags
   * @param maxCardinality determines the maximum number of allocations within
   *          the scope
   * @param allocationTags allocation tags
   * @return the resulting placement constraint
   */
  public static AbstractConstraint maxCardinality(String scope,
      String tagNamespace, int maxCardinality, String... allocationTags) {
    return cardinality(scope, tagNamespace, 0, maxCardinality, allocationTags);
  }

  /**
   * This constraint generalizes the cardinality and target constraints.
   *
   * Consider a set of nodes N that belongs to the scope specified in the
   * constraint. If the target expressions are satisfied at least minCardinality
   * times and at most maxCardinality times in the node set N, then the
   * constraint is satisfied.
   *
   * For example, {@code targetCardinality(RACK, 2, 10, allocationTag("zk"))},
   * requires an allocation to be placed within a rack that has at least 2 and
   * at most 10 other allocations with tag "zk".
   *
   * @param scope the scope of the constraint
   * @param minCardinality the minimum number of times the target expressions
   *          have to be satisfied with the given scope
   * @param maxCardinality the maximum number of times the target expressions
   *          have to be satisfied with the given scope
   * @param targetExpressions the target expressions
   * @return the resulting placement constraint
   */
  public static AbstractConstraint targetCardinality(String scope,
      int minCardinality, int maxCardinality,
      TargetExpression... targetExpressions) {
    return new SingleConstraint(scope, minCardinality, maxCardinality,
        targetExpressions);
  }

  // Creation of target expressions to be used in simple constraints.

  /**
   * Class with static methods for constructing target expressions to be used in
   * placement constraints.
   */
  public static class PlacementTargets {

    /**
     * Constructs a target expression on a node attribute. It is satisfied if
     * the specified node attribute has one of the specified values.
     *
     * @param attributeKey the name of the node attribute
     * @param attributeValues the set of values that the attribute should take
     *          values from
     * @return the resulting expression on the node attribute
     */
    public static TargetExpression nodeAttribute(String attributeKey,
        String... attributeValues) {
      return new TargetExpression(TargetType.NODE_ATTRIBUTE, attributeKey,
          attributeValues);
    }

    /**
     * Constructs a target expression on a node partition. It is satisfied if
     * the specified node partition has one of the specified nodePartitions.
     *
     * @param nodePartitions the set of values that the attribute should take
     *          values from
     * @return the resulting expression on the node attribute
     */
    public static TargetExpression nodePartition(
        String... nodePartitions) {
      return new TargetExpression(TargetType.NODE_ATTRIBUTE, NODE_PARTITION,
          nodePartitions);
    }

    /**
     * Constructs a target expression on an allocation tag. It is satisfied if
     * there are allocations with one of the given tags. The default namespace
     * for these tags is {@link AllocationTagNamespaceType#SELF}, this only
     * checks tags within the application.
     *
     * @param allocationTags the set of tags that the attribute should take
     *          values from
     * @return the resulting expression on the allocation tags
     */
    public static TargetExpression allocationTag(String... allocationTags) {
      return allocationTagWithNamespace(
          AllocationTagNamespaceType.SELF.toString(), allocationTags);
    }

    /**
     * Constructs a target expression on a set of allocation tags under
     * a certain namespace.
     *
     * @param namespace namespace of the allocation tags
     * @param allocationTags allocation tags
     * @return a target expression
     */
    public static TargetExpression allocationTagWithNamespace(String namespace,
        String... allocationTags) {
      return new TargetExpression(TargetType.ALLOCATION_TAG,
          namespace, allocationTags);
    }
  }

  // Creation of compound constraints.

  /**
   * A conjunction of constraints.
   *
   * @param children the children constraints that should all be satisfied
   * @return the resulting placement constraint
   */
  public static And and(AbstractConstraint... children) {
    return new And(children);
  }

  /**
   * A disjunction of constraints.
   *
   * @param children the children constraints, one of which should be satisfied
   * @return the resulting placement constraint
   */
  public static Or or(AbstractConstraint... children) {
    return new Or(children);
  }

  /**
   * Creates a composite constraint that includes a list of timed placement
   * constraints. The scheduler should try to satisfy first the first timed
   * child constraint within the specified time window. If this is not possible,
   * it should attempt to satisfy the second, and so on.
   *
   * @param children the timed children constraints
   * @return the resulting composite constraint
   */
  public static DelayedOr delayedOr(TimedPlacementConstraint... children) {
    return new DelayedOr(children);
  }

  // Creation of timed constraints to be used in a DELAYED_OR constraint.

  /**
   * Creates a placement constraint that has to be satisfied within a time
   * window.
   *
   * @param constraint the placement constraint
   * @param delay the length of the time window within which the constraint has
   *          to be satisfied
   * @param timeUnit the unit of time of the time window
   * @return the resulting timed placement constraint
   */
  public static TimedPlacementConstraint timedClockConstraint(
      AbstractConstraint constraint, long delay, TimeUnit timeUnit) {
    return new TimedPlacementConstraint(constraint, timeUnit.toMillis(delay),
        TimedPlacementConstraint.DelayUnit.MILLISECONDS);
  }

  /**
   * Creates a placement constraint that has to be satisfied within a number of
   * placement opportunities (invocations of the scheduler).
   *
   * @param constraint the placement constraint
   * @param delay the number of scheduling opportunities within which the
   *          constraint has to be satisfied
   * @return the resulting timed placement constraint
   */
  public static TimedPlacementConstraint timedOpportunitiesConstraint(
      AbstractConstraint constraint, long delay) {
    return new TimedPlacementConstraint(constraint, delay,
        TimedPlacementConstraint.DelayUnit.OPPORTUNITIES);
  }

  /**
   * Creates a {@link PlacementConstraint} given a constraint expression.
   *
   * @param constraintExpr the constraint expression
   * @return the placement constraint
   */
  public static PlacementConstraint build(AbstractConstraint constraintExpr) {
    return constraintExpr.build();
  }
}

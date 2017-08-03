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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * {@code PlacementConstraint} represents a placement constraint for a resource
 * allocation.
 */
@Public
@Unstable
public class PlacementConstraint {

  /**
   * The constraint expression tree.
   */
  private AbstractConstraint constraintExpr;

  public PlacementConstraint(AbstractConstraint constraintExpr) {
    this.constraintExpr = constraintExpr;
  }

  /**
   * Get the constraint expression of the placement constraint.
   *
   * @return the constraint expression
   */
  public AbstractConstraint getConstraintExpr() {
    return constraintExpr;
  }

  /**
   * Interface used to enable the elements of the constraint tree to be visited.
   */
  @Private
  public interface Visitable {
    /**
     * Visitor pattern.
     *
     * @param visitor visitor to be used
     * @param <T> defines the type that the visitor will use and the return type
     *          of the accept.
     * @return the result of visiting a given object.
     */
    <T> T accept(Visitor<T> visitor);

  }

  /**
   * Visitor API for a constraint tree.
   *
   * @param <T> determines the return type of the visit methods.
   */
  @Private
  public interface Visitor<T> {
    T visit(SingleConstraint constraint);

    T visit(TargetExpression target);

    T visit(TargetConstraint constraint);

    T visit(CardinalityConstraint constraint);

    T visit(And constraint);

    T visit(Or constraint);

    T visit(DelayedOr constraint);

    T visit(TimedPlacementConstraint constraint);
  }

  /**
   * Abstract class that acts as the superclass of all placement constraint
   * classes.
   */
  public abstract static class AbstractConstraint implements Visitable {
    public PlacementConstraint build() {
      return new PlacementConstraint(this);
    }
  }

  static final String NODE_SCOPE = "node";
  static final String RACK_SCOPE = "rack";

  /**
   * Consider a set of nodes N that belongs to the scope specified in the
   * constraint. If the target expressions are satisfied at least minCardinality
   * times and at most max-cardinality times in the node set N, then the
   * constraint is satisfied.
   *
   * For example, a constraint of the form {@code {RACK, 2, 10,
   * allocationTag("zk")}}, requires an allocation to be placed within a rack
   * that has at least 2 and at most 10 other allocations with tag "zk".
   */
  public static class SingleConstraint extends AbstractConstraint {
    private String scope;
    private int minCardinality;
    private int maxCardinality;
    private Set<TargetExpression> targetExpressions;

    public SingleConstraint(String scope, int minCardinality,
        int maxCardinality, Set<TargetExpression> targetExpressions) {
      this.scope = scope;
      this.minCardinality = minCardinality;
      this.maxCardinality = maxCardinality;
      this.targetExpressions = targetExpressions;
    }

    public SingleConstraint(String scope, int minC, int maxC,
        TargetExpression... targetExpressions) {
      this(scope, minC, maxC, new HashSet<>(Arrays.asList(targetExpressions)));
    }

    /**
     * Get the scope of the constraint.
     *
     * @return the scope of the constraint
     */
    public String getScope() {
      return scope;
    }

    /**
     * Get the minimum cardinality of the constraint.
     *
     * @return the minimum cardinality of the constraint
     */
    public int getMinCardinality() {
      return minCardinality;
    }

    /**
     * Get the maximum cardinality of the constraint.
     *
     * @return the maximum cardinality of the constraint
     */
    public int getMaxCardinality() {
      return maxCardinality;
    }

    /**
     * Get the target expressions of the constraint.
     *
     * @return the set of target expressions
     */
    public Set<TargetExpression> getTargetExpressions() {
      return targetExpressions;
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
      return visitor.visit(this);
    }
  }

  /**
   * Class representing the target expressions that are used in placement
   * constraints. They might refer to expressions on node attributes, allocation
   * tags, or be self-targets (referring to the allocation to which the
   * constraint is attached).
   */
  public static class TargetExpression implements Visitable {
    /**
     * Enum specifying the type of the target expression.
     */
    public enum TargetType {
      NODE_ATTRIBUTE, ALLOCATION_TAG, SELF
    }

    private TargetType targetType;
    private String targetKey;
    private Set<String> targetValues;

    public TargetExpression(TargetType targetType, String targetKey,
        Set<String> targetValues) {
      this.targetType = targetType;
      this.targetKey = targetKey;
      this.targetValues = targetValues;
    }

    public TargetExpression(TargetType targetType) {
      this(targetType, null, new HashSet<>());
    }

    public TargetExpression(TargetType targetType, String targetKey,
        String... targetValues) {
      this(targetType, targetKey, new HashSet<>(Arrays.asList(targetValues)));
    }

    /**
     * Get the type of the target expression.
     *
     * @return the type of the target expression
     */
    public TargetType getTargetType() {
      return targetType;
    }

    /**
     * Get the key of the target expression.
     *
     * @return the key of the target expression
     */
    public String getTargetKey() {
      return targetKey;
    }

    /**
     * Get the set of values of the target expression.
     *
     * @return the set of values of the target expression
     */
    public Set<String> getTargetValues() {
      return targetValues;
    }

    @Override
    public int hashCode() {
      int result = targetType != null ? targetType.hashCode() : 0;
      result = 31 * result + (targetKey != null ? targetKey.hashCode() : 0);
      result =
          31 * result + (targetValues != null ? targetValues.hashCode() : 0);
      return result;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null) {
        return false;
      }
      if (!(o instanceof TargetExpression)) {
        return false;
      }

      TargetExpression that = (TargetExpression) o;
      if (targetType != that.targetType) {
        return false;
      }
      if (targetKey != null ? !targetKey.equals(that.targetKey)
          : that.targetKey != null) {
        return false;
      }
      return targetValues != null ? targetValues.equals(that.targetValues)
          : that.targetValues == null;
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
      return visitor.visit(this);
    }
  }

  /**
   * Class that represents a target constraint. Such a constraint requires an
   * allocation to be placed within a scope that satisfies some specified
   * expressions on node attributes and allocation tags.
   *
   * It is a specialized version of the {@link SingleConstraint}, where the
   * minimum and the maximum cardinalities take specific values based on the
   * {@link TargetOperator} used.
   */
  public static class TargetConstraint extends AbstractConstraint {
    enum TargetOperator {
      IN, NOT_IN
    }

    private TargetOperator op;
    private String scope;
    private Set<TargetExpression> targetExpressions;

    public TargetConstraint(TargetOperator op, String scope,
        Set<TargetExpression> targetExpressions) {
      this.op = op;
      this.scope = scope;
      this.targetExpressions = targetExpressions;
    }

    /**
     * Get the target operator of the constraint.
     *
     * @return the target operator
     */
    public TargetOperator getOp() {
      return op;
    }

    /**
     * Get the scope of the constraint.
     *
     * @return the scope of the constraint
     */
    public String getScope() {
      return scope;
    }

    /**
     * Get the set of target expressions.
     *
     * @return the set of target expressions
     */
    public Set<TargetExpression> getTargetExpressions() {
      return targetExpressions;
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
      return visitor.visit(this);
    }
  }

  /**
   * Class that represents a cardinality constraint. Such a constraint the
   * number of allocations within a given scope to some minimum and maximum
   * values.
   *
   * It is a specialized version of the {@link SingleConstraint}, where the
   * target is self (i.e., the allocation to which the constraint is attached).
   */
  public static class CardinalityConstraint extends AbstractConstraint {
    private String scope;
    private int minCardinality;
    private int maxCardinality;

    public CardinalityConstraint(String scope, int minCardinality,
        int maxCardinality) {
      this.scope = scope;
      this.minCardinality = minCardinality;
      this.maxCardinality = maxCardinality;
    }

    /**
     * Get the scope of the constraint.
     *
     * @return the scope of the constraint
     */
    public String getScope() {
      return scope;
    }

    /**
     * Get the minimum cardinality of the constraint.
     *
     * @return the minimum cardinality of the constraint
     */
    public int getMinCardinality() {
      return minCardinality;
    }

    /**
     * Get the maximum cardinality of the constraint.
     *
     * @return the maximum cardinality of the constraint
     */
    public int getMaxCardinality() {
      return maxCardinality;
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
      return visitor.visit(this);
    }
  }

  /**
   * Class that represents composite constraints, which comprise other
   * constraints, forming a constraint tree.
   *
   * @param <R> the type of constraints that are used as children of the
   *          specific composite constraint
   */
  public abstract static class CompositeConstraint<R extends Visitable>
      extends AbstractConstraint {

    /**
     * Get the children of this composite constraint.
     *
     * @return the children of the composite constraint
     */
    public abstract List<R> getChildren();
  }

  /**
   * Class that represents a composite constraint that is a conjunction of other
   * constraints.
   */
  public static class And extends CompositeConstraint<AbstractConstraint> {
    private List<AbstractConstraint> children;

    public And(List<AbstractConstraint> children) {
      this.children = children;
    }

    public And(AbstractConstraint... children) {
      this(Arrays.asList(children));
    }

    @Override
    public List<AbstractConstraint> getChildren() {
      return children;
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
      return visitor.visit(this);
    }
  }

  /**
   * Class that represents a composite constraint that is a disjunction of other
   * constraints.
   */
  public static class Or extends CompositeConstraint<AbstractConstraint> {
    private List<AbstractConstraint> children;

    public Or(List<AbstractConstraint> children) {
      this.children = children;
    }

    public Or(AbstractConstraint... children) {
      this(Arrays.asList(children));
    }

    @Override
    public List<AbstractConstraint> getChildren() {
      return children;
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
      return visitor.visit(this);
    }
  }

  /**
   * Class that represents a composite constraint that comprises a list of timed
   * placement constraints (see {@link TimedPlacementConstraint}). The scheduler
   * should try to satisfy first the first timed child constraint within the
   * specified time window. If this is not possible, it should attempt to
   * satisfy the second, and so on.
   */
  public static class DelayedOr
      extends CompositeConstraint<TimedPlacementConstraint> {
    private List<TimedPlacementConstraint> children = new ArrayList<>();

    public DelayedOr(List<TimedPlacementConstraint> children) {
      this.children = children;
    }

    public DelayedOr(TimedPlacementConstraint... children) {
      this(Arrays.asList(children));
    }

    @Override
    public List<TimedPlacementConstraint> getChildren() {
      return children;
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
      return visitor.visit(this);
    }
  }

  /**
   * Represents a timed placement constraint that has to be satisfied within a
   * time window.
   */
  public static class TimedPlacementConstraint implements Visitable {
    /**
     * The unit of scheduling delay.
     */
    public enum DelayUnit {
      MILLISECONDS, OPPORTUNITIES
    }

    private AbstractConstraint constraint;
    private long schedulingDelay;
    private DelayUnit delayUnit;

    public TimedPlacementConstraint(AbstractConstraint constraint,
        long schedulingDelay, DelayUnit delayUnit) {
      this.constraint = constraint;
      this.schedulingDelay = schedulingDelay;
      this.delayUnit = delayUnit;
    }

    public TimedPlacementConstraint(AbstractConstraint constraint,
        long schedulingDelay) {
      this(constraint, schedulingDelay, DelayUnit.MILLISECONDS);
    }

    public TimedPlacementConstraint(AbstractConstraint constraint) {
      this(constraint, Long.MAX_VALUE, DelayUnit.MILLISECONDS);
    }

    /**
     * Get the constraint that has to be satisfied within the time window.
     *
     * @return the constraint to be satisfied
     */
    public AbstractConstraint getConstraint() {
      return constraint;
    }

    /**
     * Sets the constraint that has to be satisfied within the time window.
     *
     * @param constraint the constraint to be satisfied
     */
    public void setConstraint(AbstractConstraint constraint) {
      this.constraint = constraint;
    }

    /**
     * Get the scheduling delay value that determines the time window within
     * which the constraint has to be satisfied.
     *
     * @return the value of the scheduling delay
     */
    public long getSchedulingDelay() {
      return schedulingDelay;
    }

    /**
     * The unit of the scheduling delay.
     *
     * @return the unit of the delay
     */
    public DelayUnit getDelayUnit() {
      return delayUnit;
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
      return visitor.visit(this);
    }
  }
}

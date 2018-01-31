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

import java.util.ListIterator;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.AbstractConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.And;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.CardinalityConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.CompositeConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.DelayedOr;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.Or;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.SingleConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TargetConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TargetConstraint.TargetOperator;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TargetExpression;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TimedPlacementConstraint;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 * This class contains inner classes that define transformation on a
 * {@link PlacementConstraint} expression.
 */
@Private
public class PlacementConstraintTransformations {

  /**
   * The default implementation of the {@link PlacementConstraint.Visitor} that
   * does a traversal of the constraint tree, performing no action for the lead
   * constraints.
   */
  public static class AbstractTransformer
      implements PlacementConstraint.Visitor<AbstractConstraint> {

    private PlacementConstraint placementConstraint;

    public AbstractTransformer(PlacementConstraint placementConstraint) {
      this.placementConstraint = placementConstraint;
    }

    /**
     * This method performs the transformation of the
     * {@link #placementConstraint}.
     *
     * @return the transformed placement constraint.
     */
    public PlacementConstraint transform() {
      AbstractConstraint constraintExpr =
          placementConstraint.getConstraintExpr();

      // Visit the constraint tree to perform the transformation.
      constraintExpr = constraintExpr.accept(this);

      return new PlacementConstraint(constraintExpr);
    }

    @Override
    public AbstractConstraint visit(SingleConstraint constraint) {
      // Do nothing.
      return constraint;
    }

    @Override
    public AbstractConstraint visit(TargetExpression expression) {
      // Do nothing.
      return null;
    }

    @Override
    public AbstractConstraint visit(TargetConstraint constraint) {
      // Do nothing.
      return constraint;
    }

    @Override
    public AbstractConstraint visit(CardinalityConstraint constraint) {
      // Do nothing.
      return constraint;
    }

    private AbstractConstraint visitAndOr(
        CompositeConstraint<AbstractConstraint> constraint) {
      for (ListIterator<AbstractConstraint> iter =
          constraint.getChildren().listIterator(); iter.hasNext();) {
        AbstractConstraint child = iter.next();
        child = child.accept(this);
        iter.set(child);
      }
      return constraint;
    }

    @Override
    public AbstractConstraint visit(And constraint) {
      return visitAndOr(constraint);
    }

    @Override
    public AbstractConstraint visit(Or constraint) {
      return visitAndOr(constraint);
    }

    @Override
    public AbstractConstraint visit(DelayedOr constraint) {
      constraint.getChildren().forEach(
          child -> child.setConstraint(child.getConstraint().accept(this)));
      return constraint;
    }

    @Override
    public AbstractConstraint visit(TimedPlacementConstraint constraint) {
      // Do nothing.
      return null;
    }
  }

  /**
   * Visits a {@link PlacementConstraint} tree and substitutes each
   * {@link TargetConstraint} and {@link CardinalityConstraint} with an
   * equivalent {@link SingleConstraint}.
   */
  public static class SingleConstraintTransformer extends AbstractTransformer {

    public SingleConstraintTransformer(PlacementConstraint constraint) {
      super(constraint);
    }

    @Override
    public AbstractConstraint visit(TargetConstraint constraint) {
      AbstractConstraint newConstraint;
      if (constraint.getOp() == TargetOperator.IN) {
        newConstraint = new SingleConstraint(constraint.getScope(), 1,
            Integer.MAX_VALUE, constraint.getTargetExpressions());
      } else if (constraint.getOp() == TargetOperator.NOT_IN) {
        newConstraint = new SingleConstraint(constraint.getScope(), 0, 0,
            constraint.getTargetExpressions());
      } else {
        throw new YarnRuntimeException(
            "Encountered unexpected type of constraint target operator: "
                + constraint.getOp());
      }
      return newConstraint;
    }

    @Override
    public AbstractConstraint visit(CardinalityConstraint constraint) {
      return new SingleConstraint(constraint.getScope(),
          constraint.getMinCardinality(), constraint.getMaxCardinality(),
          new TargetExpression(TargetExpression.TargetType.ALLOCATION_TAG, null,
              constraint.getAllocationTags()));
    }
  }

  /**
   * Visits a {@link PlacementConstraint} tree and, whenever possible,
   * substitutes each {@link SingleConstraint} with a {@link TargetConstraint}.
   * When such a substitution is not possible, we keep the original
   * {@link SingleConstraint}.
   */
  public static class SpecializedConstraintTransformer
      extends AbstractTransformer {

    public SpecializedConstraintTransformer(PlacementConstraint constraint) {
      super(constraint);
    }

    @Override
    public AbstractConstraint visit(SingleConstraint constraint) {
      AbstractConstraint transformedConstraint = constraint;
      // Check if it is a target constraint.
      if (constraint.getMinCardinality() == 1
          && constraint.getMaxCardinality() == Integer.MAX_VALUE) {
        transformedConstraint = new TargetConstraint(TargetOperator.IN,
            constraint.getScope(), constraint.getTargetExpressions());
      } else if (constraint.getMinCardinality() == 0
          && constraint.getMaxCardinality() == 0) {
        transformedConstraint = new TargetConstraint(TargetOperator.NOT_IN,
            constraint.getScope(), constraint.getTargetExpressions());
      }

      return transformedConstraint;
    }
  }
}

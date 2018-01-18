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

import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.NODE;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.RACK;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.and;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.maxCardinality;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetCardinality;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetIn;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetNotIn;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.PlacementTargets.allocationTag;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.PlacementTargets.nodeAttribute;

import org.apache.hadoop.yarn.api.resource.PlacementConstraint.AbstractConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.And;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.SingleConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TargetExpression;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TargetExpression.TargetType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for the various static methods in
 * {@link org.apache.hadoop.yarn.api.resource.PlacementConstraints}.
 */
public class TestPlacementConstraints {

  @Test
  public void testNodeAffinityToTag() {
    AbstractConstraint constraintExpr =
        targetIn(NODE, allocationTag("hbase-m"));

    SingleConstraint sConstraint = (SingleConstraint) constraintExpr;
    Assert.assertEquals(NODE, sConstraint.getScope());
    Assert.assertEquals(1, sConstraint.getMinCardinality());
    Assert.assertEquals(Integer.MAX_VALUE, sConstraint.getMaxCardinality());

    Assert.assertEquals(1, sConstraint.getTargetExpressions().size());
    TargetExpression tExpr =
        sConstraint.getTargetExpressions().iterator().next();
    Assert.assertNull(tExpr.getTargetKey());
    Assert.assertEquals(TargetType.ALLOCATION_TAG, tExpr.getTargetType());
    Assert.assertEquals(1, tExpr.getTargetValues().size());
    Assert.assertEquals("hbase-m", tExpr.getTargetValues().iterator().next());

    PlacementConstraint constraint = PlacementConstraints.build(constraintExpr);
    Assert.assertNotNull(constraint.getConstraintExpr());
  }

  @Test
  public void testNodeAntiAffinityToAttribute() {
    AbstractConstraint constraintExpr =
        targetNotIn(NODE, nodeAttribute("java", "1.8"));

    SingleConstraint sConstraint = (SingleConstraint) constraintExpr;
    Assert.assertEquals(NODE, sConstraint.getScope());
    Assert.assertEquals(0, sConstraint.getMinCardinality());
    Assert.assertEquals(0, sConstraint.getMaxCardinality());

    Assert.assertEquals(1, sConstraint.getTargetExpressions().size());
    TargetExpression tExpr =
        sConstraint.getTargetExpressions().iterator().next();
    Assert.assertEquals("java", tExpr.getTargetKey());
    Assert.assertEquals(TargetType.NODE_ATTRIBUTE, tExpr.getTargetType());
    Assert.assertEquals(1, tExpr.getTargetValues().size());
    Assert.assertEquals("1.8", tExpr.getTargetValues().iterator().next());
  }

  @Test
  public void testAndConstraint() {
    AbstractConstraint constraintExpr =
        and(targetIn(RACK, allocationTag("spark")),
            maxCardinality(NODE, 3, "spark"),
            targetCardinality(RACK, 2, 10, allocationTag("zk")));

    And andExpr = (And) constraintExpr;
    Assert.assertEquals(3, andExpr.getChildren().size());
    SingleConstraint sConstr = (SingleConstraint) andExpr.getChildren().get(0);
    TargetExpression tExpr = sConstr.getTargetExpressions().iterator().next();
    Assert.assertEquals("spark", tExpr.getTargetValues().iterator().next());

    sConstr = (SingleConstraint) andExpr.getChildren().get(1);
    Assert.assertEquals(0, sConstr.getMinCardinality());
    Assert.assertEquals(3, sConstr.getMaxCardinality());

    sConstr = (SingleConstraint) andExpr.getChildren().get(2);
    Assert.assertEquals(2, sConstr.getMinCardinality());
    Assert.assertEquals(10, sConstr.getMaxCardinality());
  }
}

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

package org.apache.hadoop.yarn.api;

import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.NODE;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.RACK;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.cardinality;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.maxCardinality;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.or;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetCardinality;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetIn;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.PlacementTargets.allocationTag;

import java.util.Iterator;

import org.apache.hadoop.yarn.api.pb.PlacementConstraintFromProtoConverter;
import org.apache.hadoop.yarn.api.pb.PlacementConstraintToProtoConverter;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.AbstractConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.Or;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.SingleConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints;
import org.apache.hadoop.yarn.proto.YarnProtos.CompositePlacementConstraintProto;
import org.apache.hadoop.yarn.proto.YarnProtos.CompositePlacementConstraintProto.CompositeType;
import org.apache.hadoop.yarn.proto.YarnProtos.PlacementConstraintProto;
import org.apache.hadoop.yarn.proto.YarnProtos.SimplePlacementConstraintProto;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for {@link PlacementConstraintToProtoConverter} and
 * {@link PlacementConstraintFromProtoConverter}.
 */
public class TestPlacementConstraintPBConversion {

  @Test
  public void testTargetConstraintProtoConverter() {
    AbstractConstraint sConstraintExpr =
        targetIn(NODE, allocationTag("hbase-m"));
    Assert.assertTrue(sConstraintExpr instanceof SingleConstraint);
    SingleConstraint single = (SingleConstraint) sConstraintExpr;
    PlacementConstraint sConstraint =
        PlacementConstraints.build(sConstraintExpr);

    // Convert to proto.
    PlacementConstraintToProtoConverter toProtoConverter =
        new PlacementConstraintToProtoConverter(sConstraint);
    PlacementConstraintProto protoConstraint = toProtoConverter.convert();

    Assert.assertTrue(protoConstraint.hasSimpleConstraint());
    Assert.assertFalse(protoConstraint.hasCompositeConstraint());
    SimplePlacementConstraintProto sProto =
        protoConstraint.getSimpleConstraint();
    Assert.assertEquals(single.getScope(), sProto.getScope());
    Assert.assertEquals(single.getMinCardinality(), sProto.getMinCardinality());
    Assert.assertEquals(single.getMaxCardinality(), sProto.getMaxCardinality());
    Assert.assertEquals(single.getTargetExpressions().size(),
        sProto.getTargetExpressionsList().size());

    // Convert from proto.
    PlacementConstraintFromProtoConverter fromProtoConverter =
        new PlacementConstraintFromProtoConverter(protoConstraint);
    PlacementConstraint newConstraint = fromProtoConverter.convert();

    AbstractConstraint newConstraintExpr = newConstraint.getConstraintExpr();
    Assert.assertTrue(newConstraintExpr instanceof SingleConstraint);
    SingleConstraint newSingle = (SingleConstraint) newConstraintExpr;
    Assert.assertEquals(single.getScope(), newSingle.getScope());
    Assert.assertEquals(single.getMinCardinality(),
        newSingle.getMinCardinality());
    Assert.assertEquals(single.getMaxCardinality(),
        newSingle.getMaxCardinality());
    Assert.assertEquals(single.getTargetExpressions(),
        newSingle.getTargetExpressions());
  }

  @Test
  public void testCardinalityConstraintProtoConverter() {
    AbstractConstraint sConstraintExpr = cardinality(RACK, 3, 10);
    Assert.assertTrue(sConstraintExpr instanceof SingleConstraint);
    SingleConstraint single = (SingleConstraint) sConstraintExpr;
    PlacementConstraint sConstraint =
        PlacementConstraints.build(sConstraintExpr);

    // Convert to proto.
    PlacementConstraintToProtoConverter toProtoConverter =
        new PlacementConstraintToProtoConverter(sConstraint);
    PlacementConstraintProto protoConstraint = toProtoConverter.convert();

    compareSimpleConstraintToProto(single, protoConstraint);

    // Convert from proto.
    PlacementConstraintFromProtoConverter fromProtoConverter =
        new PlacementConstraintFromProtoConverter(protoConstraint);
    PlacementConstraint newConstraint = fromProtoConverter.convert();

    AbstractConstraint newConstraintExpr = newConstraint.getConstraintExpr();
    Assert.assertTrue(newConstraintExpr instanceof SingleConstraint);
    SingleConstraint newSingle = (SingleConstraint) newConstraintExpr;
    compareSimpleConstraints(single, newSingle);
  }

  @Test
  public void testCompositeConstraintProtoConverter() {
    AbstractConstraint constraintExpr =
        or(targetIn(RACK, allocationTag("spark")), maxCardinality(NODE, 3),
            targetCardinality(RACK, 2, 10, allocationTag("zk")));
    Assert.assertTrue(constraintExpr instanceof Or);
    PlacementConstraint constraint = PlacementConstraints.build(constraintExpr);
    Or orExpr = (Or) constraintExpr;

    // Convert to proto.
    PlacementConstraintToProtoConverter toProtoConverter =
        new PlacementConstraintToProtoConverter(constraint);
    PlacementConstraintProto protoConstraint = toProtoConverter.convert();

    Assert.assertFalse(protoConstraint.hasSimpleConstraint());
    Assert.assertTrue(protoConstraint.hasCompositeConstraint());
    CompositePlacementConstraintProto cProto =
        protoConstraint.getCompositeConstraint();

    Assert.assertEquals(CompositeType.OR, cProto.getCompositeType());
    Assert.assertEquals(3, cProto.getChildConstraintsCount());
    Assert.assertEquals(0, cProto.getTimedChildConstraintsCount());
    Iterator<AbstractConstraint> orChildren = orExpr.getChildren().iterator();
    Iterator<PlacementConstraintProto> orProtoChildren =
        cProto.getChildConstraintsList().iterator();
    while (orChildren.hasNext() && orProtoChildren.hasNext()) {
      AbstractConstraint orChild = orChildren.next();
      PlacementConstraintProto orProtoChild = orProtoChildren.next();
      compareSimpleConstraintToProto((SingleConstraint) orChild, orProtoChild);
    }

    // Convert from proto.
    PlacementConstraintFromProtoConverter fromProtoConverter =
        new PlacementConstraintFromProtoConverter(protoConstraint);
    PlacementConstraint newConstraint = fromProtoConverter.convert();

    AbstractConstraint newConstraintExpr = newConstraint.getConstraintExpr();
    Assert.assertTrue(newConstraintExpr instanceof Or);
    Or newOrExpr = (Or) newConstraintExpr;
    Assert.assertEquals(3, newOrExpr.getChildren().size());
    orChildren = orExpr.getChildren().iterator();
    Iterator<AbstractConstraint> newOrChildren =
        newOrExpr.getChildren().iterator();
    while (orChildren.hasNext() && newOrChildren.hasNext()) {
      AbstractConstraint orChild = orChildren.next();
      AbstractConstraint newOrChild = newOrChildren.next();
      compareSimpleConstraints((SingleConstraint) orChild,
          (SingleConstraint) newOrChild);
    }
  }

  private void compareSimpleConstraintToProto(SingleConstraint constraint,
      PlacementConstraintProto proto) {
    Assert.assertTrue(proto.hasSimpleConstraint());
    Assert.assertFalse(proto.hasCompositeConstraint());
    SimplePlacementConstraintProto sProto = proto.getSimpleConstraint();
    Assert.assertEquals(constraint.getScope(), sProto.getScope());
    Assert.assertEquals(constraint.getMinCardinality(),
        sProto.getMinCardinality());
    Assert.assertEquals(constraint.getMaxCardinality(),
        sProto.getMaxCardinality());
    Assert.assertEquals(constraint.getTargetExpressions().size(),
        sProto.getTargetExpressionsList().size());
  }

  private void compareSimpleConstraints(SingleConstraint single,
      SingleConstraint newSingle) {
    Assert.assertEquals(single.getScope(), newSingle.getScope());
    Assert.assertEquals(single.getMinCardinality(),
        newSingle.getMinCardinality());
    Assert.assertEquals(single.getMaxCardinality(),
        newSingle.getMaxCardinality());
    Assert.assertEquals(single.getTargetExpressions(),
        newSingle.getTargetExpressions());
  }

}

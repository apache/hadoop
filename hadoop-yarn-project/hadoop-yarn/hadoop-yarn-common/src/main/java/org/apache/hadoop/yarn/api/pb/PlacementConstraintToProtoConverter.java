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

package org.apache.hadoop.yarn.api.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.records.NodeAttributeOpCode;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.AbstractConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.And;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.CardinalityConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.CompositeConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.DelayedOr;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.Or;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.SingleConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TargetConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TargetExpression;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TimedPlacementConstraint;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnProtos.CompositePlacementConstraintProto;
import org.apache.hadoop.yarn.proto.YarnProtos.CompositePlacementConstraintProto.CompositeType;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeAttributeOpCodeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PlacementConstraintProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PlacementConstraintTargetProto;
import org.apache.hadoop.yarn.proto.YarnProtos.SimplePlacementConstraintProto;
import org.apache.hadoop.yarn.proto.YarnProtos.TimedPlacementConstraintProto;

import com.google.protobuf.GeneratedMessage;

/**
 * {@code PlacementConstraintToProtoConverter} generates a
 * {@link PlacementConstraintProto} given a
 * {@link PlacementConstraint.AbstractConstraint}.
 */
@Private
public class PlacementConstraintToProtoConverter
    implements PlacementConstraint.Visitor<GeneratedMessage> {

  private PlacementConstraint placementConstraint;

  public PlacementConstraintToProtoConverter(
      PlacementConstraint placementConstraint) {
    this.placementConstraint = placementConstraint;
  }

  public PlacementConstraintProto convert() {
    return (PlacementConstraintProto) placementConstraint.getConstraintExpr()
        .accept(this);
  }

  @Override
  public GeneratedMessage visit(SingleConstraint constraint) {
    SimplePlacementConstraintProto.Builder sb =
        SimplePlacementConstraintProto.newBuilder();

    if (constraint.getScope() != null) {
      sb.setScope(constraint.getScope());
    }
    sb.setMinCardinality(constraint.getMinCardinality());
    sb.setMaxCardinality(constraint.getMaxCardinality());
    if (constraint.getNodeAttributeOpCode() != null) {
      sb.setAttributeOpCode(
          convertToProtoFormat(constraint.getNodeAttributeOpCode()));
    }
    if (constraint.getTargetExpressions() != null) {
      for (TargetExpression target : constraint.getTargetExpressions()) {
        sb.addTargetExpressions(
            (PlacementConstraintTargetProto) target.accept(this));
      }

    }
    SimplePlacementConstraintProto sProto = sb.build();

    // Wrap around PlacementConstraintProto object.
    PlacementConstraintProto.Builder pb = PlacementConstraintProto.newBuilder();
    pb.setSimpleConstraint(sProto);
    return pb.build();
  }

  @Override
  public GeneratedMessage visit(TargetExpression target) {
    PlacementConstraintTargetProto.Builder tb =
        PlacementConstraintTargetProto.newBuilder();

    tb.setTargetType(ProtoUtils.convertToProtoFormat(target.getTargetType()));
    if (target.getTargetKey() != null) {
      tb.setTargetKey(target.getTargetKey());
    }
    if (target.getTargetValues() != null) {
      tb.addAllTargetValues(target.getTargetValues());
    }
    return tb.build();
  }

  @Override
  public GeneratedMessage visit(TargetConstraint constraint) {
    throw new YarnRuntimeException("Unexpected TargetConstraint found.");
  }

  @Override
  public GeneratedMessage visit(CardinalityConstraint constraint) {
    throw new YarnRuntimeException("Unexpected CardinalityConstraint found.");
  }

  private GeneratedMessage visitAndOr(
      CompositeConstraint<AbstractConstraint> composite, CompositeType type) {
    CompositePlacementConstraintProto.Builder cb =
        CompositePlacementConstraintProto.newBuilder();

    cb.setCompositeType(type);

    for (AbstractConstraint c : composite.getChildren()) {
      cb.addChildConstraints((PlacementConstraintProto) c.accept(this));
    }
    CompositePlacementConstraintProto cProto = cb.build();

    // Wrap around PlacementConstraintProto object.
    PlacementConstraintProto.Builder pb = PlacementConstraintProto.newBuilder();
    pb.setCompositeConstraint(cProto);
    return pb.build();
  }

  @Override
  public GeneratedMessage visit(And constraint) {
    return visitAndOr(constraint, CompositeType.AND);
  }

  @Override
  public GeneratedMessage visit(Or constraint) {
    return visitAndOr(constraint, CompositeType.OR);
  }

  @Override
  public GeneratedMessage visit(DelayedOr constraint) {
    CompositePlacementConstraintProto.Builder cb =
        CompositePlacementConstraintProto.newBuilder();

    cb.setCompositeType(CompositeType.DELAYED_OR);

    for (TimedPlacementConstraint c : constraint.getChildren()) {
      cb.addTimedChildConstraints(
          (TimedPlacementConstraintProto) c.accept(this));
    }
    CompositePlacementConstraintProto cProto = cb.build();

    // Wrap around PlacementConstraintProto object.
    PlacementConstraintProto.Builder pb = PlacementConstraintProto.newBuilder();
    pb.setCompositeConstraint(cProto);
    return pb.build();
  }

  @Override
  public GeneratedMessage visit(TimedPlacementConstraint constraint) {
    TimedPlacementConstraintProto.Builder tb =
        TimedPlacementConstraintProto.newBuilder();

    tb.setDelayUnit(ProtoUtils.convertToProtoFormat(constraint.getDelayUnit()));
    tb.setSchedulingDelay(constraint.getSchedulingDelay());
    tb.setPlacementConstraint(
        (PlacementConstraintProto) constraint.getConstraint().accept(this));

    return tb.build();
  }

  private static NodeAttributeOpCodeProto convertToProtoFormat(
      NodeAttributeOpCode p) {
    return NodeAttributeOpCodeProto.valueOf(p.name());
  }
}

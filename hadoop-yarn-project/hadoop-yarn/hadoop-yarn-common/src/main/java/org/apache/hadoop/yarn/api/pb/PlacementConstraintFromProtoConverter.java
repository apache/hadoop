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

import static org.apache.hadoop.yarn.proto.YarnProtos.CompositePlacementConstraintProto.CompositeType.AND;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.records.NodeAttributeOpCode;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.AbstractConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.And;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.DelayedOr;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.Or;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.SingleConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TargetExpression;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TimedPlacementConstraint;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnProtos.CompositePlacementConstraintProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeAttributeOpCodeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PlacementConstraintProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PlacementConstraintTargetProto;
import org.apache.hadoop.yarn.proto.YarnProtos.SimplePlacementConstraintProto;
import org.apache.hadoop.yarn.proto.YarnProtos.TimedPlacementConstraintProto;

/**
 * {@code PlacementConstraintFromProtoConverter} generates an
 * {@link PlacementConstraint.AbstractConstraint} given a
 * {@link PlacementConstraintProto}.
 */
@Private
public class PlacementConstraintFromProtoConverter {

  private PlacementConstraintProto constraintProto;

  public PlacementConstraintFromProtoConverter(
      PlacementConstraintProto constraintProto) {
    this.constraintProto = constraintProto;
  }

  public PlacementConstraint convert() {
    return new PlacementConstraint(convert(constraintProto));
  }

  private AbstractConstraint convert(PlacementConstraintProto proto) {
    return proto.hasSimpleConstraint() ? convert(proto.getSimpleConstraint())
        : convert(proto.getCompositeConstraint());
  }

  private SingleConstraint convert(SimplePlacementConstraintProto proto) {
    Set<TargetExpression> targets = new HashSet<>();
    for (PlacementConstraintTargetProto tp : proto.getTargetExpressionsList()) {
      targets.add(convert(tp));
    }

    return new SingleConstraint(proto.getScope(), proto.getMinCardinality(),
        proto.getMaxCardinality(),
        convertFromProtoFormat(proto.getAttributeOpCode()), targets);
  }

  private TargetExpression convert(PlacementConstraintTargetProto proto) {
    return new TargetExpression(
        ProtoUtils.convertFromProtoFormat(proto.getTargetType()),
        proto.hasTargetKey() ? proto.getTargetKey() : null,
        new HashSet<>(proto.getTargetValuesList()));
  }

  private AbstractConstraint convert(CompositePlacementConstraintProto proto) {
    switch (proto.getCompositeType()) {
    case AND:
    case OR:
      List<AbstractConstraint> children = new ArrayList<>();
      for (PlacementConstraintProto cp : proto.getChildConstraintsList()) {
        children.add(convert(cp));
      }
      return (proto.getCompositeType() == AND) ? new And(children)
          : new Or(children);
    case DELAYED_OR:
      List<TimedPlacementConstraint> tChildren = new ArrayList<>();
      for (TimedPlacementConstraintProto cp : proto
          .getTimedChildConstraintsList()) {
        tChildren.add(convert(cp));
      }
      return new DelayedOr(tChildren);
    default:
      throw new YarnRuntimeException(
          "Encountered unexpected type of composite constraint.");
    }
  }

  private TimedPlacementConstraint convert(
      TimedPlacementConstraintProto proto) {
    AbstractConstraint pConstraint = convert(proto.getPlacementConstraint());

    return new TimedPlacementConstraint(pConstraint, proto.getSchedulingDelay(),
        ProtoUtils.convertFromProtoFormat(proto.getDelayUnit()));
  }

  private static NodeAttributeOpCode convertFromProtoFormat(
      NodeAttributeOpCodeProto p) {
    return NodeAttributeOpCode.valueOf(p.name());
  }
}

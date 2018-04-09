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
package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.protocolrecords.GetAttributesToNodesRequest;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeAttributePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeAttributeProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetAttributesToNodesRequestProto;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.classification.InterfaceAudience.*;
import static org.apache.hadoop.classification.InterfaceStability.*;

/**
 * Attributes to nodes mapping request.
 */
@Private
@Unstable
public class GetAttributesToNodesRequestPBImpl
    extends GetAttributesToNodesRequest {

  private Set<NodeAttribute> nodeAttributes = null;

  private GetAttributesToNodesRequestProto proto =
      GetAttributesToNodesRequestProto.getDefaultInstance();
  private GetAttributesToNodesRequestProto.Builder builder = null;
  private boolean viaProto = false;

  public GetAttributesToNodesRequestPBImpl() {
    builder = GetAttributesToNodesRequestProto.newBuilder();
  }

  public GetAttributesToNodesRequestPBImpl(
      GetAttributesToNodesRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetAttributesToNodesRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (this.nodeAttributes != null) {
      addLocalAttributesToProto();
    }
  }

  private void addLocalAttributesToProto() {
    maybeInitBuilder();
    builder.clearNodeAttributes();
    if (nodeAttributes == null) {
      return;
    }
    Iterable<NodeAttributeProto> iterable =
        () -> new Iterator<NodeAttributeProto>() {
          private Iterator<NodeAttribute> iter = nodeAttributes.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public NodeAttributeProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

    builder.addAllNodeAttributes(iterable);
  }

  private NodeAttributePBImpl convertFromProtoFormat(NodeAttributeProto p) {
    return new NodeAttributePBImpl(p);
  }

  private NodeAttributeProto convertToProtoFormat(NodeAttribute t) {
    return ((NodeAttributePBImpl) t).getProto();
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetAttributesToNodesRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void initNodeAttributes() {
    if (this.nodeAttributes != null) {
      return;
    }
    YarnServiceProtos.GetAttributesToNodesRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    List<NodeAttributeProto> nodeAttributesList = p.getNodeAttributesList();
    this.nodeAttributes = new HashSet<>();
    nodeAttributesList
        .forEach((v) -> nodeAttributes.add(convertFromProtoFormat(v)));
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  @Override
  public void setNodeAttributes(Set<NodeAttribute> attributes) {
    maybeInitBuilder();
    if (nodeAttributes == null) {
      builder.clearNodeAttributes();
    }
    this.nodeAttributes = attributes;
  }

  @Override
  public Set<NodeAttribute> getNodeAttributes() {
    initNodeAttributes();
    return this.nodeAttributes;
  }
}

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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeAttributesResponse;
import org.apache.hadoop.yarn.api.records.NodeAttributeInfo;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeAttributeInfoPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeAttributeInfoProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodeAttributesResponseProto;

/**
 * Cluster node attributes response.
 */
@Private
@Unstable
public class GetClusterNodeAttributesResponsePBImpl
    extends GetClusterNodeAttributesResponse {

  private GetClusterNodeAttributesResponseProto proto =
      GetClusterNodeAttributesResponseProto.getDefaultInstance();
  private GetClusterNodeAttributesResponseProto.Builder builder = null;
  private Set<NodeAttributeInfo> clusterNodeAttributes;
  private boolean viaProto = false;

  public GetClusterNodeAttributesResponsePBImpl() {
    builder = GetClusterNodeAttributesResponseProto.newBuilder();
  }

  public GetClusterNodeAttributesResponsePBImpl(
      GetClusterNodeAttributesResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public synchronized GetClusterNodeAttributesResponseProto getProto() {
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
    if (this.clusterNodeAttributes != null) {
      addNodeAttributesToProto();
    }
  }

  private void addNodeAttributesToProto() {
    maybeInitBuilder();
    builder.clearNodeAttributes();
    if (clusterNodeAttributes == null || clusterNodeAttributes.isEmpty()) {
      return;
    }

    builder.addAllNodeAttributes(clusterNodeAttributes.stream()
        .map(s -> convertToProtoFormat(s)).collect(Collectors.toSet()));
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
  public int hashCode() {
    assert false : "hashCode not designed";
    return 0;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetClusterNodeAttributesResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public synchronized void setNodeAttributes(
      Set<NodeAttributeInfo> attributes) {
    maybeInitBuilder();
    this.clusterNodeAttributes = new HashSet<>();
    if (attributes == null) {
      builder.clearNodeAttributes();
      return;
    }
    this.clusterNodeAttributes.addAll(attributes);
  }

  @Override
  public synchronized Set<NodeAttributeInfo> getNodeAttributes() {
    if (this.clusterNodeAttributes != null) {
      return this.clusterNodeAttributes;
    }
    initLocalNodeAttributes();
    return this.clusterNodeAttributes;
  }

  private void initLocalNodeAttributes() {
    YarnServiceProtos.GetClusterNodeAttributesResponseProtoOrBuilder p =
        viaProto ? proto : builder;
    List<NodeAttributeInfoProto> attributesProtoList =
        p.getNodeAttributesList();
    this.clusterNodeAttributes = new HashSet<>();
    clusterNodeAttributes.addAll(attributesProtoList.stream()
        .map(attr -> convertFromProtoFormat(attr)).collect(Collectors.toSet()));
  }

  private NodeAttributeInfoProto convertToProtoFormat(
      NodeAttributeInfo attributeInfo) {
    return ((NodeAttributeInfoPBImpl)attributeInfo).getProto();
  }

  private NodeAttributeInfo convertFromProtoFormat(
      NodeAttributeInfoProto nodeAttributeInfoProto) {
    return new NodeAttributeInfoPBImpl(nodeAttributeInfoProto);
  }

  @Override
  public String toString() {
    return getProto().toString();
  }
}

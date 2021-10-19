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

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.CheckForDecommissioningNodesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.CheckForDecommissioningNodesResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesResponse;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

@Private
@Unstable
public class CheckForDecommissioningNodesResponsePBImpl extends
    CheckForDecommissioningNodesResponse {

  CheckForDecommissioningNodesResponseProto proto = CheckForDecommissioningNodesResponseProto
      .getDefaultInstance();
  CheckForDecommissioningNodesResponseProto.Builder builder = null;
  boolean viaProto = false;

  private Set<NodeId> decommissioningNodes;

  public CheckForDecommissioningNodesResponsePBImpl() {
    builder = CheckForDecommissioningNodesResponseProto.newBuilder();
  }

  public CheckForDecommissioningNodesResponsePBImpl(
      CheckForDecommissioningNodesResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public CheckForDecommissioningNodesResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = CheckForDecommissioningNodesResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.decommissioningNodes != null) {
      addDecommissioningNodesToProto();
    }
  }

  private void addDecommissioningNodesToProto() {
    maybeInitBuilder();
    builder.clearDecommissioningNodes();
    if (this.decommissioningNodes == null)
      return;
    Set<NodeIdProto> nodeIdProtos = new HashSet<NodeIdProto>();
    for (NodeId nodeId : decommissioningNodes) {
      nodeIdProtos.add(convertToProtoFormat(nodeId));
    }
    builder.addAllDecommissioningNodes(nodeIdProtos);
  }

  private NodeIdProto convertToProtoFormat(NodeId nodeId) {
    return ((NodeIdPBImpl) nodeId).getProto();
  }

  @Override
  public void setDecommissioningNodes(Set<NodeId> decommissioningNodes) {
    maybeInitBuilder();
    if (decommissioningNodes == null)
      builder.clearDecommissioningNodes();
    this.decommissioningNodes = decommissioningNodes;
  }

  @Override
  public Set<NodeId> getDecommissioningNodes() {
    initNodesDecommissioning();
    return this.decommissioningNodes;
  }

  private void initNodesDecommissioning() {
    if (this.decommissioningNodes != null) {
      return;
    }
    CheckForDecommissioningNodesResponseProtoOrBuilder p = viaProto ? proto
        : builder;
    List<NodeIdProto> nodeIds = p.getDecommissioningNodesList();
    this.decommissioningNodes = new HashSet<NodeId>();
    for (NodeIdProto nodeIdProto : nodeIds) {
      this.decommissioningNodes.add(convertFromProtoFormat(nodeIdProto));
    }
  }

  private NodeId convertFromProtoFormat(NodeIdProto nodeIdProto) {
    return new NodeIdPBImpl(nodeIdProto);
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }
}

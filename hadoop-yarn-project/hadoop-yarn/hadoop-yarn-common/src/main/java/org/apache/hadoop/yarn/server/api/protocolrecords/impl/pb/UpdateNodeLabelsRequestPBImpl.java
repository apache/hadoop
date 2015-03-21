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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeLabelPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeLabelProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.UpdateNodeLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.UpdateNodeLabelsRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeLabelsRequest;

public class UpdateNodeLabelsRequestPBImpl extends
    UpdateNodeLabelsRequest {
  UpdateNodeLabelsRequestProto proto =
      UpdateNodeLabelsRequestProto.getDefaultInstance();
  UpdateNodeLabelsRequestProto.Builder builder = null;
  private List<NodeLabel> updatedNodeLabels;
  boolean viaProto = false;

  public UpdateNodeLabelsRequestPBImpl() {
    builder = UpdateNodeLabelsRequestProto.newBuilder();
  }

  public UpdateNodeLabelsRequestPBImpl(
      UpdateNodeLabelsRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public UpdateNodeLabelsRequestProto getProto() {
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

  private void mergeLocalToBuilder() {
    if (this.updatedNodeLabels != null) {
      addNodeLabelsToProto();
    }
  }

  private void addNodeLabelsToProto() {
    maybeInitBuilder();
    builder.clearNodeLabels();
    List<NodeLabelProto> protoList =
        new ArrayList<NodeLabelProto>();
    for (NodeLabel r : this.updatedNodeLabels) {
      protoList.add(convertToProtoFormat(r));
    }
    builder.addAllNodeLabels(protoList);
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
  public int hashCode() {
    assert false : "hashCode not designed";
    return 0;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = UpdateNodeLabelsRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public void setNodeLabels(List<NodeLabel> updatedNodeLabels) {
    maybeInitBuilder();
    if (updatedNodeLabels == null) {
      builder.clearNodeLabels();
    }
    this.updatedNodeLabels = updatedNodeLabels;
  }

  private void initLocalNodeLabels() {
    UpdateNodeLabelsRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<NodeLabelProto> attributesProtoList =
        p.getNodeLabelsList();
    this.updatedNodeLabels = new ArrayList<NodeLabel>();
    for (NodeLabelProto r : attributesProtoList) {
      this.updatedNodeLabels.add(convertFromProtoFormat(r));
    }
  }

  @Override
  public List<NodeLabel> getNodeLabels() {
    if (this.updatedNodeLabels != null) {
      return this.updatedNodeLabels;
    }
    initLocalNodeLabels();
    return this.updatedNodeLabels;
  }

  private NodeLabel
      convertFromProtoFormat(NodeLabelProto p) {
    return new NodeLabelPBImpl(p);
  }

  private NodeLabelProto convertToProtoFormat(NodeLabel t) {
    return ((NodeLabelPBImpl) t).getProto();
  }

  @Override
  public String toString() {
    return getProto().toString();
  }
}

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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeLabelPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeLabelProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodeLabelsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodeLabelsResponseProtoOrBuilder;

public class GetClusterNodeLabelsResponsePBImpl extends
    GetClusterNodeLabelsResponse {
  GetClusterNodeLabelsResponseProto proto = GetClusterNodeLabelsResponseProto
      .getDefaultInstance();
  GetClusterNodeLabelsResponseProto.Builder builder = null;
  private List<NodeLabel> updatedNodeLabels;
  boolean viaProto = false;

  public GetClusterNodeLabelsResponsePBImpl() {
    builder = GetClusterNodeLabelsResponseProto.newBuilder();
  }

  public GetClusterNodeLabelsResponsePBImpl(
      GetClusterNodeLabelsResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public synchronized GetClusterNodeLabelsResponseProto getProto() {
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
    builder.clearDeprecatedNodeLabels();
    List<NodeLabelProto> protoList = new ArrayList<NodeLabelProto>();
    List<String> protoListString = new ArrayList<String>();
    for (NodeLabel r : this.updatedNodeLabels) {
      protoList.add(convertToProtoFormat(r));
      protoListString.add(r.getName());
    }
    builder.addAllNodeLabels(protoList);
    builder.addAllDeprecatedNodeLabels(protoListString);
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
      builder = GetClusterNodeLabelsResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public synchronized void setNodeLabelList(List<NodeLabel> nodeLabels) {
    maybeInitBuilder();
    this.updatedNodeLabels = new ArrayList<>();
    if (nodeLabels == null) {
      builder.clearNodeLabels();
      return;
    }
    this.updatedNodeLabels.addAll(nodeLabels);
  }

  /**
   * @deprecated Use {@link #getNodeLabelList()} instead.
   */
  @Override
  @Deprecated
  public synchronized Set<String> getNodeLabels() {
    Set<String> set = new HashSet<>();
    List<NodeLabel> labelList = getNodeLabelList();
    if (labelList != null) {
      for (NodeLabel label : labelList) {
        set.add(label.getName());
      }
    }
    return set;
  }

  /**
   * @deprecated Use {@link #setNodeLabelList(List)} instead.
   */
  @Override
  @Deprecated
  public void setNodeLabels(Set<String> labels) {
    List<NodeLabel> list = new ArrayList<>();
    for (String s : labels) {
      list.add(NodeLabel.newInstance(s));
    }
    setNodeLabelList(list);
  }

  private void initLocalNodeLabels() {
    GetClusterNodeLabelsResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<NodeLabelProto> attributesProtoList = p.getNodeLabelsList();
    this.updatedNodeLabels = new ArrayList<NodeLabel>();
    for (NodeLabelProto r : attributesProtoList) {
      this.updatedNodeLabels.add(convertFromProtoFormat(r));
    }
  }

  @Override
  public synchronized List<NodeLabel> getNodeLabelList() {
    if (this.updatedNodeLabels != null) {
      return this.updatedNodeLabels;
    }
    initLocalNodeLabels();
    return this.updatedNodeLabels;
  }

  private NodeLabel convertFromProtoFormat(NodeLabelProto p) {
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

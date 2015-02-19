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

import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesRequest;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetLabelsToNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetLabelsToNodesRequestProtoOrBuilder;

import com.google.protobuf.TextFormat;

public class GetLabelsToNodesRequestPBImpl extends GetLabelsToNodesRequest {

  Set<String> nodeLabels = null;

  GetLabelsToNodesRequestProto proto =
      GetLabelsToNodesRequestProto.getDefaultInstance();
  GetLabelsToNodesRequestProto.Builder builder = null;
  boolean viaProto = false;

  public GetLabelsToNodesRequestPBImpl() {
    builder = GetLabelsToNodesRequestProto.newBuilder();
  }

  public GetLabelsToNodesRequestPBImpl(GetLabelsToNodesRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetLabelsToNodesRequestProto getProto() {
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
    if (nodeLabels != null && !nodeLabels.isEmpty()) {
      builder.clearNodeLabels();
      builder.addAllNodeLabels(nodeLabels);
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetLabelsToNodesRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void initNodeLabels() {
    if (this.nodeLabels != null) {
      return;
    }
    GetLabelsToNodesRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<String> nodeLabelsList = p.getNodeLabelsList();
    this.nodeLabels = new HashSet<String>();
    this.nodeLabels.addAll(nodeLabelsList);
  }

  @Override
  public Set<String> getNodeLabels() {
    initNodeLabels();
    return this.nodeLabels;
  }

  @Override
  public void setNodeLabels(Set<String> nodeLabels) {
    maybeInitBuilder();
    if (nodeLabels == null)
      builder.clearNodeLabels();
    this.nodeLabels = nodeLabels;
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
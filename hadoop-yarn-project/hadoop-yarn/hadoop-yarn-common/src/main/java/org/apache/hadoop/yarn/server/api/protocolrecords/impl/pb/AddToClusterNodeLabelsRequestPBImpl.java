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
import java.util.Set;

import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;

public class AddToClusterNodeLabelsRequestPBImpl extends
    AddToClusterNodeLabelsRequest {
  Set<String> labels;
  AddToClusterNodeLabelsRequestProto proto = AddToClusterNodeLabelsRequestProto
      .getDefaultInstance();
  AddToClusterNodeLabelsRequestProto.Builder builder = null;
  boolean viaProto = false;

  public AddToClusterNodeLabelsRequestPBImpl() {
    this.builder = AddToClusterNodeLabelsRequestProto.newBuilder();
  }

  public AddToClusterNodeLabelsRequestPBImpl(
      AddToClusterNodeLabelsRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = AddToClusterNodeLabelsRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.labels != null && !this.labels.isEmpty()) {
      builder.clearNodeLabels();
      builder.addAllNodeLabels(this.labels);
    }
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  public AddToClusterNodeLabelsRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void initLabels() {
    if (this.labels != null) {
      return;
    }
    AddToClusterNodeLabelsRequestProtoOrBuilder p = viaProto ? proto : builder;
    this.labels = new HashSet<String>();
    this.labels.addAll(p.getNodeLabelsList());
  }

  @Override
  public void setNodeLabels(Set<String> labels) {
    maybeInitBuilder();
    if (labels == null || labels.isEmpty()) {
      builder.clearNodeLabels();
    }
    this.labels = labels;
  }

  @Override
  public Set<String> getNodeLabels() {
    initLabels();
    return this.labels;
  }

  @Override
  public int hashCode() {
    assert false : "hashCode not designed";
    return 0;
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
}

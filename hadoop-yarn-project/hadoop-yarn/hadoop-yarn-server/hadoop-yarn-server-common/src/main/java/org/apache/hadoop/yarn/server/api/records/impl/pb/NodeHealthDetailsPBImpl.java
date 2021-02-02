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

package org.apache.hadoop.yarn.server.api.records.impl.pb;

import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeHealthDetailsProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeHealthDetailsProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.records.NodeHealthDetails;

import java.util.Collections;
import java.util.Map;

public class NodeHealthDetailsPBImpl extends NodeHealthDetails {
  private NodeHealthDetailsProto proto = NodeHealthDetailsProto
      .getDefaultInstance();
  private NodeHealthDetailsProto.Builder builder = null;
  private boolean viaProto = false;

  public NodeHealthDetailsPBImpl() {
    builder = NodeHealthDetailsProto.newBuilder();
  }

  public NodeHealthDetailsPBImpl(NodeHealthDetailsProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  public NodeHealthDetailsProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = NodeHealthDetailsProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public void setOverallScore(Integer overallScore) {
    maybeInitBuilder();
    this.builder.setOverallScore(overallScore);
  }

  @Override
  public Integer getOverallScore() {
    NodeHealthDetailsProtoOrBuilder p =
        this.viaProto ? this.proto : this.builder;
    if (!p.hasOverallScore()) {
      return null;
    }
    return (p.getOverallScore());
  }

  @Override
  public void setNodeResourceScores(Map<String, Integer> nodeResourceScores) {
    maybeInitBuilder();
    this.builder.addAllNodeResourceScores(ProtoUtils
        .convertStringIntMapToProtoList(nodeResourceScores));
  }

  @Override
  public Map<String, Integer> getNodeResourceScores() {
    NodeHealthDetailsProtoOrBuilder p =
        this.viaProto ? this.proto : this.builder;
    return p.getNodeResourceScoresCount() > 0 ?
        ProtoUtils.convertProtoListToStringIntMap(
            p.getNodeResourceScoresList()) : Collections.emptyMap();
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.records.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterHeartbeatRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterHeartbeatRequestProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterIdProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterStateProto;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * Protocol buffer based implementation of {@link SubClusterHeartbeatRequest}.
 */
@Private
@Unstable
public class SubClusterHeartbeatRequestPBImpl
    extends SubClusterHeartbeatRequest {

  private SubClusterHeartbeatRequestProto proto =
      SubClusterHeartbeatRequestProto.getDefaultInstance();
  private SubClusterHeartbeatRequestProto.Builder builder = null;
  private boolean viaProto = false;

  private SubClusterId subClusterId = null;

  public SubClusterHeartbeatRequestPBImpl() {
    builder = SubClusterHeartbeatRequestProto.newBuilder();
  }

  public SubClusterHeartbeatRequestPBImpl(
      SubClusterHeartbeatRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public SubClusterHeartbeatRequestProto getProto() {
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

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SubClusterHeartbeatRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.subClusterId != null) {
      builder.setSubClusterId(convertToProtoFormat(this.subClusterId));
    }
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
  public SubClusterId getSubClusterId() {
    SubClusterHeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.subClusterId != null) {
      return this.subClusterId;
    }
    if (!p.hasSubClusterId()) {
      return null;
    }
    this.subClusterId = convertFromProtoFormat(p.getSubClusterId());
    return this.subClusterId;
  }

  @Override
  public void setSubClusterId(SubClusterId subClusterId) {
    maybeInitBuilder();
    if (subClusterId == null) {
      builder.clearSubClusterId();
    }
    this.subClusterId = subClusterId;
  }

  @Override
  public long getLastHeartBeat() {
    SubClusterHeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getLastHeartBeat();
  }

  @Override
  public void setLastHeartBeat(long time) {
    maybeInitBuilder();
    builder.setLastHeartBeat(time);
  }

  @Override
  public SubClusterState getState() {
    SubClusterHeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasState()) {
      return null;
    }
    return convertFromProtoFormat(p.getState());
  }

  @Override
  public void setState(SubClusterState state) {
    maybeInitBuilder();
    if (state == null) {
      builder.clearState();
      return;
    }
    builder.setState(convertToProtoFormat(state));
  }

  @Override
  public String getCapability() {
    SubClusterHeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasCapability()) ? p.getCapability() : null;
  }

  @Override
  public void setCapability(String capability) {
    maybeInitBuilder();
    if (capability == null) {
      builder.clearCapability();
      return;
    }
    builder.setCapability(capability);
  }

  private SubClusterId convertFromProtoFormat(SubClusterIdProto clusterId) {
    return new SubClusterIdPBImpl(clusterId);
  }

  private SubClusterIdProto convertToProtoFormat(SubClusterId clusterId) {
    return ((SubClusterIdPBImpl) clusterId).getProto();
  }

  private SubClusterState convertFromProtoFormat(SubClusterStateProto state) {
    return SubClusterState.valueOf(state.name());
  }

  private SubClusterStateProto convertToProtoFormat(SubClusterState state) {
    return SubClusterStateProto.valueOf(state.name());
  }

}

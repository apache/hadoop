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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterDeregisterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterDeregisterRequestProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterIdProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterStateProto;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * Protocol buffer based implementation of {@link SubClusterDeregisterRequest}.
 */
@Private
@Unstable
public class SubClusterDeregisterRequestPBImpl
    extends SubClusterDeregisterRequest {

  private SubClusterDeregisterRequestProto proto =
      SubClusterDeregisterRequestProto.getDefaultInstance();
  private SubClusterDeregisterRequestProto.Builder builder = null;
  private boolean viaProto = false;

  public SubClusterDeregisterRequestPBImpl() {
    builder = SubClusterDeregisterRequestProto.newBuilder();
  }

  public SubClusterDeregisterRequestPBImpl(
      SubClusterDeregisterRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public SubClusterDeregisterRequestProto getProto() {
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
      builder = SubClusterDeregisterRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
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
    SubClusterDeregisterRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasSubClusterId()) {
      return null;
    }
    return convertFromProtoFormat(p.getSubClusterId());
  }

  @Override
  public void setSubClusterId(SubClusterId subClusterId) {
    maybeInitBuilder();
    if (subClusterId == null) {
      builder.clearSubClusterId();
      return;
    }
    builder.setSubClusterId(convertToProtoFormat(subClusterId));
  }

  @Override
  public SubClusterState getState() {
    SubClusterDeregisterRequestProtoOrBuilder p = viaProto ? proto : builder;
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

  private SubClusterId convertFromProtoFormat(SubClusterIdProto sc) {
    return new SubClusterIdPBImpl(sc);
  }

  private SubClusterIdProto convertToProtoFormat(SubClusterId sc) {
    return ((SubClusterIdPBImpl) sc).getProto();
  }

  private SubClusterState convertFromProtoFormat(SubClusterStateProto state) {
    return SubClusterState.valueOf(state.name());
  }

  private SubClusterStateProto convertToProtoFormat(SubClusterState state) {
    return SubClusterStateProto.valueOf(state.name());
  }

}

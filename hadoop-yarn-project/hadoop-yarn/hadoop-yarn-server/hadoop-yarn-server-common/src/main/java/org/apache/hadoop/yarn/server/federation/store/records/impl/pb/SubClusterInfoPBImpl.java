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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterIdProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterInfoProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterInfoProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterStateProto;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * Protocol buffer based implementation of {@link SubClusterInfo}.
 */
@Private
@Unstable
public class SubClusterInfoPBImpl extends SubClusterInfo {

  private SubClusterInfoProto proto = SubClusterInfoProto.getDefaultInstance();
  private SubClusterInfoProto.Builder builder = null;
  private boolean viaProto = false;

  private SubClusterId subClusterId = null;

  public SubClusterInfoPBImpl() {
    builder = SubClusterInfoProto.newBuilder();
  }

  public SubClusterInfoPBImpl(SubClusterInfoProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public SubClusterInfoProto getProto() {
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
      builder = SubClusterInfoProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.subClusterId != null) {
      builder.setSubClusterId(convertToProtoFormat(this.subClusterId));
    }
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  @Override
  public SubClusterId getSubClusterId() {
    SubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
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
  public String getAMRMServiceAddress() {
    SubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasAMRMServiceAddress()) ? p.getAMRMServiceAddress() : null;
  }

  @Override
  public void setAMRMServiceAddress(String amRMServiceAddress) {
    maybeInitBuilder();
    if (amRMServiceAddress == null) {
      builder.clearAMRMServiceAddress();
      return;
    }
    builder.setAMRMServiceAddress(amRMServiceAddress);
  }

  @Override
  public String getClientRMServiceAddress() {
    SubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasClientRMServiceAddress()) ? p.getClientRMServiceAddress()
        : null;
  }

  @Override
  public void setClientRMServiceAddress(String clientRMServiceAddress) {
    maybeInitBuilder();
    if (clientRMServiceAddress == null) {
      builder.clearClientRMServiceAddress();
      return;
    }
    builder.setClientRMServiceAddress(clientRMServiceAddress);
  }

  @Override
  public String getRMAdminServiceAddress() {
    SubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasRMAdminServiceAddress()) ? p.getRMAdminServiceAddress() : null;
  }

  @Override
  public void setRMAdminServiceAddress(String rmAdminServiceAddress) {
    maybeInitBuilder();
    if (rmAdminServiceAddress == null) {
      builder.clearRMAdminServiceAddress();
      return;
    }
    builder.setRMAdminServiceAddress(rmAdminServiceAddress);
  }

  @Override
  public String getRMWebServiceAddress() {
    SubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasRMWebServiceAddress()) ? p.getRMWebServiceAddress() : null;
  }

  @Override
  public void setRMWebServiceAddress(String rmWebServiceAddress) {
    maybeInitBuilder();
    if (rmWebServiceAddress == null) {
      builder.clearRMWebServiceAddress();
      return;
    }
    builder.setRMWebServiceAddress(rmWebServiceAddress);
  }

  @Override
  public long getLastHeartBeat() {
    SubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
    return p.getLastHeartBeat();
  }

  @Override
  public void setLastHeartBeat(long time) {
    maybeInitBuilder();
    builder.setLastHeartBeat(time);
  }

  @Override
  public SubClusterState getState() {
    SubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
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
  public long getLastStartTime() {
    SubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasLastStartTime()) ? p.getLastStartTime() : 0;
  }

  @Override
  public void setLastStartTime(long lastStartTime) {
    Preconditions.checkNotNull(builder);
    builder.setLastStartTime(lastStartTime);
  }

  @Override
  public String getCapability() {
    SubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
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

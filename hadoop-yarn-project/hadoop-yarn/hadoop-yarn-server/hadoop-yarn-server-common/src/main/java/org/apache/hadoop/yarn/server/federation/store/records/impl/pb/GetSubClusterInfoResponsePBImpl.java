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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterInfoResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterInfoResponseProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterInfoProto;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * Protocol buffer based implementation of {@link GetSubClusterInfoResponse}.
 */
@Private
@Unstable
public class GetSubClusterInfoResponsePBImpl extends GetSubClusterInfoResponse {

  private GetSubClusterInfoResponseProto proto =
      GetSubClusterInfoResponseProto.getDefaultInstance();
  private GetSubClusterInfoResponseProto.Builder builder = null;
  private boolean viaProto = false;

  private SubClusterInfo subClusterInfo = null;

  public GetSubClusterInfoResponsePBImpl() {
    builder = GetSubClusterInfoResponseProto.newBuilder();
  }

  public GetSubClusterInfoResponsePBImpl(GetSubClusterInfoResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetSubClusterInfoResponseProto getProto() {
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
      builder = GetSubClusterInfoResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.subClusterInfo != null) {
      builder.setSubClusterInfo(convertToProtoFormat(this.subClusterInfo));
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
  public SubClusterInfo getSubClusterInfo() {
    GetSubClusterInfoResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.subClusterInfo != null) {
      return this.subClusterInfo;
    }
    if (!p.hasSubClusterInfo()) {
      return null;
    }
    this.subClusterInfo = convertFromProtoFormat(p.getSubClusterInfo());
    return this.subClusterInfo;
  }

  @Override
  public void setSubClusterInfo(SubClusterInfo subClusterInfo) {
    maybeInitBuilder();
    if (subClusterInfo == null) {
      builder.clearSubClusterInfo();
    }
    this.subClusterInfo = subClusterInfo;
  }

  private SubClusterInfo convertFromProtoFormat(
      SubClusterInfoProto clusterInfo) {
    return new SubClusterInfoPBImpl(clusterInfo);
  }

  private SubClusterInfoProto convertToProtoFormat(SubClusterInfo clusterInfo) {
    return ((SubClusterInfoPBImpl) clusterInfo).getProto();
  }

}

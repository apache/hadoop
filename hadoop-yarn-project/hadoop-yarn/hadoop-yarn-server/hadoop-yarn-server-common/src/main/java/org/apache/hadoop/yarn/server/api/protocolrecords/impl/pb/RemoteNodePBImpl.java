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

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RemoteNodeProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RemoteNodeProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoteNode;

/**
 * Implementation of {@link RemoteNode}.
 */
public class RemoteNodePBImpl extends RemoteNode {

  private RemoteNodeProto proto = RemoteNodeProto.getDefaultInstance();
  private RemoteNodeProto.Builder builder = null;
  private boolean viaProto = false;

  private NodeId nodeId = null;

  public RemoteNodePBImpl() {
    builder = RemoteNodeProto.newBuilder();
  }

  public RemoteNodePBImpl(RemoteNodeProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public RemoteNodeProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.nodeId != null
        && !((NodeIdPBImpl) nodeId).getProto().equals(
        builder.getNodeId())) {
      builder.setNodeId(ProtoUtils.convertToProtoFormat(this.nodeId));
    }
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
      builder = RemoteNodeProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public NodeId getNodeId() {
    RemoteNodeProtoOrBuilder p = viaProto ? proto : builder;
    if (this.nodeId != null) {
      return this.nodeId;
    }
    if (!p.hasNodeId()) {
      return null;
    }
    this.nodeId = ProtoUtils.convertFromProtoFormat(p.getNodeId());
    return this.nodeId;
  }

  @Override
  public void setNodeId(NodeId nodeId) {
    maybeInitBuilder();
    if (nodeId == null) {
      builder.clearNodeId();
    }
    this.nodeId = nodeId;
  }

  @Override
  public String getHttpAddress() {
    RemoteNodeProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasHttpAddress()) {
      return null;
    }
    return (p.getHttpAddress());
  }

  @Override
  public void setHttpAddress(String httpAddress) {
    maybeInitBuilder();
    if (httpAddress == null) {
      builder.clearHttpAddress();
      return;
    }
    builder.setHttpAddress(httpAddress);
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
}

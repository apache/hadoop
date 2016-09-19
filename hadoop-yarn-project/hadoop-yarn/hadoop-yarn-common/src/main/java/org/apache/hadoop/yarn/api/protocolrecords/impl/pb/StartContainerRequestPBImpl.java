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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerLaunchContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.TokenPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerLaunchContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainerRequestProtoOrBuilder;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class StartContainerRequestPBImpl extends StartContainerRequest {
  StartContainerRequestProto proto = StartContainerRequestProto.getDefaultInstance();
  StartContainerRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private ContainerLaunchContext containerLaunchContext = null;

  private Token containerToken = null;
  private ContainerId originContainerId = null;
  private NodeId originNodeId = null;
  private Token originNMToken = null;
  
  public StartContainerRequestPBImpl() {
    builder = StartContainerRequestProto.newBuilder();
  }

  public StartContainerRequestPBImpl(StartContainerRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public StartContainerRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
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

  private void mergeLocalToBuilder() {
    if (this.containerLaunchContext != null) {
      builder.setContainerLaunchContext(convertToProtoFormat(this.containerLaunchContext));
    }
    if(this.containerToken != null) {
      builder.setContainerToken(convertToProtoFormat(this.containerToken));
    }
    if(this.originContainerId != null) {
      builder.setOriginContainerId(convertToProtoFormat(this.originContainerId));
    }
    if(this.originNodeId != null) {
      builder.setOriginNodeId(convertToProtoFormat(this.originNodeId));
    }
    if(this.originNMToken != null) {
      builder.setOriginNmToken(convertToProtoFormat(this.originNMToken));
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = StartContainerRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public ContainerLaunchContext getContainerLaunchContext() {
    StartContainerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerLaunchContext != null) {
      return this.containerLaunchContext;
    }
    if (!p.hasContainerLaunchContext()) {
      return null;
    }
    this.containerLaunchContext = convertFromProtoFormat(p.getContainerLaunchContext());
    return this.containerLaunchContext;
  }

  @Override
  public void setContainerLaunchContext(ContainerLaunchContext containerLaunchContext) {
    maybeInitBuilder();
    if (containerLaunchContext == null) 
      builder.clearContainerLaunchContext();
    this.containerLaunchContext = containerLaunchContext;
  }

  @Override
  public Token getContainerToken() {
    StartContainerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerToken != null) {
      return this.containerToken;
    }
    if (!p.hasContainerToken()) {
      return null;
    }
    this.containerToken = convertFromProtoFormat(p.getContainerToken());
    return this.containerToken;
  }

  @Override
  public void setContainerToken(Token containerToken) {
    maybeInitBuilder();
    if(containerToken == null) {
      builder.clearContainerToken();
    }
    this.containerToken = containerToken;
  }
  
  @Override
  public boolean getIsMove() {
    StartContainerRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getIsMove();
  }
  
  @Override
  public void setIsMove(boolean isMove) {
    maybeInitBuilder();
    builder.setIsMove(isMove);
  }
  
  @Override
  public ContainerId getOriginContainerId() {
    StartContainerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.originContainerId != null) {
      return this.originContainerId;
    }
    if (!p.hasOriginContainerId()) {
      return null;
    }
    this.originContainerId = convertFromProtoFormat(p.getOriginContainerId());
    return this.originContainerId;
  }
  
  @Override
  public void setOriginContainerId(ContainerId originContainerId) {
    maybeInitBuilder();
    if(originContainerId == null) {
      builder.clearOriginContainerId();
    }
    this.originContainerId = originContainerId;
  }
  
  @Override
  public NodeId getOriginNodeId() {
    StartContainerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.originNodeId != null) {
      return this.originNodeId;
    }
    if (!p.hasOriginNodeId()) {
      return null;
    }
    this.originNodeId = convertFromProtoFormat(p.getOriginNodeId());
    return this.originNodeId;
  }
  
  @Override
  public void setOriginNodeId(NodeId originNodeId) {
    maybeInitBuilder();
    if(originNodeId == null) {
      builder.clearOriginNodeId();
    }
    this.originNodeId = originNodeId;
  }
  
  @Override
  public Token getOriginNMToken() {
    StartContainerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.originNMToken != null) {
      return this.originNMToken;
    }
    if (!p.hasOriginNmToken()) {
      return null;
    }
    this.originNMToken = convertFromProtoFormat(p.getOriginNmToken());
    return this.originNMToken;
  }
  
  @Override
  public void setOriginNMToken(Token originNMToken) {
    maybeInitBuilder();
    if(originNMToken == null) {
      builder.clearOriginNmToken();
    }
    this.originNMToken = originNMToken;
  }
  
  private ContainerLaunchContextPBImpl convertFromProtoFormat(ContainerLaunchContextProto p) {
    return new ContainerLaunchContextPBImpl(p);
  }

  private ContainerLaunchContextProto convertToProtoFormat(ContainerLaunchContext t) {
    return ((ContainerLaunchContextPBImpl)t).getProto();
  }
  
  private TokenPBImpl convertFromProtoFormat(TokenProto containerProto) {
    return new TokenPBImpl(containerProto);
  }

  private TokenProto convertToProtoFormat(Token container) {
    return ((TokenPBImpl)container).getProto();
  }
  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }
  
  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl)t).getProto();
  }
  
  private NodeIdPBImpl convertFromProtoFormat(NodeIdProto p) {
    return new NodeIdPBImpl(p);
  }
  
  private NodeIdProto convertToProtoFormat(NodeId t) {
    return ((NodeIdPBImpl)t).getProto();
  }
}  

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


import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerLaunchContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerTokenPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerLaunchContextProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainerRequestProtoOrBuilder;


    
public class StartContainerRequestPBImpl extends ProtoBase<StartContainerRequestProto> implements StartContainerRequest {
  StartContainerRequestProto proto = StartContainerRequestProto.getDefaultInstance();
  StartContainerRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private ContainerLaunchContext containerLaunchContext = null;

  private ContainerToken containerToken = null;
  
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

  private void mergeLocalToBuilder() {
    if (this.containerLaunchContext != null) {
      builder.setContainerLaunchContext(convertToProtoFormat(this.containerLaunchContext));
    }
    if(this.containerToken != null) {
      builder.setContainerToken(convertToProtoFormat(this.containerToken));
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
  public ContainerToken getContainerToken() {
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
  public void setContainerToken(ContainerToken containerToken) {
    maybeInitBuilder();
    if(containerToken == null) {
      builder.clearContainerToken();
    }
    this.containerToken = containerToken;
  }

  private ContainerLaunchContextPBImpl convertFromProtoFormat(ContainerLaunchContextProto p) {
    return new ContainerLaunchContextPBImpl(p);
  }

  private ContainerLaunchContextProto convertToProtoFormat(ContainerLaunchContext t) {
    return ((ContainerLaunchContextPBImpl)t).getProto();
  }



  private ContainerTokenPBImpl convertFromProtoFormat(TokenProto containerProto) {
    return new ContainerTokenPBImpl(containerProto);
  }

  private TokenProto convertToProtoFormat(ContainerToken container) {
    return ((ContainerTokenPBImpl)container).getProto();
  }
}  

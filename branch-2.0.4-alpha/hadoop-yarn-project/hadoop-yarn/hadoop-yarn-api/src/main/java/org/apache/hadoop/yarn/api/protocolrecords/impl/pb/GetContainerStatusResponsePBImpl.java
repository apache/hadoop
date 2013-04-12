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


import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerStatusPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStatusProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerStatusResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerStatusResponseProtoOrBuilder;


    
public class GetContainerStatusResponsePBImpl extends ProtoBase<GetContainerStatusResponseProto> implements GetContainerStatusResponse {
  GetContainerStatusResponseProto proto = GetContainerStatusResponseProto.getDefaultInstance();
  GetContainerStatusResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private ContainerStatus containerStatus = null;
  
  
  public GetContainerStatusResponsePBImpl() {
    builder = GetContainerStatusResponseProto.newBuilder();
  }

  public GetContainerStatusResponsePBImpl(GetContainerStatusResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetContainerStatusResponseProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.containerStatus != null) {
      builder.setStatus(convertToProtoFormat(this.containerStatus));
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
      builder = GetContainerStatusResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public ContainerStatus getStatus() {
    GetContainerStatusResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerStatus != null) {
      return this.containerStatus;
    }
    if (!p.hasStatus()) {
      return null;
    }
    this.containerStatus = convertFromProtoFormat(p.getStatus());
    return this.containerStatus;
  }

  @Override
  public void setStatus(ContainerStatus status) {
    maybeInitBuilder();
    if (status == null) 
      builder.clearStatus();
    this.containerStatus = status;
  }

  private ContainerStatusPBImpl convertFromProtoFormat(ContainerStatusProto p) {
    return new ContainerStatusPBImpl(p);
  }

  private ContainerStatusProto convertToProtoFormat(ContainerStatus t) {
    return ((ContainerStatusPBImpl)t).getProto();
  }



}  

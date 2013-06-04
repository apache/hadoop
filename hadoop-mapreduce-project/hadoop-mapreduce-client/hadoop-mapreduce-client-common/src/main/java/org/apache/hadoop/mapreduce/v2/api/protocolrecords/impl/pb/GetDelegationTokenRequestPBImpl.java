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
package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;

import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;


public class GetDelegationTokenRequestPBImpl extends
      ProtoBase<GetDelegationTokenRequestProto> implements GetDelegationTokenRequest {
  
  String renewer;
  

  GetDelegationTokenRequestProto proto = 
      GetDelegationTokenRequestProto.getDefaultInstance();
  GetDelegationTokenRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  public GetDelegationTokenRequestPBImpl() {
    builder = GetDelegationTokenRequestProto.newBuilder();
  }
  
  public GetDelegationTokenRequestPBImpl (
      GetDelegationTokenRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public String getRenewer(){
    GetDelegationTokenRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.renewer != null) {
      return this.renewer;
    }
    this.renewer = p.getRenewer();
    return this.renewer;
  }
  
  @Override
  public void setRenewer(String renewer) {
    maybeInitBuilder();
    if (renewer == null) 
      builder.clearRenewer();
    this.renewer = renewer;
  }

  @Override
  public GetDelegationTokenRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  

  private void mergeLocalToBuilder() {
    if (renewer != null) {
      builder.setRenewer(this.renewer);
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
      builder = GetDelegationTokenRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }   
}

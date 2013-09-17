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

import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenResponseProtoOrBuilder;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.TokenPBImpl;

public class GetDelegationTokenResponsePBImpl extends
      ProtoBase<GetDelegationTokenResponseProto> implements GetDelegationTokenResponse {
  
  Token mrToken;
  

  GetDelegationTokenResponseProto proto = 
      GetDelegationTokenResponseProto.getDefaultInstance();
  GetDelegationTokenResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public GetDelegationTokenResponsePBImpl() {
    builder = GetDelegationTokenResponseProto.newBuilder();
  }
  
  public GetDelegationTokenResponsePBImpl (
      GetDelegationTokenResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public Token getDelegationToken() {
    GetDelegationTokenResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.mrToken != null) {
      return this.mrToken;
    }
    if (!p.hasToken()) {
      return null;
    }
    this.mrToken = convertFromProtoFormat(p.getToken());
    return this.mrToken;  
  }
  
  @Override
  public void setDelegationToken(Token mrToken) {
    maybeInitBuilder();
    if (mrToken == null) 
      builder.getToken();
    this.mrToken = mrToken;
  }

  @Override
  public GetDelegationTokenResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  

  private void mergeLocalToBuilder() {
    if (mrToken != null) {
      builder.setToken(convertToProtoFormat(this.mrToken));
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
      builder = GetDelegationTokenResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private TokenPBImpl convertFromProtoFormat(TokenProto p) {
    return new TokenPBImpl(p);
  }

  private TokenProto convertToProtoFormat(Token t) {
    return ((TokenPBImpl)t).getProto();
  }
}

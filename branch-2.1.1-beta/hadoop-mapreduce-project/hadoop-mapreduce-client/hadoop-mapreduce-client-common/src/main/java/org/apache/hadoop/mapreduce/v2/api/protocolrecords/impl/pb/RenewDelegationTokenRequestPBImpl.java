/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;

import org.apache.hadoop.mapreduce.v2.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProtoOrBuilder;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.TokenPBImpl;

public class RenewDelegationTokenRequestPBImpl extends
    ProtoBase<RenewDelegationTokenRequestProto> implements
    RenewDelegationTokenRequest {

  RenewDelegationTokenRequestProto proto = RenewDelegationTokenRequestProto
      .getDefaultInstance();
  RenewDelegationTokenRequestProto.Builder builder = null;
  boolean viaProto = false;

  public RenewDelegationTokenRequestPBImpl() {
    this.builder = RenewDelegationTokenRequestProto.newBuilder();
  }

  public RenewDelegationTokenRequestPBImpl(
      RenewDelegationTokenRequestProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  Token token;

  @Override
  public Token getDelegationToken() {
    RenewDelegationTokenRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.token != null) {
      return this.token;
    }
    this.token = convertFromProtoFormat(p.getToken());
    return this.token;
  }

  @Override
  public void setDelegationToken(Token token) {
    maybeInitBuilder();
    if (token == null)
      builder.clearToken();
    this.token = token;
  }

  @Override
  public RenewDelegationTokenRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (token != null) {
      builder.setToken(convertToProtoFormat(this.token));
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
      builder = RenewDelegationTokenRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private TokenPBImpl convertFromProtoFormat(TokenProto p) {
    return new TokenPBImpl(p);
  }

  private TokenProto convertToProtoFormat(Token t) {
    return ((TokenPBImpl) t).getProto();
  }
}

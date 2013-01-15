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

import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenResponseProtoOrBuilder;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.records.DelegationToken;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.DelegationTokenPBImpl;

public class GetDelegationTokenResponsePBImpl extends
ProtoBase<GetDelegationTokenResponseProto> implements GetDelegationTokenResponse {

  DelegationToken appToken;


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
  public DelegationToken getRMDelegationToken() {
    GetDelegationTokenResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.appToken != null) {
      return this.appToken;
    }
    if (!p.hasToken()) {
      return null;
    }
    this.appToken = convertFromProtoFormat(p.getToken());
    return this.appToken;  
  }

  @Override
  public void setRMDelegationToken(DelegationToken appToken) {
    maybeInitBuilder();
    if (appToken == null) 
      builder.clearToken();
    this.appToken = appToken;
  }

  @Override
  public GetDelegationTokenResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }


  private void mergeLocalToBuilder() {
    if (appToken != null) {
      builder.setToken(convertToProtoFormat(this.appToken));
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


  private DelegationTokenPBImpl convertFromProtoFormat(TokenProto p) {
    return new DelegationTokenPBImpl(p);
  }

  private TokenProto convertToProtoFormat(DelegationToken t) {
    return ((DelegationTokenPBImpl)t).getProto();
  }
}

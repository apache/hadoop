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
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenResponseProtoOrBuilder;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.impl.pb.TokenPBImpl;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

@Private
@Unstable
public class GetDelegationTokenResponsePBImpl extends GetDelegationTokenResponse {

  Token appToken;


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
  public Token getRMDelegationToken() {
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
  public void setRMDelegationToken(Token appToken) {
    maybeInitBuilder();
    if (appToken == null) 
      builder.clearToken();
    this.appToken = appToken;
  }

  public GetDelegationTokenResponseProto getProto() {
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


  private TokenPBImpl convertFromProtoFormat(TokenProto p) {
    return new TokenPBImpl(p);
  }

  private TokenProto convertToProtoFormat(Token t) {
    return ((TokenPBImpl)t).getProto();
  }
}

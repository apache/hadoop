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


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenResponseProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

public class RenewDelegationTokenResponsePBImpl extends
    ProtoBase<RenewDelegationTokenResponseProto> implements
    RenewDelegationTokenResponse {
  
  RenewDelegationTokenResponseProto proto =
      RenewDelegationTokenResponseProto.getDefaultInstance();
  RenewDelegationTokenResponseProto.Builder builder = null;
  boolean viaProto = false;

  public RenewDelegationTokenResponsePBImpl() {
    this.builder = RenewDelegationTokenResponseProto.newBuilder();
  }

  public RenewDelegationTokenResponsePBImpl (
      RenewDelegationTokenResponseProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  @Override
  public RenewDelegationTokenResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RenewDelegationTokenResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
  @Override
  public long getNextExpirationTime() {
    RenewDelegationTokenResponseProtoOrBuilder p = viaProto ? proto : builder;
    return p.getNewExpiryTime();
  }

  @Override
  public void setNextExpirationTime(long expTime) {
    maybeInitBuilder();
    builder.setNewExpiryTime(expTime);
  }
}

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


import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.RegistrationResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.RegistrationResponse;
import org.apache.hadoop.yarn.server.api.records.impl.pb.RegistrationResponsePBImpl;


    
public class RegisterNodeManagerResponsePBImpl extends ProtoBase<RegisterNodeManagerResponseProto> implements RegisterNodeManagerResponse {
  RegisterNodeManagerResponseProto proto = RegisterNodeManagerResponseProto.getDefaultInstance();
  RegisterNodeManagerResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private RegistrationResponse registartionResponse = null;
  
  private boolean rebuild = false;
  
  public RegisterNodeManagerResponsePBImpl() {
    builder = RegisterNodeManagerResponseProto.newBuilder();
  }

  public RegisterNodeManagerResponsePBImpl(RegisterNodeManagerResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public RegisterNodeManagerResponseProto getProto() {
    if (rebuild)
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.registartionResponse != null) {
      builder.setRegistrationResponse(convertToProtoFormat(this.registartionResponse));
      this.registartionResponse = null;
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    rebuild = false;
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RegisterNodeManagerResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public RegistrationResponse getRegistrationResponse() {
    RegisterNodeManagerResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.registartionResponse != null) {
      return this.registartionResponse;
    }
    if (!p.hasRegistrationResponse()) {
      return null;
    }
    this.registartionResponse = convertFromProtoFormat(p.getRegistrationResponse());
    rebuild = true;
    return this.registartionResponse;
  }

  @Override
  public void setRegistrationResponse(RegistrationResponse registrationResponse) {
    maybeInitBuilder();
    if (registrationResponse == null) 
      builder.clearRegistrationResponse();
    this.registartionResponse = registrationResponse;
    rebuild = true;
  }

  private RegistrationResponsePBImpl convertFromProtoFormat(RegistrationResponseProto p) {
    return new RegistrationResponsePBImpl(p);
  }

  private RegistrationResponseProto convertToProtoFormat(RegistrationResponse t) {
    return ((RegistrationResponsePBImpl)t).getProto();
  }



}  

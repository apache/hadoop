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

package org.apache.hadoop.yarn.server.api.records.impl.pb;


import java.nio.ByteBuffer;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.RegistrationResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.RegistrationResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.records.RegistrationResponse;


    
public class RegistrationResponsePBImpl extends ProtoBase<RegistrationResponseProto> implements RegistrationResponse {
  RegistrationResponseProto proto = RegistrationResponseProto.getDefaultInstance();
  RegistrationResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private ByteBuffer secretKey = null;
  
  public RegistrationResponsePBImpl() {
    builder = RegistrationResponseProto.newBuilder();
  }

  public RegistrationResponsePBImpl(RegistrationResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public RegistrationResponseProto getProto() {
    
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.secretKey != null) {
      builder.setSecretKey(convertToProtoFormat(this.secretKey));
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
      builder = RegistrationResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public ByteBuffer getSecretKey() {
    RegistrationResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.secretKey != null) {
      return this.secretKey;
    }
    if (!p.hasSecretKey()) {
      return null;
    }
    this.secretKey = convertFromProtoFormat(p.getSecretKey());
    return this.secretKey;
  }

  @Override
  public void setSecretKey(ByteBuffer secretKey) {
    maybeInitBuilder();
    if (secretKey == null) 
      builder.clearSecretKey();
    this.secretKey = secretKey;
  }

}  

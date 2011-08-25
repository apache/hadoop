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


import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.AMResponsePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.AMResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateResponseProtoOrBuilder;


    
public class AllocateResponsePBImpl extends ProtoBase<AllocateResponseProto> implements AllocateResponse {
  AllocateResponseProto proto = AllocateResponseProto.getDefaultInstance();
  AllocateResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private AMResponse amResponse;
  
  
  public AllocateResponsePBImpl() {
    builder = AllocateResponseProto.newBuilder();
  }

  public AllocateResponsePBImpl(AllocateResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public AllocateResponseProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.amResponse != null) {
      builder.setAMResponse(convertToProtoFormat(this.amResponse));
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
      builder = AllocateResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public AMResponse getAMResponse() {
    AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.amResponse != null) {
      return this.amResponse;
    }
    if (!p.hasAMResponse()) {
      return null;
    }
    this.amResponse= convertFromProtoFormat(p.getAMResponse());
    return this.amResponse;
  }

  @Override
  public void setAMResponse(AMResponse aMResponse) {
    maybeInitBuilder();
    if (aMResponse == null) 
      builder.clearAMResponse();
    this.amResponse = aMResponse;
  }

  private AMResponsePBImpl convertFromProtoFormat(AMResponseProto p) {
    return new AMResponsePBImpl(p);
  }

  private AMResponseProto convertToProtoFormat(AMResponse t) {
    return ((AMResponsePBImpl)t).getProto();
  }



}  

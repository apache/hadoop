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


import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueInfoProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RegisterApplicationMasterResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RegisterApplicationMasterResponseProtoOrBuilder;


    
public class RegisterApplicationMasterResponsePBImpl 
extends ProtoBase<RegisterApplicationMasterResponseProto> 
implements RegisterApplicationMasterResponse {
  RegisterApplicationMasterResponseProto proto = 
    RegisterApplicationMasterResponseProto.getDefaultInstance();
  RegisterApplicationMasterResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private Resource minimumResourceCapability;
  private Resource maximumResourceCapability;
  
  public RegisterApplicationMasterResponsePBImpl() {
    builder = RegisterApplicationMasterResponseProto.newBuilder();
  }

  public RegisterApplicationMasterResponsePBImpl(RegisterApplicationMasterResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public RegisterApplicationMasterResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  
  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (this.minimumResourceCapability != null) {
      builder.setMinimumCapability(
          convertToProtoFormat(this.minimumResourceCapability));
    }
    if (this.maximumResourceCapability != null) {
      builder.setMaximumCapability(
          convertToProtoFormat(this.maximumResourceCapability));
    }
  }


  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RegisterApplicationMasterResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public Resource getMaximumResourceCapability() {
    if (this.maximumResourceCapability != null) {
      return this.maximumResourceCapability;
    }

    RegisterApplicationMasterResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasMaximumCapability()) {
      return null;
    }
    
    this.maximumResourceCapability = convertFromProtoFormat(p.getMaximumCapability());
    return this.maximumResourceCapability;
  }

  @Override
  public Resource getMinimumResourceCapability() {
    if (this.minimumResourceCapability != null) {
      return this.minimumResourceCapability;
    }

    RegisterApplicationMasterResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasMinimumCapability()) {
      return null;
    }
    
    this.minimumResourceCapability = convertFromProtoFormat(p.getMinimumCapability());
    return this.minimumResourceCapability;
  }

  @Override
  public void setMaximumResourceCapability(Resource capability) {
    maybeInitBuilder();
    if(maximumResourceCapability == null) {
      builder.clearMaximumCapability();
    }
    this.maximumResourceCapability = capability;
  }

  @Override
  public void setMinimumResourceCapability(Resource capability) {
    maybeInitBuilder();
    if(minimumResourceCapability == null) {
      builder.clearMinimumCapability();
    }
    this.minimumResourceCapability = capability;
  }

  private Resource convertFromProtoFormat(ResourceProto resource) {
    return new ResourcePBImpl(resource);
  }

  private ResourceProto convertToProtoFormat(Resource resource) {
    return ((ResourcePBImpl)resource).getProto();
  }

}  

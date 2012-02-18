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


import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNewApplicationResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNewApplicationResponseProtoOrBuilder;
    
public class GetNewApplicationResponsePBImpl extends ProtoBase<GetNewApplicationResponseProto> implements GetNewApplicationResponse {
  GetNewApplicationResponseProto proto = GetNewApplicationResponseProto.getDefaultInstance();
  GetNewApplicationResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationId applicationId = null;
  private Resource minimumResourceCapability = null;
  private Resource maximumResourceCapability = null;
  
  public GetNewApplicationResponsePBImpl() {
    builder = GetNewApplicationResponseProto.newBuilder();
  }

  public GetNewApplicationResponsePBImpl(GetNewApplicationResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetNewApplicationResponseProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (applicationId != null) {
      builder.setApplicationId(convertToProtoFormat(this.applicationId));
    }
    if (minimumResourceCapability != null) {
    	builder.setMinimumCapability(convertToProtoFormat(this.minimumResourceCapability));
    }
    if (maximumResourceCapability != null) {
    	builder.setMaximumCapability(convertToProtoFormat(this.maximumResourceCapability));
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
      builder = GetNewApplicationResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public ApplicationId getApplicationId() {
    if (this.applicationId != null) {
      return this.applicationId;
    }
    
    GetNewApplicationResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationId()) {
      return null;
    }
    
    this.applicationId = convertFromProtoFormat(p.getApplicationId());
    return this.applicationId;
  }

  @Override
  public void setApplicationId(ApplicationId applicationId) {
    maybeInitBuilder();
    if (applicationId == null) 
      builder.clearApplicationId();
    this.applicationId = applicationId;
  }

  @Override
  public Resource getMaximumResourceCapability() {
    if (this.maximumResourceCapability != null) {
      return this.maximumResourceCapability;
    }
 
    GetNewApplicationResponseProtoOrBuilder p = viaProto ? proto : builder;
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
    
    GetNewApplicationResponseProtoOrBuilder p = viaProto ? proto : builder;
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
    
  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl)t).getProto();
  }
  
  private Resource convertFromProtoFormat(ResourceProto resource) {
	  return new ResourcePBImpl(resource);
  }

  private ResourceProto convertToProtoFormat(Resource resource) {
	  return ((ResourcePBImpl)resource).getProto();
  }

}  

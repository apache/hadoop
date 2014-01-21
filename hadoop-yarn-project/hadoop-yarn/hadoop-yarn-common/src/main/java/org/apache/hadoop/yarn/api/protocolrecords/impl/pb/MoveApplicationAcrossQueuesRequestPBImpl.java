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
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.MoveApplicationAcrossQueuesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.MoveApplicationAcrossQueuesRequestProtoOrBuilder;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class MoveApplicationAcrossQueuesRequestPBImpl extends MoveApplicationAcrossQueuesRequest {
  MoveApplicationAcrossQueuesRequestProto proto = MoveApplicationAcrossQueuesRequestProto.getDefaultInstance();
  MoveApplicationAcrossQueuesRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationId applicationId;
  private String targetQueue;
  
  public MoveApplicationAcrossQueuesRequestPBImpl() {
    builder = MoveApplicationAcrossQueuesRequestProto.newBuilder();
  }

  public MoveApplicationAcrossQueuesRequestPBImpl(MoveApplicationAcrossQueuesRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public MoveApplicationAcrossQueuesRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  
  @Override
  public ApplicationId getApplicationId() {
    if (this.applicationId != null) {
      return this.applicationId;
    }
    
    MoveApplicationAcrossQueuesRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationId()) {
      return null;
    }
    
    this.applicationId = convertFromProtoFormat(p.getApplicationId());
    return this.applicationId;
  }
  
  @Override
  public void setApplicationId(ApplicationId appId) {
    maybeInitBuilder();
    if (applicationId == null) {
      builder.clearApplicationId();
    }
    applicationId = appId;
  }
  
  @Override
  public String getTargetQueue() {
    if (this.targetQueue != null) {
      return this.targetQueue;
    }
    
    MoveApplicationAcrossQueuesRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationId()) {
      return null;
    }
    
    this.targetQueue = p.getTargetQueue();
    return this.targetQueue;
  }
  
  @Override
  public void setTargetQueue(String queue) {
    maybeInitBuilder();
    if (applicationId == null) {
      builder.clearTargetQueue();
    }
    targetQueue = queue;
  }
  
  private void mergeLocalToBuilder() {
    if (applicationId != null) {
      builder.setApplicationId(convertToProtoFormat(this.applicationId));
    }
    if (targetQueue != null) {
      builder.setTargetQueue(this.targetQueue);
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = MoveApplicationAcrossQueuesRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }
  
  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl)t).getProto();
  }
}

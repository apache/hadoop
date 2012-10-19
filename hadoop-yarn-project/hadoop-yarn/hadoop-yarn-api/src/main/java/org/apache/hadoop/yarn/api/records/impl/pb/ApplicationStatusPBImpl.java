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

package org.apache.hadoop.yarn.api.records.impl.pb;


import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationStatusProtoOrBuilder;


    
public class ApplicationStatusPBImpl extends ProtoBase<ApplicationStatusProto> implements ApplicationStatus {
  ApplicationStatusProto proto = ApplicationStatusProto.getDefaultInstance();
  ApplicationStatusProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationAttemptId applicationAttemptId = null;  
  
  
  public ApplicationStatusPBImpl() {
    builder = ApplicationStatusProto.newBuilder();
  }

  public ApplicationStatusPBImpl(ApplicationStatusProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public ApplicationStatusProto getProto() {
  
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.applicationAttemptId != null && !((ApplicationAttemptIdPBImpl)this.applicationAttemptId).getProto().equals(builder.getApplicationAttemptId())) {
      builder.setApplicationAttemptId(convertToProtoFormat(this.applicationAttemptId));
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
      builder = ApplicationStatusProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public int getResponseId() {
    ApplicationStatusProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getResponseId());
  }

  @Override
  public void setResponseId(int responseId) {
    maybeInitBuilder();
    builder.setResponseId((responseId));
  }

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    ApplicationStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationAttemptId != null) {
      return this.applicationAttemptId;
    }
    if (!p.hasApplicationAttemptId()) {
      return null;
    }
    this.applicationAttemptId = convertFromProtoFormat(p.getApplicationAttemptId());
    return this.applicationAttemptId;
  }

  @Override
  public void setApplicationAttemptId(ApplicationAttemptId applicationAttemptId) {
    maybeInitBuilder();
    if (applicationAttemptId == null) 
      builder.clearApplicationAttemptId();
    this.applicationAttemptId = applicationAttemptId;
  }

  @Override
  public float getProgress() {
    ApplicationStatusProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getProgress());
  }

  @Override
  public void setProgress(float progress) {
    maybeInitBuilder();
    builder.setProgress((progress));
  }

  private ApplicationAttemptIdPBImpl convertFromProtoFormat(ApplicationAttemptIdProto p) {
    return new ApplicationAttemptIdPBImpl(p);
  }

  private ApplicationAttemptIdProto convertToProtoFormat(ApplicationAttemptId t) {
    return ((ApplicationAttemptIdPBImpl)t).getProto();
  }



}  

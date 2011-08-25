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


import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.FinishApplicationMasterRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.FinishApplicationMasterRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RegisterApplicationMasterRequestProtoOrBuilder;


    
public class FinishApplicationMasterRequestPBImpl extends ProtoBase<FinishApplicationMasterRequestProto> implements FinishApplicationMasterRequest {
  FinishApplicationMasterRequestProto proto = FinishApplicationMasterRequestProto.getDefaultInstance();
  FinishApplicationMasterRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationAttemptId appAttemptId = null;
  
  
  public FinishApplicationMasterRequestPBImpl() {
    builder = FinishApplicationMasterRequestProto.newBuilder();
  }

  public FinishApplicationMasterRequestPBImpl(FinishApplicationMasterRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public FinishApplicationMasterRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.appAttemptId != null) {
      builder.setApplicationAttemptId(convertToProtoFormat(this.appAttemptId));
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
      builder = FinishApplicationMasterRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    FinishApplicationMasterRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.appAttemptId != null) {
      return this.appAttemptId;
    }
    if (!p.hasApplicationAttemptId()) {
      return null;
    }
    this.appAttemptId = convertFromProtoFormat(p.getApplicationAttemptId());
    return this.appAttemptId;
  }

  @Override
  public void setAppAttemptId(ApplicationAttemptId applicationAttemptId) {
    maybeInitBuilder();
    if (applicationAttemptId == null) 
      builder.clearApplicationAttemptId();
    this.appAttemptId = applicationAttemptId;
  }

  @Override
  public String getDiagnostics() {
    FinishApplicationMasterRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getDiagnostics();
  }

  @Override
  public void setDiagnostics(String diagnostics) {
    maybeInitBuilder();
    builder.setDiagnostics(diagnostics);
  }

  @Override
  public String getTrackingUrl() {
    FinishApplicationMasterRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getTrackingUrl();
  }

  @Override
  public void setTrackingUrl(String url) {
    maybeInitBuilder();
    builder.setTrackingUrl(url);
  }

  @Override
  public String getFinalState() {
    FinishApplicationMasterRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getFinalState();
  }

  @Override
  public void setFinalState(String state) {
    maybeInitBuilder();
    builder.setFinalState(state);
  }

  private ApplicationAttemptIdPBImpl convertFromProtoFormat(ApplicationAttemptIdProto p) {
    return new ApplicationAttemptIdPBImpl(p);
  }

  private ApplicationAttemptIdProto convertToProtoFormat(ApplicationAttemptId t) {
    return ((ApplicationAttemptIdPBImpl)t).getProto();
  }



}  

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

package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsRequest;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskAttemptIdPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetDiagnosticsRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetDiagnosticsRequestProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;


    
public class GetDiagnosticsRequestPBImpl extends ProtoBase<GetDiagnosticsRequestProto> implements GetDiagnosticsRequest {
  GetDiagnosticsRequestProto proto = GetDiagnosticsRequestProto.getDefaultInstance();
  GetDiagnosticsRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private TaskAttemptId taskAttemptId = null;
  
  
  public GetDiagnosticsRequestPBImpl() {
    builder = GetDiagnosticsRequestProto.newBuilder();
  }

  public GetDiagnosticsRequestPBImpl(GetDiagnosticsRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetDiagnosticsRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.taskAttemptId != null) {
      builder.setTaskAttemptId(convertToProtoFormat(this.taskAttemptId));
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
      builder = GetDiagnosticsRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public TaskAttemptId getTaskAttemptId() {
    GetDiagnosticsRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.taskAttemptId != null) {
      return this.taskAttemptId;
    }
    if (!p.hasTaskAttemptId()) {
      return null;
    }
    this.taskAttemptId = convertFromProtoFormat(p.getTaskAttemptId());
    return this.taskAttemptId;
  }

  @Override
  public void setTaskAttemptId(TaskAttemptId taskAttemptId) {
    maybeInitBuilder();
    if (taskAttemptId == null) 
      builder.clearTaskAttemptId();
    this.taskAttemptId = taskAttemptId;
  }

  private TaskAttemptIdPBImpl convertFromProtoFormat(TaskAttemptIdProto p) {
    return new TaskAttemptIdPBImpl(p);
  }

  private TaskAttemptIdProto convertToProtoFormat(TaskAttemptId t) {
    return ((TaskAttemptIdPBImpl)t).getProto();
  }



}  

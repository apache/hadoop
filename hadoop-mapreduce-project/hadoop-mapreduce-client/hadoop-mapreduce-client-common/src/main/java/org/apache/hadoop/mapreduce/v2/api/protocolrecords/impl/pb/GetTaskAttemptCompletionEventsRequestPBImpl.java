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


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptCompletionEventsRequest;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.JobIdPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskAttemptCompletionEventsRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskAttemptCompletionEventsRequestProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;


    
public class GetTaskAttemptCompletionEventsRequestPBImpl extends ProtoBase<GetTaskAttemptCompletionEventsRequestProto> implements GetTaskAttemptCompletionEventsRequest {
  GetTaskAttemptCompletionEventsRequestProto proto = GetTaskAttemptCompletionEventsRequestProto.getDefaultInstance();
  GetTaskAttemptCompletionEventsRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private JobId jobId = null;
  
  
  public GetTaskAttemptCompletionEventsRequestPBImpl() {
    builder = GetTaskAttemptCompletionEventsRequestProto.newBuilder();
  }

  public GetTaskAttemptCompletionEventsRequestPBImpl(GetTaskAttemptCompletionEventsRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetTaskAttemptCompletionEventsRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.jobId != null) {
      builder.setJobId(convertToProtoFormat(this.jobId));
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
      builder = GetTaskAttemptCompletionEventsRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public JobId getJobId() {
    GetTaskAttemptCompletionEventsRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.jobId != null) {
      return this.jobId;
    }
    if (!p.hasJobId()) {
      return null;
    }
    this.jobId = convertFromProtoFormat(p.getJobId());
    return this.jobId;
  }

  @Override
  public void setJobId(JobId jobId) {
    maybeInitBuilder();
    if (jobId == null) 
      builder.clearJobId();
    this.jobId = jobId;
  }
  @Override
  public int getFromEventId() {
    GetTaskAttemptCompletionEventsRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getFromEventId());
  }

  @Override
  public void setFromEventId(int fromEventId) {
    maybeInitBuilder();
    builder.setFromEventId((fromEventId));
  }
  @Override
  public int getMaxEvents() {
    GetTaskAttemptCompletionEventsRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getMaxEvents());
  }

  @Override
  public void setMaxEvents(int maxEvents) {
    maybeInitBuilder();
    builder.setMaxEvents((maxEvents));
  }

  private JobIdPBImpl convertFromProtoFormat(JobIdProto p) {
    return new JobIdPBImpl(p);
  }

  private JobIdProto convertToProtoFormat(JobId t) {
    return ((JobIdPBImpl)t).getProto();
  }



}  

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


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportResponse;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.JobReportPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobReportProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetJobReportResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetJobReportResponseProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;


    
public class GetJobReportResponsePBImpl extends ProtoBase<GetJobReportResponseProto> implements GetJobReportResponse {
  GetJobReportResponseProto proto = GetJobReportResponseProto.getDefaultInstance();
  GetJobReportResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private JobReport jobReport = null;
  
  
  public GetJobReportResponsePBImpl() {
    builder = GetJobReportResponseProto.newBuilder();
  }

  public GetJobReportResponsePBImpl(GetJobReportResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetJobReportResponseProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.jobReport != null) {
      builder.setJobReport(convertToProtoFormat(this.jobReport));
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
      builder = GetJobReportResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public JobReport getJobReport() {
    GetJobReportResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.jobReport != null) {
      return this.jobReport;
    }
    if (!p.hasJobReport()) {
      return null;
    }
    this.jobReport = convertFromProtoFormat(p.getJobReport());
    return this.jobReport;
  }

  @Override
  public void setJobReport(JobReport jobReport) {
    maybeInitBuilder();
    if (jobReport == null) 
      builder.clearJobReport();
    this.jobReport = jobReport;
  }

  private JobReportPBImpl convertFromProtoFormat(JobReportProto p) {
    return new JobReportPBImpl(p);
  }

  private JobReportProto convertToProtoFormat(JobReport t) {
    return ((JobReportPBImpl)t).getProto();
  }



}  
